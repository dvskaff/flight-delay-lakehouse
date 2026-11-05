"""Storage layer: every read and write of a lake table goes through here.

In production the tables are Delta Lake tables (ACID commits, MERGE, partition replacement, CHECK constraints,
OPTIMIZE/Z-ORDER, VACUUM, table history and time travel). `LAKE_FORMAT=parquet` swaps in plain Parquet with the
closest equivalent of each operation so the transformations can be tested offline without the Delta jars; features
that only Delta has (history, constraints, time travel) are then skipped.
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .config import Config

log = logging.getLogger(__name__)


def _in_list(values: list[str]) -> str:
    return ", ".join(f"'{v}'" for v in values)


class Lake:
    def __init__(self, spark: SparkSession, cfg: Config):
        self.spark, self.cfg, self.fmt = spark, cfg, cfg.lake_format
        self.delta = self.fmt == "delta"

    def path(self, table: str) -> str:
        return self.cfg.table(table)

    # ------------------------------------------------------------------ basics
    def exists(self, table: str) -> bool:
        if self.delta:
            from delta.tables import DeltaTable

            return Path(self.path(table), "_delta_log").exists() and DeltaTable.isDeltaTable(
                self.spark, self.path(table)
            )
        return any(Path(self.path(table)).rglob("*.parquet"))

    def read(self, table: str) -> DataFrame:
        return self.spark.read.format(self.fmt).load(self.path(table))

    def _writer(self, df: DataFrame, mode: str, partition_by: str | None):
        w = df.write.format(self.fmt).mode(mode)
        return w.partitionBy(partition_by) if partition_by else w

    def overwrite(self, df: DataFrame, table: str, partition_by: str | None = None) -> None:
        w = self._writer(df, "overwrite", partition_by)
        if self.delta:
            w = w.option("overwriteSchema", "true")
        w.save(self.path(table))

    def append(self, df: DataFrame, table: str) -> None:
        self._writer(df, "append", None).save(self.path(table))

    # ------------------------------------------------------------------ partition-level writes
    def replace_partitions(self, df: DataFrame, table: str, col: str, values: list[str]) -> None:
        """Atomically replace whole partitions (idempotent re-loads of a month)."""
        if not self.exists(table):
            self._writer(df, "overwrite", col).save(self.path(table))
        elif self.delta:
            (
                self._writer(df, "overwrite", col)
                .option("replaceWhere", f"{col} IN ({_in_list(values)})")
                .save(self.path(table))
            )
        else:  # dynamic partition overwrite only touches the partitions present in df
            self._writer(df, "overwrite", col).option("partitionOverwriteMode", "dynamic").save(
                self.path(table)
            )

    def merge(self, df: DataFrame, table: str, key: str, col: str, values: list[str]) -> dict:
        """Upsert the given partitions: insert new rows, update changed rows (by row_hash) and delete rows that
        disappeared from the source, touching only those partitions. Returns Delta's operation metrics."""
        if not self.exists(table):
            self._writer(df, "overwrite", col).save(self.path(table))
            return {"numTargetRowsInserted": str(self.read(table).count()), "note": "table created"}
        if not self.delta:
            self.replace_partitions(df, table, col, values)
            return {"note": "parquet: partitions replaced"}
        from delta.tables import DeltaTable

        scope = f"t.{col} IN ({_in_list(values)})"
        (
            DeltaTable.forPath(self.spark, self.path(table))
            .alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key} AND {scope}")
            .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete(condition=scope)
            .execute()
        )
        last = self.history(table, 1)
        return last[0]["metrics"] if last else {}

    def drop_partitions_before(self, table: str, col: str, cutoff: str) -> None:
        """Retention: remove partitions older than the cutoff."""
        if not self.exists(table):
            return
        if self.delta:
            from delta.tables import DeltaTable

            DeltaTable.forPath(self.spark, self.path(table)).delete(f"{col} < '{cutoff}'")
            return
        for p in Path(self.path(table)).glob(f"{col}=*"):
            if p.name.split("=", 1)[1] < cutoff:
                shutil.rmtree(p)

    def partition_values(self, table: str, col: str) -> list[str]:
        if not self.exists(table):
            return []
        return sorted(r[0] for r in self.read(table).select(col).distinct().collect())

    # ------------------------------------------------------------------ Delta-only features
    def add_constraints(self, table: str, constraints: dict[str, str]) -> list[str]:
        """Delta CHECK constraints: every future write that violates one fails the commit."""
        if not self.delta or not self.exists(table):
            return []
        props = {r[0] for r in self.spark.sql(f"SHOW TBLPROPERTIES delta.`{self.path(table)}`").collect()}
        added = []
        for name, expr in constraints.items():
            if f"delta.constraints.{name}" not in props:
                self.spark.sql(f"ALTER TABLE delta.`{self.path(table)}` ADD CONSTRAINT {name} CHECK ({expr})")
                added.append(name)
        return added

    def constraints(self, table: str) -> dict[str, str]:
        if not self.delta or not self.exists(table):
            return {}
        rows = self.spark.sql(f"SHOW TBLPROPERTIES delta.`{self.path(table)}`").collect()
        return {
            r[0].removeprefix("delta.constraints."): r[1]
            for r in rows
            if r[0].startswith("delta.constraints.")
        }

    def optimize(self, table: str, where: str | None, zorder: list[str]) -> dict:
        if not self.delta or not self.exists(table):
            return {}
        from delta.tables import DeltaTable

        builder = DeltaTable.forPath(self.spark, self.path(table)).optimize()
        if where:
            builder = builder.where(where)
        row = builder.executeZOrderBy(*zorder).collect()
        metrics = row[0]["metrics"].asDict(recursive=True) if row else {}
        return {k: metrics.get(k) for k in ("numFilesAdded", "numFilesRemoved", "totalConsideredFiles")}

    def vacuum(self, table: str, retain_hours: float | None = None) -> None:
        """Delete data files no longer referenced by the table. Default retention is 7 days (time travel back a
        week); staging tables that never need time travel pass 0 so the saved lake stays small."""
        if self.delta and self.exists(table):
            from delta.tables import DeltaTable

            DeltaTable.forPath(self.spark, self.path(table)).vacuum(retain_hours)

    def history(self, table: str, limit: int = 20) -> list[dict]:
        if not self.delta or not self.exists(table):
            return []
        from delta.tables import DeltaTable

        rows = DeltaTable.forPath(self.spark, self.path(table)).history(limit).collect()
        return [
            {
                "version": r["version"],
                "timestamp": str(r["timestamp"]),
                "operation": r["operation"],
                "metrics": dict(r["operationMetrics"] or {}),
                "parameters": dict(r["operationParameters"] or {}),
            }
            for r in rows
        ]

    def read_version(self, table: str, version: int) -> DataFrame:
        """Time travel: the table as it was at an earlier version."""
        return self.spark.read.format("delta").option("versionAsOf", version).load(self.path(table))

    def detail(self, table: str) -> dict:
        if not self.exists(table):
            return {"exists": False}
        if self.delta:
            from delta.tables import DeltaTable

            d = DeltaTable.forPath(self.spark, self.path(table)).detail().collect()[0]
            hist = self.history(table, 1)
            return {
                "exists": True,
                "files": int(d["numFiles"]),
                "bytes": int(d["sizeInBytes"]),
                "partitioned_by": list(d["partitionColumns"]),
                "version": hist[0]["version"] if hist else None,
                "last_operation": hist[0]["operation"] if hist else None,
            }
        files = list(Path(self.path(table)).rglob("*.parquet"))
        return {"exists": True, "files": len(files), "bytes": sum(f.stat().st_size for f in files),
                "partitioned_by": [], "version": None, "last_operation": None}  # fmt: skip


def to_json(obj) -> str:
    return json.dumps(obj, default=str)
