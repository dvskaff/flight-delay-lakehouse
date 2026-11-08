"""Pipeline tasks. Each function is one Airflow task (see dags/flight_lakehouse.py) and returns a small,
JSON-serializable dict that Airflow passes to the next task. `run_all` runs the same tasks in order without
Airflow (tests and quick local runs).

python -m flights.pipeline --fixture     # offline run on synthetic data
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import shutil
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F

from . import gold, transform
from .config import Config
from .lake import Lake
from .source import check_header, extract_csv, get_source, month_add, month_range
from .spark import get_spark

log = logging.getLogger("flights")
MAX_QUARANTINE_SHARE = 0.01  # stop before publishing if more than 1% of a load breaks a hard rule


def _lake(cfg: Config) -> Lake:
    return Lake(get_spark(cfg), cfg)


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------- 1. plan
def plan(cfg: Config) -> dict:
    """Newest published month, the rolling window, and which months still need loading."""
    lake = _lake(cfg)
    src = get_source(cfg)
    latest = src.latest_month()
    window = [m for m in month_range(month_add(latest, -(cfg.window_months - 1)), latest) if src.has(m)]
    loaded = set(lake.partition_values("silver/flights", "year_month"))
    reload = {m.strip() for m in os.environ.get("FLIGHTS_RELOAD", "").split(",") if m.strip()}
    to_load = [m for m in window if m not in loaded or m in reload]
    log.info(
        "Latest BTS month %s; window %s..%s; %d month(s) to load", latest, window[0], window[-1], len(to_load)
    )
    return {
        "latest": latest,
        "window": window,
        "to_load": to_load,
        "already_loaded": sorted(loaded & set(window)),
    }


# --------------------------------------------------------------------------- 2. bronze
def ingest_month(cfg: Config, ym: str) -> dict:
    """Download one month, check its schema, and land it in bronze (replacing that month if it was there)."""
    lake = _lake(cfg)
    zip_path = get_source(cfg).download(ym, cfg.landing)
    with zipfile.ZipFile(zip_path) as z:
        name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        with z.open(name) as fh:
            header = next(csv.reader(io.TextIOWrapper(fh, encoding="utf-8-sig")))
    new_columns = check_header(header)
    csv_path = extract_csv(zip_path, cfg.landing)
    raw = transform.read_month_csv(lake.spark, str(csv_path))
    bronze = transform.to_bronze(raw, ym, zip_path.name)
    lake.replace_partitions(bronze, "bronze/flights", "year_month", [ym])
    rows = lake.read("bronze/flights").filter(F.col("year_month") == ym).count()
    entry = {"year_month": ym, "rows": rows, "source_file": zip_path.name,
             "zip_bytes": zip_path.stat().st_size, "new_columns": ",".join(new_columns), "loaded_at": _now()}  # fmt: skip
    lake.append(lake.spark.createDataFrame([entry]), "ops/load_log")
    csv_path.unlink()
    zip_path.unlink()
    log.info("Bronze %s: %s rows", ym, f"{rows:,}")
    return {"year_month": ym, "rows": rows}


# --------------------------------------------------------------------------- 3. silver
def build_silver(cfg: Config, months: list[str], window: list[str]) -> dict:
    """Type, check and de-duplicate the newly landed months, then MERGE them into silver."""
    lake = _lake(cfg)
    out: dict = {"months": months, "merge": {}, "dq": [], "bronze_rows": 0, "quarantined": 0, "duplicates": 0}
    if months:
        bronze = lake.read("bronze/flights").filter(F.col("year_month").isin(months))
        out["bronze_rows"] = bronze.count()
        valid, quarantine, results = transform.apply_quality(transform.to_silver(bronze))
        out["dq"] = results
        out["quarantined"] = quarantine.count()
        share = out["quarantined"] / max(out["bronze_rows"], 1)
        if share > MAX_QUARANTINE_SHARE:
            raise RuntimeError(
                f"{share:.1%} of rows failed hard data-quality rules; silver was not updated: {results}"
            )
        deduped, out["duplicates"] = transform.deduplicate(valid)
        out["merge"] = lake.merge(deduped, "silver/flights", "flight_id", "year_month", months)
        lake.replace_partitions(quarantine, "silver/quarantine", "year_month", months)
        run = _now()
        lake.append(
            lake.spark.createDataFrame([{**r, "months": ",".join(months), "run_at": run} for r in results]),
            "ops/dq_results",
        )
        out["constraints_added"] = lake.add_constraints("silver/flights", transform.SILVER_CONSTRAINTS)
    # Retention: keep the rolling window in silver, a few recent months of raw copies in bronze.
    lake.drop_partitions_before("silver/flights", "year_month", window[0])
    lake.drop_partitions_before("silver/quarantine", "year_month", window[0])
    lake.drop_partitions_before(
        "bronze/flights", "year_month", month_add(window[-1], -(cfg.bronze_retention_months - 1))
    )
    log.info("Silver: %s", {k: v for k, v in out.items() if k != "dq"})
    return out


# --------------------------------------------------------------------------- 4. validate
def validate(cfg: Config, silver_stats: dict, window: list[str]) -> dict:
    """Checks on the published silver table; any failure fails the run before gold and the site are rebuilt."""
    lake = _lake(cfg)
    silver = lake.read("silver/flights")
    checks = {}
    total = silver.count()
    checks["flight_id_unique"] = silver.select("flight_id").distinct().count() == total
    months = lake.partition_values("silver/flights", "year_month")
    checks["months_inside_window"] = bool(months) and set(months) <= set(window)
    loaded = silver_stats["months"]
    if loaded:
        in_silver = silver.filter(F.col("year_month").isin(loaded)).count()
        reconciled = in_silver + silver_stats["quarantined"] + silver_stats["duplicates"]
        checks["bronze_rows_reconcile"] = reconciled == silver_stats["bronze_rows"]
    if cfg.lake_format == "delta":
        checks["constraints_in_place"] = set(transform.SILVER_CONSTRAINTS) <= set(
            lake.constraints("silver/flights")
        )
    failed = [k for k, ok in checks.items() if not ok]
    if failed:
        raise RuntimeError(f"Silver validation failed: {failed}")
    log.info("Validation passed: %s", checks)
    return {"checks": checks, "silver_rows": total, "months": months}


# --------------------------------------------------------------------------- 5. gold
def build_gold(cfg: Config, window: list[str]) -> dict:
    lake = _lake(cfg)
    silver = lake.read("silver/flights")
    last_12 = window[-12:]
    rows = {}
    for table, df in gold.build_all(lake.spark, silver, last_12).items():
        lake.overwrite(df, table)
        rows[table] = lake.read(table).count()
    log.info("Gold tables: %s", rows)
    return {"rows": rows}


# --------------------------------------------------------------------------- 6. maintain
def maintain(cfg: Config, months: list[str]) -> dict:
    """Compact and Z-order the partitions written this run (fast lookups by airport and carrier), then VACUUM."""
    lake = _lake(cfg)
    out = {}
    if months:
        where = f"year_month IN ({', '.join(repr(m) for m in months)})"
        out["optimize"] = lake.optimize("silver/flights", where, ["origin", "carrier"])
    lake.vacuum("silver/flights")  # 7-day retention: time travel to last week's versions still works
    for t in ["bronze/flights", "silver/quarantine", "ops/load_log", "ops/dq_results"]:
        lake.vacuum(t, retain_hours=0)  # raw/staging copies: no time travel needed
    log.info("Maintenance: %s", out)
    return out


# --------------------------------------------------------------------------- 7. publish
def publish(cfg: Config, run: dict) -> dict:
    from . import explore
    from .readme import update_readme
    from .report import build_site

    lake = _lake(cfg)
    summary = build_site(cfg, lake, run)
    # SQL in the browser: the gold tables (and the newest month of flights) for DuckDB-WASM
    meta = explore.export(lake, cfg.site_dir / "data")
    runtime = explore.copy_runtime(cfg.vendor_dir, cfg.site_dir)
    explore.write_page(cfg.site_dir, meta, summary["window"], runtime, cfg.repo_url, cfg.fixture)
    summary["browser_sql"] = {"tables": len(meta), "runtime": runtime,
                              "megabytes": round(sum(m["bytes"] for m in meta) / 1e6, 1)}  # fmt: skip
    (cfg.site_dir / "results.json").write_text(json.dumps(summary, indent=2, default=str))
    if not cfg.fixture:
        update_readme(cfg.readme_path, summary)
    return {"site": str(cfg.site_dir / "index.html")}


def run_all(cfg: Config) -> dict:
    """The DAG's tasks in order, without Airflow."""
    t0 = time.time()
    p = plan(cfg)
    loads = [ingest_month(cfg, m) for m in p["to_load"]]
    s = build_silver(cfg, p["to_load"], p["window"])
    v = validate(cfg, s, p["window"])
    g = build_gold(cfg, p["window"])
    mnt = maintain(cfg, p["to_load"])
    run = {"plan": p, "loads": loads, "silver": s, "validate": v, "gold": g, "maintain": mnt,
           "orchestrator": "python", "seconds": round(time.time() - t0)}  # fmt: skip
    publish(cfg, run)
    return run


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--fixture", action="store_true", help="offline run on synthetic data")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.fixture:
        from . import fixture

        tmp = Path(os.environ.get("FLIGHTS_FIXTURE_DIR", "/tmp/flights-fixture")).resolve()
        shutil.rmtree(tmp, ignore_errors=True)
        os.environ.update({"FLIGHTS_FIXTURE": "1", "FLIGHTS_DATA_DIR": str(tmp), "FLIGHTS_SITE_DIR": str(tmp / "site"),
                           "FLIGHTS_WINDOW_MONTHS": "3"})  # fmt: skip
        cfg = Config()
        fixture.make_months(cfg.data_dir / "fixture-source", fixture.MONTHS)
    else:
        cfg = Config()
    run = run_all(cfg)
    print(json.dumps({k: run[k] for k in ("plan", "validate", "gold")}, default=str)[:2000])


if __name__ == "__main__":
    main()
