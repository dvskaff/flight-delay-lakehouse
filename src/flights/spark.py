"""One shared SparkSession, configured for Delta Lake (or plain Parquet for offline tests)."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from .config import Config

log = logging.getLogger(__name__)


def get_spark(cfg: Config) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("flight-delay-lakehouse")
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.warehouse.dir", str(cfg.data_dir / "spark-warehouse"))
    )
    if cfg.lake_format == "delta":
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Pass the lake's own DELETE/MERGE metrics through to the table history.
            .config("spark.databricks.delta.history.metricsEnabled", "true")
            # Bronze and ops tables are vacuumed with zero retention (see Lake.vacuum).
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        )
        builder = configure_spark_with_delta_pip(
            builder
        )  # fetches the matching Delta jars from Maven Central
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log.info("Spark %s started (%s tables)", spark.version, cfg.lake_format)
    return spark
