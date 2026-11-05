"""Bronze -> silver: typing, cleaning, data-quality rules and de-duplication. Pure DataFrame functions."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .source import REQUIRED_COLUMNS

NATURAL_KEY = ["flight_date", "carrier", "flight_number", "origin", "dest", "crs_dep_time"]


# --------------------------------------------------------------------------- bronze
def read_month_csv(spark: SparkSession, csv_path: str) -> DataFrame:
    """The raw file, every column as text (types are applied in silver, so bad values never fail a load)."""
    header = spark.read.text(csv_path).first()[0]
    names = [h.strip().strip('"') for h in header.split(",")]
    schema = StructType([StructField(n or f"_unnamed_{i}", StringType()) for i, n in enumerate(names)])
    return spark.read.csv(csv_path, header=True, schema=schema, quote='"', escape='"', mode="PERMISSIVE")


def to_bronze(raw: DataFrame, ym: str, source_file: str) -> DataFrame:
    """Keep the source columns the lake needs, untouched, plus lineage columns."""
    return raw.select(
        *[F.col(c) for c in REQUIRED_COLUMNS],
        F.lit(ym).alias("year_month"),
        F.lit(source_file).alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


# --------------------------------------------------------------------------- silver
def _num(c: str) -> Column:
    return F.expr(f"try_cast(trim({c}) AS DOUBLE)")


def _int(c: str) -> Column:
    return F.expr(f"try_cast(try_cast(trim({c}) AS DOUBLE) AS INT)")


def _flag(c: str) -> Column:
    return F.coalesce(_num(c), F.lit(0.0)) >= 1


def _text(c: str) -> Column:
    return F.nullif(F.trim(F.col(c)), F.lit(""))


def to_silver(bronze: DataFrame) -> DataFrame:
    """Typed, renamed flights with derived fields and a row hash for change detection."""
    df = bronze.select(
        F.to_date(_text("FlightDate")).alias("flight_date"),
        F.col("year_month"),
        F.upper(_text("Reporting_Airline")).alias("carrier"),
        _int("Flight_Number_Reporting_Airline").alias("flight_number"),
        _text("Tail_Number").alias("tail_number"),
        F.upper(_text("Origin")).alias("origin"),
        _text("OriginCityName").alias("origin_city"),
        _text("OriginState").alias("origin_state"),
        F.upper(_text("Dest")).alias("dest"),
        _text("DestCityName").alias("dest_city"),
        _text("DestState").alias("dest_state"),
        _int("CRSDepTime").alias("crs_dep_time"),
        _int("DepTime").alias("dep_time"),
        _num("DepDelay").alias("dep_delay"),
        _num("DepDel15").alias("dep_del15"),
        _num("TaxiOut").alias("taxi_out"),
        _num("TaxiIn").alias("taxi_in"),
        _int("CRSArrTime").alias("crs_arr_time"),
        _int("ArrTime").alias("arr_time"),
        _num("ArrDelay").alias("arr_delay"),
        _num("ArrDel15").alias("arr_del15"),
        _flag("Cancelled").alias("cancelled"),
        _text("CancellationCode").alias("cancellation_code"),
        _flag("Diverted").alias("diverted"),
        _num("CRSElapsedTime").alias("crs_elapsed_min"),
        _num("ActualElapsedTime").alias("actual_elapsed_min"),
        _num("AirTime").alias("air_time_min"),
        _num("Distance").alias("distance_mi"),
        _num("CarrierDelay").alias("carrier_delay"),
        _num("WeatherDelay").alias("weather_delay"),
        _num("NASDelay").alias("nas_delay"),
        _num("SecurityDelay").alias("security_delay"),
        _num("LateAircraftDelay").alias("late_aircraft_delay"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )
    df = (
        df.withColumn("dep_hour", (F.col("crs_dep_time") / 100).cast("int") % 24)
        .withColumn("day_of_week", F.dayofweek("flight_date"))  # 1 = Sunday
        .withColumn("completed", ~F.col("cancelled") & ~F.col("diverted"))
        .withColumn("on_time_arrival", F.when(F.col("completed"), F.col("arr_delay") < 15))
        # 64-bit hashes keep the 14M-row table small; validation fails the run if two flights ever collide.
        .withColumn("flight_id", F.xxhash64(*NATURAL_KEY))
    )
    payload = [c for c in df.columns if not c.startswith("_") and c != "flight_id"]
    return df.withColumn("row_hash", F.xxhash64(*payload))


# --------------------------------------------------------------------------- data quality
@dataclass(frozen=True)
class Rule:
    name: str
    description: str
    passes: str  # SQL expression that is TRUE for a good row
    severity: str  # "error": row goes to quarantine; "warn": row is kept and counted


RULES = [
    Rule("key_not_null", "Date, carrier, origin, destination and scheduled departure are present",
         "flight_date IS NOT NULL AND carrier IS NOT NULL AND origin IS NOT NULL AND dest IS NOT NULL "
         "AND crs_dep_time IS NOT NULL", "error"),
    Rule("origin_not_dest", "Origin and destination differ", "origin <> dest", "error"),
    Rule("valid_schedule_time", "Scheduled departure is a valid hhmm time", "crs_dep_time BETWEEN 1 AND 2400",
         "error"),
    Rule("positive_distance", "Distance is positive", "distance_mi > 0", "error"),
    Rule("month_matches_file", "Flight date falls in the file's month",
         "date_format(flight_date, 'yyyy-MM') = year_month", "error"),
    Rule("completed_has_arrival", "Completed flights have an arrival delay",
         "NOT completed OR arr_delay IS NOT NULL", "warn"),
    Rule("cancelled_has_reason", "Cancelled flights have a cancellation code",
         "NOT cancelled OR cancellation_code IS NOT NULL", "warn"),
    Rule("plausible_delay", "Departure delay between -2 h and +2 days", "dep_delay IS NULL OR dep_delay BETWEEN -120 AND 2880",
         "warn"),
    Rule("causes_add_up", "Delay-cause minutes add up to the arrival delay (flights 15+ min late)",
         "NOT (arr_delay >= 15 AND carrier_delay IS NOT NULL) OR abs(carrier_delay + weather_delay + nas_delay "
         "+ security_delay + late_aircraft_delay - arr_delay) <= 1", "warn"),
]  # fmt: skip


def apply_quality(df: DataFrame) -> tuple[DataFrame, DataFrame, list[dict]]:
    """Split into (valid rows, quarantined rows with the failed rule names) and per-rule failure counts."""
    flags = [F.coalesce(F.expr(r.passes), F.lit(False)).alias(f"_ok_{r.name}") for r in RULES]
    checked = df.select("*", *flags).cache()
    counts = checked.agg(
        F.count("*").alias("_rows"),
        *[F.sum((~F.col(f"_ok_{r.name}")).cast("long")).alias(r.name) for r in RULES],
    ).first()
    results = [
        {"rule": r.name, "description": r.description, "severity": r.severity, "failed": int(counts[r.name] or 0),
         "rows_checked": int(counts["_rows"])}
        for r in RULES
    ]  # fmt: skip
    errors = [r for r in RULES if r.severity == "error"]
    all_ok = F.lit(True)
    for r in errors:
        all_ok = all_ok & F.col(f"_ok_{r.name}")
    failed_names = F.concat_ws(",", *[F.when(~F.col(f"_ok_{r.name}"), F.lit(r.name)) for r in errors])
    flag_cols = [f"_ok_{r.name}" for r in RULES]
    valid = checked.filter(all_ok).drop(*flag_cols)
    quarantine = checked.filter(~all_ok).withColumn("failed_rules", failed_names).drop(*flag_cols)
    return valid, quarantine, results


def deduplicate(df: DataFrame) -> tuple[DataFrame, int]:
    """One row per flight (natural key); keeps the latest ingested copy."""
    from pyspark.sql.window import Window

    w = Window.partitionBy("flight_id").orderBy(F.col("_ingested_at").desc(), F.col("row_hash"))
    ranked = df.withColumn("_rn", F.row_number().over(w)).persist()  # counted, then merged: compute once
    dupes = ranked.filter(F.col("_rn") > 1).count()
    return ranked.filter(F.col("_rn") == 1).drop("_rn"), dupes


SILVER_CONSTRAINTS = {
    "origin_not_dest": "origin <> dest",
    "positive_distance": "distance_mi > 0",
    "valid_schedule_time": "crs_dep_time BETWEEN 1 AND 2400",
    "key_present": "flight_date IS NOT NULL AND carrier IS NOT NULL",
}
