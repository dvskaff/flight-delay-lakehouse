"""Silver -> gold: the business-level tables behind the dashboard. Pure DataFrame functions.

Definitions follow BTS: a flight is on time if it arrives less than 15 minutes after its scheduled time; on-time
rates use completed flights (not cancelled or diverted); cancellation rates use all scheduled flights.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .config import CARRIERS

CAUSES = {
    "carrier_delay": "Carrier",
    "weather_delay": "Extreme weather",
    "nas_delay": "National Aviation System",
    "security_delay": "Security",
    "late_aircraft_delay": "Late-arriving aircraft",
}


def _rates(*keys: str) -> list:
    return [
        F.count("*").alias("flights"),
        F.sum(F.col("completed").cast("long")).alias("completed"),
        F.sum(F.col("on_time_arrival").cast("long")).alias("on_time"),
        F.sum(F.col("cancelled").cast("long")).alias("cancelled"),
        F.sum(F.col("diverted").cast("long")).alias("diverted"),
        F.avg(F.when(F.col("completed"), F.col("arr_delay"))).alias("avg_arr_delay"),
        F.avg(F.when(F.col("completed"), F.col("dep_delay"))).alias("avg_dep_delay"),
    ]


def _finish(df: DataFrame) -> DataFrame:
    return df.withColumn("on_time_rate", F.col("on_time") / F.col("completed")).withColumn(
        "cancel_rate", F.col("cancelled") / F.col("flights")
    )


def dim_carrier(spark: SparkSession, silver: DataFrame) -> DataFrame:
    seen = silver.select("carrier").distinct()
    names = spark.createDataFrame(list(CARRIERS.items()), "carrier string, carrier_name string")
    return seen.join(names, "carrier", "left").withColumn(
        "carrier_name", F.coalesce("carrier_name", "carrier")
    )


def dim_airport(silver: DataFrame) -> DataFrame:
    """One row per airport code with its most recent city and state names (names change over time)."""
    both = silver.select(
        F.col("origin").alias("airport"), F.col("origin_city").alias("city"), F.col("origin_state").alias("state"),
        "flight_date",
    ).unionByName(
        silver.select(F.col("dest").alias("airport"), F.col("dest_city").alias("city"),
                      F.col("dest_state").alias("state"), "flight_date")
    )  # fmt: skip
    w = Window.partitionBy("airport").orderBy(F.col("flight_date").desc())
    return both.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "flight_date")


def carrier_month(silver: DataFrame) -> DataFrame:
    causes = [F.sum(c).alias(f"{c}_min") for c in CAUSES]
    return _finish(silver.groupBy("year_month", "carrier").agg(*_rates(), *causes))


def airport_month(silver: DataFrame) -> DataFrame:
    df = silver.groupBy("year_month", F.col("origin").alias("airport")).agg(
        *_rates(),
        F.avg(F.when(F.col("completed"), F.col("taxi_out"))).alias("avg_taxi_out"),
        F.avg(F.when(~F.col("cancelled"), (F.col("dep_delay") < 15).cast("double"))).alias(
            "on_time_departure_rate"
        ),
    )
    return _finish(df)


def route_month(silver: DataFrame) -> DataFrame:
    df = silver.groupBy("year_month", "origin", "dest").agg(
        *_rates(),
        F.percentile_approx(F.when(F.col("completed"), F.col("arr_delay")), 0.9).alias("p90_arr_delay"),
    )
    return _finish(df)


def daily(silver: DataFrame) -> DataFrame:
    return _finish(silver.groupBy("flight_date").agg(*_rates()))


def hour_weekday(silver: DataFrame, months: list[str]) -> DataFrame:
    """On-time departure rate by scheduled hour and weekday over the given months."""
    df = silver.filter(F.col("year_month").isin(months) & ~F.col("cancelled"))
    return df.groupBy("day_of_week", "dep_hour").agg(
        F.count("*").alias("flights"),
        F.avg((F.col("dep_delay") < 15).cast("double")).alias("on_time_departure_rate"),
    )


def delay_causes(silver: DataFrame) -> DataFrame:
    """Minutes of arrival delay attributed to each cause, per month (BTS reports causes for flights 15+ min late)."""
    stack = ", ".join(f"'{label}', {c}_sum" for c, label in CAUSES.items())
    sums = silver.groupBy("year_month").agg(*[F.sum(c).alias(f"{c}_sum") for c in CAUSES])
    long = sums.select("year_month", F.expr(f"stack({len(CAUSES)}, {stack}) AS (cause, minutes)"))
    total = Window.partitionBy("year_month")
    return long.withColumn("share", F.col("minutes") / F.sum("minutes").over(total))


def delay_propagation(silver: DataFrame, months: list[str]) -> DataFrame:
    """How a plane's late arrival carries into its next departure the same day (tail-number rotations)."""
    legs = silver.filter(
        F.col("year_month").isin(months) & F.col("tail_number").isNotNull() & ~F.col("cancelled")
        & ~F.col("tail_number").isin("", "-")
    ).select("tail_number", "flight_date", "crs_dep_time", "dep_delay", "arr_delay")  # fmt: skip
    w = Window.partitionBy("tail_number", "flight_date").orderBy("crs_dep_time")
    pairs = legs.withColumn("prev_arr_delay", F.lag("arr_delay").over(w)).filter(
        F.col("prev_arr_delay").isNotNull() & F.col("dep_delay").isNotNull()
    )
    bucket = (
        F.when(F.col("prev_arr_delay") < 0, "Early")
        .when(F.col("prev_arr_delay") < 15, "0–14 min")
        .when(F.col("prev_arr_delay") < 30, "15–29 min")
        .when(F.col("prev_arr_delay") < 60, "30–59 min")
        .when(F.col("prev_arr_delay") < 120, "60–119 min")
        .otherwise("120+ min")
    )
    order = (
        F.when(F.col("prev_arr_delay") < 0, 0).when(F.col("prev_arr_delay") < 15, 1)
        .when(F.col("prev_arr_delay") < 30, 2).when(F.col("prev_arr_delay") < 60, 3)
        .when(F.col("prev_arr_delay") < 120, 4).otherwise(5)
    )  # fmt: skip
    return (
        pairs.withColumn("bucket", bucket)
        .withColumn("bucket_order", order)
        .groupBy("bucket", "bucket_order")
        .agg(
            F.count("*").alias("legs"),
            F.avg((F.col("dep_delay") >= 15).cast("double")).alias("next_departs_late_rate"),
            F.percentile_approx("dep_delay", 0.5).alias("median_next_dep_delay"),
        )
        .orderBy("bucket_order")
    )


def build_all(spark: SparkSession, silver: DataFrame, last_12: list[str]) -> dict[str, DataFrame]:
    return {
        "gold/dim_carrier": dim_carrier(spark, silver),
        "gold/dim_airport": dim_airport(silver),
        "gold/carrier_month": carrier_month(silver),
        "gold/airport_month": airport_month(silver),
        "gold/route_month": route_month(silver),
        "gold/daily": daily(silver),
        "gold/hour_weekday": hour_weekday(silver, last_12),
        "gold/delay_causes": delay_causes(silver),
        "gold/delay_propagation": delay_propagation(silver, last_12),
    }
