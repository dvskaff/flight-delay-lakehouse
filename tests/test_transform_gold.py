from datetime import date

import pytest
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from flights import fixture, gold, source, transform

BASE = {c: None for c in source.REQUIRED_COLUMNS}


def bronze_row(**kw):
    r = {**BASE, "FlightDate": "2025-01-05", "Reporting_Airline": "aa", "Tail_Number": "N1", "Origin": "ATL",
         "Dest": "ORD", "OriginCityName": "Atlanta, GA", "DestCityName": "Chicago, IL",
         "Flight_Number_Reporting_Airline": "100", "CRSDepTime": "0005", "DepDelay": "-3.00", "ArrDelay": "20.00",
         "Cancelled": "0.00", "Diverted": "0.00", "Distance": "606.00", "CarrierDelay": "5.00",
         "WeatherDelay": "0.00", "NASDelay": "15.00", "SecurityDelay": "0.00", "LateAircraftDelay": "0.00"}  # fmt: skip
    r.update(kw)
    return {**r, "year_month": "2025-01", "_source_file": "f.zip", "_ingested_at": None}


SCHEMA = StructType(
    [StructField(c, StringType()) for c in [*source.REQUIRED_COLUMNS, "year_month", "_source_file"]]
    + [StructField("_ingested_at", TimestampType())]
)


def bronze_df(spark, rows):
    return spark.createDataFrame([[r[c] for c in SCHEMA.names] for r in rows], SCHEMA)


def test_to_silver_types_and_derives(spark):
    df = transform.to_silver(bronze_df(spark, [bronze_row(), bronze_row(Cancelled="1.00", CancellationCode="B",
                                                                             ArrDelay="", Flight_Number_Reporting_Airline="101")]))  # fmt: skip
    a, b = sorted(df.collect(), key=lambda r: r.flight_number)
    assert a.flight_date == date(2025, 1, 5) and a.carrier == "AA" and a.crs_dep_time == 5 and a.dep_hour == 0
    assert a.dep_delay == -3.0 and a.on_time_arrival is False and a.completed and not a.cancelled
    assert b.cancelled and not b.completed and b.on_time_arrival is None and b.arr_delay is None
    assert a.flight_id != b.flight_id and a.row_hash != b.row_hash


def test_quality_rules_quarantine_and_warn(spark):
    rows = [bronze_row(), bronze_row(Dest="ATL", Flight_Number_Reporting_Airline="2"),
            bronze_row(Reporting_Airline="", Flight_Number_Reporting_Airline="3"),
            bronze_row(FlightDate="2025-02-01", Flight_Number_Reporting_Airline="4"),
            bronze_row(NASDelay="99.00", Flight_Number_Reporting_Airline="5")]  # fmt: skip
    valid, quarantine, results = transform.apply_quality(transform.to_silver(bronze_df(spark, rows)))
    res = {r["rule"]: r["failed"] for r in results}
    assert res["origin_not_dest"] == 1 and res["key_not_null"] == 1 and res["month_matches_file"] == 1
    assert res["causes_add_up"] == 1  # warn only: the row stays
    assert valid.count() == 2 and quarantine.count() == 3
    assert set(r.failed_rules for r in quarantine.collect()) == {
        "origin_not_dest",
        "key_not_null",
        "month_matches_file",
    }


def test_deduplicate_keeps_one_copy(spark):
    df = transform.to_silver(
        bronze_df(spark, [bronze_row(), bronze_row(), bronze_row(Flight_Number_Reporting_Airline="7")])
    )
    out, dupes = transform.deduplicate(df)
    assert dupes == 1 and out.count() == 2


def test_read_month_csv_handles_bts_layout(spark, tmp_path):
    z = fixture.make_month_zip("2025-03", tmp_path)
    raw = transform.read_month_csv(spark, str(source.extract_csv(z, tmp_path)))
    assert "OriginCityName" in raw.columns and raw.columns[-1].startswith("_unnamed")
    city = raw.filter("Origin = 'DFW'").select("OriginCityName").first()[0]
    assert city == "Dallas/Fort Worth, TX"  # the comma inside quotes is not a column break


def test_gold_definitions(spark):
    legs = [bronze_row(CRSDepTime="0700", ArrDelay="45.00", DepDelay="40.00", Flight_Number_Reporting_Airline="1",
                       CarrierDelay="45.00", NASDelay="0.00"),
            bronze_row(CRSDepTime="1000", DepDelay="30.00", ArrDelay="10.00", Flight_Number_Reporting_Airline="2",
                       Origin="ORD", Dest="ATL"),
            bronze_row(CRSDepTime="1300", Cancelled="1.00", CancellationCode="A", ArrDelay="", DepDelay="",
                       Flight_Number_Reporting_Airline="3")]  # fmt: skip
    silver = transform.to_silver(bronze_df(spark, legs))
    cm = gold.carrier_month(silver).first()
    assert cm.flights == 3 and cm.completed == 2 and cm.on_time == 1 and cm.cancelled == 1
    assert cm.on_time_rate == pytest.approx(0.5) and cm.cancel_rate == pytest.approx(1 / 3)
    prop = {r.bucket: r for r in gold.delay_propagation(silver, ["2025-01"]).collect()}
    assert prop["30–59 min"].legs == 1 and prop["30–59 min"].next_departs_late_rate == 1.0
    causes = gold.delay_causes(silver).collect()
    assert sum(r.share for r in causes if r.share is not None) == pytest.approx(1.0)
