import json
import shutil
from pathlib import Path

import pytest
from conftest import DELTA
from pyspark.sql import functions as F

from flights import fixture, pipeline
from flights.lake import Lake
from flights.readme import update_readme
from flights.spark import get_spark

ROOT = Path(__file__).resolve().parents[1]


def test_incremental_runs_retention_and_idempotency(cfg, monkeypatch):
    src = cfg.data_dir / "fixture-source"
    fixture.make_months(src, fixture.MONTHS[:3])
    first = pipeline.run_all(cfg)
    assert first["plan"]["to_load"] == ["2025-01", "2025-02", "2025-03"]
    assert first["validate"]["checks"]["bronze_rows_reconcile"]
    assert first["silver"]["quarantined"] == 9 and first["silver"]["duplicates"] == 15

    fixture.make_months(src, fixture.MONTHS[3:])  # a new month is published
    second = pipeline.run_all(cfg)
    assert second["plan"]["to_load"] == ["2025-04"]
    assert second["validate"]["months"] == ["2025-02", "2025-03", "2025-04"]  # 2025-01 aged out of the window
    lake = Lake(get_spark(cfg), cfg)
    assert lake.partition_values("bronze/flights", "year_month") == ["2025-02", "2025-03", "2025-04"]
    rows = second["validate"]["silver_rows"]

    nothing = pipeline.run_all(cfg)  # no new month: nothing loaded, everything still rebuilt
    assert nothing["plan"]["to_load"] == [] and nothing["validate"]["silver_rows"] == rows

    monkeypatch.setenv("FLIGHTS_RELOAD", "2025-03")  # re-loading a month changes nothing
    again = pipeline.run_all(cfg)
    assert again["plan"]["to_load"] == ["2025-03"] and again["validate"]["silver_rows"] == rows
    if DELTA:
        m = again["silver"]["merge"]
        assert int(m["numTargetRowsInserted"]) == 0 and int(m["numTargetRowsUpdated"]) == 0
        assert int(m["numTargetRowsDeleted"]) == 0

    page = (cfg.site_dir / "index.html").read_text()
    assert "SYNTHETIC TEST DATA" in page and "The lakehouse" in page
    summary = json.loads((cfg.site_dir / "results.json").read_text())
    assert summary["lake"]["silver/flights"]["rows"] == rows
    shutil.copy(ROOT / "README.md", cfg.readme_path)
    update_readme(cfg.readme_path, summary)
    text = cfg.readme_path.read_text()
    assert "| Flights |" in text and text.count("<!-- RESULTS:START -->") == 1


def test_bad_load_is_stopped_before_silver(cfg, monkeypatch):
    fixture.make_months(cfg.data_dir / "fixture-source", ["2025-01"])
    monkeypatch.setattr(pipeline, "MAX_QUARANTINE_SHARE", 0.0001)
    p = pipeline.plan(cfg)
    pipeline.ingest_month(cfg, "2025-01")
    with pytest.raises(RuntimeError, match="silver was not updated"):
        pipeline.build_silver(cfg, p["to_load"], p["window"])
    assert not Lake(get_spark(cfg), cfg).exists("silver/flights")


@pytest.mark.skipif(not DELTA, reason="Delta Lake features need LAKE_FORMAT=delta (set in CI)")
def test_delta_features(cfg):
    fixture.make_months(cfg.data_dir / "fixture-source", fixture.MONTHS[:2])
    pipeline.run_all(cfg)
    lake = Lake(get_spark(cfg), cfg)
    hist = lake.history("silver/flights")
    assert hist and hist[0]["version"] >= 1
    assert set(lake.constraints("silver/flights")) >= {"origin_not_dest", "positive_distance"}
    assert lake.read_version("silver/flights", 0).count() > 0  # time travel
    bad = lake.read("silver/flights").limit(1).withColumn("dest", F.col("origin"))
    with pytest.raises(Exception, match="(?i)constraint|CHECK"):
        bad.write.format("delta").mode("append").save(lake.path("silver/flights"))
