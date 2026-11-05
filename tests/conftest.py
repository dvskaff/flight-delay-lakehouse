import logging
import os
from dataclasses import replace

import pytest

# Offline default: plain Parquet. CI sets LAKE_FORMAT=delta so the same tests exercise Delta Lake.
os.environ.setdefault("LAKE_FORMAT", "parquet")
os.environ.setdefault("FLIGHTS_DRIVER_MEMORY", "1g")
os.environ.setdefault("FLIGHTS_SHUFFLE_PARTITIONS", "4")

from flights.config import Config  # noqa: E402
from flights.spark import get_spark  # noqa: E402

DELTA = os.environ["LAKE_FORMAT"] == "delta"
logging.getLogger("py4j").setLevel(logging.WARNING)  # quiet shutdown messages


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    cfg = replace(Config(), data_dir=tmp_path_factory.mktemp("spark"))
    s = get_spark(cfg)
    yield s
    s.stop()


@pytest.fixture
def cfg(tmp_path, spark, monkeypatch):
    monkeypatch.setenv("FLIGHTS_FIXTURE", "1")
    return replace(Config(), data_dir=tmp_path / "data", site_dir=tmp_path / "site", readme_path=tmp_path / "README.md",
                   window_months=3, fixture=True)  # fmt: skip
