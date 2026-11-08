"""The browser SQL page: exported tables, their types and metadata, the example queries and the page itself."""

import gzip
import json

import pytest

from flights import explore, fixture, pipeline

duckdb = pytest.importorskip("duckdb")  # stands in for DuckDB-WASM (same engine, same SQL)


def test_exports_load_with_lake_types_and_examples_run(cfg):
    fixture.make_months(cfg.data_dir / "fixture-source", fixture.MONTHS[:3])
    pipeline.run_all(cfg)
    site = cfg.site_dir
    meta = json.loads((site / "data" / "tables.json").read_text())
    tables = {t["name"]: t for t in meta["tables"]}
    assert list(tables) == [t[0] for t in explore.TABLES] and meta["window"] == ["2025-01", "2025-03"]
    assert "2025-03" in tables["flights"]["description"]
    types = {c["name"]: c["type"] for c in tables["flights"]["columns"]}
    assert (
        types["flight_date"] == "DATE" and types["cancelled"] == "BOOLEAN" and types["dep_delay"] == "DOUBLE"
    )
    assert all(c["description"] for t in tables.values() for c in t["columns"])  # every column documented
    with gzip.open(site / tables["dim_airport"]["file"], "rt") as f:
        assert f.readline().strip() == "airport,city,state" and '"' in f.read()  # "City, ST" is quoted

    con = duckdb.connect()
    for t in tables.values():
        con.execute(explore.create_sql(t, str(site / t["file"])))
        assert con.execute(f"select count(*) from {t['name']}").fetchall()[0][0] == t["rows"] > 0
    assert (
        con.execute("select min(year_month) = max(year_month) from carrier_month").fetchall()[0][0] is False
    )
    assert (
        con.execute("select count(distinct strftime(flight_date, '%Y-%m')) from flights").fetchall()[0][0]
        == 1
    )
    for _, sql in explore.EXAMPLES:
        con.execute(sql).fetchall()
    assert con.execute(explore.EXAMPLES[0][1]).fetchall()[0][2] > 0  # carriers ranked by on-time rate

    page = (site / "sql.html").read_text()
    assert "SYNTHETIC TEST DATA" in page and "castDecimalToDouble" in page and "flights" in page
    assert 'href="sql.html"' in (site / "index.html").read_text()
    assert json.loads((site / "results.json").read_text())["browser_sql"]["tables"] == len(explore.TABLES)


def test_runtime_copy(tmp_path):
    vendor, site = tmp_path / "vendor", tmp_path / "site"
    assert (
        explore.copy_runtime(vendor, site) is False
    )  # missing runtime: the page still builds, with a notice
    vendor.mkdir()
    for f in explore.RUNTIME:
        (vendor / f).write_text("x")
    assert explore.copy_runtime(vendor, site) is True
    assert all((site / "vendor" / f).exists() for f in explore.RUNTIME)
