import zipfile
from datetime import UTC, datetime

import pytest

from flights import fixture, source


def test_month_math():
    assert source.month_add("2025-01", -1) == "2024-12"
    assert source.month_add("2024-11", 14) == "2026-01"
    assert source.month_range("2024-11", "2025-02") == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_check_header_flags_missing_and_new_columns():
    header = [*source.REQUIRED_COLUMNS, "Year", "BrandNewColumn", ""]
    assert source.check_header(header) == ["Year", "BrandNewColumn"]
    with pytest.raises(source.SchemaDrift, match="ArrDelay"):
        source.check_header([c for c in header if c != "ArrDelay"])


def test_fixture_zip_matches_bts_layout(tmp_path):
    z = fixture.make_month_zip("2025-01", tmp_path)
    names = zipfile.ZipFile(z).namelist()
    assert any(
        n.startswith("On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2025_1") for n in names
    )
    csv_path = source.extract_csv(z, tmp_path / "out")
    first = csv_path.read_text().splitlines()[0]
    assert first.startswith('"Year","Quarter"') and first.endswith(",")  # quoted header, trailing comma


class FakeResp:
    def __init__(self, status, body=b""):
        self.status_code, self.body = status, body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_content(self, n):
        yield self.body[:n]


class FakeHTTP:
    """September 2026 is the newest real zip; October returns 404; August returns an HTML error page."""

    def get(self, url, timeout, stream):
        if url.endswith("_2026_10.zip"):
            return FakeResp(404)
        if url.endswith("_2026_9.zip"):
            return FakeResp(200, b"PK\x03\x04rest")
        return FakeResp(200, b"<html>error</html>")


def test_latest_month_probes_backwards():
    src = source.BTSSource(source.Config())
    src.http = FakeHTTP()
    assert src.url("2026-09").endswith("_2026_9.zip")
    assert src.latest_month(today=datetime(2026, 11, 5, tzinfo=UTC)) == "2026-09"
    assert not src.available("2026-08")  # an HTML page is not a data file
