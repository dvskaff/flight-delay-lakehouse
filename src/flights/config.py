"""Project settings. Everything can be overridden with environment variables (used by tests and CI)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _env_path(name: str, default: Path) -> Path:
    return Path(os.environ[name]).resolve() if os.environ.get(name) else default


@dataclass
class Config:
    # BTS "Reporting Carrier On-Time Performance (1987-present)": one prezipped CSV per month, no key needed.
    source_url: str = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
    window_months: int = field(default_factory=lambda: int(os.environ.get("FLIGHTS_WINDOW_MONTHS", "24")))
    bronze_retention_months: int = 3  # raw copies are re-downloadable, so bronze keeps only recent months
    probe_back_months: int = 12  # how far back to look for the newest published month

    # "delta" in production; "parquet" runs the same pipeline without Delta's features (offline tests only).
    lake_format: str = field(default_factory=lambda: os.environ.get("LAKE_FORMAT", "delta"))
    driver_memory: str = field(default_factory=lambda: os.environ.get("FLIGHTS_DRIVER_MEMORY", "2g"))
    shuffle_partitions: int = field(
        default_factory=lambda: int(os.environ.get("FLIGHTS_SHUFFLE_PARTITIONS", "16"))
    )
    fixture: bool = field(default_factory=lambda: os.environ.get("FLIGHTS_FIXTURE") == "1")

    data_dir: Path = field(default_factory=lambda: _env_path("FLIGHTS_DATA_DIR", ROOT / "data"))
    site_dir: Path = field(default_factory=lambda: _env_path("FLIGHTS_SITE_DIR", ROOT / "site"))
    readme_path: Path = field(default_factory=lambda: _env_path("FLIGHTS_README", ROOT / "README.md"))
    # DuckDB-WASM for the in-browser SQL page (fetched from npm by scripts/fetch_web_runtime.sh)
    vendor_dir: Path = field(default_factory=lambda: _env_path("FLIGHTS_VENDOR_DIR", ROOT / "web" / "vendor"))

    repo_url: str = "https://github.com/dvskaff/flight-delay-lakehouse"
    site_url: str = "https://dvskaff.github.io/flight-delay-lakehouse/"

    @property
    def lake(self) -> Path:
        return self.data_dir / "lake"

    @property
    def landing(self) -> Path:
        return self.data_dir / "landing"

    def table(self, name: str) -> str:
        """Path of a lake table, e.g. table('silver/flights')."""
        return str(self.lake / name)


# Reporting carriers (BTS two-letter codes). Codes not listed are shown as the bare code.
CARRIERS = {
    "9E": "Endeavor Air",
    "AA": "American Airlines",
    "AS": "Alaska Airlines",
    "B6": "JetBlue Airways",
    "C5": "CommuteAir",
    "DL": "Delta Air Lines",
    "EV": "ExpressJet Airlines",
    "F9": "Frontier Airlines",
    "G4": "Allegiant Air",
    "G7": "GoJet Airlines",
    "HA": "Hawaiian Airlines",
    "MQ": "Envoy Air",
    "MX": "Breeze Airways",
    "NK": "Spirit Airlines",
    "OH": "PSA Airlines",
    "OO": "SkyWest Airlines",
    "PT": "Piedmont Airlines",
    "QX": "Horizon Air",
    "UA": "United Airlines",
    "WN": "Southwest Airlines",
    "YV": "Mesa Airlines",
    "YX": "Republic Airways",
    "ZW": "Air Wisconsin",
}
