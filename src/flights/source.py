"""The source system: BTS monthly on-time files (one zip per month, about 600K flights each).

Months are "YYYY-MM" strings. `BTSSource` talks to transtats.bts.gov; `FixtureSource` serves synthetic zips in the
same layout so the whole pipeline can be tested offline.
"""

from __future__ import annotations

import logging
import shutil
import ssl
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import certifi
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Config

log = logging.getLogger(__name__)

# Columns the pipeline needs from the BTS file. A missing one stops the run before anything is written.
REQUIRED_COLUMNS = [
    "FlightDate", "Reporting_Airline", "Tail_Number", "Flight_Number_Reporting_Airline",
    "Origin", "OriginCityName", "OriginState", "Dest", "DestCityName", "DestState",
    "CRSDepTime", "DepTime", "DepDelay", "DepDel15", "TaxiOut", "TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay",
    "ArrDel15", "Cancelled", "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime",
    "Distance", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
]  # fmt: skip


class SchemaDrift(RuntimeError):
    pass


def month_add(ym: str, n: int) -> str:
    y, m = map(int, ym.split("-"))
    k = y * 12 + (m - 1) + n
    return f"{k // 12:04d}-{k % 12 + 1:02d}"


def month_range(start: str, end: str) -> list[str]:
    out, cur = [], start
    while cur <= end:
        out.append(cur)
        cur = month_add(cur, 1)
    return out


def check_header(header: list[str]) -> list[str]:
    """Raise if a required column is missing; return columns that are new to us (logged, not fatal)."""
    cols = [h.strip() for h in header if h.strip()]
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise SchemaDrift(f"BTS file is missing columns {missing}; it has {cols}")
    return [c for c in cols if c not in REQUIRED_COLUMNS]


def extract_csv(zip_path: Path, dest_dir: Path) -> Path:
    """Unzip the single CSV inside a monthly file."""
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise RuntimeError(f"{zip_path.name}: expected one CSV inside, found {z.namelist()}")
        dest_dir.mkdir(parents=True, exist_ok=True)
        out = dest_dir / (zip_path.stem + ".csv")
        with z.open(names[0]) as src, out.open("wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
    return out


# --------------------------------------------------------------------------- BTS
def _session(verify: str | bool = True) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, backoff_factor=4, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "flight-delay-lakehouse (github.com/dvskaff/flight-delay-lakehouse)"
    s.verify = verify
    return s


def bundle_with_intermediates(host: str, max_depth: int = 3) -> str:
    """Some government servers send their certificate without the intermediate certificate that links it to a
    trusted root. Browsers fetch the missing link themselves (from the certificate's "CA Issuers" address); Python
    does not. This does the same: it downloads the intermediates and returns a CA bundle (certifi + intermediates),
    so the connection is still fully verified against a trusted root."""
    from cryptography import x509
    from cryptography.hazmat.primitives.serialization import Encoding
    from cryptography.x509.oid import AuthorityInformationAccessOID, ExtensionOID

    cert = x509.load_pem_x509_certificate(ssl.get_server_certificate((host, 443), timeout=60).encode())
    extra = []
    for _ in range(max_depth):
        try:
            aia = cert.extensions.get_extension_for_oid(ExtensionOID.AUTHORITY_INFORMATION_ACCESS).value
        except x509.ExtensionNotFound:
            break
        urls = [
            d.access_location.value
            for d in aia
            if d.access_method == AuthorityInformationAccessOID.CA_ISSUERS
        ]
        if not urls:
            break
        raw = requests.get(urls[0], timeout=60).content
        try:
            cert = x509.load_der_x509_certificate(raw)
        except ValueError:
            cert = x509.load_pem_x509_certificate(raw)
        extra.append(cert.public_bytes(Encoding.PEM).decode())
        if cert.issuer == cert.subject:
            break
    path = Path(tempfile.gettempdir()) / f"ca-{host}.pem"
    path.write_text(Path(certifi.where()).read_text() + "\n" + "\n".join(extra))
    log.info("Using CA bundle with %d downloaded intermediate certificate(s) for %s", len(extra), host)
    return str(path)


class BTSSource:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.http = _session()

    def url(self, ym: str) -> str:
        y, m = ym.split("-")
        return self.cfg.source_url.format(year=int(y), month=int(m))

    def _get(self, url: str, **kw) -> requests.Response:
        try:
            return self.http.get(url, timeout=600, **kw)
        except requests.exceptions.SSLError as exc:
            log.warning("TLS verification failed (%s); completing the certificate chain and retrying", exc)
            self.http = _session(bundle_with_intermediates(urlparse(url).hostname))
            return self.http.get(url, timeout=600, **kw)

    def has(self, ym: str) -> bool:
        return True  # BTS keeps every month since 1987

    def available(self, ym: str) -> bool:
        with self._get(self.url(ym), stream=True) as r:
            if r.status_code != 200:
                return False
            return next(r.iter_content(4), b"").startswith(b"PK")  # a zip, not an HTML error page

    def latest_month(self, today: datetime | None = None) -> str:
        now = today or datetime.now(UTC)
        ym = month_add(f"{now.year:04d}-{now.month:02d}", -1)
        for _ in range(self.cfg.probe_back_months):
            if self.available(ym):
                return ym
            log.info("BTS has not published %s yet", ym)
            ym = month_add(ym, -1)
        raise RuntimeError(f"No BTS on-time file found in the last {self.cfg.probe_back_months} months")

    def download(self, ym: str, dest_dir: Path) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)
        out = dest_dir / f"ontime_{ym}.zip"
        with self._get(self.url(ym), stream=True) as r:
            r.raise_for_status()
            with out.open("wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
        if not zipfile.is_zipfile(out):
            out.unlink()
            raise RuntimeError(f"BTS returned something other than a zip for {ym}")
        log.info("Downloaded %s (%.0f MB)", ym, out.stat().st_size / 1e6)
        return out


class FixtureSource:
    """Serves the synthetic zips written by fixture.make_months()."""

    def __init__(self, cfg: Config):
        self.dir = cfg.data_dir / "fixture-source"

    def months(self) -> list[str]:
        return sorted(p.stem.removeprefix("ontime_") for p in self.dir.glob("ontime_*.zip"))

    def latest_month(self, today: datetime | None = None) -> str:
        return self.months()[-1]

    def has(self, ym: str) -> bool:
        return ym in self.months()

    def download(self, ym: str, dest_dir: Path) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)
        return Path(shutil.copy(self.dir / f"ontime_{ym}.zip", dest_dir / f"ontime_{ym}.zip"))


def get_source(cfg: Config) -> BTSSource | FixtureSource:
    return FixtureSource(cfg) if cfg.fixture else BTSSource(cfg)
