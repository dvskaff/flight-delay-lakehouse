"""SYNTHETIC monthly files in the BTS on-time layout, used only by tests and the offline CI run.

Planted on purpose: exact duplicate rows, rows that break hard data-quality rules (origin = destination, missing
carrier, a flight dated outside its file's month), cancellations with codes, delay causes that add up to the arrival
delay, and planes whose late arrival delays their next departure. Pages built from this data carry a red banner.
"""

from __future__ import annotations

import calendar
import csv
import io
import random
import zipfile
from pathlib import Path

MONTHS = ["2025-01", "2025-02", "2025-03", "2025-04"]
CARRIERS = ["AA", "DL", "UA", "WN", "ZZ"]  # ZZ is deliberately not in the carrier lookup
AIRPORTS = {
    "ATL": ("Atlanta, GA", "GA"), "ORD": ("Chicago, IL", "IL"), "DFW": ("Dallas/Fort Worth, TX", "TX"),
    "DEN": ("Denver, CO", "CO"), "LAX": ("Los Angeles, CA", "CA"), "JFK": ("New York, NY", "NY"),
    "SEA": ("Seattle, WA", "WA"), "BOS": ("Boston, MA", "MA"),
}  # fmt: skip
EXTRA_LEADING = ["Year", "Quarter", "Month", "DayofMonth", "DayOfWeek"]
COLUMNS = EXTRA_LEADING + [
    "FlightDate", "Reporting_Airline", "Tail_Number", "Flight_Number_Reporting_Airline", "OriginAirportID",
    "Origin", "OriginCityName", "OriginState", "Dest", "DestCityName", "DestState", "CRSDepTime", "DepTime",
    "DepDelay", "DepDel15", "TaxiOut", "TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay", "ArrDel15", "Cancelled",
    "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance", "CarrierDelay",
    "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "DivAirportLandings",
]  # fmt: skip


def _hhmm(minutes: float) -> str:
    m = int(round(minutes)) % 1440
    return f"{m // 60:02d}{m % 60:02d}"


def _f(x) -> str:
    return "" if x is None else f"{x:.2f}"


def make_rows(ym: str, rng: random.Random, tails: int = 60) -> list[dict]:
    y, mo = map(int, ym.split("-"))
    days = calendar.monthrange(y, mo)[1]
    codes = list(AIRPORTS)
    rows = []
    for t in range(tails):
        carrier = CARRIERS[t % len(CARRIERS)]
        tail = f"N{100 + t}{carrier}"
        for d in range(1, days + 1, 2):  # every other day, 3-4 legs
            date = f"{y:04d}-{mo:02d}-{d:02d}"
            here = codes[(t + d) % len(codes)]
            sched = 360 + rng.randint(0, 120)
            carry = 0.0
            for leg in range(rng.choice([3, 4])):
                there = rng.choice([c for c in codes if c != here])
                block = rng.randint(70, 280)
                dep_delay = max(-8.0, carry * 0.8 - 10 + rng.expovariate(1 / 12))
                cancelled = rng.random() < 0.02
                row = {"date": date, "carrier": carrier, "tail": tail, "fn": 1000 + t * 10 + leg, "origin": here,
                       "dest": there, "crs_dep": sched, "block": block, "distance": block * 7.5}  # fmt: skip
                if cancelled:
                    row.update(cancelled=True, code=rng.choice("ABC"), dep_delay=None, arr_delay=None)
                    carry = 0.0
                else:
                    arr_delay = dep_delay + rng.uniform(-12, 10)
                    row.update(cancelled=False, code=None, dep_delay=dep_delay, arr_delay=arr_delay)
                    carry = max(arr_delay, 0)
                rows.append(row)
                here, sched = there, sched + block + 45
    return rows


def to_csv_rows(rows: list[dict], rng: random.Random) -> list[list[str]]:
    out = []
    for r in rows:
        y, mo, d = map(int, r["date"].split("-"))
        dep, arr = r["dep_delay"], r["arr_delay"]
        causes = [None] * 5
        if arr is not None and arr >= 15:
            late = max(0.0, min(arr, dep or 0))  # part of the delay came in with the plane
            weather = round(rng.random() < 0.1) * min(10.0, arr - late)
            rest = arr - late - weather
            carrier = round(rest * rng.random(), 2)
            causes = [carrier, weather, round(rest - carrier, 2), 0.0, late]
        city_o, st_o = AIRPORTS[r["origin"]]
        city_d, st_d = AIRPORTS[r["dest"]]
        values = {
            "Year": str(y), "Quarter": str((mo - 1) // 3 + 1), "Month": str(mo), "DayofMonth": str(d),
            "DayOfWeek": "1", "FlightDate": r["date"], "Reporting_Airline": r["carrier"] or "",
            "Tail_Number": r["tail"], "Flight_Number_Reporting_Airline": str(r["fn"]), "OriginAirportID": "10000",
            "Origin": r["origin"], "OriginCityName": city_o, "OriginState": st_o, "Dest": r["dest"],
            "DestCityName": city_d, "DestState": st_d, "CRSDepTime": _hhmm(r["crs_dep"]),
            "DepTime": "" if dep is None else _hhmm(r["crs_dep"] + dep), "DepDelay": _f(dep),
            "DepDel15": "" if dep is None else _f(float(dep >= 15)), "TaxiOut": "" if dep is None else _f(15.0),
            "TaxiIn": "" if arr is None else _f(7.0), "CRSArrTime": _hhmm(r["crs_dep"] + r["block"]),
            "ArrTime": "" if arr is None else _hhmm(r["crs_dep"] + r["block"] + arr), "ArrDelay": _f(arr),
            "ArrDel15": "" if arr is None else _f(float(arr >= 15)), "Cancelled": _f(float(r["cancelled"])),
            "CancellationCode": r["code"] or "", "Diverted": "0.00", "CRSElapsedTime": _f(float(r["block"])),
            "ActualElapsedTime": "" if arr is None else _f(r["block"] + arr - dep),
            "AirTime": "" if arr is None else _f(r["block"] - 25.0), "Distance": _f(r["distance"]),
            "CarrierDelay": _f(causes[0]), "WeatherDelay": _f(causes[1]), "NASDelay": _f(causes[2]),
            "SecurityDelay": _f(causes[3]), "LateAircraftDelay": _f(causes[4]), "DivAirportLandings": "0",
        }  # fmt: skip
        out.append([values[c] for c in COLUMNS])
    return out


def make_month_zip(ym: str, dest_dir: Path, seed: int = 0) -> Path:
    rng = random.Random(f"{ym}-{seed}")
    rows = make_rows(ym, rng)
    # Planted problems: 3 hard-rule failures and 5 exact duplicates.
    bad = [dict(rows[0], origin="ATL", dest="ATL", fn=9001), dict(rows[1], carrier=None, fn=9002),
           dict(rows[2], date=f"{ym[:4]}-{'12' if ym[5:] != '12' else '11'}-01", fn=9003)]  # fmt: skip
    rows += bad + rows[10:15]
    lines = to_csv_rows(rows, rng)
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_NONNUMERIC, lineterminator="\n")
    # BTS files quote every header, quote text values and end each line with a trailing comma.
    w.writerow(COLUMNS)
    for line in lines:
        w.writerow(line)
    text = "".join(f"{line}," + "\n" for line in buf.getvalue().splitlines())
    y, mo = ym.split("-")
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / f"ontime_{ym}.zip"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{int(y)}_{int(mo)}.csv",
                   text)  # fmt: skip
        z.writestr("readme.html", "<html>SYNTHETIC TEST DATA</html>")
    return out


def make_months(dest_dir: Path, months: list[str]) -> list[Path]:
    return [make_month_zip(m, dest_dir) for m in months]
