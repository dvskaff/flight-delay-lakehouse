"""Query the lakehouse in the browser: exports the gold tables for DuckDB-WASM and writes the SQL page (sql.html).

After every run, publish writes each gold table, plus every flight of the newest month from silver, as a gzipped CSV
(DuckDB in the browser reads these with no extensions) and describes them in data/tables.json. The page loads them
into an in-browser DuckDB with the exact column types from the lake, and runs whatever SQL the visitor types.
"""

from __future__ import annotations

import html
import json
import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .lake import Lake

log = logging.getLogger(__name__)
RUNTIME = ["duckdb.bundle.mjs", "duckdb-eh.wasm", "duckdb-browser-eh.worker.js", "duckdb-mvp.wasm",
           "duckdb-browser-mvp.worker.js"]  # fmt: skip

FLIGHT_COLUMNS = ["flight_date", "carrier", "flight_number", "tail_number", "origin", "dest", "crs_dep_time",
                  "dep_hour", "day_of_week", "dep_delay", "arr_delay", "on_time_arrival", "cancelled",
                  "cancellation_code", "diverted", "distance_mi", "air_time_min", "taxi_out", "carrier_delay",
                  "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"]  # fmt: skip
# (browser table name, lake table, description)
TABLES = [
    ("flights", "silver/flights", "Every flight of the newest month in the lake (silver layer, one row per flight). "
     "The full 24 months are in the Delta tables saved with each release."),
    ("carrier_month", "gold/carrier_month", "On-time, cancellation and delay-cause totals by carrier and month"),
    ("airport_month", "gold/airport_month", "Departure performance and taxi-out times by origin airport and month"),
    ("route_month", "gold/route_month", "Performance of every origin-destination route by month"),
    ("daily", "gold/daily", "National totals for every day"),
    ("hour_weekday", "gold/hour_weekday", "On-time departures by scheduled hour and weekday (last 12 months)"),
    ("delay_causes", "gold/delay_causes", "Minutes of arrival delay by reported cause and month"),
    ("delay_propagation", "gold/delay_propagation",
     "How late a plane's next departure is, by how late it arrived from its previous flight (last 12 months)"),
    ("dim_carrier", "gold/dim_carrier", "Carrier codes and names"),
    ("dim_airport", "gold/dim_airport", "Airport codes with their latest city and state"),
]  # fmt: skip
COLUMNS = {
    "year_month": "Month, as YYYY-MM", "carrier": "Two-letter BTS carrier code (join dim_carrier for the name)",
    "carrier_name": "Airline name", "airport": "Three-letter airport code", "city": "City, as BTS writes it",
    "state": "State or territory", "origin": "Departure airport", "dest": "Arrival airport",
    "flights": "Scheduled flights", "completed": "Flights that were neither cancelled nor diverted",
    "on_time": "Completed flights that arrived less than 15 minutes late", "cancelled": "Cancelled flights",
    "diverted": "Diverted flights", "on_time_rate": "on_time / completed", "cancel_rate": "cancelled / flights",
    "avg_arr_delay": "Average arrival delay of completed flights, minutes (early arrivals count as negative)",
    "avg_dep_delay": "Average departure delay of completed flights, minutes",
    "carrier_delay_min": "Delay minutes the airline attributed to itself",
    "weather_delay_min": "Delay minutes from extreme weather", "nas_delay_min": "Delay minutes from the "
    "National Aviation System (air traffic control, non-extreme weather, airport operations)",
    "security_delay_min": "Delay minutes from security", "late_aircraft_delay_min": "Delay minutes because the "
    "plane arrived late from its previous flight",
    "avg_taxi_out": "Average minutes from gate to wheels-off",
    "on_time_departure_rate": "Share of flights that left less than 15 minutes late",
    "p90_arr_delay": "90th-percentile arrival delay, minutes (approximate)", "flight_date": "Date of the flight",
    "day_of_week": "1 = Sunday … 7 = Saturday", "dep_hour": "Scheduled departure hour, local time (0-23)",
    "cause": "Delay cause as reported by the airline", "minutes": "Delay minutes", "share": "Share of the "
    "month's delay minutes", "bucket": "How late the plane arrived from its previous flight that day",
    "bucket_order": "Sort order of the buckets", "legs": "Plane rotations (arrival followed by a next departure)",
    "next_departs_late_rate": "Share of next departures that left 15+ minutes late",
    "median_next_dep_delay": "Median departure delay of the next flight, minutes",
    "flight_number": "Flight number", "tail_number": "Aircraft registration (tail number)",
    "crs_dep_time": "Scheduled departure time, local, as HHMM", "dep_delay": "Departure delay, minutes",
    "arr_delay": "Arrival delay, minutes (negative = early)", "on_time_arrival": "Arrived less than 15 minutes late "
    "(empty if cancelled or diverted)", "cancellation_code": "A = carrier, B = weather, C = National Aviation "
    "System, D = security", "distance_mi": "Distance, miles", "air_time_min": "Minutes in the air",
    "taxi_out": "Minutes from gate to wheels-off", "carrier_delay": "Delay minutes: carrier",
    "weather_delay": "Delay minutes: extreme weather", "nas_delay": "Delay minutes: National Aviation System",
    "security_delay": "Delay minutes: security", "late_aircraft_delay": "Delay minutes: late-arriving aircraft",
}  # fmt: skip
LAST_12 = "(select distinct year_month from carrier_month order by year_month desc limit 12)"
EXAMPLES = [
    ("Airlines ranked, last 12 months",
     "select c.carrier_name, sum(flights) as flights,\n"
     "       round(100 * sum(on_time) / sum(completed), 1) as pct_on_time,\n"
     "       round(100 * sum(cancelled) / sum(flights), 2) as pct_cancelled\n"
     "from carrier_month join dim_carrier c using (carrier)\n"
     f"where year_month in {LAST_12}\ngroup by c.carrier_name\norder by pct_on_time desc;"),
    ("Best time of day to fly",
     "select dep_hour, sum(flights) as flights,\n"
     "       round(100 * sum(on_time_departure_rate * flights) / sum(flights), 1) as pct_left_on_time\n"
     "from hour_weekday\ngroup by dep_hour\nhaving sum(flights) >= 1000\norder by dep_hour;"),
    ("One route, month by month",
     "-- change the airports to any pair\nselect year_month, flights, round(100 * on_time_rate, 1) as pct_on_time,\n"
     "       round(100 * cancel_rate, 1) as pct_cancelled, p90_arr_delay\n"
     "from route_month\nwhere origin = 'ORD' and dest = 'LGA'\norder by year_month;"),
    ("Longest delays last month",
     "select flight_date, c.carrier_name, flight_number, origin, dest,\n       arr_delay as minutes_late\n"
     "from flights join dim_carrier c using (carrier)\nwhere arr_delay is not null\n"
     "order by arr_delay desc\nlimit 20;"),
    ("Why flights were cancelled",
     "select case cancellation_code when 'A' then 'Carrier' when 'B' then 'Weather'\n"
     "         when 'C' then 'National Aviation System' when 'D' then 'Security' else 'Not given' end as reason,\n"
     "       count(*) as flights\nfrom flights\nwhere cancelled\ngroup by reason\norder by flights desc;"),
    ("How delays snowball",
     "select bucket as previous_arrival, legs,\n       round(100 * next_departs_late_rate, 1) as pct_next_left_late,\n"
     "       median_next_dep_delay\nfrom delay_propagation\norder by bucket_order;"),
    ("Busiest airports",
     "select airport, d.city, sum(flights) as departures,\n"
     "       round(100 * sum(on_time_departure_rate * (flights - cancelled)) / sum(flights - cancelled), 1)"
     " as pct_left_on_time,\n       round(sum(avg_taxi_out * completed) / sum(completed), 1) as taxi_out_min\n"
     f"from airport_month join dim_airport d using (airport)\nwhere year_month in {LAST_12}\n"
     "group by airport, d.city\norder by departures desc\nlimit 15;"),
]  # fmt: skip
DUCK_TYPES = {T.StringType: "VARCHAR", T.IntegerType: "INTEGER", T.LongType: "BIGINT", T.ShortType: "SMALLINT",
              T.DoubleType: "DOUBLE", T.FloatType: "DOUBLE", T.BooleanType: "BOOLEAN", T.DateType: "DATE"}  # fmt: skip


def _duck_type(dt) -> str:
    return next((v for k, v in DUCK_TYPES.items() if isinstance(dt, k)), "VARCHAR")


def _write_csv(df: DataFrame, path: Path) -> None:
    """One gzipped, RFC 4180 CSV file (Spark writes a folder of parts; this keeps the single part)."""
    parts = path.parent / (path.name + ".parts")
    (df.coalesce(1).write.mode("overwrite").option("header", True).option("compression", "gzip")
       .option("escape", '"').option("dateFormat", "yyyy-MM-dd").csv(str(parts)))  # fmt: skip
    shutil.move(next(parts.glob("part-*.csv.gz")), path)
    shutil.rmtree(parts)


def _prepare(df: DataFrame) -> DataFrame:
    """Round doubles to 4 decimals (smaller files; nothing on the site needs more)."""
    return df.select(*[F.round(F.col(f.name), 4).alias(f.name) if isinstance(f.dataType, T.DoubleType)
                       else F.col(f.name) for f in df.schema.fields])  # fmt: skip


def export(lake: Lake, out_dir: Path) -> list[dict]:
    """Write one gzipped CSV per table and return their metadata (for tables.json)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    meta = []
    for name, table, description in TABLES:
        df = lake.read(table)
        if name == "flights":
            newest = df.agg(F.max("year_month")).first()[0]
            df = df.filter(F.col("year_month") == newest).select(*FLIGHT_COLUMNS)
            description = description.replace("the newest month", f"the newest month ({newest})")
            df = df.repartition(1).sortWithinPartitions("flight_date", "carrier", "flight_number", "origin")
        elif "year_month" in df.columns:
            df = df.orderBy(
                *[c for c in ("year_month", "carrier", "airport", "origin", "dest") if c in df.columns]
            )
        df = _prepare(df)
        path = out_dir / f"{name}.csv.gz"
        _write_csv(df, path)
        meta.append({
            "name": name, "file": f"data/{path.name}", "rows": df.count(), "bytes": path.stat().st_size,
            "description": description, "source": table,
            "columns": [{"name": f.name, "type": _duck_type(f.dataType), "description": COLUMNS.get(f.name, "")}
                        for f in df.schema.fields],
        })  # fmt: skip
    log.info("Browser SQL: exported %d tables (%.1f MB)", len(meta), sum(m["bytes"] for m in meta) / 1e6)
    return meta


def copy_runtime(vendor: Path, site: Path) -> bool:
    if not all((vendor / f).exists() for f in RUNTIME):
        log.warning("DuckDB-WASM runtime not found in %s; the SQL page will show a notice", vendor)
        return False
    (site / "vendor").mkdir(parents=True, exist_ok=True)
    for f in RUNTIME:
        shutil.copy(vendor / f, site / "vendor" / f)
    return True


def create_sql(t: dict, path: str) -> str:
    """CREATE TABLE statement used by the page (and by the tests), with the lake's column types."""
    cols = ", ".join(f"'{c['name']}': '{c['type']}'" for c in t["columns"])
    return (
        f"create table {t['name']} as select * from read_csv('{path}', header = true, columns = {{{cols}}})"
    )


# --------------------------------------------------------------------------- page
PAGE_CSS = """
.sqlgrid { display:grid; grid-template-columns:minmax(0,1fr) 300px; gap:16px; }
@media (max-width: 900px) { .sqlgrid { grid-template-columns:minmax(0,1fr); } }
textarea#sql { width:100%; min-height:190px; font:13.5px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
  padding:10px 12px; border:1px solid #c3c2b7; border-radius:10px; background:#fff; resize:vertical; }
.bar { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin:8px 0 10px; }
.run { background:#104281; color:#fff; border:0; border-radius:9px; padding:8px 16px; font-weight:600; cursor:pointer; }
.run:disabled { opacity:.55; cursor:default; }
.ex { border:1px solid #c3c2b7; background:#fff; border-radius:999px; padding:3px 11px; font-size:13px;
  color:#104281; cursor:pointer; }
.ghost { border:1px solid #c3c2b7; background:#fff; border-radius:9px; padding:7px 12px; font-size:13px; cursor:pointer; }
#status { font-size:13px; color:#52514e; } #status.err { color:#c4352b; white-space:pre-wrap; }
.tables details { border-bottom:1px solid #e1e0d9; padding:6px 0; }
.tables summary { color:#0b0b0b; font-weight:600; margin:0; }
.tables summary span { color:#898781; font-weight:400; font-size:12px; }
.tables p { font-size:12.5px; color:#52514e; margin:4px 0; }
.tables ul { padding-left:18px; margin:4px 0; } .tables li { font-size:12.5px; } .tables code { font-size:12px; }
.results { max-height:520px; overflow:auto; border:1px solid #e1e0d9; border-radius:10px; }
.results table { margin:0; } .results th { position:sticky; top:0; background:#fcfcfb; }
"""

PAGE_JS = """
const $ = (id) => document.getElementById(id);
const status = (t, err = false) => { $("status").textContent = t; $("status").className = err ? "err" : ""; };
EXAMPLES.forEach(([label, sql]) => {
  const b = document.createElement("button"); b.className = "ex"; b.textContent = label;
  b.onclick = () => { $("sql").value = sql; run(); }; $("examples").appendChild(b);
});
let conn = null, last = null;
// Arrow hands dates and timestamps back as epoch milliseconds: format them by column type
const kind = (f) => { const t = String(f.type); return t.startsWith("Date") ? "date" : t.startsWith("Timestamp") ? "ts"
  : /^(Int|Uint|Float|Decimal)/.test(t) ? "num" : "other"; };
const raw = (v, k) => v === null || v === undefined ? "" : k === "date" ? new Date(Number(v)).toISOString().slice(0, 10)
  : k === "ts" ? new Date(Number(v)).toISOString().slice(0, 19).replace("T", " ") : typeof v === "bigint" ? v.toString() : String(v);
const fmt = (v, k) => typeof v === "number" && k !== "date" && k !== "ts"
  ? (Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, {maximumFractionDigits: 4}))
  : typeof v === "bigint" ? v.toLocaleString() : raw(v, k);
const esc = (s) => s.replace(/[&<>"]/g, (c) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"})[c]);
const createSql = (t, path) => `create table ${t.name} as select * from read_csv('${path}', header = true, columns = {`
  + t.columns.map((c) => `'${c.name}': '${c.type}'`).join(", ") + "})";
async function init() {
  if (!RUNTIME) throw new Error("The DuckDB runtime is not bundled in this build.");
  const duckdb = await import("./vendor/duckdb.bundle.mjs");
  const base = new URL("./vendor/", location.href).href;
  const bundle = await duckdb.selectBundle({
    mvp: { mainModule: base + "duckdb-mvp.wasm", mainWorker: base + "duckdb-browser-mvp.worker.js" },
    eh: { mainModule: base + "duckdb-eh.wasm", mainWorker: base + "duckdb-browser-eh.worker.js" },
  });
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), new Worker(bundle.mainWorker));
  await db.instantiate(bundle.mainModule);
  // DECIMAL results (e.g. 1.5, sums of integers) come back from Arrow unscaled unless cast to double
  await db.open({ query: { castDecimalToDouble: true } });
  conn = await db.connect();
  const meta = await (await fetch("data/tables.json")).json();
  for (const t of meta.tables) {
    status(`Loading ${t.name} (${t.rows.toLocaleString()} rows)…`);
    const buf = new Uint8Array(await (await fetch(t.file)).arrayBuffer());
    await db.registerFileBuffer(t.name + ".csv.gz", buf);
    await conn.query(createSql(t, t.name + ".csv.gz"));
  }
  status(`Ready: ${meta.tables.length} tables loaded.`);
  $("run").disabled = false;
}
async function run() {
  if (!conn) return;
  $("run").disabled = true; status("Running…");
  const t0 = performance.now();
  try {
    const res = await conn.query($("sql").value);
    const cols = res.schema.fields.map((f) => f.name);
    const kinds = res.schema.fields.map(kind);
    const rows = res.toArray();
    last = {cols, kinds, rows};
    const cls = (i) => kinds[i] === "num" ? "" : "l";  // numbers right-aligned
    const head = "<tr>" + cols.map((c, i) => `<th class="${cls(i)}">${esc(c)}</th>`).join("") + "</tr>";
    const body = rows.slice(0, 1000).map((r) => "<tr>" + cols.map((c, i) =>
      `<td class="${cls(i)}">${esc(fmt(r[c], kinds[i]))}</td>`).join("") + "</tr>").join("");
    $("results").innerHTML = `<table><thead>${head}</thead><tbody>${body}</tbody></table>`;
    status(`${rows.length.toLocaleString()} row${rows.length === 1 ? "" : "s"} in ${(performance.now() - t0).toFixed(0)} ms`);
    $("csv").disabled = rows.length === 0;
  } catch (e) { status(e.message, true); }
  finally { $("run").disabled = false; }
}
$("run").onclick = run;
$("sql").addEventListener("keydown", (e) => { if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); run(); } });
$("csv").onclick = () => {
  if (!last) return;
  const q = (s) => /[",\\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  const csv = [last.cols.map(q).join(","),
    ...last.rows.map((r) => last.cols.map((c, i) => q(raw(r[c], last.kinds[i]))).join(","))].join("\\n");
  const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob([csv], {type: "text/csv"}));
  a.download = "query_results.csv"; a.click();
};
init().then(() => run()).catch((e) => status("Could not start DuckDB here: " + e.message, true));
"""


def write_page(
    site: Path, meta: list[dict], window: list[str], runtime_ok: bool, repo_url: str, synthetic: bool
) -> None:
    from .report_base import CSS

    (site / "data").mkdir(parents=True, exist_ok=True)
    (site / "data" / "tables.json").write_text(json.dumps({"window": window, "tables": meta}, indent=1))
    tables_html = "".join(
        f"<details><summary><code>{m['name']}</code> <span>{m['rows']:,} rows</span></summary>"
        f"<p>{html.escape(m['description'])}</p><ul>"
        + "".join(f"<li><code>{c['name']}</code> <span class='muted'>{c['type'].lower()}</span>"
                  f"{' – ' + html.escape(c['description']) if c['description'] else ''}</li>" for c in m["columns"])
        + "</ul></details>"
        for m in meta
    )  # fmt: skip
    total_mb = sum(m["bytes"] for m in meta) / 1e6
    banner = "<div class='synthetic'>SYNTHETIC TEST DATA: built by the test suite</div>" if synthetic else ""
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Query the Flight Delay Lakehouse</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>✈️</text></svg>">
<style>{CSS}{PAGE_CSS}</style></head><body>{banner}
<header><div class="wrap"><h1>Query the lakehouse</h1>
<p>Write SQL against the gold tables of the flight-delay lakehouse, plus every flight of the newest month. DuckDB
runs inside your browser on files exported from the Delta tables after the latest run (BTS data
{html.escape(window[0])} to {html.escape(window[-1])}), so nothing is sent to a server.</p>
<a class="btn" href="./">Back to the dashboard</a><a class="btn" href="{repo_url}">Code on GitHub</a></div></header>
<main><section><div class="sqlgrid"><div>
<div class="bar" id="examples"></div>
<textarea id="sql" spellcheck="false">{html.escape(EXAMPLES[0][1])}</textarea>
<div class="bar"><button class="run" id="run" disabled>Run (Ctrl+Enter)</button>
<button class="ghost" id="csv" disabled>Download results as CSV</button><span id="status">Loading DuckDB…</span></div>
<div class="results" id="results"></div>
<p class="muted">The first visit downloads DuckDB and the tables ({total_mb:.1f} MB); your browser caches them
afterwards. Results show up to 1,000 rows; the CSV download has them all.</p>
</div><div class="tables"><h3>Tables</h3>{tables_html}</div></div></section></main>
<footer>Data: U.S. Bureau of Transportation Statistics, Reporting Carrier On-Time Performance ·
<a href="{repo_url}">Source code</a></footer>
<script type="module">
const EXAMPLES = {json.dumps(EXAMPLES)};
const RUNTIME = {"true" if runtime_ok else "false"};
{PAGE_JS}
</script></body></html>"""
    (site / "sql.html").write_text(page, encoding="utf-8")
