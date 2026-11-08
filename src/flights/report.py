"""The dashboard (site/index.html) and results.json, built from the gold tables and the lake's own metadata."""

from __future__ import annotations

import html
import json
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.offline import get_plotlyjs_version

from .config import Config
from .lake import Lake
from .report_base import (
    BLUE,
    CSS,
    GRID,
    LAYOUT_JS,
    ORANGE,
    SERIES,
    VIOLET,
    _card,
    _div,
    _int,
    _pct,
    _style,
    _table,
)
from .transform import RULES

TABLES = [
    ("Bronze", "bronze/flights", "Raw monthly files, every value as text, plus lineage columns"),
    ("Silver", "silver/flights", "Typed, checked, de-duplicated flights (MERGE target)"),
    ("Silver", "silver/quarantine", "Rows that broke a hard data-quality rule, with the rule names"),
    ("Gold", "gold/carrier_month", "On-time, cancellation and delay-cause totals by carrier and month"),
    ("Gold", "gold/airport_month", "Departure performance and taxi-out times by airport and month"),
    ("Gold", "gold/route_month", "Route-level performance with 90th-percentile arrival delay"),
    ("Gold", "gold/daily", "National daily totals"),
    ("Gold", "gold/hour_weekday", "On-time departures by scheduled hour and weekday (last 12 months)"),
    ("Gold", "gold/delay_causes", "Delay minutes by cause and month"),
    ("Gold", "gold/delay_propagation", "Next-departure delays after a late arrival (tail-number rotations)"),
    ("Gold", "gold/dim_carrier", "Carrier names"),
    ("Gold", "gold/dim_airport", "Airport city and state (latest)"),
    ("Ops", "ops/load_log", "One row per loaded monthly file"),
    ("Ops", "ops/dq_results", "Data-quality rule results for every load"),
]
WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
EXTRA_CSS = """
.cards { grid-template-columns:repeat(auto-fit,minmax(165px,1fr)); }
.arch { display:flex; gap:10px; flex-wrap:wrap; align-items:stretch; margin:8px 0 16px; }
.arch .box { flex:1 1 170px; border-radius:10px; padding:10px 12px; font-size:13px; color:#52514e; }
.arch .box b { display:block; color:#0b0b0b; font-size:14px; margin-bottom:3px; }
.arch .src { background:#f0efec; } .arch .bronze { background:#f6e7d8; } .arch .silver { background:#e9ecef; }
.arch .gold { background:#fbf0d0; } .arch .out { background:#e3eefb; }
.arch .arrow { align-self:center; color:#898781; font-size:20px; flex:0 0 auto; }
main a { color:#2a78d6; }
td.l:first-child { min-width:140px; }
"""


def _pp(v) -> str:
    return "–" if v is None or pd.isna(v) else f"{v * 100:+.1f} pts"


def _mb(b) -> str:
    return "–" if not b else f"{b / 1e6:,.1f} MB"


def _f1(v) -> str:
    return "–" if v is None or pd.isna(v) else f"{v:.1f}"


# --------------------------------------------------------------------------- data
def load(lake: Lake) -> dict[str, pd.DataFrame]:
    out = {}
    for t in ("carrier_month", "airport_month", "route_month", "daily", "hour_weekday", "delay_causes",
              "delay_propagation", "dim_carrier", "dim_airport"):  # fmt: skip
        out[t] = lake.read(f"gold/{t}").toPandas()
    out["load_log"] = lake.read("ops/load_log").toPandas().sort_values("loaded_at")
    dq = lake.read("ops/dq_results").toPandas()
    out["dq_latest"] = dq[dq["run_at"] == dq["run_at"].max()] if len(dq) else dq
    return out


def window_totals(cm: pd.DataFrame, months: list[str]) -> dict:
    d = cm[cm["year_month"].isin(months)]
    if d.empty:
        return {}
    s = d[["flights", "completed", "on_time", "cancelled"]].sum()
    delay = (d["avg_arr_delay"] * d["completed"]).sum() / s["completed"]
    return {"months": months, "flights": int(s["flights"]), "on_time_rate": s["on_time"] / s["completed"],
            "cancel_rate": s["cancelled"] / s["flights"], "avg_arr_delay": float(delay)}  # fmt: skip


def carrier_scorecard(cm: pd.DataFrame, names: pd.DataFrame, months: list[str]) -> pd.DataFrame:
    d = cm[cm["year_month"].isin(months)].copy()
    d["delay_x_completed"] = d["avg_arr_delay"] * d["completed"]
    g = d.groupby("carrier")[["flights", "completed", "on_time", "cancelled", "delay_x_completed"]].sum()
    g["on_time_rate"] = g["on_time"] / g["completed"]
    g["cancel_rate"] = g["cancelled"] / g["flights"]
    g["avg_arr_delay"] = g["delay_x_completed"] / g["completed"]
    g = g.join(names.set_index("carrier")["carrier_name"]).reset_index()
    return g.sort_values("on_time_rate", ascending=False).reset_index(drop=True)


# --------------------------------------------------------------------------- figures
def fig_daily(daily: pd.DataFrame) -> go.Figure:
    d = daily.sort_values("flight_date").copy()
    d["flight_date"] = pd.to_datetime(d["flight_date"])
    roll = d.set_index("flight_date")[["on_time", "completed", "flights"]].rolling("7D").sum()
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=d["flight_date"],
            y=d["on_time_rate"],
            mode="lines",
            name="Daily",
            line=dict(color=GRID, width=1),
            hovertemplate="%{x|%b %d, %Y}: %{y:.1%}",
        )
    )
    fig.add_trace(go.Scatter(x=roll.index, y=roll["on_time"] / roll["completed"], mode="lines",
                             name="7-day rolling", line=dict(color=BLUE, width=2),
                             hovertemplate="%{x|%b %d, %Y}: %{y:.1%}"))  # fmt: skip
    _style(fig, "On-time arrival rate, every day", height=360, y_title="Arrived < 15 min late")
    fig.update_yaxes(tickformat=".0%")
    return fig


def fig_carrier_trend(cm: pd.DataFrame, names: pd.DataFrame, top: list[str]) -> go.Figure:
    fig = go.Figure()
    nm = names.set_index("carrier")["carrier_name"]
    for c, color in zip(top, SERIES, strict=False):
        d = cm[cm["carrier"] == c].sort_values("year_month")
        fig.add_trace(go.Scatter(x=pd.to_datetime(d["year_month"]), y=d["on_time_rate"], mode="lines+markers",
                                 name=nm.get(c, c), line=dict(color=color, width=2), marker=dict(size=6),
                                 hovertemplate="%{x|%b %Y}: %{y:.1%}"))  # fmt: skip
    _style(
        fig,
        "Monthly on-time arrival rate, five largest carriers",
        height=420,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0, bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=64, r=20, t=60, b=100),
    )
    fig.update_yaxes(tickformat=".0%")
    return fig


def fig_causes(dc: pd.DataFrame, months: list[str]) -> go.Figure:
    d = dc[dc["year_month"].isin(months)].groupby("cause", as_index=False)["minutes"].sum()
    d["share"] = d["minutes"] / d["minutes"].sum()
    d = d.sort_values("share")
    fig = go.Figure(go.Bar(x=d["share"], y=d["cause"], orientation="h", marker_color=ORANGE,
                           text=[f"{s:.0%}" for s in d["share"]], textposition="outside",
                           customdata=d["minutes"] / 60,
                           hovertemplate="%{y}: %{x:.1%} of delay minutes<br>%{customdata:,.0f} hours<extra></extra>"))  # fmt: skip
    fig.update_traces(marker_line_width=0)
    _style(fig, "Where the delay minutes come from (last 12 months)", height=320, hovermode="closest",
           showlegend=False, margin=dict(l=190, r=40, t=74, b=40))  # fmt: skip
    fig.update_xaxes(tickformat=".0%", range=[0, d["share"].max() * 1.2 if len(d) else 1])
    return fig


def fig_propagation(dp: pd.DataFrame) -> go.Figure:
    d = dp.sort_values("bucket_order")
    fig = go.Figure(go.Bar(x=d["bucket"], y=d["next_departs_late_rate"], marker_color=VIOLET,
                           text=[f"{v:.0%}" for v in d["next_departs_late_rate"]], textposition="outside",
                           customdata=d["legs"],
                           hovertemplate="Previous arrival %{x}<br>Next departure 15+ min late: %{y:.1%}"
                                         "<br>%{customdata:,} plane rotations<extra></extra>"))  # fmt: skip
    fig.update_traces(marker_line_width=0)
    _style(fig, "Chance the plane's next flight departs late, by how late it arrived", height=340,
           hovermode="closest", showlegend=False, x_title="Arrival delay of the plane's previous flight that day")  # fmt: skip
    fig.update_yaxes(tickformat=".0%", range=[0, 1.12])
    return fig


def fig_heatmap(hw: pd.DataFrame) -> go.Figure:
    p = hw.pivot(index="day_of_week", columns="dep_hour", values="on_time_departure_rate").reindex(
        range(1, 8)
    )
    n = hw.pivot(index="day_of_week", columns="dep_hour", values="flights").reindex(range(1, 8))
    p = p.where(n >= 50)  # hide hours with too few flights to be meaningful
    fig = go.Figure(go.Heatmap(
        z=p.values, x=[f"{h:02d}:00" for h in p.columns], y=WEEKDAYS, colorscale=[[0, "#d6e6f8"], [1, "#104281"]],
        customdata=n.values, hovertemplate="%{y} %{x}<br>On-time departures %{z:.1%}<br>%{customdata:,} flights<extra></extra>",
        colorbar=dict(tickformat=".0%", title=dict(text="On time")), xgap=2, ygap=2,
        texttemplate="%{z:.0%}", textfont=dict(size=9),
    ))  # fmt: skip
    _style(fig, "On-time departure rate by scheduled hour and weekday (last 12 months)", height=380,
           hovermode="closest", showlegend=False, margin=dict(l=50, r=20, t=74, b=48))  # fmt: skip
    fig.update_yaxes(autorange="reversed")
    return fig


# --------------------------------------------------------------------------- page
def build_site(cfg: Config, lake: Lake, run: dict) -> dict:
    site = cfg.site_dir
    site.mkdir(parents=True, exist_ok=True)
    built_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    g = load(lake)
    cm = g["carrier_month"]
    months = sorted(cm["year_month"].unique())
    last12, prior12 = months[-12:], months[-24:-12]
    cur, prev = window_totals(cm, last12), window_totals(cm, prior12) if len(prior12) == 12 else {}
    score = carrier_scorecard(cm, g["dim_carrier"], last12)
    big5 = score.sort_values("flights", ascending=False)["carrier"].head(5).tolist()
    dp = g["delay_propagation"].sort_values("bucket_order")
    prop = dp.set_index("bucket")["next_departs_late_rate"].to_dict()
    late_after_60 = (
        dp[dp["bucket_order"] >= 4]["legs"] * dp[dp["bucket_order"] >= 4]["next_departs_late_rate"]
    ).sum()
    late_after_60 /= max(dp[dp["bucket_order"] >= 4]["legs"].sum(), 1)
    causes = g["delay_causes"][g["delay_causes"]["year_month"].isin(last12)].groupby("cause")["minutes"].sum()
    late_aircraft_share = (
        causes.get("Late-arriving aircraft", np.nan) / causes.sum() if causes.sum() else np.nan
    )

    # Lake metadata
    details = {t: lake.detail(t) for _, t, _ in TABLES}
    counts = {t: (lake.read(t).count() if details[t]["exists"] else 0) for _, t, _ in TABLES}
    history = lake.history("silver/flights", 12)
    travel = None
    if len(history) >= 2:
        prev_v = history[1]["version"]
        travel = {"version": prev_v, "rows": lake.read_version("silver/flights", prev_v).count(),
                  "current_version": history[0]["version"], "current_rows": counts["silver/flights"]}  # fmt: skip
    constraints = lake.constraints("silver/flights")
    silver_run = run.get("silver", {})
    loaded_now = silver_run.get("months", [])
    dq = pd.DataFrame(silver_run.get("dq") or g["dq_latest"].to_dict(orient="records"))
    if len(dq):
        order = {r.name: i for i, r in enumerate(RULES)}
        dq = dq.sort_values("rule", key=lambda s: s.map(order))

    banner = "<div class='synthetic'>SYNTHETIC TEST DATA: generated for automated tests; these numbers are not real</div>" if cfg.fixture else ""  # fmt: skip
    yoy = (cur.get("on_time_rate", np.nan) - prev["on_time_rate"]) if prev else None
    cards = "".join([
        _card("Flights, last 12 months", _int(cur.get("flights")), f"{last12[0]} to {last12[-1]}"),
        _card("On-time arrivals", _pct(cur.get("on_time_rate")),
              f"{_pp(yoy)} vs. prior 12 months" if yoy is not None else "arrived < 15 min late"),
        _card("Cancelled", f"{cur.get('cancel_rate', np.nan):.1%}", "of scheduled flights"),
        _card("Average arrival delay", f"{cur.get('avg_arr_delay', np.nan):.1f} min", "completed flights, incl. early"),
        _card("Late plane → late next flight", _pct(late_after_60), "after arriving 60+ min late"),
        _card("Rows in the lake", _int(counts["silver/flights"]), f"silver, {len(months)} months"),
    ])  # fmt: skip

    score_rows = [[f"{html.escape(str(r.carrier_name))} <span class='muted'>({r.carrier})</span>", _int(r.flights),
                   _pct(r.on_time_rate), f"{r.cancel_rate:.1%}", _f1(r.avg_arr_delay)] for r in score.itertuples()]  # fmt: skip
    am = g["airport_month"][g["airport_month"]["year_month"].isin(last12)].copy()
    am["dep_on_time_x"] = am["on_time_departure_rate"] * (am["flights"] - am["cancelled"])
    am["taxi_x"] = am["avg_taxi_out"] * am["completed"]
    ap = am.groupby("airport")[["flights", "cancelled", "completed", "dep_on_time_x", "taxi_x"]].sum()
    ap["dep_rate"] = ap["dep_on_time_x"] / (ap["flights"] - ap["cancelled"])
    ap["taxi"] = ap["taxi_x"] / ap["completed"]
    ap = ap.join(g["dim_airport"].set_index("airport")).sort_values("flights", ascending=False).head(15)
    ap_rows = [[f"<b>{a}</b> <span class='muted'>{html.escape(str(r.city))}</span>", _int(r.flights),
                _pct(r.dep_rate), f"{r.cancelled / r.flights:.1%}", _f1(r.taxi)] for a, r in ap.iterrows()]  # fmt: skip
    rm = g["route_month"][g["route_month"]["year_month"].isin(last12)]
    rt = rm.groupby(["origin", "dest"])[["flights", "completed", "on_time", "cancelled"]].sum()
    min_route = int(rt["flights"].quantile(0.9)) if len(rt) else 0
    rt = rt[rt["flights"] >= min_route]
    rt["rate"] = rt["on_time"] / rt["completed"]
    worst = rt.sort_values("rate").head(10)
    route_rows = [[f"{o} → {d}", _int(r.flights), _pct(r.rate), f"{r.cancelled / r.flights:.1%}"]
                  for (o, d), r in worst.iterrows()]  # fmt: skip

    lake_rows = []
    for layer, t, desc in TABLES:
        d = details[t]
        lake_rows.append([f"<b>{layer}</b>", f"<code>{t}</code><div class='muted'>{desc}</div>", _int(counts[t]),
                          _int(d.get("files")), _mb(d.get("bytes")), "–" if d.get("version") is None else str(d["version"]),
                          html.escape(str(d.get("last_operation") or "–"))])  # fmt: skip
    hist_rows = []
    for h in history:
        m = h["metrics"]
        keys = ["numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted", "numDeletedRows",
                "numOutputRows", "numFilesAdded", "numFilesRemoved", "numRemovedFiles"]  # fmt: skip
        shown = ", ".join(
            f"{k.removeprefix('num')}: {int(m[k]):,}" for k in keys if m.get(k) not in (None, "0")
        )
        hist_rows.append(
            [str(h["version"]), h["timestamp"][:16], html.escape(h["operation"]), html.escape(shown) or "–"]
        )
    dq_rows = [[html.escape(r["description"]), "Quarantine" if r["severity"] == "error" else "Warn",
                _int(r["failed"]), f"{r['failed'] / max(r['rows_checked'], 1):.3%}"] for r in dq.to_dict(orient="records")]  # fmt: skip
    checks = run.get("validate", {}).get("checks", {})
    check_html = "".join(f"<span class='chip {'good' if ok else 'bad'}'>{'✓' if ok else '✗'} {k.replace('_', ' ')}</span>"
                         for k, ok in checks.items())  # fmt: skip
    con_html = ", ".join(f"<code>{html.escape(k)}</code>" for k in constraints) or "–"
    travel_html = (
        f"Time travel: silver version {travel['version']} had {_int(travel['rows'])} rows; version "
        f"{travel['current_version']} has {_int(travel['current_rows'])}."
        if travel
        else ""
    )
    merge = silver_run.get("merge", {})
    merge_txt = ", ".join(f"{k.removeprefix('numTargetRows')}: {int(v):,}" for k, v in merge.items()
                          if k.startswith("numTargetRows") and str(v).isdigit())  # fmt: skip
    ll = g["load_log"].tail(6)
    ll_rows = [[r.year_month, _int(r.rows), _mb(r.zip_bytes), r.loaded_at[:16],
                html.escape(r.new_columns or "–")[:60]] for r in ll.itertuples()]  # fmt: skip
    run_line = (
        f"This run loaded <b>{', '.join(loaded_now)}</b> ({_int(silver_run.get('bronze_rows'))} rows): "
        f"{_int(silver_run.get('quarantined'))} quarantined, {_int(silver_run.get('duplicates'))} duplicates removed"
        f"{'; MERGE ' + merge_txt if merge_txt else ''}." if loaded_now else
        "No new month was published since the last run, so nothing was loaded; gold and this page were rebuilt."
    )  # fmt: skip

    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Flight Delay Lakehouse</title>
<meta name="description" content="U.S. flight on-time performance from Bureau of Transportation Statistics data, built as a PySpark and Delta Lake medallion lakehouse orchestrated by Airflow.">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>✈️</text></svg>">
<script src="https://cdn.plot.ly/plotly-{get_plotlyjs_version()}.min.js" charset="utf-8"></script>
<style>{CSS}{EXTRA_CSS}</style></head>
<body>{banner}
<header><div class="wrap">
  <h1>U.S. flight delays: a Delta Lake lakehouse</h1>
  <p>Every domestic flight by the largest U.S. airlines, from the Bureau of Transportation Statistics, loaded monthly
  into a bronze/silver/gold lakehouse built with PySpark and Delta Lake and orchestrated by Apache Airflow.
  {months[0]} to {months[-1]}.</p>
  <a class="btn" href="sql.html">Query the lakehouse (SQL in your browser)</a><a class="btn" href="#lakehouse">How the lakehouse works</a>
  <a class="btn" href="{cfg.repo_url}">Code on GitHub</a>
</div></header>
<main>
<div class="cards">{cards}</div>

<section><h2>On-time performance</h2>
<p>A flight is on time if it arrives less than 15 minutes after its scheduled time (the BTS definition); cancelled
and diverted flights are left out of the on-time rate and counted in the cancellation rate.</p>
{_div(fig_daily(g["daily"]))}</section>

<section><h2>Carriers</h2>
<div class="grid2"><div><h3>Last 12 months, best on-time rate first</h3>
{_table(["Carrier", "Flights", "On time", "Cancelled", "Avg arr. delay (min)"], score_rows)}</div>
{_div(fig_carrier_trend(cm, g["dim_carrier"], big5))}</div></section>

<section><h2>Why flights are late, and how delays spread</h2>
<p>Airlines report the cause of every flight that arrives 15+ minutes late. Late-arriving aircraft, a delay carried
over from the plane's previous flight, account for {_pct(late_aircraft_share)} of delay minutes. Following each
plane through its day (by tail number) shows how strongly delays propagate: after arriving 15–29 minutes late, the
next flight leaves late {_pct(prop.get("15–29 min"))} of the time; after an on-time arrival (0–14 min) only
{_pct(prop.get("0–14 min"))}.</p>
<div class="grid2">{_div(fig_causes(g["delay_causes"], last12))}{_div(fig_propagation(dp))}</div></section>

<section><h2>When and where to fly</h2>
{_div(fig_heatmap(g["hour_weekday"]))}
<div class="grid2"><div><h3>Busiest airports (departures, last 12 months)</h3>
{_table(["Airport", "Flights", "On time", "Cancelled", "Taxi-out (min)"], ap_rows)}</div>
<div><h3>Least punctual busy routes</h3>
{_table(["Route", "Flights", "On time", "Cancelled"], route_rows)}
<p class="muted">Routes in the top 10% by volume (at least {_int(min_route)} flights in 12 months).</p></div></div></section>

<section id="lakehouse"><h2>The lakehouse</h2>
<div class="arch">
<div class="box src"><b>Source</b>BTS monthly zip (≈600K flights), newest month found automatically</div><div class="arrow">→</div>
<div class="box bronze"><b>Bronze</b>raw text + lineage, partition replaced per month (idempotent)</div><div class="arrow">→</div>
<div class="box silver"><b>Silver</b>typed, 9 quality rules, quarantine, de-dup, MERGE upsert, CHECK constraints</div><div class="arrow">→</div>
<div class="box gold"><b>Gold</b>carrier, airport, route, daily, cause and propagation tables</div><div class="arrow">→</div>
<div class="box out"><b>This page</b>rebuilt by Airflow in GitHub Actions every week</div>
</div>
<p>{run_line}</p>
<p><b>Query it yourself:</b> the <a href="sql.html">SQL page</a> loads every gold table and the newest month of flights
into DuckDB running inside your browser, with example queries to start from.</p>
<p><b>Validation:</b> {check_html or "–"}</p>
<p class="muted">Delta CHECK constraints on silver: {con_html}. {travel_html}</p>
{_table(["Layer", "Table", "Rows", "Files", "Size", "Version", "Last operation"], lake_rows, left_cols=2)}
<div class="grid2"><div><h3>Data-quality rules (latest load)</h3>
{_table(["Rule", "Action", "Rows failing", "Share"], dq_rows)}</div>
<div><h3>Silver table history (Delta log)</h3>
{_table(["Version", "When (UTC)", "Operation", "Metrics"], hist_rows) if hist_rows else "<p class='muted'>Table history needs Delta Lake.</p>"}</div></div>
<h3>Recently loaded files</h3>
{_table(["Month", "Rows", "Zip size", "Loaded (UTC)", "New source columns"], ll_rows)}
</section>

<section><h2>How it works</h2><ul class="method">
<li><b>Orchestration:</b> an Airflow DAG (plan → ingest each month in parallel-ready mapped tasks → silver MERGE →
validation → gold → OPTIMIZE/VACUUM → publish), executed by GitHub Actions every week. The Delta tables are saved as
a release asset between runs, so each run only loads months it has not seen.</li>
<li><b>Processing:</b> PySpark 4 with Delta Lake: partition replacement in bronze, MERGE with insert/update/delete in
silver, Delta CHECK constraints, Z-ORDER on origin and carrier, table history and time travel.</li>
<li><b>Quality:</b> schema-drift check on every file, hard rules that quarantine rows (the load stops if more than 1%
fail), warning rules that are counted, row-count reconciliation from bronze to silver, and pytest on synthetic data.</li>
<li><b>Data:</b> Bureau of Transportation Statistics, Reporting Carrier On-Time Performance (public domain). Carrier
names come from a small lookup; unknown codes are shown as codes.</li>
</ul></section>
</main>
<footer>Data: U.S. Bureau of Transportation Statistics · Built {built_at} ·
<a href="{cfg.repo_url}">Source code</a></footer>
<script>{LAYOUT_JS}</script>
</body></html>"""
    (site / "index.html").write_text(page, encoding="utf-8")
    (site / ".nojekyll").touch()

    summary = {
        "window": [months[0], months[-1]], "months": len(months), "last12": cur, "prior12": prev,
        "carriers": score[["carrier", "carrier_name", "flights", "on_time_rate", "cancel_rate"]].to_dict(orient="records"),
        "late_aircraft_share": float(late_aircraft_share), "late_after_60": float(late_after_60),
        "propagation": {k: float(v) for k, v in prop.items()},
        "lake": {t: {"rows": counts[t], **details[t]} for _, t, _ in TABLES},
        "loaded_now": loaded_now, "validation": checks, "lake_format": cfg.lake_format,
        "silver_versions": len(history), "constraints": list(constraints), "synthetic": cfg.fixture,
        "built_at": built_at, "dq_rules": len(RULES),
    }  # fmt: skip
    (site / "results.json").write_text(json.dumps(summary, indent=2, default=str))
    return summary
