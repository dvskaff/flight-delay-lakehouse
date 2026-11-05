"""Rewrites the results block in README.md after each run."""

from __future__ import annotations

import re
from pathlib import Path


def results_block(s: dict) -> str:
    cur, prev = s["last12"], s.get("prior12") or {}
    yoy = (
        f" ({(cur['on_time_rate'] - prev['on_time_rate']) * 100:+.1f} pts vs. prior 12 months)"
        if prev
        else ""
    )
    best, worst = s["carriers"][0], s["carriers"][-1]
    silver = s["lake"]["silver/flights"]
    lines = [
        f"_BTS on-time data {s['window'][0]} to {s['window'][1]} · {silver['rows']:,} flights in silver · "
        f"last run {s['built_at']}_",
        "",
        "| Last 12 months | Value |",
        "|---|---|",
        f"| Flights | {cur['flights']:,} |",
        f"| On-time arrivals (< 15 min late) | {cur['on_time_rate']:.1%}{yoy} |",
        f"| Cancelled | {cur['cancel_rate']:.1%} |",
        f"| Most / least punctual carrier | {best['carrier_name']} {best['on_time_rate']:.1%} · "
        f"{worst['carrier_name']} {worst['on_time_rate']:.1%} |",
        f"| Share of delay minutes from late-arriving aircraft | {s['late_aircraft_share']:.0%} |",
        f"| Next flight departs late after the plane arrived 60+ min late | {s['late_after_60']:.0%} |",
        "",
        f"**Lake:** {sum(1 for t in s['lake'].values() if t.get('exists'))} {s['lake_format'].title()} tables · silver "
        f"{silver.get('files', 0):,} files, {silver.get('bytes', 0) / 1e6:,.0f} MB, {s['silver_versions']} recent "
        f"versions in the log · CHECK constraints: {', '.join(s['constraints']) or 'none'} · "
        f"months loaded this run: {', '.join(s['loaded_now']) or 'none (no new BTS month)'}",
    ]
    return "\n".join(lines)


def update_readme(readme: Path, summary: dict) -> None:
    text = readme.read_text()
    pattern = r"(<!-- RESULTS:START -->\n).*?(\n<!-- RESULTS:END -->)"
    readme.write_text(
        re.sub(pattern, lambda m: m.group(1) + results_block(summary) + m.group(2), text, flags=re.S)
    )
