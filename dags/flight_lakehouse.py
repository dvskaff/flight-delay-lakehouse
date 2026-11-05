"""Airflow DAG for the flight delay lakehouse.

plan ─► ingest_month (one mapped task per month) ─► build_silver ─► validate ─► build_gold ─► maintain ─► publish

GitHub Actions runs it with `python dags/flight_lakehouse.py`, which calls `dag.test()`: the real scheduler code
path, executed in one process (no Airflow server needed). On an Airflow deployment the same file is picked up from
the dags folder and runs weekly.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))  # the `flights` package

from flights import pipeline  # noqa: E402
from flights.config import Config  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": int(os.environ.get("FLIGHTS_TASK_RETRIES", "2")),  # network hiccups on the BTS server
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="flight_lakehouse",
    schedule="@weekly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "delta", "spark"],
    doc_md=__doc__,
)
def flight_lakehouse():
    @task
    def plan() -> dict:
        return {**pipeline.plan(Config()), "started": time.time()}

    @task
    def months_to_load(p: dict) -> list[str]:
        return p["to_load"]

    @task(max_active_tis_per_dag=1)  # one Spark job at a time on a single machine
    def ingest_month(ym: str) -> dict:
        return pipeline.ingest_month(Config(), ym)

    @task(trigger_rule="none_failed")
    def build_silver(p: dict, loads: list[dict]) -> dict:
        return pipeline.build_silver(Config(), p["to_load"], p["window"])

    @task
    def validate(p: dict, silver: dict) -> dict:
        return pipeline.validate(Config(), silver, p["window"])

    @task
    def build_gold(p: dict, checks: dict) -> dict:
        return pipeline.build_gold(Config(), p["window"])

    @task
    def maintain(p: dict, gold: dict) -> dict:
        return pipeline.maintain(Config(), p["to_load"])

    @task(trigger_rule="none_failed")
    def publish(p: dict, loads: list[dict], silver: dict, checks: dict, gold: dict, mnt: dict) -> dict:
        run = {"plan": p, "loads": list(loads), "silver": silver, "validate": checks, "gold": gold, "maintain": mnt,
               "orchestrator": "airflow", "seconds": round(time.time() - p["started"])}  # fmt: skip
        return pipeline.publish(Config(), run)

    p = plan()
    loads = ingest_month.expand(ym=months_to_load(p))
    s = build_silver(p, loads)
    v = validate(p, s)
    g = build_gold(p, v)
    m = maintain(p, g)
    publish(p, loads, s, v, g, m)


dag_object = flight_lakehouse()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    result = dag_object.test()
    state = getattr(result, "state", None)
    print(f"DAG run finished: {state}")
    if str(state) not in ("success", "DagRunState.SUCCESS"):
        sys.exit(1)
