import os
import subprocess
import sys
from pathlib import Path

from flights import fixture

ROOT = Path(__file__).resolve().parents[1]


def _env(tmp_path: Path) -> dict:
    return {**os.environ, "AIRFLOW_HOME": str(tmp_path / "airflow"), "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__DAGS_FOLDER": str(ROOT / "dags"), "FLIGHTS_FIXTURE": "1",
            "FLIGHTS_DATA_DIR": str(tmp_path / "data"), "FLIGHTS_SITE_DIR": str(tmp_path / "site"),
            "FLIGHTS_WINDOW_MONTHS": "3", "PYTHONPATH": str(ROOT / "src"), "FLIGHTS_TASK_RETRIES": "0"}  # fmt: skip


def test_dag_structure():
    sys.path.insert(0, str(ROOT / "dags"))
    import flight_lakehouse

    dag = flight_lakehouse.dag_object
    ids = {t.task_id for t in dag.tasks}
    assert ids == {"plan", "months_to_load", "ingest_month", "build_silver", "validate", "build_gold", "maintain",
                   "publish"}  # fmt: skip
    assert dag.get_task("build_silver").upstream_task_ids >= {"ingest_month", "plan"}
    assert dag.get_task("publish").trigger_rule == "none_failed"
    assert dag.max_active_runs == 1


def test_dag_runs_end_to_end_with_airflow(tmp_path):
    env = _env(tmp_path)
    fixture.make_months(tmp_path / "data" / "fixture-source", fixture.MONTHS[:2])
    subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"], env=env, check=True, capture_output=True
    )
    out = subprocess.run([sys.executable, str(ROOT / "dags" / "flight_lakehouse.py")], env=env, capture_output=True,
                         text=True)  # fmt: skip
    assert out.returncode == 0, out.stdout[-3000:] + out.stderr[-3000:]
    assert "DAG run finished: success" in out.stdout + out.stderr
    assert (tmp_path / "site" / "index.html").exists()
