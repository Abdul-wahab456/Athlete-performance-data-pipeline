"""
AIRFLOW TESTS (without Docker): the DAG file is loaded with a stand-in for the airflow package.
Checks that the five tasks are chained in order, that each task runs main.execute(<step>), and
that docker-compose.yml points at folders that exist. Starting the real container needs Docker.
"""

import importlib.util
import sys
import types

import pytest

from common import settings

AIRFLOW_DIR = settings.ROOT / "orchestration" / "airflow"


class FakeTask:
    def __init__(self, task_id, python_callable, op_args):
        self.task_id, self.python_callable, self.op_args = task_id, python_callable, op_args
        self.downstream = []

    def __rshift__(self, other):
        self.downstream.append(other)
        return other


class FakeDag:
    def __init__(self, **settings_given):
        self.settings = settings_given

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


@pytest.fixture
def dag_module(monkeypatch):
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDag
    operators = types.ModuleType("airflow.operators")
    python = types.ModuleType("airflow.operators.python")
    python.PythonOperator = FakeTask
    monkeypatch.setitem(sys.modules, "airflow", airflow)
    monkeypatch.setitem(sys.modules, "airflow.operators", operators)
    monkeypatch.setitem(sys.modules, "airflow.operators.python", python)
    spec = importlib.util.spec_from_file_location(
        "stride_pipeline_dag", AIRFLOW_DIR / "dags" / "stride_pipeline_dag.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dag_has_the_five_steps_in_order(dag_module):
    ids = [task.task_id for task in dag_module.tasks]
    assert ids == ["ingestion", "silver", "gold", "export", "verify"]
    for upstream, downstream in zip(dag_module.tasks, dag_module.tasks[1:], strict=False):
        assert upstream.downstream == [downstream]
    assert dag_module.tasks[-1].downstream == []
    assert dag_module.dag.settings["dag_id"] == "stride_pipeline"
    assert dag_module.dag.settings["schedule"] is None  # started by hand: make airflow-run
    assert dag_module.dag.settings["max_active_runs"] == 1  # DuckDB allows one writer


def test_each_task_runs_the_same_function_as_the_command_line(dag_module):
    import main

    for task in dag_module.tasks:
        assert task.python_callable is main.execute
    assert [task.op_args for task in dag_module.tasks] == [
        ["ingest"],
        ["silver"],
        ["gold"],
        ["export"],
        ["verify"],
    ]


def test_the_tasks_build_the_warehouse_when_run_one_after_another(
    dag_module, tmp_path, monkeypatch
):
    fixtures = settings.ROOT / "tests" / "fixtures" / "raw"
    monkeypatch.setattr(settings, "RAW_DIR", fixtures)
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "warehouse.duckdb")
    monkeypatch.setattr(settings, "OUTPUT_DIR", tmp_path / "output")
    monkeypatch.setattr(settings, "SESSION_QUALITY_COPY", tmp_path / "session_quality.csv")
    steps_before_verify = dag_module.tasks[:-1]  # verify needs the real data, not the fake files
    for task in steps_before_verify:
        task.python_callable(*task.op_args)
    assert (tmp_path / "session_quality.csv").exists()  # the committed deliverable copy
    assert (tmp_path / "output" / "weekly_training_load.csv").exists()


def test_compose_file_points_at_folders_that_exist():
    text = (AIRFLOW_DIR / "docker-compose.yml").read_text(encoding="utf-8")
    assert "../..:/opt/project" in text  # project root, from orchestration/airflow
    assert (AIRFLOW_DIR / ".." / ".." / "src" / "main.py").resolve().exists()
    assert "PYTHONPATH: /opt/project/src" in text
    assert "AIRFLOW__CORE__DAGS_FOLDER: /opt/project/orchestration/airflow/dags" in text
    assert (settings.ROOT / "orchestration" / "airflow" / "dags").is_dir()
    assert "duckdb==1.5.6" in (AIRFLOW_DIR / "Dockerfile").read_text(encoding="utf-8")
