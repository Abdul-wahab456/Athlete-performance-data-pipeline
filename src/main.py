"""
MAIN: start here. Runs the whole pipeline, or one step of it.

    python src/main.py run [--drop 1]            [1/4] Ingestion -> [2/4] Silver -> [3/4] Gold -> [4/4] Export
    python src/main.py ingest | silver | gold | export
    python src/main.py reprocess --day 2026-08-14
    python src/main.py audit | verify | test | format | dashboard | ui | clean
    python src/main.py airflow-up | airflow-run | airflow-down

After `pip install -r requirements.txt` the same commands work as one word: run, verify, test ...
Expected problems end with one line "ERROR: ..." and exit code 1, never a traceback.
"""

import argparse
import shutil
import subprocess
import sys
import time
import urllib.request
import webbrowser
from collections.abc import Callable

import duckdb

from common import errors, settings, sql_helpers
from common.errors import PipelineError
from ingestion import load_raw_files
from reporting import export_answers
from transformation import gold_star_schema, silver_clean_sources
from validation import verify_pipeline

AIRFLOW_DIR = settings.ROOT / "orchestration" / "airflow"
AIRFLOW_URL = "http://localhost:8080"
Con = duckdb.DuckDBPyConnection


def run_step(label: str, step: Callable, *args) -> None:
    """Print the step name, run it, and say which step failed if it does."""
    print(label)
    name = label.split("]")[1].strip()
    try:
        step(*args)
    except PipelineError as error:
        raise PipelineError(f"{name} failed. {error}") from None
    except duckdb.Error as error:
        raise PipelineError(f"{name} failed. {str(error).splitlines()[0]}") from None


def step_ingestion(con: Con, drops: list[int] | None) -> None:
    load_raw_files.run_ingestion(con, drops)
    for table in [
        "raw_device_sync",
        "raw_vendor_session",
        "raw_roster",
        "raw_coach_label",
        "dlq_rejected_record",
        "load_log",
    ]:
        sql_helpers.print_rows(con, f"bronze.{table}")


def step_export(con: Con) -> None:
    counts = export_answers.export_answers(con)
    export_answers.copy_session_quality(con)
    folder = f"{settings.OUTPUT_DIR.name}/"
    print(f"      {folder} {'.' * (36 - len(folder))} {len(counts):>9} files")


def build_all(db_path, drops: list[int] | None = None) -> None:
    """Ingest + silver + gold into the given database (used by the proofs)."""
    con = duckdb.connect(str(db_path))
    load_raw_files.run_ingestion(con, drops)
    silver_clean_sources.run_silver(con)
    gold_star_schema.run_gold(con)
    con.close()


def reprocess_day(con: Con, day: str) -> None:
    """Re-copy that day's watch files, then rebuild only that day's rows."""
    load_raw_files.run_ingestion(con, day=day)
    silver_clean_sources.run_silver(con, day)
    gold_star_schema.run_gold(con, day)


def cmd_run(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_raw_data()
    errors.check_drops(drops)
    started = time.perf_counter()
    con = errors.open_warehouse()
    try:
        run_step("[1/4] Ingestion", step_ingestion, con, drops)
        run_step("[2/4] Silver", silver_clean_sources.run_silver, con)
        run_step("[3/4] Gold", gold_star_schema.run_gold, con)
        run_step("[4/4] Export", step_export, con)
        print(f"Done in {time.perf_counter() - started:.1f}s\n")
        export_answers.print_control_figures(con)
        export_answers.print_rejected_count(con)
    finally:
        con.close()


def cmd_ingest(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_raw_data()
    errors.check_drops(drops)
    con = errors.open_warehouse()
    try:
        run_step("[1/1] Ingestion", step_ingestion, con, drops)
    finally:
        con.close()


def cmd_silver(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_warehouse()
    con = errors.open_warehouse()
    try:
        run_step("[1/1] Silver", silver_clean_sources.run_silver, con)
    finally:
        con.close()


def cmd_gold(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_warehouse()
    con = errors.open_warehouse()
    try:
        run_step("[1/1] Gold", gold_star_schema.run_gold, con)
    finally:
        con.close()


def cmd_export(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_warehouse()
    con = errors.open_warehouse(read_only=True)
    try:
        run_step("[1/1] Export", step_export, con)
        print()
        export_answers.print_control_figures(con)
    finally:
        con.close()


def cmd_reprocess(drops: list[int] | None = None, day: str | None = None) -> None:
    day = day or settings.ROSTER_SNAPSHOT_DATE
    errors.check_day(day)
    errors.require_raw_data()
    errors.require_warehouse()
    con = errors.open_warehouse()
    try:
        print(f"Reprocess {day}")
        reprocess_day(con, day)
        print("      other days untouched")
    finally:
        con.close()


def cmd_audit(drops: list[int] | None = None, day: str | None = None) -> None:
    from validation import (
        profile_raw_data,
    )  # needs pandas, so it is not loaded for the other commands

    errors.require_raw_data()
    profile_raw_data.main([])


def cmd_verify(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_raw_data()
    errors.require_warehouse()
    con = errors.open_warehouse(read_only=True)
    try:
        print("Verify")
        if not verify_pipeline.run_verify(con, build_all, reprocess_day):
            raise PipelineError("At least one proof failed. See the FAIL lines above.")
    finally:
        con.close()


def cmd_test(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_warehouse()
    if subprocess.call([sys.executable, "-m", "pytest", "-q", "tests"], cwd=settings.ROOT) != 0:
        raise PipelineError("Some tests failed. See the output above.")


def cmd_format(drops: list[int] | None = None, day: str | None = None) -> None:
    for command in (["format", "."], ["check", "--fix", "."]):
        if subprocess.call([sys.executable, "-m", "ruff", *command], cwd=settings.ROOT) != 0:
            raise PipelineError("ruff found problems it could not fix. See the output above.")


def cmd_dashboard(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_warehouse()
    errors.open_warehouse(
        read_only=True
    ).close()  # a lock error is explained here, not in the browser
    url = "http://localhost:8501"
    server = subprocess.Popen(
        [
            sys.executable, "-m", "streamlit", "run", "dashboard/app.py",
            "--server.headless", "true",  # no first-run e-mail question; we open the browser ourselves
            "--server.port", "8501",
            "--browser.gatherUsageStats", "false",
        ],
        cwd=settings.ROOT,
    )  # fmt: skip
    time.sleep(4)
    print(f"Dashboard: {url}   (press Ctrl + C to close it)")
    webbrowser.open(url)
    try:
        server.wait()
    except KeyboardInterrupt:
        server.terminate()
        print("Dashboard closed.")


def cmd_clean(drops: list[int] | None = None, day: str | None = None) -> None:
    """Delete the warehouse and output/. Never touches data/raw or data/reference."""
    for path in [settings.DB_PATH, settings.DB_PATH.with_suffix(".duckdb.wal")]:
        path.unlink(missing_ok=True)
    shutil.rmtree(settings.OUTPUT_DIR, ignore_errors=True)
    print(f"Removed {settings.DB_PATH.name} and {settings.OUTPUT_DIR.name}/")


def airflow(*args: str, quiet: bool = False) -> int:
    out = subprocess.DEVNULL if quiet else None
    return subprocess.call(["docker", "compose", *args], cwd=AIRFLOW_DIR, stdout=out, stderr=out)


def airflow_is_up() -> bool:
    try:
        with urllib.request.urlopen(f"{AIRFLOW_URL}/health", timeout=2) as response:
            return response.status == 200
    except OSError:
        return False


def cmd_airflow_up(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_docker()
    if airflow("up", "-d", "--build") != 0:
        raise PipelineError("Docker could not start Airflow. See the output above.")
    print("Starting Airflow (the first time takes a few minutes) ...")
    password_file = AIRFLOW_DIR / "airflow_home" / "standalone_admin_password.txt"
    for _ in range(600):
        if password_file.exists() and airflow_is_up():
            print(
                f"Airflow is ready: {AIRFLOW_URL}   user: admin   password: {password_file.read_text().strip()}"
            )
            webbrowser.open(AIRFLOW_URL)
            return
        time.sleep(1)
    print(f"Airflow is still starting. Open {AIRFLOW_URL} in a minute.")


def cmd_airflow_run(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_docker()
    airflow("exec", "-T", "airflow", "airflow", "dags", "unpause", "stride_pipeline", quiet=True)
    if (
        airflow(
            "exec", "-T", "airflow", "airflow", "dags", "trigger", "stride_pipeline", quiet=True
        )
        != 0
    ):
        raise PipelineError("Airflow is not running. Run 'make airflow-up' first.")
    page = f"{AIRFLOW_URL}/dags/stride_pipeline/grid"
    print(f"Pipeline started in Airflow. Watch it here: {page}")
    webbrowser.open(page)


def cmd_airflow_down(drops: list[int] | None = None, day: str | None = None) -> None:
    errors.require_docker()
    airflow("down")
    print("Airflow stopped.")


COMMANDS: dict[str, Callable] = {
    "run": cmd_run,
    "ingest": cmd_ingest,
    "silver": cmd_silver,
    "gold": cmd_gold,
    "export": cmd_export,
    "reprocess": cmd_reprocess,
    "audit": cmd_audit,
    "verify": cmd_verify,
    "test": cmd_test,
    "format": cmd_format,
    "dashboard": cmd_dashboard,
    "clean": cmd_clean,
    "airflow-up": cmd_airflow_up,
    "airflow-run": cmd_airflow_run,
    "airflow-down": cmd_airflow_down,
}


def execute(command: str, drops: list[int] | None = None, day: str | None = None) -> None:
    """Run one command. Airflow calls this too, so there is one copy of the logic."""
    COMMANDS[command](drops, day)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="main.py", description="Stride pipeline")
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument(
        "--drop", nargs="+", type=int, help="watch delivery number(s) 1-3, e.g. --drop 1"
    )
    parser.add_argument("--day", help="day to reprocess, YYYY-MM-DD")
    args = parser.parse_args(argv)
    try:
        execute(args.command, args.drop, args.day)
    except PipelineError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)


def __getattr__(name: str):
    """One-word commands: pyproject points `run`, `verify`, ... at cli_run, cli_verify, ..."""
    if name.startswith("cli_"):
        command = name[4:].replace("_", "-")
        return lambda: main([command, *sys.argv[1:]])
    raise AttributeError(name)


if __name__ == "__main__":
    main()
