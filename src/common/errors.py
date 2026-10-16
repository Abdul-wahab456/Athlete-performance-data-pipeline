"""
ERRORS: clear messages for expected problems
Every command calls a check from here first, so the user sees one line, not a traceback.
"""

import re
import shutil
import subprocess
from datetime import date

import duckdb

from common import settings


class PipelineError(Exception):
    """An expected problem: main.py prints 'ERROR: <message>' and exits with code 1."""


def require_raw_data() -> None:
    if not (settings.RAW_DIR / "device_sync").is_dir():
        raise PipelineError("No raw data in data/raw. See README > How to run.")


def require_warehouse() -> None:
    if not settings.DB_PATH.exists():
        raise PipelineError("Warehouse not built yet. Run 'make run' first.")


def check_drops(drops: list[int] | None) -> None:
    if drops and any(d not in range(1, settings.LAST_DROP + 1) for d in drops):
        raise PipelineError(f"DROP must be 1 to {settings.LAST_DROP}, you gave {drops}.")


def check_day(day: str) -> None:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        raise PipelineError(f"DAY must look like YYYY-MM-DD, you gave '{day}'.")
    try:
        parsed = date.fromisoformat(day)
    except ValueError:
        raise PipelineError(f"DAY '{day}' is not a real date.") from None
    if (
        not date.fromisoformat(settings.SEASON_START)
        <= parsed
        <= date.fromisoformat(settings.SEASON_END)
    ):
        raise PipelineError(
            f"DAY {day} is outside the season ({settings.SEASON_START} to {settings.SEASON_END})."
        )


def require_docker() -> None:
    if shutil.which("docker") is None:
        raise PipelineError("Docker is not installed. Install Docker Desktop (docker.com).")
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        raise PipelineError(
            "Docker is not running. Open Docker Desktop and wait for 'Engine running'."
        )


def open_warehouse(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open the DuckDB file; a lock means another program has it open."""
    settings.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        return duckdb.connect(str(settings.DB_PATH), read_only=read_only)
    except duckdb.IOException as error:
        message = str(error).lower()
        if "lock" in message or "used by another process" in message:
            raise PipelineError(
                "The warehouse is open in another program. Close the dashboard, DuckDB UI or Airflow run, then try again."
            ) from None
        raise
