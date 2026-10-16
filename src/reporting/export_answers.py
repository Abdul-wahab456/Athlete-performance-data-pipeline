"""
REPORTING LAYER: answers for people
Input : gold.* tables, reporting/business_questions.sql
Output: output/<question>.csv (one file per business question) and the control figures on screen
"""

import re
from pathlib import Path

import duckdb

from common import settings

Con = duckdb.DuckDBPyConnection


def read_queries(path: Path = settings.QUERIES_FILE) -> dict[str, str]:
    """Split the .sql file on '-- name: x' markers into {name: query}."""
    parts = re.split(r"^-- name:\s*(\w+)\s*$", path.read_text(encoding="utf-8"), flags=re.M)
    return {
        name: sql.strip().rstrip(";") for name, sql in zip(parts[1::2], parts[2::2], strict=True)
    }


def export_answers(con: Con, out_dir: Path | None = None) -> dict[str, int]:
    """One CSV per query (print_* queries are only shown on screen). Returns {file name: rows}."""
    out_dir = out_dir or settings.OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, sql in read_queries().items():
        if name.startswith("print_"):
            continue
        path = (out_dir / f"{name}.csv").as_posix()
        con.execute(f"COPY ({sql}) TO '{path}' (HEADER, DELIMITER ',')")
        counts[name] = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]
    return counts


def copy_session_quality(con: Con) -> None:
    """Keep a committed copy of the required session_quality deliverable in docs/."""
    path = settings.SESSION_QUALITY_COPY.as_posix()
    con.execute(
        f"COPY (SELECT * FROM gold.session_quality ORDER BY session_id) TO '{path}' (HEADER, DELIMITER ',')"
    )


def print_rejected_count(con: Con) -> None:
    """The last line of every run: how many records went to the dead letter queue."""
    count = con.execute("SELECT count(*) FROM bronze.dlq_rejected_record").fetchone()[0]
    hint = " - see output/dlq_rejected_records.csv" if count else ""
    print(f"Rejected records (DLQ): {count}{hint}")


def print_control_figures(con: Con) -> None:
    queries = read_queries()
    sessions = con.execute(queries["print_sessions_per_athlete"]).fetchall()
    minutes = con.execute(queries["print_endurance_minutes"]).fetchone()[0]
    print(f"  {'Athlete':<14}{'Sessions':>8}")
    for athlete, count in sessions:
        print(f"  {athlete:<14}{count:>8}")
    print(f"  Total logged minutes, Endurance squad: {minutes}")
