"""
VERIFY: proofs that the pipeline is safe to re-run
Input : the real warehouse (read only) and the raw files (built again in scratch databases)
Output: PASS/FAIL lines: control figures (689), rerun identical, drops 1 -> 2 -> 3 = all at once, one-day reprocess
A fingerprint = row count + sum of a hash of every row (load/build times left out), so any
changed value changes it. Proofs 2-4 build scratch databases; your real warehouse is only read.
"""

import filecmp
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path

import duckdb

from common import settings, sql_helpers
from reporting import export_answers

Con = duckdb.DuckDBPyConnection

# day-based tables and their date column: the only tables a one-day reprocess rebuilds
DAY_TABLES = {
    "silver.stg_session": "session_date_local",
    "silver.stg_device_reading": "local_date",
    "gold.fact_session": "session_date_local",
}


def fingerprints(con: Con, by_day: bool = False) -> dict:
    """{table: (rows, hash)} for every silver and gold table; by_day splits the day-based tables per date."""
    out = {}
    tables = con.execute("""
        SELECT table_schema || '.' || table_name
        FROM information_schema.tables
        WHERE table_catalog = current_database()
            AND table_type = 'BASE TABLE'
            AND table_schema IN ('silver', 'gold')
        ORDER BY 1""").fetchall()
    for (table,) in tables:
        schema, name = table.split(".")
        columns = [
            row[0]
            for row in con.execute(
                """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_catalog = current_database() AND table_schema = ? AND table_name = ?
                AND column_name NOT IN ('loaded_at', 'built_at')""",
                [schema, name],
            ).fetchall()
        ]
        column_list = ", ".join(f'"{c}"' for c in columns)
        if by_day and table in DAY_TABLES:
            day_column = DAY_TABLES[table]
            for day, rows, digest in con.execute(f"""
                    SELECT {day_column}, count(*), sum(hash(x))
                    FROM (SELECT {column_list} FROM {table}) AS x
                    GROUP BY {day_column}""").fetchall():
                out[(table, str(day))] = (rows, digest)
        else:
            out[table] = con.execute(
                f"SELECT count(*), sum(hash(x)) FROM (SELECT {column_list} FROM {table}) AS x"
            ).fetchone()
    return out


def built_times(con: Con) -> dict:
    """When each day's rows were written, per day-based table."""
    return {
        (table, str(day)): built
        for table, day_column in DAY_TABLES.items()
        for day, built in con.execute(
            f"SELECT {day_column}, max(built_at) FROM {table} GROUP BY 1"
        ).fetchall()
    }


def result(name: str, ok: bool, detail: str) -> bool:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    return ok


def check_control_figures(con: Con) -> bool:
    """README control figures: 4/5/4/4/4/2 sessions and 689 Endurance minutes."""
    queries = export_answers.read_queries()
    sessions = dict(con.execute(queries["print_sessions_per_athlete"]).fetchall())
    minutes = con.execute(queries["print_endurance_minutes"]).fetchone()[0]
    ok = sessions == settings.CONTROL_SESSIONS and minutes == settings.CONTROL_ENDURANCE_MINUTES
    return result(
        "Control figures", ok, f"sessions {list(sessions.values())}, Endurance minutes {minutes}"
    )


def check_rerun_identical(build_all: Callable, work: Path) -> bool:
    """Build twice; every table and every CSV must be the same."""
    db = work / "rerun.duckdb"
    build_all(db)
    con = duckdb.connect(str(db))
    first = fingerprints(con)
    export_answers.export_answers(con, work / "csv_1")
    con.close()
    build_all(db)
    con = duckdb.connect(str(db))
    second = fingerprints(con)
    export_answers.export_answers(con, work / "csv_2")
    con.close()
    names = sorted(p.name for p in (work / "csv_1").glob("*.csv"))
    same_csv = filecmp.cmpfiles(work / "csv_1", work / "csv_2", names, shallow=False)[0] == names
    return result(
        "Rerun identical",
        first == second and same_csv,
        f"{len(first)} tables and {len(names)} CSVs identical after a second full run",
    )


def check_drop_by_drop(build_all: Callable, work: Path) -> bool:
    """Load drops 1, 2, 3 one at a time; the final result must equal loading all at once."""
    all_db, step_db = work / "rerun.duckdb", work / "drop_by_drop.duckdb"
    con = duckdb.connect(str(all_db), read_only=True)
    target = fingerprints(con)
    con.close()
    steps, final = [], {}
    for drop in range(1, settings.LAST_DROP + 1):
        build_all(step_db, drops=[drop])
        con = duckdb.connect(str(step_db))
        con.execute(f"ATTACH '{all_db.as_posix()}' AS final (READ_ONLY)")
        complete, differ = con.execute("""
            SELECT
                count(*),
                count(*) FILTER (WHERE NOT EXISTS (
                    SELECT 1 FROM final.gold.fact_session AS f
                    WHERE f.session_id = s.session_id
                        AND f.observed_samples = s.observed_samples
                        AND f.completeness_pct = s.completeness_pct))
            FROM gold.fact_session AS s
            WHERE s.is_complete""").fetchone()
        steps.append(f"drop {drop}: {complete} complete runs, {differ} differ from final")
        if drop == settings.LAST_DROP:
            final = fingerprints(con)
        con.close()
    same = [t for t in target if final.get(t) == target[t]]
    ok = len(same) == len(target) and all(s.endswith(" 0 differ from final") for s in steps)
    return result(
        "Drop by drop = all at once",
        ok,
        "; ".join(steps)
        + f"; after drop {settings.LAST_DROP}, {len(same)} of {len(target)} tables identical to all at once",
    )


def check_one_day_reprocess(
    reprocess: Callable, work: Path, day: str = settings.ROSTER_SNAPSHOT_DATE
) -> bool:
    """Rebuild one day; every other day's rows keep their build time and every value stays the same."""
    con = duckdb.connect(str(work / "rerun.duckdb"))
    values_before, built_before = fingerprints(con, by_day=True), built_times(con)
    logged_before = dict(
        con.execute("SELECT source_file, loaded_at FROM bronze.load_log").fetchall()
    )
    sql_helpers.VERBOSE = False
    reprocess(con, day)
    values_after, built_after = fingerprints(con, by_day=True), built_times(con)
    logged_after = dict(
        con.execute("SELECT source_file, loaded_at FROM bronze.load_log").fetchall()
    )
    con.close()
    recopied = [f for f in logged_before if logged_before[f] != logged_after[f]]
    other_days = [k for k in built_before if k[1] != day]
    untouched = [k for k in other_days if built_after.get(k) == built_before[k]]
    that_day = [k for k in built_before if k[1] == day]
    rebuilt = [k for k in that_day if built_after.get(k) != built_before[k]]
    unchanged = [k for k in values_before if values_after.get(k) == values_before[k]]
    ok = (
        bool(recopied)
        and len(untouched) == len(other_days)
        and len(rebuilt) == len(that_day)
        and len(unchanged) == len(values_before) == len(values_after)
    )
    return result(
        "One-day reprocess",
        ok,
        f"{day}: re-copied {len(recopied)} watch files; rebuilt that day in {len(rebuilt)} of {len(that_day)} tables; "
        f"{len(untouched)} of {len(other_days)} other table-days untouched (built_at unchanged); every value unchanged",
    )


def run_verify(con: Con, build_all: Callable, reprocess: Callable) -> bool:
    """Run the four proofs. Returns True when all pass."""
    work = Path(tempfile.mkdtemp(prefix="stride_verify_"))
    sql_helpers.VERBOSE = False
    try:
        results = [
            check_control_figures(con),
            check_rerun_identical(build_all, work),
            check_drop_by_drop(build_all, work),
            check_one_day_reprocess(reprocess, work),
        ]
    finally:
        sql_helpers.VERBOSE = True
        shutil.rmtree(work, ignore_errors=True)
    print(f"  {sum(results)} of {len(results)} checks passed")
    return all(results)
