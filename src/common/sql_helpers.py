"""
SQL HELPERS: save a SELECT as a table and print its row count
Each build_* function keeps its own full SQL; this only runs it,
reports it, and names the table if it fails.
"""

import duckdb

from common.errors import PipelineError

VERBOSE = True  # print one progress line per table


def run_sql(
    con: duckdb.DuckDBPyConnection, table: str, sql: str, params: list | None = None
) -> None:
    """Run one statement; if it fails, say which table."""
    try:
        con.execute(sql, params or [])
    except duckdb.Error as error:
        raise PipelineError(f"{table} could not be built: {str(error).splitlines()[0]}") from None


def print_rows(
    con: duckdb.DuckDBPyConnection, table: str, where: str = "TRUE", note: str = "rows"
) -> None:
    """Print one progress line: the table and its number of rows."""
    if VERBOSE:
        rows = con.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
        print(f"      {table} {'.' * max(2, 36 - len(table))} {rows:>9,} {note}")


def save_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    select_sql: str,
    day_column: str | None = None,
    day: str | None = None,
) -> None:
    """Rebuild the whole table, or (with day_column + day) only the rows of that one day."""
    if day is None:
        run_sql(
            con,
            table,
            f"CREATE OR REPLACE TABLE {table} AS SELECT *, now() AS built_at FROM ({select_sql})",
        )
        print_rows(con, table)
        return
    one_day = f"{day_column} = DATE '{day}'"
    run_sql(con, table, f"DELETE FROM {table} WHERE {one_day}")  # remove the old rows of that day
    run_sql(
        con,
        table,
        f"INSERT INTO {table} SELECT *, now() AS built_at FROM ({select_sql}) WHERE {one_day}",
    )
    print_rows(con, table, one_day, "rows rebuilt")


def replace_rejects(
    con: duckdb.DuckDBPyConnection,
    pipeline_step: str,
    rejects_sql: str,
    source_file: str | None = None,
) -> None:
    """Replace one step's rows in the dead letter queue (bronze.dlq_rejected_record).

    rejects_sql selects source_file, reason, raw_payload. Re-running never duplicates a rejected
    record, and a record rejected before keeps its first rejected_at. With source_file, only that
    file's rows of this step are replaced.
    """
    table = "bronze.dlq_rejected_record"
    scope, params = "pipeline_step = ?", [pipeline_step]
    if source_file is not None:
        scope, params = scope + " AND source_file = ?", params + [source_file]
    run_sql(
        con,
        table,
        f"CREATE OR REPLACE TEMP TABLE dlq_previous AS SELECT * FROM {table} WHERE {scope}",
        params,
    )
    run_sql(con, table, f"DELETE FROM {table} WHERE {scope}", params)
    run_sql(
        con,
        table,
        f"""
        INSERT INTO {table}
        SELECT coalesce(p.rejected_at, now()), ?, n.source_file, n.reason, n.raw_payload
        FROM ({rejects_sql}) AS n
        LEFT JOIN (
            SELECT source_file, reason, raw_payload, min(rejected_at) AS rejected_at
            FROM dlq_previous
            GROUP BY source_file, reason, raw_payload
        ) AS p
            ON p.source_file IS NOT DISTINCT FROM n.source_file
            AND p.reason = n.reason
            AND p.raw_payload IS NOT DISTINCT FROM n.raw_payload""",
        [pipeline_step],
    )
