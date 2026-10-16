"""
BRONZE LAYER: copy raw files as they are
Input : data/raw (device_sync/*.json, vendor_api/*.json, roster/*.csv, coach_app/*.csv)
Output: bronze.raw_device_sync, raw_vendor_session, raw_roster, raw_coach_label, load_log
A file's SHA-256 is kept in bronze.load_log: an unchanged file is skipped, a changed file
replaces its own rows (one transaction per file), so re-running never duplicates data.
"""

import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from common import settings
from common.sql_helpers import replace_rejects

Con = duckdb.DuckDBPyConnection
NO_NULLS = "\x01"  # keep empty CSV cells as '' (as received), never NULL


def sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def drop_of(source_file: str) -> int | None:
    match = re.search(r"drop_(\d+)/", source_file)
    return int(match.group(1)) if match else None


def build_bronze_tables(con: Con) -> None:
    """BRONZE | RAW COPY
    Writes: all five bronze tables (empty, if they do not exist yet)"""
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_device_sync (
            source_file VARCHAR, drop_no INTEGER, batch_id VARCHAR, device_id VARCHAR,
            firmware VARCHAR, app_version VARCHAR, uploaded_at VARCHAR,
            sample_no INTEGER, record_json JSON, loaded_at TIMESTAMPTZ)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_vendor_session (
            source_file VARCHAR, record_no INTEGER, page_fetched_at VARCHAR,
            record_json JSON, loaded_at TIMESTAMPTZ)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_roster (
            source_file VARCHAR, row_no INTEGER, record_json JSON, loaded_at TIMESTAMPTZ)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_coach_label (
            source_file VARCHAR, row_no INTEGER, record_json JSON, loaded_at TIMESTAMPTZ)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.dlq_rejected_record (
            rejected_at TIMESTAMPTZ, pipeline_step VARCHAR, source_file VARCHAR,
            reason VARCHAR, raw_payload VARCHAR)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.load_log (
            source_file VARCHAR PRIMARY KEY, source VARCHAR, drop_no INTEGER,
            file_sha256 VARCHAR, row_count BIGINT, loaded_at TIMESTAMPTZ)""")


def load_device_sync_file(
    con: Con, path: Path, source_file: str, drop_no: int | None, loaded_at: datetime
) -> int:
    """BRONZE | RAW COPY
    Writes: bronze.raw_device_sync (1 row per watch reading, batch header repeated on every row)"""
    con.execute("DELETE FROM bronze.raw_device_sync WHERE source_file = ?", [source_file])
    con.execute(
        f"""
        INSERT INTO bronze.raw_device_sync
        SELECT
            ?, ?,
            json->>'batch_id', json->>'device_id', json->>'firmware', json->>'app_version', json->>'uploaded_at',
            sample_no, sample, ?
        FROM (
            SELECT
                json,
                unnest(json_extract(json, '$.samples[*]')) AS sample,
                unnest(range(json_array_length(json, '$.samples')::BIGINT))::INTEGER AS sample_no
            FROM read_json_objects('{sql_path(path)}')
        )""",
        [source_file, drop_no, loaded_at],
    )
    return con.execute(
        "SELECT count(*) FROM bronze.raw_device_sync WHERE source_file = ?", [source_file]
    ).fetchone()[0]


def load_vendor_file(
    con: Con, path: Path, source_file: str, drop_no: int | None, loaded_at: datetime
) -> int:
    """BRONZE | RAW COPY
    Writes: bronze.raw_vendor_session (1 row per entry on a vendor page)"""
    con.execute("DELETE FROM bronze.raw_vendor_session WHERE source_file = ?", [source_file])
    con.execute(
        f"""
        INSERT INTO bronze.raw_vendor_session
        SELECT ?, record_no, page_fetched_at, session, ?
        FROM (
            SELECT
                json->>'fetched_at' AS page_fetched_at,
                unnest(json_extract(json, '$.sessions[*]')) AS session,
                unnest(range(json_array_length(json, '$.sessions')::BIGINT))::INTEGER AS record_no
            FROM read_json_objects('{sql_path(path)}')
        )""",
        [source_file, loaded_at],
    )
    return con.execute(
        "SELECT count(*) FROM bronze.raw_vendor_session WHERE source_file = ?", [source_file]
    ).fetchone()[0]


def load_roster_file(
    con: Con, path: Path, source_file: str, drop_no: int | None, loaded_at: datetime
) -> int:
    """BRONZE | RAW COPY
    Writes: bronze.raw_roster (1 row per CSV row, every value kept as text)"""
    con.execute("DELETE FROM bronze.raw_roster WHERE source_file = ?", [source_file])
    con.execute(
        f"""
        INSERT INTO bronze.raw_roster
        SELECT ?, (row_number() OVER ())::INTEGER, to_json(t), ?
        FROM read_csv('{sql_path(path)}', all_varchar = true, header = true, nullstr = '{NO_NULLS}') AS t""",
        [source_file, loaded_at],
    )
    return con.execute(
        "SELECT count(*) FROM bronze.raw_roster WHERE source_file = ?", [source_file]
    ).fetchone()[0]


def load_coach_file(
    con: Con, path: Path, source_file: str, drop_no: int | None, loaded_at: datetime
) -> int:
    """BRONZE | RAW COPY
    Writes: bronze.raw_coach_label (1 row per CSV row, every value kept as text)"""
    con.execute("DELETE FROM bronze.raw_coach_label WHERE source_file = ?", [source_file])
    con.execute(
        f"""
        INSERT INTO bronze.raw_coach_label
        SELECT ?, (row_number() OVER ())::INTEGER, to_json(t), ?
        FROM read_csv('{sql_path(path)}', all_varchar = true, header = true, nullstr = '{NO_NULLS}') AS t""",
        [source_file, loaded_at],
    )
    return con.execute(
        "SELECT count(*) FROM bronze.raw_coach_label WHERE source_file = ?", [source_file]
    ).fetchone()[0]


REQUIRED_COLUMNS = {  # CSV sources: the columns a file must have
    "roster": {"athlete_id", "name", "squad", "device_id", "max_hr", "rest_hr"},
    "coach_app": {"label_id", "athlete_name", "session_date", "label", "rpe"},
}
NO_REJECTS = (
    "SELECT NULL::VARCHAR AS source_file, NULL::VARCHAR AS reason, NULL::VARCHAR AS raw_payload"
    " WHERE FALSE"
)


def file_problem(con: Con, source: str, path: Path) -> str | None:
    """Why a file cannot be loaded (cannot be parsed, or unknown schema); None if it is fine."""
    try:
        if source == "device_sync":
            good, total = con.execute(f"""
                SELECT
                    count(*) FILTER (WHERE json_type(json, '$.samples') = 'ARRAY'
                        AND (json->>'batch_id') IS NOT NULL AND (json->>'device_id') IS NOT NULL),
                    count(*)
                FROM read_json_objects('{sql_path(path)}')""").fetchone()
            if total == 0 or good != total:
                return "unknown schema: expected batch_id, device_id and a samples list"
        elif source == "vendor_api":
            good, total = con.execute(f"""
                SELECT count(*) FILTER (WHERE json_type(json, '$.sessions') = 'ARRAY'), count(*)
                FROM read_json_objects('{sql_path(path)}')""").fetchone()
            if total == 0 or good != total:
                return "unknown schema: expected a sessions list"
        else:
            described = con.execute(
                f"DESCRIBE SELECT * FROM read_csv('{sql_path(path)}', all_varchar = true, header = true)"
            ).fetchall()
            missing = sorted(REQUIRED_COLUMNS[source] - {row[0] for row in described})
            if missing:
                return f"unknown schema: missing columns {', '.join(missing)}"
    except duckdb.Error as error:
        return f"cannot be parsed: {str(error).splitlines()[0]}"
    return None


def quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def reject_file(con: Con, source_file: str, path: Path, reason: str) -> None:
    """Send a whole file to the dead letter queue; its first 2000 characters are the payload."""
    payload = path.read_text(encoding="utf-8", errors="replace")[:2000]
    replace_rejects(
        con,
        "ingestion",
        f"SELECT {quote(source_file)} AS source_file, {quote(reason)} AS reason, "
        f"{quote(payload)} AS raw_payload",
        source_file,
    )


SOURCES = {  # source -> (file pattern, loader)
    "device_sync": ("device_sync/drop_*/*.json", load_device_sync_file),
    "vendor_api": ("vendor_api/*.json", load_vendor_file),
    "roster": ("roster/*.csv", load_roster_file),
    "coach_app": ("coach_app/*.csv", load_coach_file),
}


def load_file(con: Con, source: str, path: Path, force: bool = False) -> str:
    """Copy one file into bronze: 'loaded', 'reloaded', 'skipped', or 'rejected' (sent to the DLQ)."""
    source_file = path.relative_to(settings.RAW_DIR).as_posix()
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    previous = con.execute(
        "SELECT file_sha256 FROM bronze.load_log WHERE source_file = ?", [source_file]
    ).fetchone()
    if previous and previous[0] == sha and not force:
        return "skipped"  # same file as last time
    problem = file_problem(con, source, path)
    if problem:
        reject_file(con, source_file, path, problem)
        return "rejected"
    drop_no, loaded_at = drop_of(source_file), datetime.now(UTC)
    loader = SOURCES[source][1]
    con.execute("BEGIN TRANSACTION")
    try:
        rows = loader(con, path, source_file, drop_no, loaded_at)
        con.execute("DELETE FROM bronze.load_log WHERE source_file = ?", [source_file])
        con.execute(
            "INSERT INTO bronze.load_log VALUES (?, ?, ?, ?, ?, ?)",
            [source_file, source, drop_no, sha, rows, loaded_at],
        )
        con.execute("COMMIT")
    except duckdb.Error as error:
        con.execute("ROLLBACK")
        reject_file(con, source_file, path, f"could not be loaded: {str(error).splitlines()[0]}")
        return "rejected"
    except Exception:
        con.execute("ROLLBACK")
        raise
    replace_rejects(con, "ingestion", NO_REJECTS, source_file)  # a fixed file leaves the queue
    return "reloaded" if previous else "loaded"


def device_files_with_day(con: Con, day: str) -> set[Path]:
    """Watch files that hold a reading on this day. A file that cannot be read is skipped (it is in the DLQ)."""
    found = set()
    for path in settings.RAW_DIR.glob(SOURCES["device_sync"][0]):
        try:
            hit = con.execute(f"""
                SELECT count(*) FROM read_json_objects('{sql_path(path)}')
                WHERE len(list_filter(json_extract_string(json, '$.samples[*].t'), t -> t LIKE '{day}%')) > 0""").fetchone()[
                0
            ]
        except duckdb.Error:
            continue
        if hit:
            found.add(path.resolve())
    return found


def find_files(
    con: Con, source: str, drops: list[int] | None = None, day: str | None = None
) -> list[Path]:
    files = sorted(settings.RAW_DIR.glob(SOURCES[source][0]))
    if source == "device_sync" and drops:
        files = [f for f in files if drop_of(f.relative_to(settings.RAW_DIR).as_posix()) in drops]
    if source == "device_sync" and day:
        wanted = device_files_with_day(con, day)
        files = [f for f in files if f.resolve() in wanted]
    return files


def run_ingestion(
    con: Con, drops: list[int] | None = None, day: str | None = None
) -> dict[str, int]:
    """Load every source. drops narrows the watch files; day re-copies only that day's watch files."""
    build_bronze_tables(con)
    status = {"loaded": 0, "reloaded": 0, "skipped": 0, "rejected": 0}
    rejected_files = []
    for source in ["device_sync"] if day else list(SOURCES):
        for path in find_files(con, source, drops, day):
            result = load_file(con, source, path, force=day is not None)
            status[result] += 1
            if result == "rejected":
                rejected_files.append(path.relative_to(settings.RAW_DIR).as_posix())
    if drops is None and day is None:
        # a file that was rejected before but is no longer in data/raw leaves the queue
        con.execute(
            "DELETE FROM bronze.dlq_rejected_record"
            " WHERE pipeline_step = 'ingestion' AND NOT list_contains(?, source_file)",
            [rejected_files],
        )
    return status
