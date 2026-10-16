"""
SILVER LAYER: clean source data
Input : bronze.raw_* tables, data/reference/*.csv
Output: silver.ref_* (reviewed lists), silver.stg_roster, silver.stg_session, silver.stg_device_reading
run_silver(con) builds everything. run_silver(con, day=...) rebuilds only the rows of that one day
in the two day-based tables (stg_session, stg_device_reading).
"""

import duckdb

from common import settings
from common.sql_helpers import print_rows, replace_rejects, save_table

Con = duckdb.DuckDBPyConnection
OFFSET = settings.LOCAL_UTC_OFFSET_MIN

# Why a bronze record cannot be used (NULL = fine). Used both to filter the staging tables and to
# fill the dead letter queue, so a record is either in silver or in the DLQ.
DEVICE_REJECT = """CASE
    WHEN TRY_CAST(record_json->>'t' AS TIMESTAMP) IS NULL THEN 'unparseable timestamp t'
    WHEN coalesce(record_json->>'hr', record_json->>'heart_rate') IS NULL
        THEN 'unknown schema: no heart rate field (hr or heart_rate)'
    WHEN TRY_CAST(coalesce(record_json->>'hr', record_json->>'heart_rate') AS INTEGER) IS NULL
        THEN 'unparseable heart rate'
    WHEN (record_json->>'cadence') IS NOT NULL AND TRY_CAST(record_json->>'cadence' AS INTEGER) IS NULL
        THEN 'unparseable cadence'
END"""
VENDOR_REJECT = """CASE
    WHEN (record_json->>'session_id') IS NULL THEN 'unknown schema: no session_id'
    WHEN TRY_CAST(record_json->>'start_time' AS TIMESTAMP) IS NULL
        OR TRY_CAST(record_json->>'end_time' AS TIMESTAMP) IS NULL
        THEN 'unparseable start_time or end_time'
    WHEN TRY_CAST(record_json->>'duration_s' AS INTEGER) IS NULL THEN 'unparseable duration_s'
    WHEN TRY_CAST(page_fetched_at AS TIMESTAMP) IS NULL THEN 'unparseable page fetched_at'
END"""
LABEL_REJECT = """CASE
    WHEN (record_json->>'label_id') IS NULL THEN 'unknown schema: no label_id'
    WHEN TRY_CAST(record_json->>'session_date' AS DATE) IS NULL THEN 'unparseable session_date'
    WHEN TRY_CAST(record_json->>'rpe' AS INTEGER) IS NULL THEN 'unparseable rpe'
END"""


def build_ref_athlete_alias(con: Con) -> None:
    """SILVER | REFERENCE
    Reads : data/reference/athlete_alias.csv (spellings checked by a person)
    Writes: silver.ref_athlete_alias (1 row per accepted spelling)"""
    save_table(
        con,
        "silver.ref_athlete_alias",
        f"""
        SELECT raw_name, athlete_id, canonical_name, review_status
        FROM read_csv_auto('{settings.reference("athlete_alias")}', all_varchar = true)
        ORDER BY raw_name""",
    )


def build_ref_device_clock_offset(con: Con) -> None:
    """SILVER | REFERENCE
    Reads : data/reference/device_clock_offset.csv (each watch's clock vs UTC, with evidence)
    Writes: silver.ref_device_clock_offset (1 row per watch)"""
    save_table(
        con,
        "silver.ref_device_clock_offset",
        f"""
        SELECT device_id, utc_offset_minutes AS utc_offset_min, evidence
        FROM read_csv_auto('{settings.reference("device_clock_offset")}')
        ORDER BY device_id""",
    )


def build_ref_device_assignment(con: Con) -> None:
    """SILVER | REFERENCE
    Reads : data/reference/device_assignment.csv (who wore which watch, in which squad, from when)
    Writes: silver.ref_device_assignment (1 row per athlete + watch + squad period)"""
    save_table(
        con,
        "silver.ref_device_assignment",
        f"""
        SELECT
            athlete_id,
            device_id,
            squad,
            CAST(valid_from AS DATE) AS valid_from,
            coalesce(CAST(valid_to AS DATE), DATE '9999-12-31') AS valid_to,  -- 9999-12-31 = still valid
            evidence
        FROM read_csv_auto('{settings.reference("device_assignment")}', all_varchar = true)
        ORDER BY athlete_id, valid_from""",
    )


def build_ref_session_label_pairing(con: Con) -> None:
    """SILVER | REFERENCE
    Reads : data/reference/session_label_pairing.csv (which coach note belongs to which run, when a day has two)
    Writes: silver.ref_session_label_pairing (1 row per paired note)"""
    save_table(
        con,
        "silver.ref_session_label_pairing",
        f"""
        SELECT label_id, session_id, reason
        FROM read_csv_auto('{settings.reference("session_label_pairing")}', all_varchar = true)
        ORDER BY label_id""",
    )


def build_stg_roster(con: Con) -> None:
    """SILVER | STAGING
    Reads : bronze.raw_roster
    Writes: silver.stg_roster (1 row per athlete per roster file)"""
    save_table(
        con,
        "silver.stg_roster",
        r"""
        SELECT
            CAST(regexp_extract(source_file, 'roster_([0-9-]+)\.csv', 1) AS DATE) AS snapshot_date,  -- date is in the file name
            record_json->>'athlete_id' AS athlete_id,
            record_json->>'name' AS athlete_name,
            record_json->>'squad' AS squad,
            record_json->>'device_id' AS device_id,
            CAST(record_json->>'max_hr' AS INTEGER) AS max_hr,
            CAST(record_json->>'rest_hr' AS INTEGER) AS rest_hr,
            source_file
        FROM bronze.raw_roster
        ORDER BY snapshot_date, athlete_id""",
    )


def build_stg_session(con: Con, day: str | None = None) -> None:
    """SILVER | STAGING
    Reads : bronze.raw_vendor_session, bronze.raw_coach_label, silver.ref_device_assignment,
            silver.ref_athlete_alias, silver.ref_session_label_pairing
    Writes: silver.stg_session (1 row per vendor session: latest version, athlete, coach label)"""
    save_table(
        con,
        "silver.stg_session",
        f"""
        WITH vendor_row AS (
            -- every page entry, with a fingerprint of its values
            SELECT
                record_json->>'session_id' AS session_id,
                record_json->>'device_id' AS device_id,
                record_json->>'sport' AS sport,
                TRY_CAST(record_json->>'start_time' AS TIMESTAMP) AS start_utc,
                TRY_CAST(record_json->>'end_time' AS TIMESTAMP) AS end_utc,
                TRY_CAST(record_json->>'duration_s' AS INTEGER) AS duration_s,
                TRY_CAST(record_json->>'distance_m' AS INTEGER) AS distance_m,
                record_json->>'sync_state' AS vendor_sync_state,
                TRY_CAST(page_fetched_at AS TIMESTAMP) AS fetched_at,
                md5(concat_ws('|', record_json->>'device_id', record_json->>'sport',
                    record_json->>'start_time', record_json->>'end_time', record_json->>'duration_s',
                    record_json->>'distance_m', record_json->>'sync_state')) AS fingerprint
            FROM bronze.raw_vendor_session
            WHERE ({VENDOR_REJECT}) IS NULL  -- rejected records go to the DLQ
        ),
        version AS (
            -- identical copies count once; a changed run keeps each version
            SELECT
                session_id, fingerprint,
                min(device_id) AS device_id, min(sport) AS sport, min(start_utc) AS start_utc,
                min(end_utc) AS end_utc, min(duration_s) AS duration_s, min(distance_m) AS distance_m,
                min(vendor_sync_state) AS vendor_sync_state, min(fetched_at) AS first_seen_at
            FROM vendor_row
            GROUP BY session_id, fingerprint
        ),
        numbered AS (
            SELECT
                *,
                row_number() OVER (PARTITION BY session_id ORDER BY first_seen_at, fingerprint) AS version_no,
                count(*) OVER (PARTITION BY session_id) AS version_count
            FROM version
        ),
        latest AS (
            -- keep latest retry; remember the duration it replaced
            SELECT
                n.*,
                p.duration_s AS previous_duration_s,
                CAST(n.start_utc + to_minutes({OFFSET}) AS DATE) AS session_date_local  -- convert to local date
            FROM numbered AS n
            LEFT JOIN numbered AS p ON p.session_id = n.session_id AND p.version_no = n.version_no - 1
            WHERE n.version_no = n.version_count
        ),
        session AS (
            -- athlete who wore the watch on that day
            SELECT l.*, a.athlete_id
            FROM latest AS l
            LEFT JOIN silver.ref_device_assignment AS a
                ON a.device_id = l.device_id
                AND l.session_date_local >= a.valid_from
                AND l.session_date_local < a.valid_to
        ),
        label AS (
            -- coach notes, athlete from the approved spelling list (exact match)
            SELECT
                c.record_json->>'label_id' AS label_id,
                a.athlete_id,
                TRY_CAST(c.record_json->>'session_date' AS DATE) AS session_date,
                c.record_json->>'label' AS label,
                TRY_CAST(c.record_json->>'rpe' AS INTEGER) AS rpe
            FROM bronze.raw_coach_label AS c
            LEFT JOIN silver.ref_athlete_alias AS a ON a.raw_name = (c.record_json->>'athlete_name')
            WHERE ({LABEL_REJECT}) IS NULL  -- rejected notes go to the DLQ
        ),
        candidate AS (
            -- every run an athlete had on the note's date
            SELECT
                l.*, s.session_id,
                count(*) OVER (PARTITION BY l.label_id) AS runs_that_day
            FROM label AS l
            JOIN session AS s ON s.athlete_id = l.athlete_id AND s.session_date_local = l.session_date
        ),
        matched AS (
            -- one run: exact. two runs: use the reviewed pairing list
            SELECT
                c.*,
                CASE WHEN c.runs_that_day = 1 THEN 'exact: athlete + date'
                    ELSE 'inferred: see data/reference/session_label_pairing.csv' END AS label_match_method
            FROM candidate AS c
            LEFT JOIN silver.ref_session_label_pairing AS p
                ON p.label_id = c.label_id AND p.session_id = c.session_id
            WHERE c.runs_that_day = 1 OR p.label_id IS NOT NULL
        )
        SELECT
            s.session_id,
            s.device_id,
            s.athlete_id,
            s.sport,
            s.start_utc,
            s.end_utc,
            s.session_date_local,
            CAST(date_trunc('week', s.session_date_local) AS DATE) AS week_start_date,  -- Monday
            s.duration_s,
            s.distance_m,
            s.vendor_sync_state,
            s.version_count,
            s.previous_duration_s,
            m.label_id,
            m.label,
            m.rpe,
            m.label_match_method
        FROM session AS s
        LEFT JOIN matched AS m USING (session_id)
        ORDER BY s.session_id""",
        "session_date_local",
        day,
    )


def build_stg_device_reading(con: Con, day: str | None = None) -> None:
    """SILVER | STAGING
    Reads : bronze.raw_device_sync, silver.ref_device_clock_offset, silver.stg_session, silver.stg_roster
    Writes: silver.stg_device_reading (1 row per real watch reading: both app formats merged, copies removed)"""
    save_table(
        con,
        "silver.stg_device_reading",
        f"""
        WITH sample AS (
            -- merge the two app formats; a file with fractional seconds has 2 readings per second
            SELECT
                source_file, drop_no, device_id, firmware, app_version, sample_no,
                TRY_CAST(uploaded_at AS TIMESTAMP) AS uploaded_at,
                TRY_CAST(record_json->>'t' AS TIMESTAMP) AS ts_device,
                TRY_CAST(coalesce(record_json->>'hr', record_json->>'heart_rate') AS INTEGER) AS hr_bpm,
                TRY_CAST(record_json->>'cadence' AS INTEGER) AS cadence_spm,
                TRY_CAST(coalesce(record_json->>'lat', record_json->'gps'->>'lat') AS DOUBLE) AS lat,
                TRY_CAST(coalesce(record_json->>'lon', record_json->'gps'->>'lon') AS DOUBLE) AS lon,
                TRY_CAST(record_json->'gps'->>'hdop' AS DOUBLE) AS hdop,
                CASE WHEN bool_or((record_json->>'t') LIKE '%.%') OVER (PARTITION BY source_file)
                    THEN 0.5 ELSE 1.0 END AS sample_interval_s
            FROM bronze.raw_device_sync
            WHERE ({DEVICE_REJECT}) IS NULL  -- rejected readings go to the DLQ
        ),
        ranked AS (
            -- same watch + same moment = a copy; the earliest upload wins
            SELECT
                *,
                row_number() OVER (PARTITION BY device_id, ts_device ORDER BY uploaded_at, source_file, sample_no) AS copy_no,
                count(*) OVER (PARTITION BY device_id, ts_device) AS copies_received
            FROM sample
        ),
        utc AS (
            -- convert the watch clock to UTC
            SELECT r.*, r.ts_device - to_minutes(o.utc_offset_min) AS ts_utc
            FROM ranked AS r
            LEFT JOIN silver.ref_device_clock_offset AS o ON o.device_id = r.device_id
            WHERE r.copy_no = 1
        ),
        in_session AS (
            -- the run of the same watch that started most recently before the reading
            SELECT
                u.*,
                CASE WHEN u.ts_utc < s.end_utc THEN s.session_id END AS session_id,
                CASE WHEN u.ts_utc < s.end_utc THEN s.athlete_id END AS athlete_id
            FROM utc AS u
            ASOF LEFT JOIN silver.stg_session AS s ON u.device_id = s.device_id AND u.ts_utc >= s.start_utc
        ),
        athlete_max AS (
            SELECT athlete_id, max_hr
            FROM silver.stg_roster
            QUALIFY row_number() OVER (PARTITION BY athlete_id ORDER BY snapshot_date DESC) = 1
        ),
        flagged AS (
            -- glitch: above max heart rate and far from BOTH neighbours in the run
            SELECT
                i.*,
                coalesce(i.hr_bpm > m.max_hr
                    AND abs(i.hr_bpm - lag(i.hr_bpm) OVER run) > {settings.HR_GLITCH_JUMP_BPM}
                    AND abs(i.hr_bpm - lead(i.hr_bpm) OVER run) > {settings.HR_GLITCH_JUMP_BPM}, FALSE) AS is_hr_glitch
            FROM in_session AS i
            LEFT JOIN athlete_max AS m USING (athlete_id)
            WINDOW run AS (PARTITION BY i.session_id ORDER BY i.ts_utc)
        )
        SELECT
            device_id || '@' || CAST(ts_device AS VARCHAR) AS reading_id,
            session_id,
            device_id,
            ts_utc,
            ts_utc + to_minutes({OFFSET}) AS ts_local,
            CAST(ts_utc + to_minutes({OFFSET}) AS DATE) AS local_date,
            hr_bpm,
            is_hr_glitch,
            cadence_spm,
            lat,
            lon,
            hdop,
            sample_interval_s,
            copies_received,
            firmware,
            app_version,
            source_file,
            drop_no
        FROM flagged
        ORDER BY reading_id""",
        "local_date",
        day,
    )


def build_dlq_rejected_record(con: Con) -> None:
    """SILVER | DEAD LETTER QUEUE
    Reads : bronze.raw_device_sync, raw_vendor_session, raw_coach_label, silver.ref_athlete_alias,
            silver.stg_session, silver.stg_device_reading
    Writes: bronze.dlq_rejected_record (pipeline_step = 'silver'): records that cannot be parsed,
            have an unknown schema, an unknown athlete name, or no matching session"""
    replace_rejects(
        con,
        "silver",
        f"""
        WITH device AS (
            -- watch readings that cannot be parsed or have an unknown layout
            SELECT source_file, {DEVICE_REJECT} AS reason, CAST(record_json AS VARCHAR) AS raw_payload
            FROM bronze.raw_device_sync
        ),
        vendor AS (
            SELECT source_file, {VENDOR_REJECT} AS reason, CAST(record_json AS VARCHAR) AS raw_payload
            FROM bronze.raw_vendor_session
        ),
        note AS (
            -- coach notes: parse problems, unknown athlete name, run not found
            SELECT
                c.source_file,
                c.record_json,
                {LABEL_REJECT} AS parse_reason,
                a.athlete_id,
                TRY_CAST(c.record_json->>'session_date' AS DATE) AS session_date
            FROM bronze.raw_coach_label AS c
            LEFT JOIN silver.ref_athlete_alias AS a ON a.raw_name = (c.record_json->>'athlete_name')
        ),
        note_runs AS (
            SELECT
                n.*,
                (SELECT count(*) FROM silver.stg_session AS s
                    WHERE s.athlete_id = n.athlete_id AND s.session_date_local = n.session_date) AS runs_that_day
            FROM note AS n
        ),
        coach AS (
            SELECT
                source_file,
                CASE
                    WHEN parse_reason IS NOT NULL THEN parse_reason
                    WHEN athlete_id IS NULL
                        THEN 'unknown athlete name: ' || coalesce(record_json->>'athlete_name', '(empty)')
                    WHEN (record_json->>'label_id') NOT IN (
                        SELECT label_id FROM silver.stg_session WHERE label_id IS NOT NULL)
                        THEN 'no matching session: ' || CASE
                            WHEN runs_that_day = 0 THEN 'no run by this athlete on that date'
                            ELSE runs_that_day || ' runs that day and no pairing in session_label_pairing.csv' END
                END AS reason,
                CAST(record_json AS VARCHAR) AS raw_payload
            FROM note_runs
        ),
        reading AS (
            -- readings that fall inside no run of their watch (kept in silver with an empty session_id)
            SELECT
                source_file,
                'no matching session: reading outside every run of watch ' || device_id AS reason,
                CAST(json_object('reading_id', reading_id, 'ts_utc', ts_utc, 'hr_bpm', hr_bpm) AS VARCHAR) AS raw_payload
            FROM silver.stg_device_reading
            WHERE session_id IS NULL
        )
        SELECT source_file, reason, raw_payload FROM device WHERE reason IS NOT NULL
        UNION ALL SELECT source_file, reason, raw_payload FROM vendor WHERE reason IS NOT NULL
        UNION ALL SELECT source_file, reason, raw_payload FROM coach WHERE reason IS NOT NULL
        UNION ALL SELECT source_file, reason, raw_payload FROM reading""",
    )
    print_rows(con, "bronze.dlq_rejected_record", "pipeline_step = 'silver'", "rows (silver)")


def run_silver(con: Con, day: str | None = None) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    if day is None:
        build_ref_athlete_alias(con)
        build_ref_device_clock_offset(con)
        build_ref_device_assignment(con)
        build_ref_session_label_pairing(con)
        build_stg_roster(con)
    build_stg_session(con, day)
    build_stg_device_reading(con, day)
    build_dlq_rejected_record(con)
