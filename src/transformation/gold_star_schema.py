"""
GOLD LAYER: star schema (answers)
Input : silver.stg_*, silver.ref_*
Output: gold.dim_athlete, gold.fact_session, gold.fact_athlete_baseline, gold.fact_reading_flag,
        gold.session_quality (view over fact_session)
run_gold(con) builds everything. run_gold(con, day=...) rebuilds only that day's rows in fact_session
and recalculates the two whole-season tables (a baseline window and the flag list span many days).
"""

import duckdb

from common import settings
from common.sql_helpers import run_sql, save_table

Con = duckdb.DuckDBPyConnection
OFFSET = settings.LOCAL_UTC_OFFSET_MIN
THRESHOLD = settings.COMPLETENESS_THRESHOLD_PCT


def build_dim_athlete(con: Con) -> None:
    """GOLD | DIMENSION | SCD TYPE 2
    Reads : silver.stg_roster, silver.ref_device_assignment
    Writes: gold.dim_athlete (1 row per athlete per version; a new version when the watch or squad changes)"""
    save_table(
        con,
        "gold.dim_athlete",
        """
        WITH athlete AS (
            -- newest roster row per athlete: name and heart-rate limits
            SELECT athlete_id, athlete_name, max_hr, rest_hr
            FROM silver.stg_roster
            QUALIFY row_number() OVER (PARTITION BY athlete_id ORDER BY snapshot_date DESC) = 1
        )
        SELECT
            row_number() OVER (ORDER BY p.athlete_id, p.valid_from) AS athlete_sk,  -- surrogate key
            p.athlete_id,
            a.athlete_name,
            p.squad,
            p.device_id,
            a.max_hr,
            a.rest_hr,
            p.valid_from,
            p.valid_to,  -- periods come already closed by the reviewed list; 9999-12-31 = current
            p.valid_to = DATE '9999-12-31' AS is_current
        FROM silver.ref_device_assignment AS p
        JOIN athlete AS a USING (athlete_id)
        ORDER BY athlete_sk""",
    )


def build_fact_session(con: Con, day: str | None = None) -> None:
    """GOLD | FACT
    Reads : silver.stg_session, silver.stg_device_reading, gold.dim_athlete
    Writes: gold.fact_session (1 row per session: load, zone minutes, completeness; squad copied on purpose)"""
    save_table(
        con,
        "gold.fact_session",
        f"""
        WITH session AS (
            -- athlete version valid on the session date (squad is copied so reports need no join)
            SELECT s.*, d.athlete_sk, d.athlete_name, d.squad, d.max_hr
            FROM silver.stg_session AS s
            LEFT JOIN gold.dim_athlete AS d
                ON d.athlete_id = s.athlete_id
                AND s.session_date_local >= d.valid_from
                AND s.session_date_local < d.valid_to
        ),
        reading AS (
            -- zone of each valid reading (whole-number maths: a boundary goes to the higher zone)
            SELECT
                r.session_id, r.ts_utc, r.sample_interval_s, r.is_hr_glitch, r.copies_received, r.firmware,
                CASE
                    WHEN r.is_hr_glitch OR r.hr_bpm IS NULL THEN NULL
                    WHEN r.hr_bpm * 10 < s.max_hr * 6 THEN 1
                    WHEN r.hr_bpm * 10 < s.max_hr * 7 THEN 2
                    WHEN r.hr_bpm * 10 < s.max_hr * 8 THEN 3
                    WHEN r.hr_bpm * 10 < s.max_hr * 9 THEN 4
                    ELSE 5
                END AS zone
            FROM silver.stg_device_reading AS r
            JOIN session AS s USING (session_id)
        ),
        per_session AS (
            SELECT
                session_id,
                count(DISTINCT date_trunc('second', ts_utc)) AS observed_samples,  -- seconds with data
                CAST(max(1 / sample_interval_s) AS INTEGER) AS sample_rate_hz,
                CAST(sum(copies_received - 1) AS INTEGER) AS duplicate_readings_removed,
                count(*) FILTER (WHERE is_hr_glitch) AS hr_glitches_removed,
                min(firmware) AS firmware,
                coalesce(sum(sample_interval_s) FILTER (WHERE zone = 1), 0) / 60 AS z1_min,
                coalesce(sum(sample_interval_s) FILTER (WHERE zone = 2), 0) / 60 AS z2_min,
                coalesce(sum(sample_interval_s) FILTER (WHERE zone = 3), 0) / 60 AS z3_min,
                coalesce(sum(sample_interval_s) FILTER (WHERE zone = 4), 0) / 60 AS z4_min,
                coalesce(sum(sample_interval_s) FILTER (WHERE zone = 5), 0) / 60 AS z5_min
            FROM reading
            GROUP BY session_id
        ),
        data_second AS (
            SELECT DISTINCT session_id, date_trunc('second', ts_utc) AS sec
            FROM reading
        ),
        edge AS (
            SELECT s.session_id, s.start_utc, s.end_utc, min(x.sec) AS first_sec, max(x.sec) AS last_sec
            FROM session AS s
            LEFT JOIN data_second AS x USING (session_id)
            GROUP BY s.session_id, s.start_utc, s.end_utc
        ),
        gap AS (
            -- missing seconds at the start, in the middle, at the end, or the whole run
            SELECT session_id, date_diff('second', start_utc, first_sec) AS missing_s
            FROM edge WHERE first_sec > start_utc
            UNION ALL
            SELECT session_id, date_diff('second', previous_sec, sec) - 1
            FROM (
                SELECT session_id, sec, lag(sec) OVER (PARTITION BY session_id ORDER BY sec) AS previous_sec
                FROM data_second
            )
            WHERE date_diff('second', previous_sec, sec) > 1
            UNION ALL
            SELECT session_id, date_diff('second', last_sec, end_utc) - 1
            FROM edge WHERE last_sec + INTERVAL 1 SECOND < end_utc
            UNION ALL
            SELECT session_id, date_diff('second', start_utc, end_utc)
            FROM edge WHERE first_sec IS NULL
        ),
        longest_gap AS (
            SELECT session_id, max(missing_s) AS longest_gap_s
            FROM gap
            GROUP BY session_id
        )
        SELECT
            s.session_id,
            s.athlete_sk,
            s.athlete_id,
            s.athlete_name,
            s.squad,
            s.device_id,
            s.session_date_local,
            s.week_start_date,
            s.sport,
            s.label,
            s.rpe,
            s.duration_s / 60.0 AS duration_min,
            s.distance_m,
            s.duration_s / 60.0 * s.rpe AS training_load,  -- minutes x coach effort score
            coalesce(p.z1_min, 0) AS z1_min,
            coalesce(p.z2_min, 0) AS z2_min,
            coalesce(p.z3_min, 0) AS z3_min,
            coalesce(p.z4_min, 0) AS z4_min,
            coalesce(p.z5_min, 0) AS z5_min,
            s.duration_s AS expected_samples,  -- one sample per second of the run
            coalesce(p.observed_samples, 0) AS observed_samples,
            round(100.0 * coalesce(p.observed_samples, 0) / s.duration_s, 1) AS completeness_pct,
            coalesce(p.observed_samples, 0) * 100 >= {THRESHOLD} * s.duration_s AS is_complete,
            coalesce(g.longest_gap_s, 0) AS longest_gap_s,
            p.sample_rate_hz,
            p.firmware,
            s.vendor_sync_state,
            coalesce(p.duplicate_readings_removed, 0) AS duplicate_readings_removed,
            coalesce(p.hr_glitches_removed, 0) AS hr_glitches_removed,
            s.version_count > 1 AS was_changed_by_vendor,
            s.previous_duration_s / 60.0 AS previous_duration_min,
            s.label_match_method
        FROM session AS s
        LEFT JOIN per_session AS p USING (session_id)
        LEFT JOIN longest_gap AS g USING (session_id)
        ORDER BY s.session_id""",
        "session_date_local",
        day,
    )


def build_fact_athlete_baseline(con: Con) -> None:
    """GOLD | FACT
    Reads : silver.stg_device_reading, silver.stg_session, gold.fact_session
    Writes: gold.fact_athlete_baseline (1 row per athlete per day with a run, vs the previous
            {days} days)""".replace("{days}", str(settings.BASELINE_WINDOW_DAYS))
    save_table(
        con,
        "gold.fact_athlete_baseline",
        f"""
        WITH minute AS (
            -- average valid heart rate of each minute of each run
            SELECT
                r.session_id,
                floor(date_diff('second', s.start_utc, r.ts_utc) / 60) AS minute_no,
                sum(r.hr_bpm * r.sample_interval_s) / sum(r.sample_interval_s) AS avg_hr_bpm,
                sum(r.sample_interval_s) AS seconds
            FROM silver.stg_device_reading AS r
            JOIN silver.stg_session AS s USING (session_id)
            WHERE r.hr_bpm IS NOT NULL AND NOT r.is_hr_glitch
            GROUP BY r.session_id, minute_no
        ),
        run_rest AS (
            -- resting heart rate of a run = its lowest minute (at least 30 s of data)
            SELECT session_id, min(avg_hr_bpm) FILTER (WHERE seconds >= 30) AS rest_hr_bpm
            FROM minute
            GROUP BY session_id
        ),
        run_cadence AS (
            SELECT session_id, median(cadence_spm) AS cadence_spm
            FROM silver.stg_device_reading
            WHERE session_id IS NOT NULL
            GROUP BY session_id
        ),
        day AS (
            -- one row per athlete per day (a day can have two runs)
            SELECT
                f.athlete_sk, f.athlete_id, f.session_date_local AS baseline_date,
                min(r.rest_hr_bpm) AS day_rest_hr_bpm,
                median(c.cadence_spm) AS day_cadence_spm
            FROM gold.fact_session AS f
            JOIN run_rest AS r USING (session_id)
            JOIN run_cadence AS c USING (session_id)
            GROUP BY f.athlete_sk, f.athlete_id, f.session_date_local
        ),
        rolling AS (
            -- median of the same athlete's previous {settings.BASELINE_WINDOW_DAYS} days; needs enough earlier days
            SELECT
                d.athlete_sk, d.baseline_date, d.day_rest_hr_bpm, d.day_cadence_spm,
                count(p.baseline_date) AS days_in_window,
                median(p.day_rest_hr_bpm) AS rolling_rest_hr_bpm,
                median(p.day_cadence_spm) AS typical_cadence_spm
            FROM day AS d
            LEFT JOIN day AS p
                ON p.athlete_id = d.athlete_id
                AND p.baseline_date >= d.baseline_date - {settings.BASELINE_WINDOW_DAYS}
                AND p.baseline_date < d.baseline_date
            GROUP BY d.athlete_sk, d.athlete_id, d.baseline_date, d.day_rest_hr_bpm, d.day_cadence_spm
        )
        SELECT
            athlete_sk,
            baseline_date,
            CASE WHEN days_in_window >= {settings.BASELINE_MIN_RUNS} THEN round(rolling_rest_hr_bpm, 1) END AS rolling_rest_hr_bpm,
            CASE WHEN days_in_window >= {settings.BASELINE_MIN_RUNS} THEN round(typical_cadence_spm, 1) END AS typical_cadence_spm,
            {settings.BASELINE_WINDOW_DAYS} AS window_days,
            round(day_rest_hr_bpm, 1) AS day_rest_hr_bpm,
            round(day_cadence_spm, 1) AS day_cadence_spm,
            coalesce(days_in_window >= {settings.BASELINE_MIN_RUNS}
                AND abs(day_rest_hr_bpm - rolling_rest_hr_bpm) > {settings.BASELINE_REST_HR_LIMIT_BPM}, FALSE) AS is_unusual_rest_hr,
            coalesce(days_in_window >= {settings.BASELINE_MIN_RUNS}
                AND abs(day_cadence_spm - typical_cadence_spm) > {settings.BASELINE_CADENCE_LIMIT_SPM}, FALSE) AS is_unusual_cadence
        FROM rolling
        ORDER BY athlete_sk, baseline_date""",
    )


def build_fact_reading_flag(con: Con) -> None:
    """GOLD | FACT
    Reads : gold.fact_session, gold.fact_athlete_baseline, silver.stg_device_reading, silver.ref_device_clock_offset
    Writes: gold.fact_reading_flag (1 row per flagged reading, run, watch or firmware group)"""
    save_table(
        con,
        "gold.fact_reading_flag",
        f"""
        WITH glitch AS (
            SELECT session_id, ts_utc, hr_bpm, prev_hr, next_hr
            FROM (
                SELECT
                    session_id, ts_utc, hr_bpm, is_hr_glitch,
                    lag(hr_bpm) OVER run AS prev_hr,
                    lead(hr_bpm) OVER run AS next_hr
                FROM silver.stg_device_reading
                WHERE session_id IS NOT NULL
                WINDOW run AS (PARTITION BY session_id ORDER BY ts_utc)
            )
            WHERE is_hr_glitch
        ),
        gps_step AS (
            SELECT
                session_id, firmware, lat, lon,
                lag(lat) OVER run AS prev_lat,
                lag(lon) OVER run AS prev_lon
            FROM silver.stg_device_reading
            WHERE session_id IS NOT NULL
            WINDOW run AS (PARTITION BY session_id ORDER BY ts_utc)
        ),
        gps_run AS (
            -- distance walked along the GPS points (haversine) vs the vendor's distance
            SELECT
                g.session_id, min(g.firmware) AS firmware, f.distance_m,
                sum(2 * 6371000 * asin(sqrt(
                    pow(sin(radians(g.lat - g.prev_lat) / 2), 2)
                    + cos(radians(g.prev_lat)) * cos(radians(g.lat)) * pow(sin(radians(g.lon - g.prev_lon) / 2), 2)
                ))) AS gps_m
            FROM gps_step AS g
            JOIN gold.fact_session AS f USING (session_id)
            WHERE g.prev_lat IS NOT NULL
            GROUP BY g.session_id, f.distance_m
        ),
        found AS (
            -- impossible heart rate
            SELECT
                'VAL-01' AS code, 'VAL-01 impossible_heart_rate' AS issue_type,
                g.session_id, f.athlete_sk, g.ts_utc AS reading_ts_utc, f.session_date_local AS sort_date,
                g.hr_bpm || ' bpm (readings around it: ' || g.prev_hr || ', ' || g.hr_bpm || ', ' || g.next_hr || ')' AS observed_value,
                'at most the roster max_hr, and within {settings.HR_GLITCH_JUMP_BPM} bpm of both neighbours' AS rule,
                'raw value kept; left out of zone minutes (is_hr_glitch); never replaced' AS treatment
            FROM glitch AS g
            JOIN gold.fact_session AS f USING (session_id)
            UNION ALL
            -- seconds with no reading inside a run
            SELECT
                'XS-03', 'XS-03 missing_readings', session_id, athlete_sk, NULL, session_date_local,
                (expected_samples - observed_samples) || ' s with no reading (longest gap ' || longest_gap_s || ' s)',
                'a reading in every second of the run',
                CASE WHEN was_changed_by_vendor
                    THEN 'not filled in; the vendor later made the run longer than the readings (see DUP-03)'
                    ELSE 'not filled in (a gap means we weren''t listening); run flagged in session_quality' END
            FROM gold.fact_session
            WHERE observed_samples < expected_samples
            UNION ALL
            -- vendor changed a run on a later download
            SELECT
                'DUP-03', 'DUP-03 vendor_changed_run', session_id, athlete_sk, NULL, session_date_local,
                'duration ' || previous_duration_min || ' min in the first download, ' || duration_min || ' min in the retry',
                'the same values in every download',
                'both versions kept in silver; the newest one counted'
            FROM gold.fact_session
            WHERE was_changed_by_vendor
            UNION ALL
            -- vendor says complete, but we measured otherwise
            SELECT
                'VAL-03', 'VAL-03 vendor_says_complete_but_is_not', session_id, athlete_sk, NULL, session_date_local,
                'sync_state = complete, but only ' || completeness_pct || '% of seconds have readings',
                'complete = at least {THRESHOLD}% of seconds',
                'vendor flag ignored; is_complete measured from the readings'
            FROM gold.fact_session
            WHERE vendor_sync_state = 'complete' AND NOT is_complete
            UNION ALL
            -- run uploaded twice
            SELECT
                'DUP-01', 'DUP-01 run_uploaded_twice', session_id, athlete_sk, NULL, session_date_local,
                duplicate_readings_removed || ' readings received twice (two upload files, different batch ids)',
                'each reading received once',
                'one copy kept per watch and moment, from the earliest upload'
            FROM gold.fact_session
            WHERE duplicate_readings_removed > 0
            UNION ALL
            -- watch clock on another time zone
            SELECT
                'TZ-02', 'TZ-02 watch_clock_wrong_timezone', NULL, NULL, NULL, min(f.session_date_local),
                'watch ' || o.device_id || ' (' || string_agg(DISTINCT f.athlete_name, ', ' ORDER BY f.athlete_name) || ') clock = UTC+'
                    || o.utc_offset_min // 60 || ' h in all ' || count(*) || ' runs',
                'UTC+{OFFSET // 60} h (Pakistan), like the other watches',
                'corrected with data/reference/device_clock_offset.csv; local time stays Pakistan time'
            FROM gold.fact_session AS f
            JOIN silver.ref_device_clock_offset AS o USING (device_id)
            WHERE o.utc_offset_min <> {OFFSET}
            GROUP BY o.device_id, o.utc_offset_min
            UNION ALL
            -- firmware 2.4.0 data sent through the old app
            SELECT
                'XS-04', 'XS-04 firmware_data_through_old_app', NULL, NULL, NULL, min(local_date),
                'watch ' || device_id || ': ' || count(*) || ' twice-per-second readings in '
                    || count(DISTINCT session_id) || ' runs sent through app 5.1.3 ('
                    || min(local_date) || ' to ' || max(local_date) || ')',
                'release notes: firmware 2.4.0 needs app 5.2.0',
                'reading length (0.5 s) decided from the timestamps, not from the app version'
            FROM silver.stg_device_reading
            WHERE sample_interval_s = 0.5 AND app_version = '5.1.3'
            GROUP BY device_id
            UNION ALL
            -- GPS distance vs vendor distance, per firmware
            SELECT
                CASE WHEN firmware = '2.4.0' THEN 'TIME-02' ELSE 'VAL-02' END,
                CASE WHEN firmware = '2.4.0' THEN 'TIME-02 gps_moves_twice_as_fast'
                    ELSE 'VAL-02 gps_distance_disagrees_with_vendor' END,
                NULL, NULL, NULL, NULL,
                'firmware ' || firmware || ': GPS distance '
                    || round(median(100 * (gps_m - distance_m) / distance_m), 1) || '% vs vendor (median; '
                    || round(min(100 * (gps_m - distance_m) / distance_m), 1) || '% to '
                    || round(max(100 * (gps_m - distance_m) / distance_m), 1) || '%) over ' || count(*) || ' runs',
                'within about 10% of the vendor distance',
                'vendor distance used; GPS not used for distance or pace'
            FROM gps_run
            GROUP BY firmware
            UNION ALL
            -- a day far from the athlete's own baseline
            SELECT
                'BASE-01', 'BASE-01 far_from_own_baseline', NULL, athlete_sk, NULL, baseline_date,
                'resting HR ' || day_rest_hr_bpm || ' vs baseline ' || rolling_rest_hr_bpm || ' bpm; cadence '
                    || day_cadence_spm || ' vs typical ' || typical_cadence_spm || ' spm',
                'within {settings.BASELINE_REST_HR_LIMIT_BPM} bpm and {settings.BASELINE_CADENCE_LIMIT_SPM} spm of the median of the previous {settings.BASELINE_WINDOW_DAYS} days',
                'listed for the coach; values kept'
            FROM gold.fact_athlete_baseline
            WHERE is_unusual_rest_hr OR is_unusual_cadence
        )
        SELECT
            code || '-' || lpad(CAST(row_number() OVER (
                PARTITION BY code ORDER BY sort_date NULLS LAST, session_id NULLS LAST, observed_value) AS VARCHAR), 3, '0') AS flag_id,
            session_id,
            athlete_sk,
            reading_ts_utc,
            issue_type,
            observed_value,
            rule,
            treatment
        FROM found
        ORDER BY flag_id""",
    )


def build_session_quality_view(con: Con) -> None:
    """GOLD | VIEW
    Reads : gold.fact_session
    Writes: gold.session_quality (the required 'session_quality' table: 1 row per session)"""
    run_sql(
        con,
        "gold.session_quality",
        f"""
        CREATE OR REPLACE VIEW gold.session_quality AS
        SELECT
            session_id,
            athlete_name,
            squad,
            device_id,
            session_date_local,
            duration_min,
            expected_samples AS duration_s,
            observed_samples AS seconds_with_data,
            expected_samples - observed_samples AS missing_s,
            completeness_pct,
            {THRESHOLD} AS threshold_pct,
            NOT is_complete AS is_below_threshold,
            longest_gap_s,
            sample_rate_hz,
            duplicate_readings_removed,
            hr_glitches_removed,
            vendor_sync_state,
            vendor_sync_state = 'complete' AND NOT is_complete AS vendor_says_complete_but_is_not,
            was_changed_by_vendor,
            previous_duration_min,
            label,
            rpe,
            label_match_method,
            concat_ws('; ',
                CASE WHEN NOT is_complete THEN 'below {THRESHOLD}% complete (' || (expected_samples - observed_samples) || ' s missing)' END,
                CASE WHEN vendor_sync_state = 'complete' AND NOT is_complete THEN 'vendor says complete' END,
                CASE WHEN was_changed_by_vendor THEN 'vendor changed duration ' || previous_duration_min || ' min -> ' || duration_min || ' min' END,
                CASE WHEN duplicate_readings_removed > 0 THEN duplicate_readings_removed || ' copied readings removed' END,
                CASE WHEN hr_glitches_removed > 0 THEN hr_glitches_removed || ' impossible heart rate left out' END
            ) AS quality_notes
        FROM gold.fact_session
        ORDER BY session_id""",
    )


def run_gold(con: Con, day: str | None = None) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    if day is None:
        build_dim_athlete(con)
    build_fact_session(con, day)
    build_fact_athlete_baseline(con)
    build_fact_reading_flag(con)
    build_session_quality_view(con)
