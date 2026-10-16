-- The README's business questions: Q1-Q5, the two control figures (print_*), and the dead letter queue.
-- Each query starts with "-- name: <file name>".
-- export_answers.py writes output/<name>.csv; queries named print_* are only shown on screen.
-- Minutes are rounded half-up through DECIMAL, so 5.925 becomes 5.93.

-- name: weekly_training_load
-- Q1: training load per athlete per week (minutes x coach effort), Monday-Sunday weeks in local time
WITH week AS (
    SELECT
        CAST(w AS DATE) AS week_start_date,
        CAST(w AS DATE) + 6 AS week_end_date
    FROM unnest(generate_series(DATE '2026-07-06', DATE '2026-08-30', INTERVAL 7 DAY)) AS t(w)
),
on_team AS (
    -- every week an athlete was on the team; quiet weeks stay, with 0 sessions
    SELECT
        d.athlete_id,
        d.athlete_name,
        w.week_start_date,
        w.week_end_date,
        string_agg(DISTINCT d.squad, ' + ' ORDER BY d.squad) AS squads_in_week
    FROM gold.dim_athlete AS d
    JOIN week AS w ON d.valid_from <= w.week_end_date AND d.valid_to > w.week_start_date
    GROUP BY d.athlete_id, d.athlete_name, w.week_start_date, w.week_end_date
),
weekly AS (
    SELECT
        athlete_id,
        week_start_date,
        count(*) AS sessions,
        sum(duration_min) AS logged_minutes,
        sum(training_load) AS session_rpe_load,
        sum(z1_min * 1 + z2_min * 2 + z3_min * 3 + z4_min * 4 + z5_min * 5) AS hr_zone_points,
        100.0 * sum(observed_samples) / sum(expected_samples) AS data_completeness_pct
    FROM gold.fact_session
    GROUP BY athlete_id, week_start_date
)
SELECT
    o.athlete_name,
    o.week_start_date,
    o.week_end_date,
    o.squads_in_week,
    coalesce(w.sessions, 0) AS sessions,
    ROUND(CAST(coalesce(w.logged_minutes, 0) AS DECIMAL(18, 6)), 2) AS logged_minutes,
    ROUND(CAST(coalesce(w.session_rpe_load, 0) AS DECIMAL(18, 6)), 1) AS session_rpe_load,
    ROUND(CAST(coalesce(w.hr_zone_points, 0) AS DECIMAL(18, 6)), 1) AS hr_zone_points,
    ROUND(CAST(w.data_completeness_pct AS DECIMAL(18, 6)), 1) AS data_completeness_pct
FROM on_team AS o
LEFT JOIN weekly AS w USING (athlete_id, week_start_date)
ORDER BY o.athlete_name, o.week_start_date;

-- name: session_hr_zones
-- Q2: minutes in each heart-rate zone, for every session
SELECT
    session_id,
    athlete_name,
    session_date_local,
    ROUND(CAST(duration_min AS DECIMAL(18, 6)), 2) AS logged_minutes,
    ROUND(CAST(z1_min AS DECIMAL(18, 6)), 2) AS z1_min,
    ROUND(CAST(z2_min AS DECIMAL(18, 6)), 2) AS z2_min,
    ROUND(CAST(z3_min AS DECIMAL(18, 6)), 2) AS z3_min,
    ROUND(CAST(z4_min AS DECIMAL(18, 6)), 2) AS z4_min,
    ROUND(CAST(z5_min AS DECIMAL(18, 6)), 2) AS z5_min,
    completeness_pct
FROM gold.fact_session
ORDER BY session_date_local, athlete_name, session_id;

-- name: sessions_below_threshold
-- Q3: sessions whose data completeness is below 95%
SELECT
    session_id,
    athlete_name,
    session_date_local,
    duration_s,
    seconds_with_data,
    completeness_pct,
    longest_gap_s,
    quality_notes
FROM gold.session_quality
WHERE is_below_threshold
ORDER BY completeness_pct, session_id;

-- name: roster_snapshot_2026_08_14
-- Q4: who was in which squad, on which watch, on 14 August 2026
SELECT
    DATE '2026-08-14' AS snapshot_date,
    athlete_id,
    athlete_name,
    squad,
    device_id
FROM gold.dim_athlete
WHERE DATE '2026-08-14' >= valid_from
    AND DATE '2026-08-14' < valid_to
ORDER BY athlete_id;

-- name: reading_discrepancies
-- Q5: every discrepancy found in the readings
SELECT
    f.flag_id,
    f.issue_type,
    a.athlete_name,
    f.session_id,
    f.reading_ts_utc,
    f.observed_value,
    f.rule,
    f.treatment
FROM gold.fact_reading_flag AS f
LEFT JOIN gold.dim_athlete AS a USING (athlete_sk)
ORDER BY f.flag_id;

-- name: dlq_rejected_records
-- Dead letter queue: records the pipeline could not use (cannot be parsed, unknown schema,
-- unknown athlete name, no matching session). Nothing crashes and nothing is silently dropped.
SELECT
    rejected_at,
    pipeline_step,
    source_file,
    reason,
    raw_payload
FROM bronze.dlq_rejected_record
ORDER BY pipeline_step, source_file, reason, raw_payload;

-- name: print_sessions_per_athlete
-- Shown on screen at the end of the run: sessions per athlete, week 10-16 Aug
SELECT
    athlete_name AS athlete,
    count(*) AS sessions
FROM gold.fact_session
WHERE session_date_local BETWEEN DATE '2026-08-10' AND DATE '2026-08-16'
GROUP BY athlete_id, athlete_name
ORDER BY athlete_id;

-- name: print_endurance_minutes
-- Shown on screen at the end of the run: total logged minutes, Endurance squad, same week
SELECT CAST(coalesce(round(sum(duration_min)), 0) AS INTEGER) AS total_logged_minutes
FROM gold.fact_session
WHERE squad = 'Endurance'
    AND session_date_local BETWEEN DATE '2026-08-10' AND DATE '2026-08-16';
