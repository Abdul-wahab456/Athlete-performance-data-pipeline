"""
TESTS: Python + SQL
Part 1 builds the whole pipeline on 5 tiny fake files (tests/fixtures/raw) and checks each rule with SQL.
Part 2 proves the error messages. Part 3 checks the real warehouse; it is skipped when data/raw or the
warehouse is missing (for example on GitHub).
"""

import json
import shutil
import subprocess
import sys
from unittest import mock

import duckdb
import pytest

import main
from common import errors, settings, sql_helpers
from ingestion import load_raw_files
from reporting import export_answers
from transformation import gold_star_schema, silver_clean_sources
from validation import verify_pipeline

FIXTURES = settings.ROOT / "tests" / "fixtures" / "raw"


def build(db_path, drops=None):
    """Ingest + silver + gold from the fake files into db_path."""
    with mock.patch.object(settings, "RAW_DIR", FIXTURES):
        con = duckdb.connect(str(db_path))
        load_raw_files.run_ingestion(con, drops)
        silver_clean_sources.run_silver(con)
        gold_star_schema.run_gold(con)
    return con


@pytest.fixture(scope="module")
def fx(tmp_path_factory):
    con = build(tmp_path_factory.mktemp("fx") / "fx.duckdb")
    yield con
    con.close()


@pytest.fixture
def fake_workspace(tmp_path, monkeypatch):
    """Point every path at fake data and a scratch folder, so main.py never touches the real files."""
    monkeypatch.setattr(settings, "RAW_DIR", FIXTURES)
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "warehouse.duckdb")
    monkeypatch.setattr(settings, "OUTPUT_DIR", tmp_path / "output")
    monkeypatch.setattr(settings, "SESSION_QUALITY_COPY", tmp_path / "session_quality.csv")
    return tmp_path


def run_main(capsys, *argv):
    """Run main.py like the command line does; returns (exit code, screen text, error text)."""
    code = 0
    try:
        main.main(list(argv))
    except SystemExit as stop:
        code = stop.code
    shown = capsys.readouterr()
    return code, shown.out, shown.err


# ---------- Part 1: the rules, on the fake files ----------


def test_both_app_formats_merge_into_the_same_columns(fx):
    # app 5.1.x sends hr / lat / lon, app 5.2.0 sends heart_rate / gps{lat, lon, hdop}
    rows = fx.execute("""
        SELECT
            device_id,
            count(*) AS readings,
            count(*) FILTER (WHERE hr_bpm IS NULL OR cadence_spm IS NULL OR lat IS NULL OR lon IS NULL) AS missing
        FROM silver.stg_device_reading
        GROUP BY device_id
        ORDER BY device_id""").fetchall()
    assert rows == [("D011", 3, 0), ("D014", 3, 0)]
    # cadence "160" (text in the old app) became the number 160
    assert fx.execute("""
        SELECT any_value(typeof(cadence_spm)), min(cadence_spm)
        FROM silver.stg_device_reading
        WHERE device_id = 'D011'""").fetchone() == ("INTEGER", 160)


def test_watch_clock_offset_is_corrected_per_device(fx):
    # D011 runs on UTC+5, D014 on UTC+3; local time is Pakistan time (UTC+5)
    rows = fx.execute("""
        SELECT device_id, strftime(min(ts_utc), '%H:%M'), strftime(min(ts_local), '%H:%M')
        FROM silver.stg_device_reading
        GROUP BY device_id
        ORDER BY device_id""").fetchall()
    assert rows == [("D011", "01:00", "06:00"), ("D014", "03:30", "08:30")]
    # after the correction every reading falls inside its vendor session
    assert (
        fx.execute(
            "SELECT count(*) FROM silver.stg_device_reading WHERE session_id IS NULL"
        ).fetchone()[0]
        == 0
    )


def test_duplicate_readings_are_removed(fx):
    # Bfix0002-b sends 06:30:01 twice: 7 readings in, 6 out, and the copy is counted
    received = fx.execute("SELECT count(*) FROM bronze.raw_device_sync").fetchone()[0]
    kept = fx.execute("SELECT count(*) FROM silver.stg_device_reading").fetchone()[0]
    assert (received, kept) == (7, 6)
    assert (
        fx.execute("""
        SELECT count(*) FROM (
            SELECT reading_id FROM silver.stg_device_reading GROUP BY reading_id HAVING count(*) > 1)""").fetchone()[
            0
        ]
        == 0
    )
    assert (
        fx.execute(
            "SELECT duplicate_readings_removed FROM gold.fact_session WHERE session_id = 'Sfix0002'"
        ).fetchone()[0]
        == 1
    )


def test_2hz_watch_is_not_counted_twice(fx):
    # D014 records every 0.5 s: 2 readings in one second still count as one second of data
    rows = fx.execute("""
        SELECT session_id, sample_rate_hz, observed_samples, expected_samples, completeness_pct, is_complete
        FROM gold.fact_session
        ORDER BY session_id""").fetchall()
    assert rows == [("Sfix0001", 1, 3, 3, 100.0, True), ("Sfix0002", 2, 2, 2, 100.0, True)]
    assert fx.execute(
        "SELECT DISTINCT device_id, sample_interval_s FROM silver.stg_device_reading ORDER BY 1"
    ).fetchall() == [
        ("D011", 1.0),
        ("D014", 0.5),
    ]


def test_coach_labels_match_despite_spelling(fx):
    # "Ali  Raza" (two spaces) and "Zainab M." are listed in data/reference/athlete_alias.csv
    rows = fx.execute(
        "SELECT session_id, athlete_id, label, rpe FROM silver.stg_session ORDER BY session_id"
    ).fetchall()
    assert rows == [("Sfix0001", "A01", "Easy", 4), ("Sfix0002", "A04", "Tempo", 7)]


def test_star_schema_joins_each_session_to_the_athlete_version_of_that_day(fx):
    assert (
        fx.execute("""
        SELECT count(*)
        FROM gold.fact_session AS f
        JOIN gold.dim_athlete AS d USING (athlete_sk)
        WHERE f.session_date_local >= d.valid_from AND f.session_date_local < d.valid_to
            AND f.squad = d.squad AND f.athlete_id = d.athlete_id""").fetchone()[0]
        == 2
    )
    # one version per athlete at any moment: versions of an athlete never overlap
    assert (
        fx.execute("""
        SELECT count(*)
        FROM gold.dim_athlete AS a
        JOIN gold.dim_athlete AS b ON a.athlete_id = b.athlete_id AND a.athlete_sk < b.athlete_sk
            AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to""").fetchone()[0]
        == 0
    )
    assert fx.execute("SELECT count(*) FROM gold.session_quality").fetchone()[0] == 2


def test_export_writes_one_csv_per_business_question(fx, tmp_path):
    counts = export_answers.export_answers(fx, tmp_path)
    questions = [name for name in export_answers.read_queries() if not name.startswith("print_")]
    assert sorted(p.stem for p in tmp_path.glob("*.csv")) == sorted(questions)
    assert counts["sessions_below_threshold"] == 0


def test_rerun_is_identical(tmp_path):
    # second run: every file skipped (same SHA-256), every table identical
    con = build(tmp_path / "a.duckdb")
    first = verify_pipeline.fingerprints(con)
    with mock.patch.object(settings, "RAW_DIR", FIXTURES):
        status = load_raw_files.run_ingestion(con)
    silver_clean_sources.run_silver(con)
    gold_star_schema.run_gold(con)
    assert status == {"loaded": 0, "reloaded": 0, "skipped": 5, "rejected": 0}
    assert verify_pipeline.fingerprints(con) == first
    con.close()


def test_drop_by_drop_equals_all_at_once(tmp_path):
    all_at_once = build(tmp_path / "all.duckdb")
    target = verify_pipeline.fingerprints(all_at_once)
    all_at_once.close()
    con = build(tmp_path / "step.duckdb", drops=[1])
    assert (
        con.execute("SELECT count(DISTINCT device_id) FROM silver.stg_device_reading").fetchone()[0]
        == 1
    )
    con.close()
    con = build(tmp_path / "step.duckdb", drops=[2])
    assert verify_pipeline.fingerprints(con) == target
    con.close()


def test_one_day_reprocess_touches_only_that_day(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "RAW_DIR", FIXTURES)
    con = build(tmp_path / "day.duckdb")
    values, built = verify_pipeline.fingerprints(con, by_day=True), verify_pipeline.built_times(con)
    logged = dict(con.execute("SELECT source_file, loaded_at FROM bronze.load_log").fetchall())
    main.reprocess_day(con, "2026-08-14")
    built_after = verify_pipeline.built_times(con)
    logged_after = dict(
        con.execute("SELECT source_file, loaded_at FROM bronze.load_log").fetchall()
    )
    assert [f for f in logged if logged[f] != logged_after[f]] == [
        "device_sync/drop_2/Bfix0002-b.json"
    ]
    assert verify_pipeline.fingerprints(con, by_day=True) == values  # no value changed
    for (table, day), when in built.items():
        if day == "2026-08-14":
            assert built_after[(table, day)] > when, table  # that day was rebuilt
        else:
            assert built_after[(table, day)] == when, table  # every other day untouched
    con.close()


def test_run_prints_the_progress_log_and_the_result(fake_workspace, capsys, monkeypatch):
    monkeypatch.setattr(sql_helpers, "VERBOSE", True)
    code, out, err = run_main(capsys, "run")
    assert code == 0 and err == ""
    for line in [
        "[1/4] Ingestion",
        "[2/4] Silver",
        "[3/4] Gold",
        "[4/4] Export",
        "silver.stg_session",
        "gold.fact_session",
        "Done in",
    ]:
        assert line in out
    assert "Total logged minutes, Endurance squad" in out
    assert len(list((fake_workspace / "output").glob("*.csv"))) == 6


# ---------- Dead letter queue: bad records land in bronze.dlq_rejected_record ----------


@pytest.fixture
def bad_workspace(fake_workspace, monkeypatch):
    """The fake files plus bad ones: unparseable and unknown-schema files, bad readings, bad coach notes."""
    raw = fake_workspace / "raw"
    shutil.copytree(FIXTURES, raw)
    drop = raw / "device_sync" / "drop_1"
    (drop / "Bbad-json.json").write_text("{ this is not json", encoding="utf-8")
    (drop / "Bbad-schema.json").write_text(
        json.dumps({"batch_id": "Bx", "device_id": "D011", "readings": []}), encoding="utf-8"
    )
    batch = {
        "batch_id": "Bfix0003-c",
        "device_id": "D011",
        "firmware": "2.3.1",
        "app_version": "5.1.3",
        "uploaded_at": "2026-07-06T02:00:00Z",
        "sample_count": 3,
        "samples": [
            {"t": "not-a-time", "hr": 120, "cadence": "160", "lat": 33.6, "lon": 73.0},
            {"t": "2026-07-06T06:00:05", "cadence": "160", "lat": 33.6, "lon": 73.0},
            {"t": "2026-07-07T06:00:00", "hr": 120, "cadence": "160", "lat": 33.6, "lon": 73.0},
        ],
    }
    (drop / "Bfix0003-c.json").write_text(json.dumps(batch), encoding="utf-8")
    coach = raw / "coach_app" / "session_labels.csv"
    with coach.open("a", encoding="utf-8", newline="") as file:
        file.write("Lfix003,Nobody Here,2026-07-06,Endurance,Easy,4,2026-07-06T21:00:00\n")
        file.write("Lfix004,Ali Raza,2026-07-07,Endurance,Easy,4,2026-07-07T21:00:00\n")
    monkeypatch.setattr(settings, "RAW_DIR", raw)
    return fake_workspace


def dlq_rows(db_path):
    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("""
        SELECT pipeline_step, source_file, reason, raw_payload, rejected_at
        FROM bronze.dlq_rejected_record
        ORDER BY pipeline_step, source_file, reason""").fetchall()
    con.close()
    return rows


def test_bad_records_land_in_the_dlq_and_the_pipeline_keeps_running(bad_workspace, capsys):
    code, out, err = run_main(capsys, "run")
    assert code == 0 and err == ""  # the pipeline did not crash
    rows = dlq_rows(settings.DB_PATH)
    expected = [
        ("ingestion", "device_sync/drop_1/Bbad-json.json", "cannot be parsed"),
        ("ingestion", "device_sync/drop_1/Bbad-schema.json", "unknown schema: expected batch_id"),
        ("silver", "coach_app/session_labels.csv", "unknown athlete name: Nobody Here"),
        ("silver", "coach_app/session_labels.csv", "no matching session: no run by this athlete"),
        ("silver", "device_sync/drop_1/Bfix0003-c.json", "unparseable timestamp t"),
        ("silver", "device_sync/drop_1/Bfix0003-c.json", "unknown schema: no heart rate field"),
        ("silver", "device_sync/drop_1/Bfix0003-c.json", "no matching session: reading outside"),
    ]
    for step, source_file, reason_start in expected:
        assert any(
            row[0] == step and row[1] == source_file and row[2].startswith(reason_start)
            for row in rows
        ), (step, source_file, reason_start)
    assert len(rows) == 7
    athlete_row = next(row for row in rows if row[2].startswith("unknown athlete name"))
    assert "Nobody Here" in athlete_row[3]  # the original record is kept
    assert "Rejected records (DLQ): 7 - see output/dlq_rejected_records.csv" in out.splitlines()[-1]
    # the good data is untouched
    con = duckdb.connect(str(settings.DB_PATH), read_only=True)
    assert con.execute(
        "SELECT count(*), min(completeness_pct) FROM gold.fact_session"
    ).fetchone() == (2, 100.0)
    exported = con.execute(
        f"SELECT count(*) FROM read_csv_auto('{(settings.OUTPUT_DIR / 'dlq_rejected_records.csv').as_posix()}')"
    ).fetchone()[0]
    con.close()
    assert exported == 7


def test_running_again_does_not_duplicate_the_dlq(bad_workspace, capsys):
    run_main(capsys, "run")
    first = dlq_rows(settings.DB_PATH)
    code, out, err = run_main(capsys, "run")
    assert code == 0
    assert dlq_rows(settings.DB_PATH) == first  # same rows, same first rejected_at


def test_a_removed_bad_file_leaves_the_dlq(bad_workspace, capsys):
    run_main(capsys, "run")
    assert len(dlq_rows(settings.DB_PATH)) == 7
    (settings.RAW_DIR / "device_sync" / "drop_1" / "Bbad-json.json").unlink()
    run_main(capsys, "run")
    rows = dlq_rows(settings.DB_PATH)
    assert len(rows) == 6
    assert all(row[1] != "device_sync/drop_1/Bbad-json.json" for row in rows)


def test_a_clean_run_has_an_empty_dlq(fake_workspace, capsys):
    code, out, err = run_main(capsys, "run")
    assert code == 0
    assert out.splitlines()[-1] == "Rejected records (DLQ): 0"
    assert dlq_rows(settings.DB_PATH) == []


# ---------- Part 2: clear error messages ----------


@pytest.mark.parametrize(
    "command", ["silver", "gold", "export", "verify", "test", "dashboard", "reprocess"]
)
def test_commands_stop_when_the_warehouse_is_not_built(fake_workspace, capsys, command):
    code, out, err = run_main(capsys, command)
    assert code == 1
    assert err.strip() == "ERROR: Warehouse not built yet. Run 'make run' first."
    assert "Traceback" not in err


def test_run_stops_when_there_is_no_raw_data(fake_workspace, capsys, monkeypatch):
    monkeypatch.setattr(settings, "RAW_DIR", fake_workspace / "empty")
    code, out, err = run_main(capsys, "run")
    assert code == 1
    assert "ERROR: No raw data in data/raw. See README > How to run." in err


@pytest.mark.parametrize("drop", ["0", "4", "7"])
def test_bad_drop_is_rejected(fake_workspace, capsys, drop):
    code, out, err = run_main(capsys, "run", "--drop", drop)
    assert code == 1
    assert "ERROR: DROP must be 1 to 3" in err
    assert not settings.DB_PATH.exists()


@pytest.mark.parametrize(
    ("day", "message"),
    [
        ("14-08-2026", "DAY must look like YYYY-MM-DD"),
        ("2026-13-45", "is not a real date"),
        ("2026-09-15", "is outside the season"),
    ],
)
def test_bad_day_is_rejected(fake_workspace, capsys, day, message):
    code, out, err = run_main(capsys, "reprocess", "--day", day)
    assert code == 1
    assert message in err


def test_locked_warehouse_says_what_to_close(fake_workspace, capsys):
    # another program (here: a second Python process) holds the warehouse open
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            f"import duckdb, time; con = duckdb.connect(r'{settings.DB_PATH}'); con.execute('CREATE TABLE held (i INTEGER)'); print('ready', flush=True); time.sleep(60)",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        code, out, err = run_main(capsys, "run")
        assert code == 1
        assert "ERROR: The warehouse is open in another program. Close the dashboard" in err
    finally:
        holder.kill()


def test_docker_problems_are_explained(monkeypatch):
    monkeypatch.setattr(errors.shutil, "which", lambda name: None)
    with pytest.raises(errors.PipelineError, match="Docker is not installed"):
        errors.require_docker()
    monkeypatch.setattr(errors.shutil, "which", lambda name: "docker")
    monkeypatch.setattr(errors.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 1))
    with pytest.raises(errors.PipelineError, match="Docker is not running"):
        errors.require_docker()


def test_a_failure_names_the_step_and_the_table(fake_workspace, capsys, monkeypatch):
    monkeypatch.setattr(settings, "REFERENCE_DIR", fake_workspace / "no_reference_lists")
    code, out, err = run_main(capsys, "run")
    assert code == 1
    assert err.startswith("ERROR: Silver failed. silver.ref_athlete_alias could not be built")
    assert "Traceback" not in err


# ---------- Part 3: the real warehouse (skipped without it) ----------


@pytest.fixture(scope="module")
def real():
    if not settings.RAW_DIR.exists() or not settings.DB_PATH.exists():
        pytest.skip(
            "real data not here (data/raw or the warehouse is missing): run 'make run' first"
        )
    con = duckdb.connect(str(settings.DB_PATH), read_only=True)
    yield con
    con.close()


KEYS = {
    "bronze.load_log": "source_file",
    "silver.ref_athlete_alias": "raw_name",
    "silver.ref_device_clock_offset": "device_id",
    "silver.ref_device_assignment": "athlete_id, valid_from",
    "silver.ref_session_label_pairing": "label_id",
    "silver.stg_roster": "snapshot_date, athlete_id",
    "silver.stg_session": "session_id",
    "silver.stg_device_reading": "reading_id",
    "gold.dim_athlete": "athlete_sk",
    "gold.fact_session": "session_id",
    "gold.fact_athlete_baseline": "athlete_sk, baseline_date",
    "gold.fact_reading_flag": "flag_id",
    "gold.session_quality": "session_id",
}


@pytest.mark.parametrize("table", KEYS)
def test_keys_are_unique_and_never_empty(real, table):
    key = KEYS[table]
    empty = " OR ".join(f"{column} IS NULL" for column in key.split(", "))
    rows, distinct, missing = real.execute(
        f"SELECT count(*), count(DISTINCT ({key})), count(*) FILTER (WHERE {empty}) FROM {table}"
    ).fetchone()
    assert rows > 0 and distinct == rows and missing == 0


def test_one_row_per_session(real):
    counts = real.execute("""
        SELECT
            (SELECT count(DISTINCT record_json->>'session_id') FROM bronze.raw_vendor_session),
            (SELECT count(*) FROM silver.stg_session),
            (SELECT count(*) FROM gold.fact_session),
            (SELECT count(*) FROM gold.session_quality)""").fetchone()
    assert counts == (158, 158, 158, 158)
    assert (
        real.execute("""
        SELECT count(*) FROM silver.stg_session AS s
        ANTI JOIN gold.fact_session AS f USING (session_id)""").fetchone()[0]
        == 0
    )


def test_every_reading_and_session_is_connected(real):
    assert (
        real.execute(
            "SELECT count(*) FROM silver.stg_device_reading WHERE session_id IS NULL"
        ).fetchone()[0]
        == 0
    )
    assert (
        real.execute("""
        SELECT count(*) FROM gold.fact_session
        WHERE athlete_sk IS NULL OR label IS NULL OR rpe IS NULL OR squad IS NULL""").fetchone()[0]
        == 0
    )


def test_values_are_in_range(real):
    problems = real.execute("""
        SELECT 'completeness outside 0-100' FROM gold.fact_session
            WHERE completeness_pct NOT BETWEEN 0 AND 100 OR observed_samples > expected_samples
        UNION ALL SELECT 'reading length not 0.5 or 1 s' FROM silver.stg_device_reading
            WHERE sample_interval_s NOT IN (0.5, 1.0)
        UNION ALL SELECT 'local time not UTC+5' FROM silver.stg_device_reading
            WHERE ts_local <> ts_utc + INTERVAL 300 MINUTE
        UNION ALL SELECT 'heart rate outside 30-230 (glitch left out)' FROM silver.stg_device_reading
            WHERE NOT is_hr_glitch AND hr_bpm NOT BETWEEN 30 AND 230
        UNION ALL SELECT 'RPE outside 1-10' FROM gold.fact_session WHERE rpe NOT BETWEEN 1 AND 10
        UNION ALL SELECT 'zone minutes more than the run' FROM gold.fact_session
            WHERE z1_min + z2_min + z3_min + z4_min + z5_min > duration_min + 1e-9
        UNION ALL SELECT 'two current versions of an athlete' FROM gold.dim_athlete
            WHERE is_current GROUP BY athlete_id HAVING count(*) > 1
        UNION ALL SELECT 'overlapping athlete versions' FROM gold.dim_athlete AS a
            JOIN gold.dim_athlete AS b ON a.athlete_id = b.athlete_id AND a.athlete_sk < b.athlete_sk
                AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to
        """).fetchall()
    assert problems == []


def test_readme_control_figures(real):
    queries = export_answers.read_queries()
    assert (
        dict(real.execute(queries["print_sessions_per_athlete"]).fetchall())
        == settings.CONTROL_SESSIONS
    )
    assert real.execute(queries["print_endurance_minutes"]).fetchone()[0] == 689
