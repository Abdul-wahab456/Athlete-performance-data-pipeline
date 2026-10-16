"""
SETTINGS: every path and every number the pipeline decides with
Environment variables override the paths (used by tests and Airflow).
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = Path(os.environ.get("STRIDE_RAW_DIR", ROOT / "data" / "raw"))
REFERENCE_DIR = Path(os.environ.get("STRIDE_REFERENCE_DIR", ROOT / "data" / "reference"))
DB_PATH = Path(os.environ.get("STRIDE_DB_PATH", ROOT / "data" / "warehouse" / "stride.duckdb"))
OUTPUT_DIR = Path(os.environ.get("STRIDE_OUTPUT_DIR", ROOT / "output"))
QUERIES_FILE = ROOT / "src" / "reporting" / "business_questions.sql"
SESSION_QUALITY_COPY = Path(
    os.environ.get("STRIDE_SESSION_QUALITY_COPY", ROOT / "docs" / "session_quality.csv")
)

# Athlete local time: Pakistan, UTC+5, no daylight saving
LOCAL_UTC_OFFSET_MIN = 300

# A reading is a glitch if above max_hr and this far from both neighbours
HR_GLITCH_JUMP_BPM = 40

# A run is complete when this % of its seconds have readings
COMPLETENESS_THRESHOLD_PCT = 95

SEASON_START = "2026-07-06"  # Monday
SEASON_END = "2026-08-30"  # Sunday
ROSTER_SNAPSHOT_DATE = "2026-08-14"
LAST_DROP = 3

# Athlete baseline: median of the previous 14 days of runs. A day is unusual if its resting HR is
# 5+ bpm away (common fatigue warning) or its cadence 8+ steps/min away (about 5%).
BASELINE_WINDOW_DAYS = 14
BASELINE_MIN_RUNS = 3
BASELINE_REST_HR_LIMIT_BPM = 5
BASELINE_CADENCE_LIMIT_SPM = 8

# README control figures (week Mon 10 Aug - Sun 16 Aug 2026)
CONTROL_WEEK_START = "2026-08-10"
CONTROL_WEEK_END = "2026-08-16"
CONTROL_SESSIONS = {
    "Ali Raza": 4,
    "Sara Khan": 5,
    "Hamza Tariq": 4,
    "Zainab Mirza": 4,
    "Bilal Ahmed": 4,
    "Mehak Noor": 2,
}
CONTROL_ENDURANCE_MINUTES = 689


def reference(name: str) -> str:
    """Path of a reference CSV in data/reference, as text for SQL."""
    return (REFERENCE_DIR / f"{name}.csv").as_posix()
