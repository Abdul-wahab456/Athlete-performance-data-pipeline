"""
DASHBOARD: one chart or table per business question
Input : the Gold tables, read only (same queries as the CSV files)
Run   : make dashboard   (opens http://localhost:8501)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import duckdb  # noqa: E402
import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from common import settings  # noqa: E402
from reporting import export_answers  # noqa: E402

st.set_page_config(page_title="Stride Performance Lab", layout="wide")


@st.cache_data(ttl=60)
def load_answers() -> dict:
    """Run every business query; the connection is closed straight away so the pipeline can still write."""
    con = duckdb.connect(str(settings.DB_PATH), read_only=True)
    try:
        return {
            name: con.execute(sql).fetchdf() for name, sql in export_answers.read_queries().items()
        }
    finally:
        con.close()


if not settings.DB_PATH.exists():
    st.error("Warehouse not built yet. Run 'make run' first.")
    st.stop()

answers = load_answers()
st.title("Stride Performance Lab")
st.caption("Summer 2026 season. Local time = Pakistan (UTC+5). Weeks run Monday to Sunday.")

athletes = sorted(answers["weekly_training_load"]["athlete_name"].unique())
picked = st.sidebar.multiselect("Athletes", athletes, default=athletes)

# README control figures
st.header("Control figures: week 10-16 Aug 2026")
per_athlete = answers["print_sessions_per_athlete"]
columns = st.columns(len(per_athlete) + 1)
for column, row in zip(columns, per_athlete.itertuples(), strict=False):
    column.metric(row.athlete, f"{row.sessions} sessions")
columns[-1].metric("Endurance squad", f"{answers['print_endurance_minutes'].iloc[0, 0]} min")

# Q1
st.header("1. Weekly training load")
st.caption("Load = logged minutes x coach effort score (RPE), per athlete per week.")
weekly = answers["weekly_training_load"].query("athlete_name in @picked")
st.plotly_chart(
    px.line(
        weekly,
        x="week_start_date",
        y="session_rpe_load",
        color="athlete_name",
        markers=True,
        labels={
            "week_start_date": "Week starting",
            "session_rpe_load": "Load (min x RPE)",
            "athlete_name": "Athlete",
        },
    ),
    width="stretch",
)

# Q2
st.header("2. Minutes in each heart-rate zone")
zones = answers["session_hr_zones"].query("athlete_name in @picked")
zone_columns = [f"z{i}_min" for i in range(1, 6)]
by_athlete = zones.groupby("athlete_name")[zone_columns].sum().reset_index()
st.plotly_chart(
    px.bar(
        by_athlete,
        x="athlete_name",
        y=zone_columns,
        labels={"value": "Minutes (season)", "athlete_name": "Athlete", "variable": "Zone"},
    ),
    width="stretch",
)
with st.expander("Per session"):
    st.dataframe(zones, hide_index=True)

# Q3
st.header(f"3. Sessions below {settings.COMPLETENESS_THRESHOLD_PCT}% data completeness")
below = answers["sessions_below_threshold"].query("athlete_name in @picked")
st.plotly_chart(
    px.bar(
        below,
        x="session_id",
        y="completeness_pct",
        color="athlete_name",
        labels={"completeness_pct": "Completeness (%)", "session_id": "Session"},
    ),
    width="stretch",
)
st.dataframe(answers["sessions_below_threshold"], hide_index=True)

# Q4
st.header("4. Roster on 14 August 2026")
st.dataframe(answers["roster_snapshot_2026_08_14"], hide_index=True)

# Q5
st.header("5. Discrepancies in the readings")
flags = answers["reading_discrepancies"]
by_type = flags.groupby("issue_type").size().reset_index(name="cases")
st.plotly_chart(
    px.bar(
        by_type,
        x="cases",
        y="issue_type",
        orientation="h",
        labels={"cases": "Cases", "issue_type": ""},
    ),
    width="stretch",
)
st.dataframe(flags, hide_index=True)
