"""
DLQ DEMO CHECK: did every broken record land in the dead letter queue, and did the pipeline keep going?
Input : dlq_demo/demo.duckdb (built by run_demo.ps1)
Output: the rejected records on screen and PASS or FAIL (exit code 0 or 1)
"""

import sys
from pathlib import Path

import duckdb

DEMO = Path(__file__).resolve().parent

# the 7 records we expect: (pipeline step, source file, how the reason starts)
EXPECTED = [
    ("ingestion", "device_sync/drop_1/Bbad-json.json", "cannot be parsed"),
    ("ingestion", "device_sync/drop_1/Bbad-schema.json", "unknown schema: expected batch_id"),
    ("silver", "device_sync/drop_1/Bfix0003-c.json", "unparseable timestamp t"),
    ("silver", "device_sync/drop_1/Bfix0003-c.json", "unknown schema: no heart rate field"),
    ("silver", "device_sync/drop_1/Bfix0003-c.json", "no matching session: reading outside"),
    ("silver", "coach_app/session_labels.csv", "unknown athlete name: Nobody Here"),
    ("silver", "coach_app/session_labels.csv", "no matching session: no run by this athlete"),
]

con = duckdb.connect(str(DEMO / "demo.duckdb"), read_only=True)
rows = con.execute(
    "SELECT pipeline_step, source_file, reason FROM bronze.dlq_rejected_record ORDER BY 1, 2, 3"
).fetchall()
sessions = con.execute("SELECT count(*) FROM gold.fact_session").fetchone()[0]
con.close()

print("\nThe dead letter queue (dlq_demo/output/dlq_rejected_records.csv):")
for step, source_file, reason in rows:
    print(f"  {step:<9} {source_file:<40} {reason[:70]}")

missing = [
    item
    for item in EXPECTED
    if not any(r[0] == item[0] and r[1] == item[1] and r[2].startswith(item[2]) for r in rows)
]
problems = []
if len(rows) != len(EXPECTED):
    problems.append(f"expected {len(EXPECTED)} rejected records, found {len(rows)}")
problems += [f"missing: {step} / {source_file} / {reason}" for step, source_file, reason in missing]
if sessions != 2:
    problems.append(f"the good data should give 2 sessions, found {sessions}")

if problems:
    print("\nFAIL")
    for problem in problems:
        print(f"  - {problem}")
    sys.exit(1)
print(
    f"\nPASS: all {len(EXPECTED)} broken records were caught, and the pipeline kept running on the good data."
)
