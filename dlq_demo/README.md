# DLQ demo: what happens when the data is broken?

## Why this folder exists

Real data is never perfect. A file can be damaged, a watch can send a reading in a format nobody expected, or a coach can
type the name of an athlete who is not on the team. A weak pipeline would **crash**, or **silently throw the record away**.

This pipeline does neither. Every record it cannot use goes into a **dead letter queue (DLQ)**: the table
`bronze.dlq_rejected_record`. Each row says **when** it was rejected, **at which step**, **which file**, **why**, and keeps a
copy of the **original record**. The pipeline then carries on with the good data.

This demo lets you **see that working**, without touching your real data. It answers two questions:

1. Does the pipeline keep running when the data is broken?
2. Does every broken record land in the DLQ, with the right reason?

## How to run it

In a terminal, in the project folder (the one with `README.md`), with the Python environment switched on:

```powershell
.venv\Scripts\Activate.ps1
.\dlq_demo\run_demo.ps1
```

If PowerShell says scripts are disabled, type this once and try again:
`Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`

## How to know it worked

You should see the progress log, then `Rejected records (DLQ): 7`, then the list below, and finally:

```
PASS: all 7 broken records were caught, and the pipeline kept running on the good data.
```

If anything is wrong you see `FAIL` and a line saying what is missing.

## What is broken on purpose, and what should happen

| Broken thing (where) | Step that catches it | Reason you should see |
|---|---|---|
| `raw/device_sync/drop_1/Bbad-json.json`: not valid JSON at all | ingestion | `cannot be parsed` |
| `raw/device_sync/drop_1/Bbad-schema.json`: valid JSON, but no `samples` list | ingestion | `unknown schema: expected batch_id, device_id and a samples list` |
| `raw/device_sync/drop_1/Bfix0003-c.json`, reading 1: the time is `not-a-time` | silver | `unparseable timestamp t` |
| `Bfix0003-c.json`, reading 2: no heart-rate field | silver | `unknown schema: no heart rate field (hr or heart_rate)` |
| `Bfix0003-c.json`, reading 3: on 7 July, when this watch had no run | silver | `no matching session: reading outside every run of watch D011` |
| `raw/coach_app/session_labels.csv`, note `Lfix003`: the name "Nobody Here" | silver | `unknown athlete name: Nobody Here` |
| `session_labels.csv`, note `Lfix004`: Ali Raza on 7 July, a day with no run | silver | `no matching session: no run by this athlete on that date` |

The other files in `raw/` (`Bfix0001-a.json`, `Bfix0002-b.json`, the vendor page and the roster) are good. They give the
pipeline 2 runs to work with, which is how you can see it keeps working.

## Where to find the results

Everything the demo makes stays inside this folder, so your real results are never overwritten. The output files are not
uploaded to GitHub; they appear when you run the demo.

| What | Where |
|---|---|
| The dead letter queue as a spreadsheet | `dlq_demo\output\dlq_rejected_records.csv` |
| The other answer files (weekly load, zones, ...) | `dlq_demo\output\*.csv` |
| The demo database (open it with the DuckDB UI or the dashboard code) | `dlq_demo\demo.duckdb` |
| The made-up input files, with the broken ones | `dlq_demo\raw\` |

## Where is the code that does this?

| Job | File |
|---|---|
| Rejects broken or unknown-layout **files** (step 1) | `src/ingestion/load_raw_files.py` → `file_problem`, `reject_file` |
| Rejects broken **records**: bad time, no heart rate, unknown athlete, no matching session (step 2) | `src/transformation/silver_clean_sources.py` → `build_dlq_rejected_record` |
| Writes the queue; never duplicates a row on re-run | `src/common/sql_helpers.py` → `replace_rejects` |
| Saves the queue as a CSV | `src/reporting/business_questions.sql` → `dlq_rejected_records` |
| Prints the count on the last line of every run | `src/reporting/export_answers.py` → `print_rejected_count` |
| Checks the result here and says PASS or FAIL | `dlq_demo/check_demo.py` |
| The same checks as automatic tests | `tests/test_pipeline.py` → section "Dead letter queue" |

## Try your own broken file

Put any file into `dlq_demo\raw\device_sync\drop_1\` (for example a `.json` file that only contains the word `hello`), or
edit `dlq_demo\raw\coach_app\session_labels.csv`, and run the demo again. It will list your file in the queue. The check will
then say `FAIL`, because it expects exactly the 7 records above. That is normal: the queue worked, and the check is only
written for the 7 records in the table.

On your real data the queue is empty: the last line of `run` says `Rejected records (DLQ): 0`.
