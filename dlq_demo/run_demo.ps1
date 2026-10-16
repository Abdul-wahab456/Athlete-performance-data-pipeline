# Runs the pipeline on dlq_demo\raw (made-up files + broken ones), shows the dead letter queue,
# and says PASS or FAIL. Your real data, database and output are NOT touched: everything goes into dlq_demo.
# Usage (project folder, .venv switched on):   .\dlq_demo\run_demo.ps1
$demo = "dlq_demo"
$env:STRIDE_RAW_DIR = "$demo\raw"
$env:STRIDE_DB_PATH = "$demo\demo.duckdb"
$env:STRIDE_OUTPUT_DIR = "$demo\output"
$env:STRIDE_SESSION_QUALITY_COPY = "$demo\output\session_quality.csv"
Remove-Item "$demo\demo.duckdb", "$demo\demo.duckdb.wal" -ErrorAction SilentlyContinue
run
python "$demo\check_demo.py"
$exitCode = $LASTEXITCODE
Remove-Item Env:STRIDE_RAW_DIR, Env:STRIDE_DB_PATH, Env:STRIDE_OUTPUT_DIR, Env:STRIDE_SESSION_QUALITY_COPY
exit $exitCode
