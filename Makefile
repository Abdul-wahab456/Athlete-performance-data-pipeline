ifeq ($(OS),Windows_NT)
PY := .venv/Scripts/python.exe
else
PY := .venv/bin/python
endif

DAY ?= 2026-08-14
DROP ?=

.PHONY: setup run ingest silver gold export reprocess audit verify test format dashboard airflow-up airflow-run airflow-down clean

setup:
	python -m venv .venv
	$(PY) -m pip install -r requirements.txt

run:
	$(PY) src/main.py run $(if $(DROP),--drop $(DROP))

reprocess:
	$(PY) src/main.py reprocess --day $(DAY)

ingest silver gold export audit verify test format dashboard airflow-up airflow-run airflow-down clean:
	$(PY) src/main.py $@
