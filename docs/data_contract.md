# Data contract: device-sync feed

A **data contract** is a written agreement between the team that *sends* data (here: the StrideBand companion app) and the
team that *receives* it (this pipeline). It says exactly what every upload must look like. An upload that breaks the
contract is **rejected at the door** and sent back, instead of quietly producing wrong numbers months later.

This contract covers `device_sync/drop_*/*.json`: one file per upload batch. Problem codes are explained in
[data_issues.md](data_issues.md).

## 1. The contract

**Owner (sender):** StrideBand companion app team. **Consumer:** Stride Performance Lab pipeline.
**Version:** `contract_version` = `1.0`. Any change to a field name, type or meaning needs a new version number,
announced before release.

### 1.1 One upload file

| Field | Type | Rule |
|---|---|---|
| `contract_version` | text | required; `"1.0"` |
| `batch_id` | text | required; unique forever; equals the file name |
| `device_id` | text | required; must be a known watch (listed in `data/reference/device_clock_offset.csv`) |
| `firmware`, `app_version` | text | required; must be on the list of released versions |
| `uploaded_at` | timestamp **with offset** | required; e.g. `2026-08-14T04:00:00Z` |
| `sample_interval_ms` | integer | required; `1000` or `500`; every sample's time must be a multiple of it |
| `session_id` | text | required; the vendor session the samples belong to |
| `sample_count` | integer | required; must equal the number of samples |
| `content_sha256` | text | required; SHA-256 of `samples`, so the same readings sent twice can be spotted |
| `samples` | list | required; at least 1 |

### 1.2 One sample

| Field | Type | Rule |
|---|---|---|
| `t` | timestamp **with offset** | required; e.g. `2026-08-14T06:30:00+05:00`; strictly increasing within the file |
| `heart_rate` | integer | required; 30-230 bpm |
| `cadence` | **integer** (never text) | required; 0-250 steps per minute |
| `gps.lat`, `gps.lon` | decimal | required; inside the programme's area (Islamabad box) |
| `gps.hdop` | decimal | required; 0-50 |

One set of names only (`heart_rate`, `gps.lat`), in every app version. No other fields allowed.

### 1.3 Delivery rules

- A file is never changed after it is sent. A correction is a **new** `batch_id` that names the batch it replaces.
- The same `(device_id, t)` must not appear in two accepted batches.
- Uploads arrive within 72 hours of the run.

### 1.4 What happens to a file that breaks the contract

It is not loaded. It is moved to a `rejected/` folder with the reason (e.g. `cadence is text in sample 12`) and the sender
is told. Nothing half-loaded ever reaches Bronze.

## 2. Problems it would have prevented

| Problem we found | Rule that stops it |
|---|---|
| **File layout 1: the app update renamed the fields** (STR-01). Over half the heart rates looked missing | One set of names, a version number, and "no other fields allowed". App 5.2.0 would have been rejected until it was announced as `contract_version 2.0` |
| **File layout 2: cadence stored as text** (STR-02) | `cadence` must be an integer: `"116"` is rejected |
| **File layout 3: GPS accuracy only in the new app** (STR-03) | `gps.hdop` required in every version |
| **Timezone 1 and 2: no clock label; Zainab's watch on UTC+3** (TZ-01, TZ-02) | Every time carries its offset. D014 would have sent `06:30:00+03:00`, which is correct world time, so no guessing and no reference list needed. A watch set to the wrong zone becomes visible (+03:00 in Islamabad) instead of silently shifting by 2 hours |
| **Timing 1: twice-per-second readings on 2.4.0** (TIME-01), **Mismatch 4: wrong release note** (XS-04) | `sample_interval_ms` says 500 or 1000 in the file itself, so nobody has to guess from firmware or app version |
| **Duplicate 1: one run uploaded twice** (DUP-01) | `content_sha256` and the "same `(device_id, t)` twice" rule reject the second copy |
| **Mismatch 1: readings don't say which run** (XS-01) | `session_id` required |
| **Odd value 1: 247 bpm** (VAL-01) | rejected by the 30-230 range. (The sample would be sent back; better: the app drops or flags it before sending) |

## 3. What it cannot prevent

A contract checks **shape and rules**. It cannot check whether the data is **true**, or fix things that happen outside the
feed.

| Problem | Why a contract can't stop it |
|---|---|
| **Mismatch 3: readings missing inside a run** (XS-03). Sara's 4-minute hole | The watch lost contact; the readings never existed. A contract can make the gap *visible* (e.g. require `sample_count` = duration ÷ interval and flag shortfalls) but cannot bring the readings back. README: "a gap means we weren't listening" |
| A believable but wrong value (e.g. heart rate 180 when the real value was 150) | It passes every range rule. Only comparison with neighbours or the athlete's baseline (`gold.fact_athlete_baseline`) can catch it |
| **Who-is-who 1: roster changes have no dates** (ID-01) | The roster is a spreadsheet, a different feed. It needs its own contract (`valid_from` on every change) |
| **Duplicate 3: vendor changed a run on a later download** (DUP-03), **Odd value 3: vendor always says "complete"** (VAL-03) | Those come from the vendor API, not the device feed |
| **Who-is-who 2: coach name spellings** (ID-02), **Duplicate 4: two runs, two notes** (DUP-04) | Coach app feed, not the device feed |
| **Timing 2: GPS moves twice as fast on 2.4.0** (TIME-02) | Each sample is in range; the error only shows up when distances are added up and compared with the vendor's |
| **Timing 3: late uploads** (TIME-03) | A rule can *flag* a file older than 72 hours, but the run happened when it happened; the pipeline must still accept late data and rebuild |
| A watch clock that drifts (wrong by minutes, offset correct) | The time looks valid. Only matching against the vendor's start time shows it |

## 4. How we would check it

The rules above fit in one DuckDB query per file, run in `src/ingestion/load_raw_files.py` before `load_device_sync_file` copies anything,
e.g. `SELECT count(*) FROM samples WHERE typeof(cadence) <> 'BIGINT' OR heart_rate NOT BETWEEN 30 AND 230`. A count above 0
moves the file to `rejected/` with the reason.
