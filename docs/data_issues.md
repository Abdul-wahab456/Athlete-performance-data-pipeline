# Data problems found before cleaning

This document lists **every problem we found in the raw data**, before any cleaning.
It also lists the questions the team must answer, and the checks that found nothing wrong.

The evidence comes from a check script that anyone can re-run. It reads the files and changes nothing.
The script is `src/validation/profile_raw_data.py`; the `audit` command re-runs it and writes `output/raw_data_audit_report.md` (112 checks).
Tables, columns and the flow map are in [architecture.md](architecture.md).

---

## 1. The short version

We have 4 kinds of data. Here is how much of each, and how much has a problem.

| Data | What it is | Rows | Rows with a problem |
|---|---|---|---|
| **Watch readings** | Heart rate, steps per minute and GPS, recorded while running | 585,660 | **All** need small fixes (see below); 2,340 are copies; 1 is a fake value |
| **Vendor sessions** | The vendor website's list of runs: who, start, end, distance | 206 | **48 rows are repeats** (only 158 real runs); 1 run changed between downloads; all 206 say "complete", but 2 are not |
| **Roster** | The team list (two copies: July and September) | 11 | **3 athletes changed** between July and September, with no date written down |
| **Coach notes** | What the coach wrote after each run (type of run, effort 1–10) | 158 | **All 158** use inconsistent names (29 spellings of 6 names); 1 day has 2 notes for 2 runs |

**Good news:** nothing is blank. No missing values, no impossible GPS points, and every reading belongs to a run.
Every coach note matches exactly one run.

**The control figures in the README can be reproduced exactly** (sessions per athlete 4/5/4/4/4/2 and
689 Endurance minutes), but only after the fixes below.

---

## 2. The clocks (timezones) explained simply

A timezone tells you which clock a time was written in. The same moment is "01:00" on one clock and "06:00" on another.

The data uses **three clocks**:

| Clock | What it means | Who uses it |
|---|---|---|
| **World time (UTC)** | The world's reference clock (London time without summer time). Written with a **Z** at the end, e.g. `01:00:00Z` | Vendor website (start, end, download times) and upload times. **Labelled correctly** ✅ |
| **Pakistan time (UTC+5)** | World time + 5 hours | 5 of the 6 watches (D011, D012, D013, D015, D017) |
| **UTC+3** | World time + 3 hours (2 hours behind Pakistan; the same clock as Saudi Arabia) | **Only Zainab's watch (D014)**, all season |

**Is there American time?** **No.** We tested every possible clock in the world, from 12 hours behind
world time (covering all of America) to 14 hours ahead, for **every one of the 158 runs**. Every run matched
either Pakistan time (128 runs) or UTC+3 (30 runs, all Zainab's). Nothing matched an American clock.
The "Z" times can look foreign because they are 5 hours behind Pakistan, but they are world time, not American time.

**Why does Zainab's watch say UTC+3?** We believe the watch is simply **set to the wrong timezone**.
Zainab was in Islamabad: the GPS is in Islamabad, runs start at the same moment as the teammates' runs, and with
UTC+3 the runs would start at 4 am instead of 6 am like everyone else's. (Team to confirm: question Q1.)

Every time column in the data:

| Where | Column | Rows | Written with a clock label? | Real clock |
|---|---|---|---|---|
| Watch readings | reading time `t` | 585,660 | ❌ no label | Pakistan time (471,900 readings); UTC+3 (113,760 readings, Zainab) |
| Watch upload files | `uploaded_at` | 160 | ✅ Z | World time |
| Vendor sessions | `start_time`, `end_time` | 206 | ✅ Z | World time |
| Vendor pages | `fetched_at` | 5 | ✅ Z | World time (all on 1 Sep) |
| Coach notes | `session_date` | 158 | date only | Pakistan date (matches the run's date) |
| Coach notes | `entered_at` | 158 | ❌ no label | Looks like Pakistan evening time (19:00–23:00), rounded to the hour |
| Roster | none | — | — | The only date is in the file name |

**Does the clock problem move any run to a different day or week?** No. No run crosses midnight, so the date is the same on every clock.
The clock only matters for **matching readings to runs**: get it wrong and they are hours apart.

---

## 3. All problems, by source

**How to read the tables:**
- **Problem name** is the full name. The short code in brackets, e.g. `(TZ-02)`, is the same problem's
  nickname, used inside the code comments so a recipe can say which problem it fixes. The letters mean:
  TZ = Timezone · TIME = Timing · STR = File layout (structure) · DUP = Duplicate · VAL = Odd value ·
  ID = Who-is-who (identity) · XS = Mismatch between two sources (cross-source).
- **Where it is** shows three places, top to bottom:
  📄 the **raw file → field** where the problem sits ·
  🥉 the **database table → column** after copying (Bronze; every Bronze record is stored whole in the column `record_json`) ·
  🥈 the **table → column** where it gets fixed (Silver) or reported (Gold).
- **Team decides?** "Yes" = more than one sensible fix, so the team chooses (section 4). "No" = only one sensible fix.

Tables and columns are described in [architecture.md](architecture.md).

### 3.1 Watch readings (585,660 rows)

| Problem name | What's wrong | Where it is | How many | Example | If we ignore it | Our fix | Team decides? |
|---|---|---|---|---|---|---|---|
| **Timezone 1: watch times have no clock label** (TZ-01) | Reading times don't say which clock they use | 📄 `device_sync/*.json` → `samples[].t`<br>🥉 `bronze.raw_device_sync` → `record_json.t`<br>🥈 `silver.stg_device_reading` → `ts_utc`, `ts_local` | all 585,660 | `2026-07-22T06:20:00` (no Z) | Readings land 5 hours away from their run | Convert each watch's time to world time using a reviewed list of watch clocks | No |
| **Timezone 2: Zainab's watch clock is set to UTC+3** (TZ-02) | Watch D014 runs 2 hours behind Pakistan time | 📄 `device_sync/*.json` (device `D014`) → `samples[].t`<br>🥉 `bronze.raw_device_sync` → `record_json.t`<br>🥈 `data/reference/device_clock_offset.csv` → `utc_offset_minutes` = 180 | 113,760 readings, 30 runs | Run starts 01:00 world time; the watch says 04:00 | None of Zainab's data matches Zainab's runs: zero minutes for Zainab | Treat it as a wrongly set clock: subtract 3 hours for D014 | **Yes (Q1)** |
| **Timing 1: heart rate recorded twice per second** (TIME-01) | Firmware 2.4.0 adds a reading at every half-second | 📄 `device_sync/*.json` → `samples[].t` (`.500`)<br>🥉 `bronze.raw_device_sync` → `record_json.t`<br>🥈 `silver.stg_device_reading` → `sample_interval_s` (0.5) | 191,640 readings on D011, D014 from 1 Aug | `06:30:00`, `06:30:00.500`, `06:30:01` | Counting readings as seconds turns 689 minutes into 921 | Each of these readings counts as half a second | **Yes (Q7)** |
| **Timing 2: GPS also moves every half-second, covering twice the distance** (TIME-02) | On firmware 2.4.0 the GPS position moves once per reading | 📄 `device_sync/*.json` → `samples[].gps.lat`, `gps.lon`<br>🥉 `bronze.raw_device_sync` → `record_json.gps`<br>🥈 not used for distance; vendor `distance_m` is used | 31 runs | GPS speed 4.82 m/s vs 2.41 m/s on other watches | Distance and pace from GPS are wrong | Never calculate distance from GPS | No |
| **File layout 1: the app update renamed the fields** (STR-01) | `hr` became `heart_rate`; `lat`/`lon` moved into a `gps` group | 📄 `device_sync/*.json` → `samples[].hr` / `heart_rate`, `lat` / `gps.lat`<br>🥉 `bronze.raw_device_sync` → `record_json`<br>🥈 `silver.stg_device_reading` → `heart_rate`, `lat`, `lon` | 321,480 new-style vs 264,180 old-style | old `"hr": 55` → new `"heart_rate": 55` | Over half the heart rates look missing | Read both names into one column | No |
| **File layout 2: steps-per-minute stored as text** (STR-02) | The old app writes cadence as text | 📄 `device_sync/*.json` → `samples[].cadence`<br>🥉 `bronze.raw_device_sync` → `record_json.cadence`<br>🥈 `silver.stg_device_reading` → `cadence` (whole number) | 264,180 | `"cadence": "116"` vs `116` | Maths on text fails; text sorts "100" before "99" | Convert to a number | No |
| **File layout 3: GPS accuracy only in the new app** (STR-03) | The hdop score is not sent by the old app | 📄 `device_sync/*.json` → `samples[].gps.hdop`<br>🥉 `bronze.raw_device_sync` → `record_json.gps.hdop`<br>🥈 `silver.stg_device_reading` → `hdop` | 264,180 without it | new: `"hdop": 1.3` | Old readings might look like "bad GPS" | Leave it empty for old readings, noted as "not supplied" | No |
| **Duplicate 1: one run uploaded twice** (DUP-01) | The same readings arrived in two files with different upload IDs | 📄 `device_sync/drop_2/B1c77ecc9-7.json` and `Baea8e6fa-8.json`<br>🥉 `bronze.raw_device_sync` (both copies kept)<br>🥈 `silver.stg_device_reading` → `copies_received` | 2,340 copied readings (Ali, 29 Jul) | the same 06:20:00 reading in both files | Ali's 39-minute run counts as 78 minutes | Keep one reading per watch per moment in time | No |
| **Odd value 1: impossible heart rate of 247** (VAL-01) | One reading far above the athlete's maximum | 📄 `device_sync/drop_1/B5c55d32a-6.json` → `samples[].hr`<br>🥉 `bronze.raw_device_sync` → `record_json.hr`<br>🥈 `silver.stg_device_reading` → `is_hr_glitch` | 1 reading | Ali, 13 Jul: `137, 247, 135` | One fake second at maximum effort | Mark it as faulty, leave it out of results, list it in the problems report | **Yes (Q8)** |
| **Mismatch 1: readings don't say which run they belong to** (XS-01) | No run ID in a reading | 📄 `device_sync/*.json` → `samples[]` (no `session_id`)<br>🥉 `bronze.raw_device_sync`<br>🥈 `silver.stg_device_reading` → `session_id` | all | only "watch + time" | Can't count minutes per run | Match by watch and time window; every reading fits exactly one run | No |
| **Mismatch 3: a 4-minute hole in one run** (XS-03) | The middle of one run never arrived | 📄 `device_sync/drop_2/B8547df65-0.json` + `B2707bfe3-7.json`<br>🥉 `bronze.raw_device_sync`<br>🥈 `gold.fact_session` → `longest_gap_s`, `completeness_pct` → completeness | 1 run (Sara, 5 Aug), 240 s missing | readings stop 12:21:59, restart 12:26:00 (world time) | Sara's heart-rate minutes are about 10% low and nobody knows | Report it as 90.2% complete and flag it | **Yes (Q5)** |
| **Timing 3: late data arrives in the next delivery** (TIME-03) | Uploads come up to 2.5 days after the run, so they can fall into the next drop | 📄 `device_sync/drop_3/*.json` → `uploaded_at`<br>🥉 `bronze.raw_device_sync` → `drop_no`, `uploaded_at`<br>🥈 `silver.device_batches` → `uploaded_at_utc` | 4 runs from 15 Aug only in delivery 3 | Sara's 15 Aug run arrives in drop 3 | After delivery 2, the 4 runs of 15 Aug are listed (the vendor list is complete) but have no readings yet, so their zone minutes and completeness look like 0 (corrected after the drop-by-drop proof: earlier this said the runs themselves were missing) | Rebuild the answers from all data received after every delivery; the data is final, so after delivery 3 every week is complete | No (was Q9; settled) |

### 3.2 Vendor sessions (206 rows → 158 real runs)

| Problem name | What's wrong | Where it is | How many | Example | If we ignore it | Our fix | Team decides? |
|---|---|---|---|---|---|---|---|
| **Duplicate 2: vendor pages 2 and 3 overlap** (DUP-02) | The same runs appear on two pages | 📄 `vendor_api/sessions_page_2.json`, `sessions_page_3.json` → `sessions[].session_id`<br>🥉 `bronze.raw_vendor_session` (both copies)<br>🥈 `silver.stg_session`, `silver.stg_session` | 5 repeated rows | `S22e1d77e` on both pages | 5 runs counted twice | Keep one row per run | No |
| **Duplicate 3: page 4 downloaded twice, one run changed** (DUP-03) | The second download changed one run's end time | 📄 `vendor_api/sessions_page_4.json`, `sessions_page_4_retry.json` → `sessions[].end_time`, `duration_s`<br>🥉 `bronze.raw_vendor_session`<br>🥈 `silver.stg_session` → `version_number`; `silver.stg_session` → `previous_duration_s` | 43 repeated rows; 1 changed | Bilal's run `Se06bc0e7`: 34 min → 40 min, same distance; the watch has only 34 min | 43 runs counted twice | Keep every version once; count only the newest | **Yes (Q2)** |
| **Odd value 3: vendor says "complete" for every run** (VAL-03) | Even incomplete runs are marked complete | 📄 `vendor_api/*.json` → `sessions[].sync_state`<br>🥉 `bronze.raw_vendor_session` → `record_json.sync_state`<br>🥈 `silver.stg_session` → `vendor_sync_state` (kept only to compare) | 206 rows (2 runs wrong) | Sara's 5 Aug run says complete but is 90.2% | Broken runs look trustworthy | Ignore it; measure completeness ourselves (`gold.fact_session`) | No |
| **File layout 4: a page holds more runs than its page size** (STR-04) | Page 4 says 40 per page but holds 43 | 📄 `vendor_api/sessions_page_4*.json` → `page_size`, `sessions[]`<br>🥉 `bronze.raw_vendor_session` (every entry kept)<br>🥈 `silver.stg_session` (all 43 read) | 2 page files | `page_size: 40`, 43 runs | A careless loader would drop 3 runs | Always read every run on a page | No |
| **Odd value 2: GPS distance disagrees with vendor distance** (VAL-02) | Distance added up from GPS points doesn't match the vendor's | 📄 `vendor_api/*.json` → `sessions[].distance_m` vs `device_sync/*.json` → `samples[].lat`, `lon`<br>🥉 `bronze.raw_vendor_session`, `bronze.raw_device_sync`<br>🥈 `silver.stg_session` → `distance_m` (vendor's, used) | all 158 runs | older watches 16% shorter, 2.4.0 watches 66% longer | Two different answers for distance | Use the vendor's distance only | **Yes (Q6)** |

### 3.3 Roster (11 rows: 5 in July, 6 in September)

| Problem name | What's wrong | Where it is | How many | Example | If we ignore it | Our fix | Team decides? |
|---|---|---|---|---|---|---|---|
| **Who-is-who 1: roster changes have no dates** (ID-01) | People changed watch or team, but no date was written | 📄 `roster/roster_2026-07-01.csv`, `roster_2026-09-01.csv` → `squad`, `device_id` (no date columns)<br>🥉 `bronze.raw_roster` → `record_json`<br>🥈 `data/reference/device_assignment.csv`, `device_assignment.csv` → `valid_from`, `valid_to`; `gold.dim_athlete`; `silver.stg_session` → `athlete_id`, `squad` | 3 athletes | Hamza D013 → D017; Mehak joins on D013; Bilal Endurance → Speed | Mehak's runs counted as Hamza's; 689 minutes becomes 644 or 824 | Write the change dates ourselves, from evidence in the data | **Yes (Q3)** |

What the evidence says: Hamza last used D013 on 10 Aug and first used D017 on 12 Aug. D013 was unused 11–13 Aug, then Mehak started
on 14 Aug. The coach wrote "Endurance" for Bilal until 10 Aug and "Speed" from 12 Aug. So all three changes happened around **11 Aug**.

### 3.4 Coach notes (158 rows)

| Problem name | What's wrong | Where it is | How many | Example | If we ignore it | Our fix | Team decides? |
|---|---|---|---|---|---|---|---|
| **Who-is-who 2: names typed 29 different ways** (ID-02) | The coach spells the same person differently | 📄 `coach_app/session_labels.csv` → `athlete_name`<br>🥉 `bronze.raw_coach_label` → `record_json.athlete_name`<br>🥈 `data/reference/athlete_alias.csv` → `athlete_id`; `silver.stg_session` → `athlete_id` | 29 spellings of 6 names, all 158 rows | `Ali Raza`, `ALI RAZA`, `ali raza`, `Ali  Raza`, `Ali R.` | Notes split across 29 "people" | A reviewed lookup list: spelling → athlete | No (just approve the list) |
| **Mismatch 2: coach notes don't say which run** (XS-02) | No run ID in a note | 📄 `coach_app/session_labels.csv` (no `session_id`)<br>🥉 `bronze.raw_coach_label`<br>🥈 `silver.stg_session` → `session_id` | all 158 | only name + date | Can't attach effort to a run | Match by athlete + date: 158 notes ↔ 158 runs | No |
| **Duplicate 4: two runs and two notes on one day** (DUP-04) | Sara ran twice on 12 Aug, so each note fits both runs | 📄 `coach_app/session_labels.csv` → `athlete_name`, `session_date`<br>🥉 `bronze.raw_coach_label`<br>🥈 `data/reference/session_label_pairing.csv`; `silver.stg_session` → `match_method` | 1 day | "Easy, effort 3" and "Tempo, effort 8"; calm morning run (avg HR 129), hard evening run (avg HR 150) | Each note joins both runs, so that day counts double | Calm run ↔ "Easy", hard run ↔ "Tempo", marked as a guess | **Yes (Q4)** |
| **Timezone 3: coach entry time has no clock label** (TZ-03) | The time the coach typed the note has no clock label and is rounded to the hour | 📄 `coach_app/session_labels.csv` → `entered_at`<br>🥉 `bronze.raw_coach_label` → `record_json.entered_at`<br>🥈 `silver.stg_session` → `entered_at_local` (information only) | 158 | `2026-07-06T21:00:00` | Nothing (we don't use it) | Keep it for information only | No |

### 3.5 Firmware notes and README (claims checked against the data)

| Problem name | Claim | Where it is | What the data shows |
|---|---|---|---|
| **Mismatch 4: release notes wrong about app 5.2.0** (XS-04) | "Firmware 2.4.0 needs app 5.2.0" | 📄 `firmware/release_notes.md` vs `device_sync/*.json` → `firmware`, `app_version`<br>🥈 `silver.stg_device_reading` → `sample_interval_s` (decided from the timestamps) | ❌ Not true: 2 watches sent 2.4.0 data through the **old** app on 1–3 Aug (4 files). So we use the timestamps, not the app version, to spot twice-per-second data |
| **Timing 2** (TIME-02) | "Cadence and GPS unchanged on 2.4.0" | 📄 `firmware/release_notes.md` vs `samples[].cadence`, `gps` | ❌ Not true: both change every half-second, and GPS covers twice the ground |
| **Mismatch 5: README wrong about per-second samples** (XS-05) | "The watch streams per-second samples" | 📄 `README.md` vs `samples[].t` | ❌ Not true for 2.4.0 heart rate (twice per second) |
| — | "2.4.0 rolled out from 1 Aug, staged" | 📄 `firmware/release_notes.md` vs `samples[].t` + `firmware` | ✅ True: D011 and D014 from 1 Aug; the other 4 watches never updated |
| — | "App 5.2.0 released 5 Aug" | 📄 `firmware/release_notes.md` vs `app_version` | ✅ True: first new-style reading on 5 Aug |

### 3.6 Not a problem, but worth knowing

| Name | What it is | Where it is | What we do |
|---|---|---|---|
| **Odd value 4: readings below the roster resting heart rate** (VAL-04) | 616 readings are up to 5 bpm below the roster `rest_hr`, at the start of runs | 📄 `samples[].hr` vs `roster/*.csv` → `rest_hr`<br>🥈 `gold.fact_athlete_baseline` | Normal for a warm-up; Part 2 measures resting heart rate from the data and compares |

---

## 4. Questions the team must answer

Each question has our recommendation. If you agree with all of them, just say so.

> **Status:** the team has not answered yet, so the pipeline uses the recommended answers. Each one lives in one place and is easy to change: a row in `data/reference/*.csv` (Q1, Q3, Q4) or a setting in `src/common/settings.py` (Q5, Q8 rule, baseline limits).

| # | Question | Options | Our recommendation and why |
|---|---|---|---|
| **Q1** | Zainab's watch says UTC+3. Wrong setting, or was Zainab abroad? | (a) wrong setting · (b) travelled · (c) ignore it | **(a)**: GPS shows Islamabad; runs start with the teammates; 4 am starts are unlikely. Option (c) gives Zainab zero data. The answer doesn't change any dates. |
| **Q2** | Bilal's run on 21 Aug: 34 or 40 minutes? | (a) 40 min, the newest download · (b) 34 min, the first download · (c) 34 min, from the watch | **(a)**, marked 85% complete and flagged: one simple rule ("newest download wins") and the README says a gap means *we weren't listening*, not that nothing happened. (Earlier I leaned towards 34; I changed my mind for these reasons.) Not in the control week. |
| **Q3** | On what date did the 3 roster changes happen? | any date 11–12 Aug gives the same result | **11 Aug** for all three. Please confirm with the head of programme. |
| **Q4** | Sara's 2 notes on 12 Aug: how do we match them? | (a) by effort (calm ↔ Easy) · (b) leave them unmatched · (c) ask the coach | **(a)**, marked as a guess; easy to change if the coach answers. |
| **Q5** | How complete must a run be to count as "good"? | 99% · 95% · 90% | **95%**: flags both problem runs (90.2% and 85%) and still allows a short Bluetooth drop (2 minutes in a 40-minute run). Easy to change later. |
| **Q6** | Which distance do we trust? | vendor · GPS | **Vendor.** GPS is 16% short on older watches and 66% long on the newer ones. |
| **Q7** | Heart rate twice per second: how do we count it? | (a) each reading = half a second · (b) average the two · (c) keep one, drop the other | **(a)**: uses all the data and gives exactly the right total time. |
| **Q8** | The 247 heart rate: what do we do with it? | (a) leave it out · (b) replace with neighbours' average · (c) keep it | **(a)**: don't invent data and don't keep impossible data. It's 1 second either way. |
| ~~Q9~~ | ~~Weeks where late data may still arrive~~ | — | **Settled, no answer needed:** the data is final (exactly 3 deliveries, no more coming), so every week is complete after delivery 3. No "may still change" flag is needed. |
| **Q10** | What does "training load" mean? (The README doesn't say.) | (a) minutes × coach effort score · (b) heart-rate-zone points · (c) minutes only | **(a) as the main number, with (b) next to it**: (a) is the standard coaching measure and has no gaps; (b) lets the sports scientist compare. |

---

## 5. Checks that found nothing wrong

These show the check was complete. Each was checked on **all** rows.

**Watch files (160 files, 585,660 readings)**
- Every file has the same layout; no file mixes old and new styles.
- No blank or empty values anywhere.
- The "number of readings" written in each file is correct in all 160 files.
- File names match their IDs, and no ID is used twice.
- Text step-counts are all clean whole numbers.
- No readings out of order and no gaps inside any file.
- No duplicate readings inside one file, and none across deliveries. The only copies are the double upload (Duplicate 1), and those copies are identical.
- Heart rate: no zeros, nothing below 30. Only one value is above 220 (Odd value 1).
- Steps per minute: 59–196, no zeros.
- GPS: no missing points, all inside Islamabad, no sudden jumps.
- No watch changes its clock during the season.

**Vendor sessions (206 rows)**
- Same layout on every page; no blanks.
- End is always after start; the duration always equals end minus start; distance is always above zero.
- Pace is realistic for every run (4.7–6.8 minutes per km).
- After removing repeats: 158 runs, each with a unique ID.
- All upload and download times are correctly labelled as world time. If upload times were Pakistan time, 114 files would have been uploaded *before* they were recorded, which proves they are world time.

**Roster (11 rows)**
- Same columns in both files; heart-rate limits are whole numbers and realistic (max 186–195, resting 46–58).
- Max heart rate, resting heart rate and names never change between the files.
- No athlete or watch appears twice in one file.

**Coach notes (158 rows)**
- No blanks; every date and effort score is valid (effort 3–9 on a 1–10 scale).
- Effort fits the run type (Easy ≈ 4, Intervals ≈ 8).
- Every note matches exactly one run and every run has a note.
- The team written by the coach matches the dated team list on every note (once the 11 Aug change is applied).

**README**
- 6 athletes, 2 teams; every run falls inside the season (6 Jul – 30 Aug).
- Control figures reproduce exactly (see section 1).
