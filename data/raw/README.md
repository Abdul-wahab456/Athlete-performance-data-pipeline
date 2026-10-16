# Data Engineering Task: Stride Performance Lab

## The situation
Stride Performance Lab is a small endurance-running programme in Islamabad: two squads, six athletes, one coach. Every athlete wears a StrideBand watch. The watch streams per-second samples to the companion app, which uploads them to the vendor cloud whenever the phone has signal. The vendor portal shows sessions; the coach keeps her own notes in a separate app; the head of programme keeps the roster in a spreadsheet.

Nobody has ever joined these sources together. The sports-science lead wants weekly training load per athlete and HR-zone minutes per session, and she wants to trust them. The one thing everyone agrees on:

> *A sensor samples on a fixed clock. A gap in the samples means we weren't listening — not that nothing happened.*

Your job is to build the pipeline that turns the raw drops into marts she can trust, and to write down what "trust" means.

We will not tell you what is wrong with the data. Finding out is the assignment.

---

## What you get
`stride_candidate_data.zip`

| Path | What it is |
| :--- | :--- |
| `device_sync/drop_1..3/*.json` | One JSON file per upload batch from the companion app. Three drops. |
| `vendor_api/sessions_page_*.json` | Pages pulled from the vendor's sessions endpoint on 1 Sep. |
| `roster/roster_2026-07-01.csv`, `roster_2026-09-01.csv` | Programme roster spreadsheet exports. |
| `coach_app/session_labels.csv` | The coach's session notes, exported from her app. |
| `firmware/release_notes.md` | Vendor release notes for the watch firmware and companion app. |

*Spend your hours on the data, not the tooling.*

- Athletes' HR zones use the standard five-zone model on `max_hr` from the roster:
  - **Z1**: <60%
  - **Z2**: 60–70%
  - **Z3**: 70–80%
  - **Z4**: 80–90%
  - **Z5**: ≥90%
- Weeks are **Monday–Sunday** in athlete local time.

---

## Control figures
From the vendor portal, week of **Mon 2026-08-10 to Sun 2026-08-16**. Your marts should reconcile to these, or you should be able to say exactly why they don't.

| Athlete | Sessions |
| :--- | :--- |
| Ali Raza | 4 |
| Sara Khan | 5 |
| Hamza Tariq | 4 |
| Zainab Mirza | 4 |
| Bilal Ahmed | 4 |
| Mehak Noor | 2 |

**Total logged minutes, Endurance squad, same week:** `689`

---

## Part 1
Build a pipeline that produces marts answering these five questions for the whole season (6 Jul – 30 Aug 2026):
1. **Weekly training load per athlete**
2. **HR-zone minutes per session** — Z1–Z5 minutes for every session.
3. **Sessions whose data completeness is below the threshold**: choose the threshold yourself but back it with rationale.
4. **The squad roster snapshot on 14 August 2026** — who was in which squad, on which device, on that day.
5. **Find and list any discrepancies in the readings**

### Requirements:
- Identical marts when the pipeline is run twice on the same drops; correct marts when the drops are loaded one at a time in order. Show both.
- Marts reconcile to the control figures; any residual is explained.
- `session_quality` table — one row per session.
- A source-to-mart flow map that marks where in the flow each hazard you found lives.

---

## Part 2
- Reprocess a single day without touching the others, and prove it.
- Add an `athlete_baseline` mart (rolling resting HR, typical cadence) and use it to answer question 5.
- Write a data contract for the device-sync feed that would have prevented at least two of the problems you found, and explain what it cannot prevent.

---

## Deliverables
1. Pipeline code and any DAG/dbt models.
2. `README.md` with these sections, in this order:
   - **How to run**
   - **Grain and keys** (what one row means in every mart, and why)
   - **What I found** (each anomaly: symptom → root cause → what you did with it and why)
   - **Reconciliation** (your figures vs the control figures)
   - **Definitions** (completeness, threshold, session, week, load)
   - **Tests** (two tests you chose to write and what each protects against)
   - **AI usage** (see below)
   - **What I would do with another day**
3. `session_quality` table
4. The flow map

---

## AI policy
Use whatever tools you like. In the AI usage section, list what you used them for, and describe at least one thing an AI tool told you that was wrong or that you had to override, and how you noticed. *"It made no mistakes" will be read as "I didn't check".*

---
