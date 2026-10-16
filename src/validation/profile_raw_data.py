"""
RAW DATA PROFILE: audit of the files in data/raw
Input : data/raw (read only, never changed)
Output: a summary on screen and output/raw_data_audit_report.md (112 checks: ISSUE, OK or INFO)
Six sections: structure, time, duplicates, values, identity, cross-source. Findings are explained in docs/data_issues.md.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]

PAKISTAN_OFFSET_MIN = 5 * 60

DEVICE_OWNERS = [
    ("D011", "A01", "2026-07-01", "2099-01-01"),
    ("D012", "A02", "2026-07-01", "2099-01-01"),
    ("D013", "A03", "2026-07-01", "2026-08-11"),
    ("D013", "A06", "2026-08-11", "2099-01-01"),
    ("D014", "A04", "2026-07-01", "2099-01-01"),
    ("D015", "A05", "2026-07-01", "2099-01-01"),
    ("D017", "A03", "2026-08-11", "2099-01-01"),
]
SQUAD_MEMBERSHIP = [
    ("A01", "Endurance", "2026-07-01", "2099-01-01"),
    ("A02", "Endurance", "2026-07-01", "2099-01-01"),
    ("A03", "Speed", "2026-07-01", "2099-01-01"),
    ("A04", "Speed", "2026-07-01", "2099-01-01"),
    ("A05", "Endurance", "2026-07-01", "2026-08-11"),
    ("A05", "Speed", "2026-08-11", "2099-01-01"),
    ("A06", "Endurance", "2026-07-01", "2099-01-01"),
]

CONTROL_WEEK = ("2026-08-10", "2026-08-16")
CONTROL_SESSIONS = {
    "Ali Raza": 4,
    "Sara Khan": 5,
    "Hamza Tariq": 4,
    "Zainab Mirza": 4,
    "Bilal Ahmed": 4,
    "Mehak Noor": 2,
}
CONTROL_ENDURANCE_MINUTES = 689
SEASON = ("2026-07-06", "2026-08-30")


@dataclass
class Check:
    id: str
    section: str
    source: str
    title: str
    status: str
    detail: str
    example: str = ""


class Audit:
    SECTIONS = {
        "1": "Structure",
        "2": "Time and timezones",
        "3": "Duplicates and keys",
        "4": "Values",
        "5": "Identity and roster",
        "6": "Cross-source matching",
    }

    def __init__(self, con):
        self.con = con
        self.checks: list[Check] = []
        self._n = Counter()

    def add(self, section, source, title, status, detail, example=""):
        self._n[section] += 1
        cid = f"{section}.{self._n[section]:02d}"
        self.checks.append(Check(cid, section, source, title, status, str(detail), str(example)))
        print(f"  [{status:5}] {cid} {source:11} {title}: {_short(detail, 110)}")

    def rows(self, sql):
        r = self.con.execute(sql)
        cols = [d[0] for d in r.description]
        return [dict(zip(cols, row, strict=True)) for row in r.fetchall()]

    def val(self, sql):
        return self.con.execute(sql).fetchone()[0]


def _short(text, n=160):
    text = str(text).replace("\n", " ")
    return text if len(text) <= n else text[: n - 3] + "..."


def _fmt_rows(rows, limit=12):
    if not rows:
        return "none"
    shown = "; ".join(", ".join(f"{k}={v}" for k, v in r.items()) for r in rows[:limit])
    return shown + (f"; ... (+{len(rows) - limit} more)" if len(rows) > limit else "")


def _py_type(v):
    return "null" if v is None else type(v).__name__


def load_raw(raw_dir: Path):
    batches, samples = [], []
    for path in sorted(raw_dir.glob("device_sync/drop_*/*.json")):
        rel = path.relative_to(raw_dir).as_posix()
        drop = int(path.parent.name.split("_")[1])
        doc = json.loads(path.read_bytes())
        smp = doc.get("samples")
        header = {k: v for k, v in doc.items() if k != "samples"}
        batches.append(
            {
                "file": rel,
                "drop_number": drop,
                "file_stem": path.stem,
                "top_keys": ",".join(sorted(doc)),
                "header_types": json.dumps(
                    {k: _py_type(v) for k, v in header.items()}, sort_keys=True
                ),
                "batch_id": doc.get("batch_id"),
                "device_id": doc.get("device_id"),
                "firmware": doc.get("firmware"),
                "app_version": doc.get("app_version"),
                "uploaded_at": doc.get("uploaded_at"),
                "sample_count": doc.get("sample_count"),
                "n_samples": len(smp) if isinstance(smp, list) else None,
                "samples_sha": hashlib.sha256(json.dumps(smp, sort_keys=True).encode()).hexdigest(),
                "header_nulls": sum(v is None for v in header.values()),
                "header_empty": sum(v == "" for v in header.values()),
            }
        )
        for i, x in enumerate(smp or []):
            gps = x.get("gps") if isinstance(x.get("gps"), dict) else {}
            hr_key = "hr" if "hr" in x else ("heart_rate" if "heart_rate" in x else None)
            hr = x.get(hr_key) if hr_key else None
            cad = x.get("cadence")
            lat = x.get("lat", gps.get("lat"))
            lon = x.get("lon", gps.get("lon"))
            values = list(x.values()) + list(gps.values())
            samples.append(
                (
                    rel,
                    drop,
                    doc.get("batch_id"),
                    doc.get("device_id"),
                    doc.get("firmware"),
                    doc.get("app_version"),
                    i,
                    ",".join(sorted(x)),
                    ",".join(sorted(gps)) if gps else None,
                    x.get("t"),
                    hr_key,
                    _py_type(hr),
                    hr if isinstance(hr, (int, float)) else None,
                    _py_type(cad),
                    None if cad is None else str(cad),
                    _py_type(lat),
                    lat,
                    lon,
                    gps.get("hdop"),
                    sum(v is None for v in values),
                    sum(v == "" for v in values),
                    json.dumps(x, sort_keys=True),
                )
            )
    s_cols = [
        "file",
        "drop_number",
        "batch_id",
        "device_id",
        "firmware",
        "app_version",
        "idx",
        "sample_keys",
        "gps_keys",
        "t_raw",
        "hr_key",
        "hr_type",
        "hr",
        "cadence_type",
        "cadence_raw",
        "lat_type",
        "lat",
        "lon",
        "hdop",
        "null_count",
        "empty_count",
        "rec",
    ]

    pages, sessions = [], []
    for path in sorted(raw_dir.glob("vendor_api/*.json")):
        rel = path.relative_to(raw_dir).as_posix()
        doc = json.loads(path.read_bytes())
        ss = doc.get("sessions") or []
        pages.append(
            {
                "file": rel,
                "env_keys": ",".join(sorted(doc)),
                "page": doc.get("page"),
                "page_size": doc.get("page_size"),
                "fetched_at": doc.get("fetched_at"),
                "next_page_token": doc.get("next_page_token"),
                "n_sessions": len(ss),
            }
        )
        for i, x in enumerate(ss):
            sessions.append(
                {
                    "file": rel,
                    "idx": i,
                    "keys": ",".join(sorted(x)),
                    "types": json.dumps({k: _py_type(v) for k, v in x.items()}, sort_keys=True),
                    "nulls": sum(v is None for v in x.values()),
                    "empties": sum(v == "" for v in x.values()),
                    **{
                        k: x.get(k)
                        for k in [
                            "session_id",
                            "device_id",
                            "sport",
                            "start_time",
                            "end_time",
                            "duration_s",
                            "distance_m",
                            "sync_state",
                        ]
                    },
                    "rec": json.dumps(x, sort_keys=True),
                }
            )

    def read_csvs(pattern):
        headers, rows = {}, []
        for path in sorted(raw_dir.glob(pattern)):
            rel = path.relative_to(raw_dir).as_posix()
            reader = csv.DictReader(io.StringIO(path.read_bytes().decode("utf-8-sig"), newline=""))
            headers[rel] = reader.fieldnames
            for n, r in enumerate(reader, start=1):
                rows.append({"file": rel, "row_number": n, **r})
        return headers, rows

    roster_headers, roster_rows = read_csvs("roster/*.csv")
    coach_headers, coach_rows = read_csvs("coach_app/*.csv")

    con = duckdb.connect()
    con.register("b_df", pd.DataFrame(batches))
    con.register("s_df", pd.DataFrame(samples, columns=s_cols))
    con.register("vp_df", pd.DataFrame(pages))
    con.register("v_df", pd.DataFrame(sessions))
    con.register("r_df", pd.DataFrame(roster_rows, dtype=str))
    con.register("c_df", pd.DataFrame(coach_rows, dtype=str))
    con.execute(
        "CREATE TABLE b AS SELECT *, TRY_CAST(uploaded_at AS TIMESTAMP) AS uploaded_utc FROM b_df"
    )
    con.execute(
        """CREATE TABLE s AS SELECT *, TRY_CAST(t_raw AS TIMESTAMP) AS t,
                  date_trunc('second', TRY_CAST(t_raw AS TIMESTAMP)) AS sec,
                  TRY_CAST(cadence_raw AS INTEGER) AS cadence FROM s_df"""
    )
    con.execute(
        "CREATE TABLE vp AS SELECT *, TRY_CAST(fetched_at AS TIMESTAMP) AS fetched_utc FROM vp_df"
    )
    con.execute(
        """CREATE TABLE v AS SELECT v_df.*, vp.fetched_utc,
                  TRY_CAST(start_time AS TIMESTAMP) AS start_utc, TRY_CAST(end_time AS TIMESTAMP) AS end_utc
           FROM v_df JOIN vp USING (file)"""
    )
    con.execute(
        """CREATE TABLE vs AS SELECT * EXCLUDE (rn) FROM (
             SELECT *, row_number() OVER (PARTITION BY session_id ORDER BY fetched_utc DESC, file DESC) AS rn FROM v)
           WHERE rn = 1"""
    )
    con.execute("CREATE TABLE r AS SELECT * FROM r_df")
    con.execute("CREATE TABLE c AS SELECT * FROM c_df")
    con.execute(
        "CREATE TABLE owners AS SELECT * FROM (VALUES "
        + ", ".join(f"('{d}','{a}',DATE '{f}',DATE '{t}')" for d, a, f, t in DEVICE_OWNERS)
        + ") AS t(device_id, athlete_id, valid_from, valid_to)"
    )
    con.execute(
        "CREATE TABLE squads AS SELECT * FROM (VALUES "
        + ", ".join(f"('{a}','{s}',DATE '{f}',DATE '{t}')" for a, s, f, t in SQUAD_MEMBERSHIP)
        + ") AS t(athlete_id, squad, valid_from, valid_to)"
    )
    con.execute("SELECT 1")
    return con, {"roster": roster_headers, "coach_app": coach_headers}


def check_structure(a: Audit, raw_dir: Path):
    print("\n1 Structure")
    rows = a.rows("SELECT top_keys, count(*) AS files FROM b GROUP BY 1")
    a.add(
        "1",
        "device_sync",
        "Top-level keys per batch file",
        "OK" if len(rows) == 1 else "ISSUE",
        _fmt_rows(rows),
    )

    rows = a.rows("SELECT header_types, count(*) AS files FROM b GROUP BY 1")
    a.add(
        "1",
        "device_sync",
        "Header field types",
        "OK" if len(rows) == 1 else "ISSUE",
        _fmt_rows(rows),
    )

    rows = a.rows(
        """SELECT app_version, firmware, sample_keys, gps_keys, count(*) AS readings
           FROM s GROUP BY ALL ORDER BY ALL"""
    )
    a.add(
        "1",
        "device_sync",
        "Sample fields by app/firmware version",
        "ISSUE" if a.val("SELECT count(DISTINCT sample_keys) FROM s") > 1 else "OK",
        _fmt_rows(rows),
        a.val("SELECT rec FROM s WHERE app_version='5.1.3' LIMIT 1")
        + "  vs  "
        + a.val("SELECT rec FROM s WHERE app_version='5.2.0' LIMIT 1"),
    )
    mixed = a.rows(
        "SELECT app_version, count(DISTINCT sample_keys) AS key_sets FROM s GROUP BY 1 HAVING count(DISTINCT sample_keys) > 1"
    )
    a.add(
        "1",
        "device_sync",
        "Sample fields consistent within each app version",
        "OK" if not mixed else "ISSUE",
        _fmt_rows(mixed),
    )
    mixed = a.rows(
        "SELECT file, count(DISTINCT sample_keys) AS key_sets FROM s GROUP BY 1 HAVING count(DISTINCT sample_keys) > 1"
    )
    a.add(
        "1",
        "device_sync",
        "One payload format per file (no mixed files)",
        "OK" if not mixed else "ISSUE",
        _fmt_rows(mixed),
    )

    rows = a.rows(
        "SELECT app_version, hr_key, hr_type, cadence_type, lat_type, count(*) AS readings FROM s GROUP BY ALL ORDER BY ALL"
    )
    a.add(
        "1",
        "device_sync",
        "Sample value types by app version",
        "ISSUE" if a.val("SELECT count(DISTINCT cadence_type) FROM s") > 1 else "OK",
        _fmt_rows(rows),
    )

    bad = a.val(
        "SELECT count(*) FROM s WHERE cadence_type='str' AND NOT regexp_full_match(cadence_raw, '[1-9][0-9]*')"
    )
    a.add(
        "1",
        "device_sync",
        "Text cadence values are clean integers (no spaces, decimals, leading zeros)",
        "OK" if bad == 0 else "ISSUE",
        f"{bad} bad text values",
    )

    n = a.rows("SELECT sum(null_count) AS sample_nulls, sum(empty_count) AS sample_empty FROM s")[0]
    h = a.rows(
        "SELECT sum(header_nulls) AS header_nulls, sum(header_empty) AS header_empty FROM b"
    )[0]
    a.add(
        "1",
        "device_sync",
        "Nulls / empty strings in headers and samples",
        "OK" if not any((n | h).values()) else "ISSUE",
        _fmt_rows([n | h]),
    )

    bad = a.rows(
        "SELECT file, sample_count, n_samples FROM b WHERE sample_count IS DISTINCT FROM n_samples"
    )
    a.add(
        "1",
        "device_sync",
        "sample_count header equals actual number of samples",
        "OK" if not bad else "ISSUE",
        f"{len(bad)} mismatching files of {a.val('SELECT count(*) FROM b')}",
        _fmt_rows(bad, 3),
    )

    bad = a.rows("SELECT file, batch_id FROM b WHERE batch_id IS DISTINCT FROM file_stem")
    a.add(
        "1",
        "device_sync",
        "batch_id equals file name",
        "OK" if not bad else "ISSUE",
        f"{len(bad)} mismatches",
        _fmt_rows(bad, 3),
    )

    dup = a.val("SELECT count(*) - count(DISTINCT batch_id) FROM b")
    a.add(
        "1",
        "device_sync",
        "batch_id unique across all drops",
        "OK" if dup == 0 else "ISSUE",
        f"{dup} repeated batch_ids",
    )

    rows = a.rows(
        "SELECT device_id, firmware, app_version, count(*) AS files FROM b GROUP BY ALL ORDER BY ALL"
    )
    bad = a.val("SELECT count(*) FROM b WHERE NOT regexp_full_match(device_id, 'D[0-9]{3}')")
    a.add(
        "1",
        "device_sync",
        "Device ids and version combinations",
        "INFO" if bad == 0 else "ISSUE",
        f"{bad} malformed device ids. " + _fmt_rows(rows, 20),
    )

    rows = a.rows(
        "SELECT file, page, page_size, n_sessions, next_page_token, fetched_at FROM vp ORDER BY file"
    )
    over = [r for r in rows if r["n_sessions"] > r["page_size"]]
    a.add(
        "1",
        "vendor_api",
        "Sessions per page vs page_size",
        "ISSUE" if over else "OK",
        _fmt_rows(rows),
        "pages with more sessions than page_size: " + _fmt_rows(over),
    )
    chain_ok = all(
        (r["next_page_token"] == f"tok_{r['page'] + 1:02d}")
        if r["page"] < 4
        else r["next_page_token"] is None
        for r in rows
    )
    dup_pages = a.rows("SELECT page, list(file) AS files FROM vp GROUP BY 1 HAVING count(*) > 1")
    a.add(
        "1",
        "vendor_api",
        "Page numbers and next_page_token chain",
        "INFO" if chain_ok else "ISSUE",
        f"chain consistent={chain_ok}; repeated page numbers: {_fmt_rows(dup_pages)}",
    )
    rows = a.rows("SELECT env_keys, count(*) AS files FROM vp GROUP BY 1")
    a.add(
        "1",
        "vendor_api",
        "Page envelope keys",
        "OK" if len(rows) == 1 else "ISSUE",
        _fmt_rows(rows),
    )
    rows = a.rows("SELECT keys, types, count(*) AS n FROM v GROUP BY ALL")
    a.add(
        "1",
        "vendor_api",
        "Session keys and types",
        "OK" if len(rows) == 1 else "ISSUE",
        _fmt_rows(rows),
    )
    n = a.rows("SELECT sum(nulls) AS nulls, sum(empties) AS empties FROM v")[0]
    a.add(
        "1",
        "vendor_api",
        "Nulls / empty strings in sessions",
        "OK" if not any(n.values()) else "ISSUE",
        _fmt_rows([n]),
    )

    rh = a.headers["roster"]
    same = len({tuple(h) for h in rh.values()}) == 1
    a.add(
        "1",
        "roster",
        "Same columns in both snapshots",
        "OK" if same else "ISSUE",
        "; ".join(f"{k}: {v}" for k, v in rh.items()),
    )
    cols = [c for c in next(iter(rh.values())) if c]
    empties = {
        col: a.val(f"SELECT count(*) FROM r WHERE coalesce(trim({col}), '') = ''") for col in cols
    }
    a.add(
        "1",
        "roster",
        "Empty values per column",
        "INFO" if empties.get("notes") else "OK",
        ", ".join(f"{k}={v}" for k, v in empties.items())
        + " (of "
        + str(a.val("SELECT count(*) FROM r"))
        + " rows)",
    )
    bad = a.val(
        "SELECT count(*) FROM r WHERE TRY_CAST(max_hr AS INT) IS NULL OR TRY_CAST(rest_hr AS INT) IS NULL"
    )
    a.add(
        "1",
        "roster",
        "max_hr / rest_hr are integers",
        "OK" if bad == 0 else "ISSUE",
        f"{bad} non-integer values",
    )
    a.add(
        "1",
        "roster",
        "Snapshot date only in the file name (no column)",
        "INFO",
        ", ".join(rh),
        "roster rows carry no valid_from / valid_to dates",
    )

    ch = a.headers["coach_app"]
    cols = next(iter(ch.values()))
    empties = {
        col: a.val(f"SELECT count(*) FROM c WHERE coalesce(trim({col}), '') = ''") for col in cols
    }
    a.add(
        "1",
        "coach_app",
        "Columns and empty values",
        "OK" if not any(empties.values()) else "ISSUE",
        f"columns={cols}; empties: " + ", ".join(f"{k}={v}" for k, v in empties.items()),
    )
    a.add(
        "1",
        "coach_app",
        "No session_id column",
        "ISSUE",
        "labels can only be linked to sessions by athlete name + date",
        a.val(
            "SELECT row_number || ': ' || label_id || ', ' || athlete_name || ', ' || session_date FROM c LIMIT 1"
        ),
    )
    bad = a.rows(
        """SELECT count(*) FILTER (TRY_CAST(rpe AS INT) IS NULL) AS rpe_not_int,
                  count(*) FILTER (TRY_CAST(session_date AS DATE) IS NULL) AS bad_date,
                  count(*) FILTER (TRY_CAST(entered_at AS TIMESTAMP) IS NULL) AS bad_entered,
                  count(*) FILTER (NOT regexp_full_match(label_id, 'L[0-9a-f]{6}')) AS bad_label_id FROM c"""
    )[0]
    a.add(
        "1",
        "coach_app",
        "rpe / dates / label_id parse",
        "OK" if not any(bad.values()) else "ISSUE",
        _fmt_rows([bad]),
    )
    rows = a.rows("SELECT squad, count(*) AS n FROM c GROUP BY 1 ORDER BY 1")
    rows2 = a.rows("SELECT label, count(*) AS n FROM c GROUP BY 1 ORDER BY 2 DESC")
    a.add(
        "1",
        "coach_app",
        "Squad and label vocabularies",
        "OK",
        f"squads: {_fmt_rows(rows)}; labels: {_fmt_rows(rows2)}",
    )

    files = sorted(p.relative_to(raw_dir).as_posix() for p in raw_dir.rglob("*") if p.is_file())
    expected = {"README.md", "firmware/release_notes.md", "coach_app/session_labels.csv"}
    a.add(
        "1",
        "all",
        "Inventory of raw files",
        "OK" if expected <= set(files) else "ISSUE",
        f"{len(files)} files: "
        + ", ".join(f"{k}={v}" for k, v in Counter(f.split("/")[0] for f in files).items()),
    )


def _signatures(values):
    sig = Counter(re.sub(r"\d", "9", v) if isinstance(v, str) else _py_type(v) for v in values)
    return dict(sig)


def _tz_marker(sig: str):
    if sig.endswith("Z"):
        return "Z (UTC)"
    if re.search(r"[+-]99:99$", sig):
        return "explicit offset"
    return "none"


def detect_session_offsets(a: Audit):
    a.con.execute("CREATE TABLE offs AS SELECT range AS off_min FROM range(-720, 841, 15)")
    a.con.execute(
        """CREATE TABLE cand AS
           SELECT vs.session_id, vs.device_id, o.off_min
           FROM vs CROSS JOIN offs o
           JOIN (SELECT DISTINCT device_id, t FROM s) x
             ON x.device_id = vs.device_id AND x.t = vs.start_utc + to_minutes(o.off_min)
           WHERE NOT EXISTS (
             SELECT 1 FROM s p WHERE p.device_id = vs.device_id
               AND p.t >= vs.start_utc + to_minutes(o.off_min) - INTERVAL 60 SECOND
               AND p.t <  vs.start_utc + to_minutes(o.off_min))"""
    )
    a.con.execute(
        """CREATE TABLE cand_cov AS
           SELECT c.session_id, c.device_id, c.off_min, vs.duration_s,
                  count(DISTINCT s.sec) AS secs, round(100.0 * count(DISTINCT s.sec) / vs.duration_s, 1) AS coverage_pct
           FROM cand c JOIN vs USING (session_id)
           JOIN s ON s.device_id = c.device_id
             AND s.t >= vs.start_utc + to_minutes(c.off_min) AND s.t < vs.end_utc + to_minutes(c.off_min)
           GROUP BY c.session_id, c.device_id, c.off_min, vs.duration_s"""
    )
    a.con.execute(
        """CREATE TABLE sess_off AS
           SELECT vs.session_id, vs.device_id, vs.start_utc, vs.end_utc, vs.duration_s,
                  arg_max(cc.off_min, cc.secs) AS off_min, max(cc.coverage_pct) AS best_cov,
                  count(cc.off_min) FILTER (cc.coverage_pct >= 50) AS strong_candidates
           FROM vs LEFT JOIN cand_cov cc USING (session_id) GROUP BY ALL"""
    )
    a.con.execute(
        """CREATE TABLE m AS
           SELECT s.*, so.session_id, so.off_min, s.t - to_minutes(so.off_min) AS t_utc
           FROM s JOIN sess_off so ON s.device_id = so.device_id
             AND s.t >= so.start_utc + to_minutes(so.off_min) AND s.t < so.end_utc + to_minutes(so.off_min)"""
    )
    a.con.execute(
        "CREATE TABLE dev_off AS SELECT device_id, mode(off_min) AS off_min FROM sess_off GROUP BY 1"
    )
    a.con.execute(
        f"""CREATE TABLE sess AS
            SELECT so.*, (so.start_utc + to_minutes({PAKISTAN_OFFSET_MIN}))::DATE AS pk_date,
                   (so.start_utc + to_minutes(so.off_min))::DATE AS device_clock_date,
                   so.start_utc::DATE AS utc_date, o.athlete_id, sq.squad
            FROM sess_off so
            LEFT JOIN owners o ON o.device_id = so.device_id
              AND (so.start_utc + to_minutes({PAKISTAN_OFFSET_MIN}))::DATE >= o.valid_from
              AND (so.start_utc + to_minutes({PAKISTAN_OFFSET_MIN}))::DATE < o.valid_to
            LEFT JOIN squads sq ON sq.athlete_id = o.athlete_id
              AND (so.start_utc + to_minutes({PAKISTAN_OFFSET_MIN}))::DATE >= sq.valid_from
              AND (so.start_utc + to_minutes({PAKISTAN_OFFSET_MIN}))::DATE < sq.valid_to"""
    )


def check_time(a: Audit):
    print("\n2 Time and timezones")
    sig_t = dict(
        a.con.execute(
            "SELECT regexp_replace(t_raw, '[0-9]', '9', 'g'), count(*) FROM s GROUP BY 1"
        ).fetchall()
    )
    inventory = [("device_sync", "samples[].t", sig_t)]
    for source, col, sql in [
        ("device_sync", "uploaded_at", "SELECT uploaded_at FROM b"),
        ("vendor_api", "start_time", "SELECT start_time FROM v"),
        ("vendor_api", "end_time", "SELECT end_time FROM v"),
        ("vendor_api", "fetched_at", "SELECT fetched_at FROM vp"),
        ("coach_app", "session_date", "SELECT session_date FROM c"),
        ("coach_app", "entered_at", "SELECT entered_at FROM c"),
    ]:
        inventory.append((source, col, _signatures([r[0] for r in a.con.execute(sql).fetchall()])))
    for source, col, sigs in inventory:
        markers = {_tz_marker(sg) for sg in sigs}
        is_date = all(sg == "9999-99-99" for sg in sigs)
        status = "OK" if markers == {"Z (UTC)"} or is_date else "ISSUE"
        a.add(
            "2",
            source,
            f"Format of {col}",
            status,
            f"formats: {sigs}; timezone marker: {'n/a (date only)' if is_date else ', '.join(sorted(markers))}",
        )
    a.add(
        "2",
        "roster",
        "Timestamps in roster",
        "INFO",
        "none; the only date is in the file name (2026-07-01, 2026-09-01)",
    )

    detect_session_offsets(a)
    rows = a.rows(
        """SELECT device_id, off_min / 60.0 AS utc_offset_h, count(*) AS sessions, min(best_cov) AS min_coverage_pct
           FROM sess_off GROUP BY ALL ORDER BY 1, 2"""
    )
    unproven = a.rows(
        """SELECT so.session_id, so.device_id, so.off_min / 60.0 AS chosen_h, so.best_cov,
                  list(cc.off_min / 60.0 || 'h=' || cc.coverage_pct || '%') FILTER (cc.off_min <> so.off_min) AS rejected
           FROM sess_off so LEFT JOIN cand_cov cc USING (session_id)
           WHERE so.off_min IS NULL OR so.strong_candidates <> 1 GROUP BY ALL"""
    )
    a.add(
        "2",
        "device_sync",
        "Device clock offset proven per session (tested UTC-12..+14 in 15-min steps)",
        "ISSUE" if any(r["utc_offset_h"] != 5 for r in rows) else "OK",
        _fmt_rows(rows)
        + f". Sessions with a second candidate (resolved by highest coverage): {_fmt_rows(unproven)}",
        a.val("""SELECT 'D014 session ' || session_id || ': vendor start ' || start_utc || ' UTC, first reading at device clock '
                   || (start_utc + to_minutes(off_min)) FROM sess_off WHERE device_id='D014' ORDER BY start_utc LIMIT 1"""),
    )
    a.add(
        "2",
        "device_sync",
        "Is Pakistan time (UTC+5) used?",
        "INFO",
        f"{a.val('SELECT count(*) FROM sess_off WHERE off_min = 300')} of {a.val('SELECT count(*) FROM sess_off')} sessions; "
        f"other offsets: {_fmt_rows(a.rows('SELECT device_id, off_min/60.0 AS h, count(*) AS n FROM sess_off WHERE off_min <> 300 GROUP BY ALL'))}",
    )

    rows = a.rows(
        """SELECT device_id, count(DISTINCT off_min) AS distinct_offsets, min(start_utc)::DATE AS first, max(start_utc)::DATE AS last
           FROM sess_off GROUP BY 1 ORDER BY 1"""
    )
    a.add(
        "2",
        "device_sync",
        "Does any device's offset change during the season?",
        "OK" if all(r["distinct_offsets"] == 1 for r in rows) else "ISSUE",
        _fmt_rows(rows),
    )
    rows = a.rows(
        """SELECT firmware, off_min/60.0 AS h, count(DISTINCT session_id) AS sessions FROM m WHERE device_id='D014' GROUP BY ALL ORDER BY 1"""
    )
    a.add(
        "2",
        "device_sync",
        "D014 offset before and after firmware 2.4.0 (release note: 'stale date after a timezone change' fixed)",
        "INFO",
        _fmt_rows(rows),
    )

    gps = a.rows(
        "SELECT device_id, round(avg(lat),3) AS lat, round(avg(lon),3) AS lon, round(min(lat),3) AS lat_min, round(max(lat),3) AS lat_max FROM s GROUP BY 1 ORDER BY 1"
    )
    a.add(
        "2",
        "device_sync",
        "D014 location evidence (GPS)",
        "INFO",
        _fmt_rows(gps),
        "all devices, D014 included, are in Islamabad",
    )
    shared = a.rows(
        """SELECT x.device_id, count(*) AS sessions,
                  count(*) FILTER (EXISTS (SELECT 1 FROM vs y WHERE y.device_id <> x.device_id AND y.start_utc = x.start_utc)) AS same_start_as_another_device
           FROM vs x GROUP BY 1 ORDER BY 1"""
    )
    a.add(
        "2",
        "device_sync",
        "Session start times shared with other athletes (UTC)",
        "INFO",
        _fmt_rows(shared),
    )
    hours = a.rows(
        f"""SELECT device_id, min(strftime(start_utc + to_minutes(off_min), '%H:%M')) AS earliest_device_clock,
                   min(strftime(start_utc + to_minutes({PAKISTAN_OFFSET_MIN}), '%H:%M')) AS earliest_at_utc5
            FROM sess_off GROUP BY 1 ORDER BY 1"""
    )
    a.add(
        "2",
        "device_sync",
        "Earliest session start: device clock vs UTC+5",
        "INFO",
        _fmt_rows(hours),
    )

    rows = a.rows(
        f"""SELECT count(*) FILTER ((start_utc + to_minutes({PAKISTAN_OFFSET_MIN}))::DATE <> (end_utc + to_minutes({PAKISTAN_OFFSET_MIN}) - INTERVAL 1 SECOND)::DATE) AS cross_midnight_pk,
                   count(*) FILTER (start_utc::DATE <> (end_utc - INTERVAL 1 SECOND)::DATE) AS cross_midnight_utc,
                   count(*) FILTER (start_utc::DATE <> (start_utc + to_minutes({PAKISTAN_OFFSET_MIN}))::DATE) AS utc_date_ne_pk_date,
                   count(*) FILTER (device_clock_date <> pk_date) AS device_clock_date_ne_pk_date,
                   count(*) FILTER (date_trunc('week', device_clock_date) <> date_trunc('week', pk_date)) AS week_differs
            FROM sess"""
    )
    a.add(
        "2",
        "vendor_api",
        "Sessions crossing midnight / on a different date in UTC vs local",
        "OK" if not any(rows[0].values()) else "ISSUE",
        _fmt_rows(rows),
        "latest local end: "
        + str(
            a.val(
                f"SELECT max(strftime(end_utc + to_minutes({PAKISTAN_OFFSET_MIN}), '%H:%M')) FROM vs"
            )
        ),
    )

    rows = a.rows(
        """WITH last AS (SELECT file, max(t) AS last_t FROM s GROUP BY 1)
           SELECT round(min(date_diff('second', last.last_t - to_minutes(d.off_min), b.uploaded_utc)) / 60.0, 1) AS min_delay_min,
                  round(max(date_diff('second', last.last_t - to_minutes(d.off_min), b.uploaded_utc)) / 3600.0, 1) AS max_delay_h,
                  count(*) FILTER (b.uploaded_utc < last.last_t - to_minutes(d.off_min)) AS uploaded_before_last_reading,
                  count(*) FILTER (b.uploaded_utc - INTERVAL 5 HOUR < last.last_t - to_minutes(d.off_min)) AS before_if_uploaded_at_were_pk_time,
                  count(*) FILTER (date_diff('hour', last.last_t - to_minutes(d.off_min), b.uploaded_utc) >= 24) AS uploads_24h_plus_late
           FROM b JOIN last USING (file) JOIN dev_off d USING (device_id)"""
    )
    a.add(
        "2",
        "device_sync",
        "uploaded_at vs last reading in the file (proves uploaded_at is UTC)",
        "OK" if rows[0]["uploaded_before_last_reading"] == 0 else "ISSUE",
        _fmt_rows(rows),
    )

    rows = a.rows(
        "SELECT min(fetched_utc) AS first_fetch, max(fetched_utc) AS last_fetch, (SELECT max(end_utc) FROM v) AS last_session_end FROM vp"
    )
    a.add(
        "2",
        "vendor_api",
        "fetched_at after every session end (README: pulled on 1 Sep)",
        "OK",
        _fmt_rows(rows),
    )

    a.con.execute(
        """CREATE TABLE c2 AS
           SELECT c.*, regexp_replace(lower(trim(replace(athlete_name, '.', ''))), '\\s+', ' ', 'g') AS norm_name,
                  CAST(session_date AS DATE) AS d, CAST(entered_at AS TIMESTAMP) AS entered FROM c"""
    )
    _attach_coach_athletes(a)
    rows = a.rows(
        """SELECT (SELECT count(*) FROM c2 WHERE EXISTS (SELECT 1 FROM sess WHERE sess.athlete_id=c2.athlete_id AND sess.pk_date=c2.d)) AS match_pk_date,
                  (SELECT count(*) FROM c2 WHERE EXISTS (SELECT 1 FROM sess WHERE sess.athlete_id=c2.athlete_id AND sess.utc_date=c2.d)) AS match_utc_date,
                  (SELECT count(*) FROM c2 WHERE EXISTS (SELECT 1 FROM sess WHERE sess.athlete_id=c2.athlete_id AND sess.device_clock_date=c2.d)) AS match_device_clock_date,
                  (SELECT count(*) FROM c2) AS labels"""
    )
    a.add(
        "2",
        "coach_app",
        "session_date vs the session's local date",
        "INFO",
        _fmt_rows(rows),
        "no session crosses midnight, so all three day definitions agree",
    )
    rows = a.rows(
        """WITH j AS (SELECT c2.label_id, c2.d, c2.entered, s.end_utc FROM c2 JOIN sess s ON s.athlete_id=c2.athlete_id AND s.pk_date=c2.d)
           SELECT count(*) FILTER (entered - INTERVAL 5 HOUR < end_utc) AS before_session_end_if_pk_time,
                  count(*) FILTER (entered < end_utc) AS before_session_end_if_utc,
                  min(strftime(entered, '%H:%M')) AS earliest_clock, max(strftime(entered, '%H:%M')) AS latest_clock,
                  count(DISTINCT label_id) FILTER (entered::DATE > d) AS entered_on_a_later_day,
                  max(date_diff('day', d, entered::DATE)) AS max_days_late,
                  count(*) FILTER (minute(entered) <> 0 OR second(entered) <> 0) AS not_on_the_hour
           FROM j"""
    )
    a.add(
        "2",
        "coach_app",
        "entered_at timezone (no marker)",
        "INFO",
        _fmt_rows(rows),
        "all values are whole hours (rounded), so 1 label 'before the end if Pakistan time' is a rounding artefact; "
        "19:00-23:00 as Pakistan time is plausible, as UTC it would mean 00:00-04:00 in Pakistan",
    )

    rows = a.rows(
        """SELECT firmware,
                  count(*) FILTER (t_raw NOT LIKE '%.%') AS whole_second,
                  count(*) FILTER (t_raw LIKE '%.500') AS half_second,
                  count(*) FILTER (t_raw LIKE '%.%' AND t_raw NOT LIKE '%.500') AS other_fraction
           FROM s GROUP BY 1 ORDER BY 1"""
    )
    a.add(
        "2",
        "device_sync",
        "Sub-second timestamps (2 Hz)",
        "ISSUE" if any(r["half_second"] for r in rows) else "OK",
        _fmt_rows(rows),
        a.val(
            "SELECT string_agg(t_raw, ', ' ORDER BY idx) FROM (SELECT * FROM s WHERE firmware='2.4.0' ORDER BY file, idx LIMIT 4)"
        ),
    )
    rows = a.rows(
        """WITH per AS (SELECT file, firmware, sec, count(*) AS n FROM s GROUP BY ALL)
           SELECT firmware, n AS readings_per_second, count(*) AS seconds FROM per GROUP BY ALL ORDER BY ALL"""
    )
    a.add("2", "device_sync", "Readings per second inside a file", "INFO", _fmt_rows(rows))
    rows = a.rows(
        """SELECT count(*) AS out_of_order FROM (SELECT t, lag(t) OVER (PARTITION BY file ORDER BY idx) AS prev FROM s) WHERE t <= prev"""
    )
    a.add(
        "2",
        "device_sync",
        "Out-of-order or repeated timestamps inside a file",
        "OK" if rows[0]["out_of_order"] == 0 else "ISSUE",
        _fmt_rows(rows),
    )
    rows = a.rows(
        """SELECT file, prev, t, date_diff('millisecond', prev, t) / 1000.0 AS gap_s FROM (
             SELECT file, firmware, t, lag(t) OVER (PARTITION BY file ORDER BY idx) AS prev FROM s)
           WHERE date_diff('millisecond', prev, t) > CASE firmware WHEN '2.4.0' THEN 500 ELSE 1000 END"""
    )
    a.add(
        "2",
        "device_sync",
        "Gaps between consecutive readings inside a file",
        "OK" if not rows else "ISSUE",
        f"{len(rows)} gaps. " + _fmt_rows(rows),
    )
    rows = a.rows(
        """WITH f AS (SELECT file, device_id, batch_id, min(t) AS t0, max(t) AS t1 FROM s GROUP BY ALL)
           SELECT x.device_id, x.batch_id AS batch_a, y.batch_id AS batch_b, x.t0, x.t1, y.t0 AS b_t0, y.t1 AS b_t1
           FROM f x JOIN f y ON x.device_id = y.device_id AND x.file < y.file AND x.t0 <= y.t1 AND y.t0 <= x.t1"""
    )
    a.add(
        "2",
        "device_sync",
        "Files of the same device whose time ranges overlap",
        "OK" if not rows else "ISSUE",
        _fmt_rows(rows),
    )
    rows = a.rows(
        """SELECT session_id, list(DISTINCT batch_id ORDER BY batch_id) AS batches, list(DISTINCT drop_number) AS drops
           FROM m GROUP BY 1 HAVING count(DISTINCT file) > 1"""
    )
    a.add("2", "device_sync", "Sessions split over several files / drops", "INFO", _fmt_rows(rows))
    rows = a.rows(
        "SELECT batch_id, list(DISTINCT session_id) AS sessions FROM m GROUP BY 1 HAVING count(DISTINCT session_id) > 1"
    )
    a.add(
        "2",
        "device_sync",
        "Files holding more than one session",
        "OK" if not rows else "INFO",
        _fmt_rows(rows),
    )
    rows = a.rows(
        """SELECT drop_number, min(uploaded_utc) AS first_upload, max(uploaded_utc) AS last_upload,
                  min(t)::DATE AS first_reading_day, max(t)::DATE AS last_reading_day FROM s JOIN b USING (file, drop_number) GROUP BY 1 ORDER BY 1"""
    )
    a.add(
        "2",
        "device_sync",
        "Drops are cut by upload time, not by session date",
        "INFO",
        _fmt_rows(rows),
    )
    rows = a.rows(
        f"""SELECT s.session_id, s.device_id, s.pk_date, list(DISTINCT m.drop_number) AS drops FROM sess s JOIN m USING (session_id)
            WHERE s.pk_date BETWEEN '{CONTROL_WEEK[0]}' AND '{CONTROL_WEEK[1]}' GROUP BY ALL HAVING max(m.drop_number) = 3 ORDER BY 3"""
    )
    a.add(
        "2",
        "device_sync",
        "Control-week sessions that only arrive in drop 3 (late data)",
        "ISSUE" if rows else "OK",
        _fmt_rows(rows),
    )


def _attach_coach_athletes(a: Audit):
    roster = a.rows("SELECT DISTINCT athlete_id, name FROM r")
    lookup = {}
    for row in roster:
        first, last = row["name"].lower().split()
        lookup[f"{first} {last}"] = row["athlete_id"]
        lookup[f"{first} {last[0]}"] = row["athlete_id"]
    names = [r[0] for r in a.con.execute("SELECT DISTINCT norm_name FROM c2").fetchall()]
    mapping = pd.DataFrame({"norm_name": names, "athlete_id": [lookup.get(n) for n in names]})
    a.con.register("name_map_df", mapping)
    a.con.execute("CREATE TABLE name_map AS SELECT * FROM name_map_df")
    a.con.execute("ALTER TABLE c2 ADD COLUMN athlete_id VARCHAR")
    a.con.execute(
        "UPDATE c2 SET athlete_id = name_map.athlete_id FROM name_map WHERE c2.norm_name = name_map.norm_name"
    )


def check_duplicates(a: Audit):
    print("\n3 Duplicates and keys")
    rows = a.rows(
        """WITH k AS (SELECT device_id, t_raw, count(*) AS copies, count(DISTINCT file) AS files,
                             count(DISTINCT drop_number) AS drops, count(DISTINCT rec) AS versions
                      FROM s GROUP BY ALL HAVING count(*) > 1)
           SELECT CASE WHEN files = 1 THEN 'within one file' WHEN drops = 1 THEN 'across files, same drop' ELSE 'across drops' END AS scope,
                  CASE WHEN versions = 1 THEN 'exact copy' ELSE 'same key, different values' END AS kind,
                  count(*) AS keys, sum(copies - 1) AS extra_rows
           FROM k GROUP BY ALL"""
    )
    a.add(
        "3",
        "device_sync",
        "Duplicate readings on (device_id, timestamp)",
        "ISSUE" if rows else "OK",
        _fmt_rows(rows),
    )
    for scope in ["within one file", "across files, same drop", "across drops"]:
        if not any(r["scope"] == scope for r in rows):
            a.add("3", "device_sync", f"Duplicate readings {scope}", "OK", "none")
    if not any(r["kind"] == "same key, different values" for r in rows):
        a.add(
            "3",
            "device_sync",
            "Duplicate keys with conflicting values",
            "OK",
            "none: every duplicate is an exact copy",
        )
    rows = a.rows(
        """SELECT device_id, list(batch_id ORDER BY batch_id) AS batch_ids, list(uploaded_at ORDER BY batch_id) AS uploaded_at,
                  any_value(n_samples) AS samples, list(drop_number) AS drops
           FROM b GROUP BY samples_sha, device_id HAVING count(*) > 1"""
    )
    a.add(
        "3",
        "device_sync",
        "Identical sample lists uploaded under different batch_ids",
        "ISSUE" if rows else "OK",
        _fmt_rows(rows),
    )

    groups = a.rows(
        """SELECT session_id, count(*) AS copies, count(DISTINCT rec) AS versions, list(file ORDER BY file) AS files
           FROM v GROUP BY 1 HAVING count(*) > 1"""
    )
    by_files = Counter((tuple(g["files"]), g["versions"]) for g in groups)
    a.add(
        "3",
        "vendor_api",
        "session_id repeated across pages",
        "ISSUE" if groups else "OK",
        f"{a.val('SELECT count(*) FROM v')} rows, {a.val('SELECT count(DISTINCT session_id) FROM v')} distinct. "
        + "; ".join(
            f"{len(set(f))} files {list(f)} -> {n} sessions ({'identical' if ver == 1 else 'DIFFERENT'})"
            for (f, ver), n in by_files.items()
        ),
    )
    changed = []
    for g in [g for g in groups if g["versions"] > 1]:
        recs = a.rows(
            f"SELECT file, fetched_utc, rec FROM v WHERE session_id = '{g['session_id']}' ORDER BY fetched_utc"
        )
        docs = [json.loads(x["rec"]) for x in recs]
        diff = {
            k: [d.get(k) for d in docs]
            for k in docs[0]
            if len({json.dumps(d.get(k)) for d in docs}) > 1
        }
        changed.append(
            f"{g['session_id']} ({' -> '.join(x['file'].split('/')[-1] for x in recs)}): {diff}"
        )
    a.add(
        "3",
        "vendor_api",
        "Repeated sessions whose values differ",
        "ISSUE" if changed else "OK",
        "; ".join(changed) or "none",
    )
    dup = a.val("SELECT count(*) - count(DISTINCT session_id) FROM vs")
    dup2 = a.val("SELECT count(*) - count(DISTINCT (device_id, start_utc)) FROM vs")
    a.add(
        "3",
        "vendor_api",
        "session_id unique after keeping the latest copy",
        "OK" if dup == 0 and dup2 == 0 else "ISSUE",
        f"{a.val('SELECT count(*) FROM vs')} sessions; repeated session_id: {dup}; repeated (device, start): {dup2}",
    )

    rows = a.rows(
        """SELECT file, count(*) - count(DISTINCT athlete_id) AS dup_athlete, count(*) - count(DISTINCT device_id) AS dup_device,
                  count(*) - count(DISTINCT name) AS dup_name FROM r GROUP BY 1"""
    )
    a.add(
        "3",
        "roster",
        "Unique athlete / device / name within each snapshot",
        "OK"
        if all(not (r["dup_athlete"] or r["dup_device"] or r["dup_name"]) for r in rows)
        else "ISSUE",
        _fmt_rows(rows),
    )
    dup = a.val("SELECT count(*) - count(DISTINCT label_id) FROM c")
    dup_content = a.val(
        "SELECT count(*) - count(DISTINCT (athlete_name, session_date, label, rpe, entered_at)) FROM c"
    )
    a.add(
        "3",
        "coach_app",
        "label_id unique; no copied rows",
        "OK" if dup == 0 and dup_content == 0 else "ISSUE",
        f"repeated label_id: {dup}; rows identical apart from label_id: {dup_content}",
    )
    rows = a.rows(
        "SELECT athlete_id, d, count(*) AS labels, list(label || ' rpe ' || rpe) AS labels_given FROM c2 GROUP BY 1, 2 HAVING count(*) > 1"
    )
    a.add(
        "3",
        "coach_app",
        "More than one label for an athlete on one day",
        "ISSUE" if rows else "OK",
        _fmt_rows(rows),
    )


def check_values(a: Audit):
    print("\n4 Values")
    a.con.execute(
        """CREATE TABLE athletes AS
           SELECT athlete_id, arg_max(name, file) AS name, arg_max(CAST(max_hr AS INT), file) AS max_hr,
                  arg_max(CAST(rest_hr AS INT), file) AS rest_hr FROM r GROUP BY 1"""
    )
    a.con.execute("CREATE TABLE md AS SELECT DISTINCT ON (device_id, t) * FROM m")
    rows = a.rows(
        """SELECT count(*) FILTER (hr IS NULL) AS hr_null, count(*) FILTER (hr = 0) AS hr_zero, count(*) FILTER (hr < 30) AS hr_below_30,
                  count(*) FILTER (hr > 220) AS hr_above_220, min(hr) AS hr_min, max(hr) AS hr_max FROM s"""
    )
    a.add(
        "4",
        "device_sync",
        "Heart rate impossible values",
        "ISSUE" if rows[0]["hr_above_220"] or rows[0]["hr_below_30"] else "OK",
        _fmt_rows(rows),
    )
    rows = a.rows(
        """SELECT device_id, t_raw, hr, prev_hr, next_hr, file FROM (
             SELECT *, lag(hr) OVER w AS prev_hr, lead(hr) OVER w AS next_hr FROM md
             WINDOW w AS (PARTITION BY session_id ORDER BY t))
           WHERE abs(hr - prev_hr) > 40 AND abs(hr - next_hr) > 40"""
    )
    a.add(
        "4",
        "device_sync",
        "Single-reading HR spikes (> 40 bpm away from both neighbours, same session)",
        "ISSUE" if rows else "OK",
        _fmt_rows(rows),
    )
    rows = a.rows(
        """SELECT md.device_id, se.athlete_id, ath.max_hr, count(*) AS readings, max(md.hr) AS highest, min(md.t_raw) AS first_at
           FROM md JOIN sess se USING (session_id) JOIN athletes ath USING (athlete_id)
           WHERE md.hr > ath.max_hr GROUP BY ALL"""
    )
    a.add(
        "4",
        "device_sync",
        "Readings above the wearer's roster max_hr",
        "ISSUE" if rows else "OK",
        _fmt_rows(rows),
    )
    near = a.rows(
        """SELECT se.athlete_id, ath.max_hr, max(md.hr) AS highest FROM md JOIN sess se USING (session_id) JOIN athletes ath USING (athlete_id)
           GROUP BY ALL ORDER BY 1"""
    )
    a.add(
        "4",
        "device_sync",
        "Highest HR per athlete vs max_hr (after spike)",
        "INFO",
        _fmt_rows(near),
    )
    rows = a.rows(
        """SELECT se.athlete_id, ath.rest_hr, count(*) AS readings_below_rest_hr, min(md.hr) AS lowest,
                  count(DISTINCT session_id) AS sessions
           FROM md JOIN sess se USING (session_id) JOIN athletes ath USING (athlete_id)
           WHERE md.hr < ath.rest_hr GROUP BY ALL ORDER BY 1"""
    )
    a.add(
        "4",
        "device_sync",
        "Readings below the wearer's roster rest_hr",
        "INFO",
        _fmt_rows(rows),
        "slightly below resting HR at the start of a session is physiologically possible",
    )
    rows = a.rows(
        """WITH p AS (SELECT device_id, sec, count(*) AS n, count(DISTINCT hr) AS hr_vals, count(DISTINCT cadence) AS cad_vals,
                             count(DISTINCT (lat, lon)) AS gps_vals FROM md WHERE firmware = '2.4.0' GROUP BY ALL)
           SELECT count(*) AS seconds_with_2_readings, count(*) FILTER (hr_vals > 1) AS hr_differs,
                  count(*) FILTER (cad_vals > 1) AS cadence_differs, count(*) FILTER (gps_vals > 1) AS gps_differs
           FROM p WHERE n = 2"""
    )
    a.add(
        "4",
        "device_sync",
        "2 Hz pairs: what changes between the two half-second readings",
        "INFO",
        _fmt_rows(rows),
    )

    rows = a.rows(
        """SELECT cadence_type, count(*) AS readings, min(cadence) AS min_cad, max(cadence) AS max_cad,
                  count(*) FILTER (cadence IS NULL) AS not_numeric, count(*) FILTER (cadence <= 0) AS zero_or_less,
                  count(*) FILTER (cadence > 250) AS above_250 FROM s GROUP BY 1"""
    )
    bad = any(r["not_numeric"] or r["zero_or_less"] or r["above_250"] for r in rows)
    a.add("4", "device_sync", "Cadence range by type", "ISSUE" if bad else "OK", _fmt_rows(rows))
    rows = a.rows(
        """SELECT count(*) FILTER (lat IS NULL OR lon IS NULL) AS missing, count(*) FILTER (lat = 0 AND lon = 0) AS zero_zero,
                  count(*) FILTER (NOT lat BETWEEN -90 AND 90 OR NOT lon BETWEEN -180 AND 180) AS out_of_range,
                  count(*) FILTER (NOT lat BETWEEN 33.4 AND 33.9 OR NOT lon BETWEEN 72.8 AND 73.3) AS outside_islamabad,
                  min(lat) AS lat_min, max(lat) AS lat_max, min(lon) AS lon_min, max(lon) AS lon_max FROM s"""
    )
    bad = rows[0]["missing"] or rows[0]["zero_zero"] or rows[0]["out_of_range"]
    a.add("4", "device_sync", "GPS coordinates valid", "ISSUE" if bad else "OK", _fmt_rows(rows))
    rows = a.rows(
        """WITH p AS (SELECT session_id, t, lat, lon, lag(lat) OVER w AS plat, lag(lon) OVER w AS plon, lag(t) OVER w AS pt
                      FROM md WHERE t_raw NOT LIKE '%.%' WINDOW w AS (PARTITION BY session_id ORDER BY t)),
           d AS (SELECT *, 2 * 6371000 * asin(sqrt(pow(sin(radians(lat - plat) / 2), 2)
                         + cos(radians(plat)) * cos(radians(lat)) * pow(sin(radians(lon - plon) / 2), 2))) AS metres FROM p WHERE plat IS NOT NULL)
           SELECT count(*) FILTER (metres / date_diff('second', pt, t) > 12) AS jumps_over_12_m_per_s,
                  round(max(metres / date_diff('second', pt, t)), 1) AS max_speed_m_per_s FROM d"""
    )
    a.add(
        "4",
        "device_sync",
        "GPS jumps (implied speed > 12 m/s between seconds)",
        "OK" if rows[0]["jumps_over_12_m_per_s"] == 0 else "ISSUE",
        _fmt_rows(rows),
    )
    rows = a.rows(
        """SELECT app_version, count(*) FILTER (hdop IS NULL) AS hdop_missing, min(hdop) AS hdop_min, max(hdop) AS hdop_max,
                  count(*) FILTER (hdop <= 0) AS hdop_zero_or_less, count(*) FILTER (hdop > 5) AS hdop_poor FROM s GROUP BY 1 ORDER BY 1"""
    )
    a.add(
        "4",
        "device_sync",
        "hdop (GPS accuracy: lower is better, > 5 is poor)",
        "OK"
        if all(
            not (r["hdop_zero_or_less"] or r["hdop_poor"])
            for r in rows
            if r["app_version"] == "5.2.0"
        )
        else "ISSUE",
        _fmt_rows(rows),
        "hdop exists only in app 5.2.0 payloads; it is not missing from 5.1.3, it never existed there",
    )

    rows = a.rows(
        """SELECT count(*) FILTER (end_utc <= start_utc) AS end_not_after_start,
                  count(*) FILTER (date_diff('second', start_utc, end_utc) <> duration_s) AS duration_mismatch,
                  count(*) FILTER (distance_m <= 0) AS no_distance, min(duration_s) / 60 AS min_minutes, max(duration_s) / 60 AS max_minutes,
                  round(min(duration_s / 60.0 / (distance_m / 1000.0)), 2) AS fastest_min_per_km,
                  round(max(duration_s / 60.0 / (distance_m / 1000.0)), 2) AS slowest_min_per_km,
                  count(*) FILTER (sport <> 'run') AS not_run FROM v"""
    )
    bad = rows[0]["end_not_after_start"] or rows[0]["duration_mismatch"] or rows[0]["no_distance"]
    a.add(
        "4",
        "vendor_api",
        "Session times, duration_s, distance and pace",
        "ISSUE" if bad else "OK",
        _fmt_rows(rows),
    )
    a.con.execute(
        """CREATE TABLE gps_dist AS
           WITH p AS (SELECT session_id, firmware, t, lat, lon, lag(lat) OVER w AS plat, lag(lon) OVER w AS plon, lag(t) OVER w AS pt
                      FROM md WINDOW w AS (PARTITION BY session_id ORDER BY t))
           SELECT session_id, firmware, t, pt, 2 * 6371000 * asin(sqrt(pow(sin(radians(lat - plat) / 2), 2)
                         + cos(radians(plat)) * cos(radians(lat)) * pow(sin(radians(lon - plon) / 2), 2))) AS metres
           FROM p WHERE plat IS NOT NULL"""
    )
    rows = a.rows(
        """WITH g AS (SELECT session_id, any_value(firmware) AS firmware, sum(metres) AS gps_m,
                             median(metres / (date_diff('millisecond', pt, t) / 1000.0)) AS speed FROM gps_dist GROUP BY 1)
           SELECT firmware, count(*) AS sessions, round(median(100 * (gps_m - distance_m) / distance_m), 1) AS median_diff_pct,
                  round(min(100 * (gps_m - distance_m) / distance_m), 1) AS min_diff_pct, round(max(100 * (gps_m - distance_m) / distance_m), 1) AS max_diff_pct,
                  round(median(speed), 2) AS gps_speed_m_per_s, round(median(distance_m / duration_s), 2) AS vendor_speed_m_per_s
           FROM vs JOIN g USING (session_id) GROUP BY 1 ORDER BY 1"""
    )
    a.add(
        "4",
        "vendor_api",
        "Vendor distance vs distance measured from GPS readings (by firmware)",
        "ISSUE",
        _fmt_rows(rows),
        "on 2.4.0 the GPS position moves once per reading, so per real second it covers twice the ground of 2.3.1",
    )

    rows = a.rows(
        """SELECT count(*) FILTER (CAST(max_hr AS INT) NOT BETWEEN 150 AND 220) AS max_hr_implausible,
                  count(*) FILTER (CAST(rest_hr AS INT) NOT BETWEEN 30 AND 90) AS rest_hr_implausible,
                  count(*) FILTER (CAST(max_hr AS INT) <= CAST(rest_hr AS INT)) AS max_not_above_rest FROM r"""
    )
    a.add(
        "4",
        "roster",
        "max_hr / rest_hr plausible",
        "OK" if not any(rows[0].values()) else "ISSUE",
        _fmt_rows(rows),
    )
    rows = a.rows(
        "SELECT min(CAST(rpe AS INT)) AS rpe_min, max(CAST(rpe AS INT)) AS rpe_max, count(*) FILTER (CAST(rpe AS INT) NOT BETWEEN 1 AND 10) AS out_of_1_10 FROM c"
    )
    a.add(
        "4",
        "coach_app",
        "RPE within 1-10",
        "OK" if rows[0]["out_of_1_10"] == 0 else "ISSUE",
        _fmt_rows(rows),
    )
    rows = a.rows(
        """SELECT label, round(avg(CAST(rpe AS INT)), 1) AS avg_rpe, min(CAST(rpe AS INT)) AS min_rpe, max(CAST(rpe AS INT)) AS max_rpe, count(*) AS n
           FROM c GROUP BY 1 ORDER BY 2"""
    )
    a.add("4", "coach_app", "RPE by label type (consistency)", "INFO", _fmt_rows(rows))


def check_identity(a: Audit, seed_paths):
    print("\n5 Identity and roster")
    snaps = a.rows("SELECT DISTINCT file FROM r ORDER BY 1")
    first, last = snaps[0]["file"], snaps[-1]["file"]
    changes = a.rows(
        f"""WITH x AS (SELECT * FROM r WHERE file = '{first}'), y AS (SELECT * FROM r WHERE file = '{last}')
            SELECT coalesce(x.athlete_id, y.athlete_id) AS athlete_id, coalesce(y.name, x.name) AS name,
                   CASE WHEN x.athlete_id IS NULL THEN 'only in ' || '{last}' WHEN y.athlete_id IS NULL THEN 'only in ' || '{first}' END AS presence,
                   CASE WHEN x.squad <> y.squad THEN x.squad || ' -> ' || y.squad END AS squad_change,
                   CASE WHEN x.device_id <> y.device_id THEN x.device_id || ' -> ' || y.device_id END AS device_change,
                   CASE WHEN x.max_hr <> y.max_hr THEN x.max_hr || ' -> ' || y.max_hr END AS max_hr_change,
                   CASE WHEN x.rest_hr <> y.rest_hr THEN x.rest_hr || ' -> ' || y.rest_hr END AS rest_hr_change,
                   CASE WHEN x.name <> y.name THEN x.name || ' -> ' || y.name END AS name_change
            FROM x FULL JOIN y USING (athlete_id) ORDER BY 1"""
    )
    changed = [
        {k: v for k, v in r.items() if v is not None}
        for r in changes
        if any(v for k, v in r.items() if k not in ("athlete_id", "name"))
    ]
    a.add(
        "5",
        "roster",
        "Changes between the two snapshots",
        "ISSUE" if changed else "OK",
        _fmt_rows(changed),
    )
    unchanged = [
        r["athlete_id"]
        for r in changes
        if not any(v for k, v in r.items() if k not in ("athlete_id", "name"))
    ]
    a.add(
        "5",
        "roster",
        "max_hr / rest_hr / name unchanged for everyone",
        "OK"
        if not any(r["max_hr_change"] or r["rest_hr_change"] or r["name_change"] for r in changes)
        else "ISSUE",
        f"athletes with no change at all: {unchanged}",
    )

    rows = a.rows(
        """SELECT device_id, count(*) AS sessions, min(pk_date) AS first_session, max(pk_date) AS last_session FROM sess GROUP BY 1 ORDER BY 1"""
    )
    a.add("5", "device_sync", "Device usage period", "INFO", _fmt_rows(rows))
    rows = a.rows(
        """SELECT device_id, prev_day, pk_date AS next_day, date_diff('day', prev_day, pk_date) AS days FROM (
             SELECT device_id, pk_date, lag(pk_date) OVER (PARTITION BY device_id ORDER BY pk_date) AS prev_day FROM sess)
           WHERE date_diff('day', prev_day, pk_date) > 2 ORDER BY 1, 2"""
    )
    a.add(
        "5", "device_sync", "Breaks of more than 2 days in a device's use", "INFO", _fmt_rows(rows)
    )
    rows = a.rows(
        """SELECT athlete_id, squad, min(d) AS first_label, max(d) AS last_label, count(*) AS labels FROM c2 GROUP BY 1, 2 ORDER BY 1, 3"""
    )
    a.add(
        "5",
        "coach_app",
        "Squad written by the coach per athlete over time",
        "INFO",
        _fmt_rows(rows),
    )
    a.add(
        "5",
        "all",
        "Working device -> athlete and squad assumptions used by later checks",
        "INFO",
        "; ".join(f"{d}->{at} {f}..{t}" for d, at, f, t in DEVICE_OWNERS)
        + " | "
        + "; ".join(f"{at} {sq} {f}..{t}" for at, sq, f, t in SQUAD_MEMBERSHIP),
        "derived from the usage timeline and coach squads above; to be confirmed by the team",
    )

    rows = a.rows(
        """SELECT nm.athlete_id, count(DISTINCT c2.athlete_name) AS spellings, list(DISTINCT c2.athlete_name ORDER BY c2.athlete_name) AS raw_names
           FROM c2 JOIN name_map nm USING (norm_name) GROUP BY 1 ORDER BY 1"""
    )
    unmatched = a.rows("SELECT DISTINCT athlete_name FROM c2 WHERE athlete_id IS NULL")
    a.add(
        "5",
        "coach_app",
        "Name spellings per athlete (deterministic match)",
        "ISSUE",
        f"{a.val('SELECT count(DISTINCT athlete_name) FROM c')} spellings for {len(rows)} athletes; unmatched: {_fmt_rows(unmatched)}. "
        + _fmt_rows(rows),
    )
    seed = next((p for p in seed_paths if p.exists()), None)
    if seed:
        a.con.execute(
            f"CREATE TABLE seed AS SELECT * FROM read_csv('{seed.as_posix()}', all_varchar = true)"
        )
        diff = a.rows(
            """SELECT DISTINCT c2.athlete_name, c2.athlete_id AS rule_id, seed.athlete_id AS seed_id FROM c2
               LEFT JOIN seed ON seed.raw_name = c2.athlete_name WHERE seed.athlete_id IS DISTINCT FROM c2.athlete_id"""
        )
        a.add(
            "5",
            "coach_app",
            f"Alias seed agrees with the deterministic rule ({seed.relative_to(REPO_ROOT).as_posix()})",
            "OK" if not diff else "ISSUE",
            _fmt_rows(diff),
        )


def check_cross_source(a: Audit):
    print("\n6 Cross-source matching")
    orphans = a.rows(
        """SELECT s.device_id, count(*) AS readings, min(s.t_raw) AS first_t, max(s.t_raw) AS last_t
           FROM s ANTI JOIN m USING (file, idx) GROUP BY 1"""
    )
    a.add(
        "6",
        "device_sync",
        "Readings outside every vendor session",
        "OK" if not orphans else "ISSUE",
        _fmt_rows(orphans),
    )
    a.con.execute(
        """CREATE TABLE completeness AS
           SELECT se.session_id, se.device_id, se.athlete_id, se.pk_date, se.duration_s,
                  count(DISTINCT m.sec) AS seconds_with_data, round(100.0 * count(DISTINCT m.sec) / se.duration_s, 1) AS completeness_pct,
                  vs.sync_state
           FROM sess se JOIN vs USING (session_id) LEFT JOIN m USING (session_id) GROUP BY se.session_id, se.device_id, se.athlete_id, se.pk_date, se.duration_s, vs.sync_state"""
    )
    rows = a.rows(
        """SELECT CASE WHEN seconds_with_data = 0 THEN 'no readings' WHEN completeness_pct >= 100 THEN '100%'
                       WHEN completeness_pct >= 95 THEN '95-99.9%' ELSE 'below 95%' END AS band, count(*) AS sessions
           FROM completeness GROUP BY 1 ORDER BY 1"""
    )
    partial = a.rows(
        "SELECT session_id, device_id, athlete_id, pk_date, duration_s, seconds_with_data, completeness_pct FROM completeness WHERE completeness_pct < 100 ORDER BY completeness_pct"
    )
    a.add(
        "6",
        "vendor_api",
        "Session completeness (distinct seconds with data / duration_s)",
        "ISSUE" if partial else "OK",
        _fmt_rows(rows),
        _fmt_rows(partial),
    )
    rows = a.rows(
        """WITH x AS (SELECT DISTINCT session_id, sec FROM m),
           g AS (SELECT session_id, lag(sec) OVER (PARTITION BY session_id ORDER BY sec) AS prev, sec FROM x)
           SELECT session_id, prev AS last_before_gap, sec AS first_after_gap, date_diff('second', prev, sec) - 1 AS missing_s
           FROM g WHERE date_diff('second', prev, sec) > 1"""
    )
    a.add("6", "device_sync", "Gaps inside sessions", "ISSUE" if rows else "OK", _fmt_rows(rows))
    rows = a.rows(
        """SELECT vs.session_id, vs.end_utc AS vendor_end, max(m.t_utc) AS last_reading_utc, min(m.t_utc) = vs.start_utc AS starts_on_time
           FROM vs JOIN m USING (session_id) GROUP BY vs.session_id, vs.end_utc, vs.start_utc
           HAVING min(m.t_utc) <> vs.start_utc OR date_diff('second', max(m.sec - to_minutes(m.off_min)), vs.end_utc) > 1"""
    )
    a.add(
        "6",
        "vendor_api",
        "Readings start at vendor start and end within 1 s of vendor end",
        "OK" if not rows else "ISSUE",
        _fmt_rows(rows),
    )
    early = a.rows(
        """SELECT v.session_id, v.file, v.end_time, v.duration_s, max(m.t_utc) AS last_reading_utc FROM v JOIN m USING (session_id)
           WHERE v.session_id IN (SELECT session_id FROM v GROUP BY 1 HAVING count(DISTINCT rec) > 1) GROUP BY ALL ORDER BY v.file"""
    )
    a.add(
        "6",
        "vendor_api",
        "Changed vendor session: which version matches the readings?",
        "ISSUE" if early else "OK",
        _fmt_rows(early),
    )

    rows = a.rows(
        """SELECT count(*) FILTER (EXISTS (SELECT 1 FROM sess s WHERE s.athlete_id = c2.athlete_id AND s.pk_date = c2.d)) AS labels_with_session,
                  count(*) FILTER (NOT EXISTS (SELECT 1 FROM sess s WHERE s.athlete_id = c2.athlete_id AND s.pk_date = c2.d)) AS labels_without_session,
                  (SELECT count(*) FROM sess s WHERE NOT EXISTS (SELECT 1 FROM c2 WHERE c2.athlete_id = s.athlete_id AND c2.d = s.pk_date)) AS sessions_without_label
           FROM c2"""
    )
    bad = rows[0]["labels_without_session"] or rows[0]["sessions_without_label"]
    a.add(
        "6",
        "coach_app",
        "Coach labels vs sessions (athlete + local date)",
        "ISSUE" if bad else "OK",
        _fmt_rows(rows),
    )
    rows = a.rows(
        """SELECT s.athlete_id, s.pk_date, list(s.session_id || ' ' || strftime(s.start_utc + INTERVAL 5 HOUR, '%H:%M') || ' local') AS sessions
           FROM sess s GROUP BY 1, 2 HAVING count(*) > 1"""
    )
    a.add(
        "6",
        "vendor_api",
        "Athletes with 2+ sessions on one day (label match ambiguous)",
        "ISSUE" if rows else "OK",
        _fmt_rows(rows),
    )
    if rows:
        detail = a.rows(
            """SELECT m.session_id, round(avg(m.hr)) AS avg_hr, max(m.hr) AS max_hr FROM m
               WHERE m.session_id IN (SELECT s1.session_id FROM sess s1 JOIN sess s2
                     ON s1.athlete_id = s2.athlete_id AND s1.pk_date = s2.pk_date AND s1.session_id <> s2.session_id) GROUP BY 1"""
        )
        a.add(
            "6",
            "vendor_api",
            "Intensity of the sessions on that day (to pair with labels)",
            "INFO",
            _fmt_rows(detail),
        )

    snaps = [r["file"] for r in a.rows("SELECT DISTINCT file FROM r ORDER BY 1")]
    for snap in snaps:
        rows = a.rows(
            f"""SELECT c2.athlete_id, c2.squad AS coach_squad, r.squad AS roster_squad, min(c2.d) AS from_day, max(c2.d) AS to_day, count(*) AS labels
                FROM c2 LEFT JOIN r ON r.athlete_id = c2.athlete_id AND r.file = '{snap}'
                WHERE r.squad IS DISTINCT FROM c2.squad GROUP BY ALL ORDER BY 1"""
        )
        a.add(
            "6",
            "coach_app",
            f"Coach squad vs roster squad ({snap.split('/')[-1]})",
            "ISSUE" if rows else "OK",
            _fmt_rows(rows),
        )
    rows = a.rows(
        """SELECT c2.athlete_id, c2.d, c2.squad AS coach_squad, se.squad AS assumed_squad FROM c2
           JOIN sess se ON se.athlete_id = c2.athlete_id AND se.pk_date = c2.d WHERE se.squad IS DISTINCT FROM c2.squad"""
    )
    a.add(
        "6",
        "coach_app",
        "Coach squad vs dated squad assumption",
        "OK" if not rows else "ISSUE",
        _fmt_rows(rows),
    )
    rows = a.rows(
        "SELECT sync_state, count(*) AS sessions, min(completeness_pct) AS min_completeness FROM completeness GROUP BY 1"
    )
    a.add(
        "6",
        "vendor_api",
        "sync_state vs real completeness",
        "ISSUE"
        if any(r["sync_state"] == "complete" and r["min_completeness"] < 100 for r in rows)
        else "OK",
        _fmt_rows(rows),
    )

    first_240 = a.rows(
        "SELECT device_id, min(t)::DATE AS first_day FROM s WHERE firmware = '2.4.0' GROUP BY 1 ORDER BY 1"
    )
    a.add(
        "6",
        "firmware",
        "Claim: 2.4.0 rolled out from 2026-08-01, staged",
        "OK" if all(r["first_day"] >= date(2026, 8, 1) for r in first_240) else "ISSUE",
        f"devices on 2.4.0: {_fmt_rows(first_240)}; still on 2.3.1 at season end: "
        + str(
            [
                r[0]
                for r in a.con.execute(
                    "SELECT device_id FROM b GROUP BY 1 HAVING bool_and(firmware = '2.3.1')"
                ).fetchall()
            ]
        ),
    )
    half = a.val("SELECT count(*) FROM s WHERE t_raw LIKE '%.500'")
    half_other = a.val("SELECT count(*) FROM s WHERE t_raw LIKE '%.500' AND firmware <> '2.4.0'")
    a.add(
        "6",
        "firmware",
        "Claim: 2.4.0 samples HR at 2 Hz",
        "OK" if half and not half_other else "ISSUE",
        f"{half} half-second readings, {half_other} of them on other firmware",
    )
    pairs = a.rows(
        """WITH p AS (SELECT device_id, sec, count(DISTINCT cadence) AS cad_vals, count(DISTINCT (lat, lon)) AS gps_vals, count(*) AS n
                      FROM md WHERE firmware = '2.4.0' GROUP BY ALL)
           SELECT count(*) FILTER (n = 2) AS pairs, count(*) FILTER (n = 2 AND cad_vals = 1 AND gps_vals = 1) AS cadence_and_gps_repeated FROM p"""
    )
    a.add(
        "6",
        "firmware",
        "Claim: cadence and GPS unchanged on 2.4.0 (still 1 Hz?)",
        "ISSUE" if pairs[0]["pairs"] and pairs[0]["cadence_and_gps_repeated"] == 0 else "OK",
        _fmt_rows(pairs),
        "cadence and GPS change at every half-second reading too, and GPS covers 2x the ground per real second (see 4.12)",
    )
    combo = a.rows(
        """SELECT device_id, app_version, min(t)::DATE AS first_day, max(t)::DATE AS last_day, count(DISTINCT file) AS files
           FROM s WHERE firmware = '2.4.0' AND app_version <> '5.2.0' GROUP BY ALL"""
    )
    a.add(
        "6",
        "firmware",
        "Claim: companion app 5.2.0 required for 2.4.0 sync",
        "ISSUE" if combo else "OK",
        "contradicted: " + _fmt_rows(combo) if combo else "holds",
    )
    first_520 = a.val("SELECT min(t)::DATE FROM s WHERE app_version = '5.2.0'")
    a.add(
        "6",
        "firmware",
        "Claim: app 5.2.0 released 2026-08-05",
        "OK" if first_520 >= date(2026, 8, 5) else "ISSUE",
        f"first 5.2.0 reading: {first_520}",
    )
    pauses = a.rows(
        """WITH x AS (SELECT DISTINCT session_id, firmware, sec FROM m),
           g AS (SELECT firmware, date_diff('second', lag(sec) OVER (PARTITION BY session_id ORDER BY sec), sec) - 1 AS missing FROM x)
           SELECT firmware, count(*) FILTER (missing BETWEEN 20 AND 45) AS gaps_20_to_45s, count(*) FILTER (missing > 0) AS any_gap FROM g GROUP BY 1"""
    )
    a.add(
        "6",
        "firmware",
        "Claim: auto-pause after 20 s (was 45 s): do pauses show as gaps?",
        "INFO",
        _fmt_rows(pauses),
        "no pause-shaped gaps; either nobody stopped or the watch keeps sampling while paused",
    )

    a.add(
        "6",
        "README",
        "Six athletes, two squads",
        "OK"
        if a.val("SELECT count(DISTINCT athlete_id) FROM r") == 6
        and a.val("SELECT count(DISTINCT squad) FROM r") == 2
        else "ISSUE",
        f"athletes={a.val('SELECT count(DISTINCT athlete_id) FROM r')}, squads={a.val('SELECT count(DISTINCT squad) FROM r')}",
    )
    out = a.val(
        f"SELECT count(*) FROM sess WHERE pk_date NOT BETWEEN '{SEASON[0]}' AND '{SEASON[1]}'"
    )
    a.add(
        "6",
        "README",
        f"All sessions inside the season {SEASON[0]}..{SEASON[1]}",
        "OK" if out == 0 else "ISSUE",
        f"{out} outside; first {a.val('SELECT min(pk_date) FROM sess')}, last {a.val('SELECT max(pk_date) FROM sess')}",
    )
    a.add(
        "6",
        "README",
        "'The watch streams per-second samples'",
        "ISSUE",
        "not true for firmware 2.4.0 heart rate (2 Hz), see section 2",
    )

    lo, hi = CONTROL_WEEK
    ours = {
        r["name"]: r["sessions"]
        for r in a.rows(
            f"""SELECT ath.name, count(*) AS sessions FROM sess JOIN athletes ath USING (athlete_id)
            WHERE pk_date BETWEEN '{lo}' AND '{hi}' GROUP BY 1"""
        )
    }
    cmp_rows = [
        {"athlete": k, "control": v, "ours": ours.get(k, 0)} for k, v in CONTROL_SESSIONS.items()
    ]
    a.add(
        "6",
        "README",
        "Control: sessions per athlete, week 10-16 Aug",
        "OK" if all(r["control"] == r["ours"] for r in cmp_rows) else "ISSUE",
        _fmt_rows(cmp_rows),
        "uses dated device ownership (Hamza on D013 until 10 Aug, D017 from 12 Aug)",
    )
    variants = a.rows(
        f"""WITH wk AS (SELECT * FROM completeness JOIN sess USING (session_id, device_id, athlete_id, pk_date, duration_s)
                     WHERE pk_date BETWEEN '{lo}' AND '{hi}'),
            readings AS (SELECT session_id, count(*) AS n_readings FROM m GROUP BY 1),
            endur AS (SELECT * FROM wk WHERE squad = 'Endurance')
            SELECT 'squad on the day, minutes from vendor duration' AS method, sum(duration_s) / 60 AS minutes FROM endur
            UNION ALL SELECT 'squad on the day, minutes from distinct seconds of readings', sum(seconds_with_data) / 60 FROM endur
            UNION ALL SELECT 'squad on the day, readings counted as seconds (2 Hz trap)', sum(n_readings) / 60 FROM endur JOIN readings USING (session_id)
            UNION ALL SELECT 'September roster squads for the whole week (Bilal = Speed)', sum(duration_s) / 60 FROM wk
                      WHERE athlete_id IN (SELECT athlete_id FROM r WHERE file LIKE '%09-01%' AND squad = 'Endurance')
            UNION ALL SELECT 'July roster squads for the whole week (Bilal = Endurance, no Mehak)', sum(duration_s) / 60 FROM wk
                      WHERE athlete_id IN (SELECT athlete_id FROM r WHERE file LIKE '%07-01%' AND squad = 'Endurance')"""
    )
    ok = any(r["minutes"] == CONTROL_ENDURANCE_MINUTES for r in variants[:2])
    a.add(
        "6",
        "README",
        f"Control: Endurance minutes = {CONTROL_ENDURANCE_MINUTES}",
        "OK" if ok else "ISSUE",
        _fmt_rows(variants),
    )


def write_report(a: Audit, path: Path, raw_dir: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    status = Counter(c.status for c in a.checks)
    lines = [
        "# Raw data audit report",
        "",
        f"Generated by `validation/profile_raw_data.py` from `{raw_dir.relative_to(REPO_ROOT).as_posix() if raw_dir.is_relative_to(REPO_ROOT) else raw_dir.as_posix()}` (read-only).",
        "Re-run: `audit`",
        "",
        f"**{len(a.checks)} checks: {status['ISSUE']} ISSUE, {status['OK']} OK, {status['INFO']} INFO.**",
        "ISSUE = needs handling in the pipeline, OK = checked and clean, INFO = fact worth knowing.",
        "",
    ]
    for sec, name in Audit.SECTIONS.items():
        lines += [
            f"## {sec}. {name}",
            "",
            "| ID | Source | Check | Result | Detail | Example |",
            "|---|---|---|---|---|---|",
        ]
        for c in [c for c in a.checks if c.section == sec]:

            def cell(s, n=600):
                return _short(s, n).replace("|", "\\|")

            lines.append(
                f"| {c.id} | {c.source} | {cell(c.title)} | **{c.status}** | {cell(c.detail)} | {cell(c.example, 300)} |"
            )
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Read-only audit of the raw data")
    parser.add_argument("--raw-dir", type=Path, default=REPO_ROOT / "data" / "raw")
    parser.add_argument(
        "--report", type=Path, default=REPO_ROOT / "output" / "raw_data_audit_report.md"
    )
    args = parser.parse_args(argv)

    print(f"Reading {args.raw_dir} (read-only)...")
    con, headers = load_raw(args.raw_dir)
    print(
        f"  {con.execute('SELECT count(*) FROM b').fetchone()[0]} device files, {con.execute('SELECT count(*) FROM s').fetchone()[0]:,} readings, "
        f"{con.execute('SELECT count(*) FROM v').fetchone()[0]} vendor rows, {con.execute('SELECT count(*) FROM r').fetchone()[0]} roster rows, "
        f"{con.execute('SELECT count(*) FROM c').fetchone()[0]} coach labels"
    )
    audit = Audit(con)
    audit.headers = headers
    check_structure(audit, args.raw_dir)
    check_time(audit)
    check_duplicates(audit)
    check_values(audit)
    seeds = [REPO_ROOT / "data" / "reference" / "athlete_alias.csv"]
    check_identity(audit, seeds)
    check_cross_source(audit)
    write_report(audit, args.report, args.raw_dir)
    status = Counter(c.status for c in audit.checks)
    print(
        f"\n{len(audit.checks)} checks: {status['ISSUE']} ISSUE, {status['OK']} OK, {status['INFO']} INFO"
    )
    print(f"Report written to {args.report}")


if __name__ == "__main__":
    main()
