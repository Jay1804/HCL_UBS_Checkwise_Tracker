"""
Client check-wise tracker (v1: client 1951, "Bridge").

Pipeline (all read-only SELECTs):
  1. Advance Tracker data  -> one row per case/check (open cases only, as in the
     original Advance Tracker: case_status NOT IN (8,14), check_status<>9,
     check_id<>193).  Case-level columns come from here.
  2. Check Status and Severity -> only CHECK_NAME, keyed on case_check_id.
  3. Pivot: per case, each CHECK_NAME gets a block of 3 columns
     (First check_closure_date, closure_comments, insuff_remarks).
     A check name that occurs more than once on a case is spread over
     "Name", "Name(1)", "Name(2)" ... in case_check_id order.

Usage:
  python build_tracker.py                       # all open cases of client 1951
  python build_tracker.py --from 2026-01-01 --to 2026-03-31
  python build_tracker.py --ars ARS1,ARS2       # specific cases
  python build_tracker.py --limit 500           # quick test run
"""
import argparse
import re
from pathlib import Path

import pandas as pd
import pymysql

BASE = Path(__file__).parent
CLIENT_EXTERNAL_ID = 1951

# Case-level columns: output header -> flex-field label stored in
# ec_client_case_fields (resolved dynamically, so this works per client later).
FLEX_COLUMNS = {
    "BGV ID1 Pre Joining": "BGV ID1 Pre Joining",
    "Verification Type": "Verification Type",
    "Employee ID": "Employee ID",
}

# Check blocks, exactly in the order requested. "Name(n)" = (n+1)th occurrence.
CHECK_ORDER = """
Passport Verification
Global Database Check
India Credit Default Database Check
India - Court Record Database Check through Law Firm - Internal
India Court Record Check through Law Firm
India Court Record Check through Law Firm(1)
India Court Record Check through Law Firm(2)
India Court Record Check through Law Firm(3)
India - Court Record Database Check through Law Firm - Internal(1)
India - Court Record Database Check through Law Firm - Internal(2)
Current Address Verification
Directorship Check
Visual Document Validation
Professional Reference Check
Professional Reference Check(1)
Indian Database Verification
Current Employment Verification
Address Verification
Database Web and Media Check
Education Verification W
CV Validation
PAN No Verification
UAN/EPFO
UAN Check for Undisclosed Employment
Self Employment Check via GSTIN
Previous Employment Verification
Gap Check
UAN/EPFO(1)
India Credit Default Database Check(1)
India Credit Default Database Check(2)
Previous Employment Verification(1)
Previous Employment Verification(2)
Address Verification(1)
Gap Check(1)
Statement of Fact for Employment
Statement of Fact for Employment(1)
Address Verification(2)
Address Verification(3)
India - Court Record Database Check through Law Firm - Internal(3)
India - Court Record Database Check through Law Firm - Internal(4)
UAN/EPFO(2)
Gap Check(2)
Previous Employment Verification(3)
UAN/EPFO(3)
UAN/EPFO(4)
Previous Employment Verification(4)
UAN/EPFO(5)
""".strip().splitlines()

SUB_COLS = ["severity", "First check_closure_date", "closure_comments", "insuff_remarks"]
SUB_SRC = {"First check_closure_date": "check_closure_date",
           "closure_comments": "closure_comments",
           "insuff_remarks": "insuff_remarks",
           "severity": "check_severity"}


def load_env():
    env = {}
    for line in (BASE / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("\"'")
    return env


def connect():
    e = load_env()
    return pymysql.connect(host=e["DB_HOST"], user=e["DB_USER"],
                           password=e["DB_PASSWORD"], database=e["DB_NAME"],
                           connect_timeout=30, read_timeout=900)


def query(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def fetch_cases(conn, client_id, ars_list):
    """Case-level data + check rows, mirroring the Advance Tracker filters."""
    # flex-field label -> case_flex_fieldN
    labels = query(conn, "SELECT FIELD_ID, FIELD_NAME FROM ec_client_case_fields "
                         "WHERE client_id=%s AND status=1", (client_id,))
    by_label = {r.FIELD_NAME.strip(): int(r.FIELD_ID) for r in labels.itertuples()}
    flex_sel = []
    for header, label in FLEX_COLUMNS.items():
        fid = by_label.get(label)
        flex_sel.append(f"ecff.CASE_FLEX_FIELD{fid} AS `{header}`" if fid
                        else f"NULL AS `{header}`")

    where = ["ecm.client_id=%s", "ecm.case_status NOT IN (8,14)",
             "ecc.check_status <> 9", "ecc.check_id <> 193",
             "ecc1.first_name <> 'dummy'"]
    frames = []
    for batch in chunks(ars_list, 1000):
        ph = ",".join(["%s"] * len(batch))
        sql = f"""
        SELECT ecm.case_id, ecc.case_check_id, ecm.case_ars_no,
               {', '.join(flex_sel)},
               ecp.Process_name AS `Process`,
               checkpoint_live.fn_case_status(ecm.case_status) AS `Case_status`,
               ecc.CHECK_NAME AS CHECK_NAME,
               ecc.check_closure_date,
               REPLACE(ecc.closure_comments,'rn','') AS closure_comments,
               ecc.insuff_remarks,
               ecc.check_severity
        FROM ec_case_master ecm
        JOIN ec_case_checks ecc            ON ecm.case_id = ecc.case_id
        LEFT JOIN ec_case_fields ecff      ON ecm.case_id = ecff.case_id
        LEFT JOIN ec_client_process ecp    ON ecm.process_id = ecp.process_id
        LEFT JOIN ec_case_candidates ecc1  ON ecc1.candidate_id = ecm.candidate_id
        WHERE {' AND '.join(where)} AND ecm.case_ars_no IN ({ph})
        ORDER BY ecm.case_id, ecc.case_check_id
        """
        frames.append(query(conn, sql, [client_id, *batch]))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_ars(conn, client_external_id, args):
    """ARS numbers from the 'Client Wise All Cases - Bridge' query (all statuses)."""
    where = ["ec.client_external_id = %s", "ecc1.first_name <> 'dummy'"]
    params = [client_external_id]
    if args.date_from:
        where.append("ecm.received_date >= %s"); params.append(args.date_from)
    if args.date_to:
        where.append("ecm.received_date < DATE_ADD(%s, INTERVAL 1 DAY)"); params.append(args.date_to)
    if getattr(args, "process", None):
        where.append("ecp.process_name LIKE %s"); params.append(f"%{args.process}%")
    if args.ars:  # optional manual restriction on top
        ars = [a.strip() for a in args.ars.split(",") if a.strip()]
        where.append("ecm.case_ars_no IN (%s)" % ",".join(["%s"] * len(ars))); params += ars
    lim = f" ORDER BY ecm.case_id DESC LIMIT {int(args.limit)}" if args.limit else ""
    sql = f"""
    SELECT ecm.case_ars_no
    FROM ec_case_master ecm
    INNER JOIN ec_master_company emc ON ecm.client_id=emc.company_id
    INNER JOIN ec_client ec ON emc.company_id=ec.client_id
    INNER JOIN ec_case_candidates ecc1 ON ecc1.candidate_id=ecm.candidate_id
    LEFT JOIN ec_client_process ecp ON ecm.process_id=ecp.process_id
    WHERE {' AND '.join(where)}{lim}
    """
    return query(conn, sql, params).case_ars_no.dropna().tolist()


def occurrence_label(name, n):
    return name if n == 0 else f"{name}({n})"


def build_tracker(df):
    # nth occurrence of each check name within a case
    df = df.copy()
    # The DB name may already carry a numeric suffix ("Passport Verification(1)",
    # "UAN/EPFO (0)") even when it is the only such check on the case. Strip it so
    # the nth occurrence is derived purely from order, matching CHECK_ORDER.
    df["CHECK_NAME"] = (df["CHECK_NAME"].fillna("").str.strip()
                        .str.replace(r"\s*\(\d+\)$", "", regex=True))
    df["occ"] = df.groupby(["case_id", "CHECK_NAME"]).cumcount()
    df["slot"] = [occurrence_label(n, o) for n, o in zip(df.CHECK_NAME, df.occ)]

    case_cols = ["case_ars_no", *FLEX_COLUMNS.keys(), "Process", "Case_status"]
    cases = df.drop_duplicates("case_id").set_index("case_id")[case_cols]
    cases = cases[["case_ars_no", "BGV ID1 Pre Joining", "Process",
                   "Verification Type", "Employee ID", "Case_status"]]

    # slots present in data but not in the requested layout are appended
    present = list(dict.fromkeys(df.slot))
    extra = [s for s in present if s not in CHECK_ORDER]
    order = CHECK_ORDER + sorted(extra)

    # Lookup key = ARS number + check name (slot), e.g. "1951-366241Passport Verification"
    df["key"] = df["case_ars_no"].astype(str).str.strip() + df["slot"]
    by_key = df.drop_duplicates("key").set_index("key")
    case_keys = cases["case_ars_no"].astype(str).str.strip()

    parts = [cases]
    for slot in order:
        keys = case_keys + slot
        for sub in SUB_COLS:
            col = keys.map(by_key[SUB_SRC[sub]])
            parts.append(col.rename((slot, sub)).to_frame())
    out = pd.concat(parts, axis=1)
    return out, extra


def write_excel(out, path):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter as L

    cols = list(out.columns)
    fixed = [c for c in cols if not isinstance(c, tuple)]
    nfix = len(fixed)

    wb = Workbook()
    ws = wb.active
    ws.title = "Tracker"
    ws.sheet_view.showGridLines = False

    side = Side(style="thin", color="808080")
    border = Border(left=side, right=side, top=side, bottom=side)
    font = Font(name="Calibri", size=10)
    hfont = Font(name="Calibri", size=10, bold=True, color="FFFFFF")
    navy = PatternFill("solid", fgColor="1F3864")
    blocks = [PatternFill("solid", fgColor="2F5597"), PatternFill("solid", fgColor="548235")]
    subs = [PatternFill("solid", fgColor="D9E2F3"), PatternFill("solid", fgColor="E2EFDA")]
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left = Alignment(horizontal="left", vertical="center", wrap_text=False)

    # Header rows 1-2
    for i, name in enumerate(fixed, start=1):
        ws.merge_cells(start_row=1, start_column=i, end_row=2, end_column=i)
        for r in (1, 2):
            c = ws.cell(row=r, column=i); c.border = border; c.fill = navy
        c = ws.cell(row=1, column=i, value=name); c.font = hfont; c.alignment = center
    col = nfix + 1
    k = 0
    while col <= len(cols):
        slot = cols[col - 1][0]
        ws.merge_cells(start_row=1, start_column=col, end_row=1, end_column=col + len(SUB_COLS) - 1)
        for j in range(len(SUB_COLS)):
            c = ws.cell(row=1, column=col + j); c.border = border; c.fill = blocks[k % 2]
            s2 = ws.cell(row=2, column=col + j, value=cols[col + j - 1][1])
            s2.font = Font(name="Calibri", size=10, bold=True); s2.fill = subs[k % 2]
            s2.alignment = Alignment(horizontal="center", vertical="center", wrap_text=False); s2.border = border
        h = ws.cell(row=1, column=col, value=slot); h.font = hfont; h.alignment = center
        col += len(SUB_COLS); k += 1
    ws.row_dimensions[1].height = 34
    ws.row_dimensions[2].height = 20

    # Data from row 3
    id_cols = {i for i, n in enumerate(fixed, start=1) if n in ("BGV ID1 Pre Joining", "Employee ID")}
    is_date = [str(t).startswith("datetime") for t in out.dtypes]
    for r, row in enumerate(out.itertuples(index=False, name=None), start=3):
        for ci, v in enumerate(row, start=1):
            if v is None or v != v:  # None / NaN / NaT
                v = None
            elif hasattr(v, "to_pydatetime"):
                v = v.to_pydatetime()
            if ci in id_cols and isinstance(v, str):
                t = v.strip()
                # numeric IDs -> real numbers (avoids "number stored as text" flag);
                # leading zeros / long IDs stay text so they are not corrupted
                v = int(t) if t.isdigit() and t[0] != "0" and len(t) <= 15 else t
            c = ws.cell(row=r, column=ci, value=v)
            c.font = font; c.border = border
            if is_date[ci - 1]:
                c.number_format = "dd-mmm-yyyy"; c.alignment = Alignment(horizontal="center", vertical="center")
            elif ci in id_cols:
                c.alignment = Alignment(horizontal="left", vertical="center"); c.number_format = "0"
            else:
                c.alignment = left

    # Widths
    widths = {"case_ars_no": 16, "BGV ID1 Pre Joining": 18, "Process": 34,
              "Verification Type": 16, "Employee ID": 16, "Case_status": 18}
    for i, name in enumerate(fixed, start=1):
        ws.column_dimensions[L(i)].width = widths.get(name, 18)
    for ci in range(nfix + 1, len(cols) + 1):
        sub = cols[ci - 1][1]
        ws.column_dimensions[L(ci)].width = len(sub) + 6  # fit the row-2 header

    ws.freeze_panes = ws.cell(row=3, column=2)
    ws.auto_filter.ref = f"A2:{L(len(cols))}{len(out) + 2}"
    ws.sheet_properties.tabColor = "1F3864"
    wb.save(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", type=int, default=CLIENT_EXTERNAL_ID, help="client_external_id")
    ap.add_argument("--from", dest="date_from")
    ap.add_argument("--to", dest="date_to")
    ap.add_argument("--process", help="process_name contains (LIKE %%text%%)")
    ap.add_argument("--ars", help="comma-separated case_ars_no list")
    ap.add_argument("--limit", type=int, help="only the latest N cases from Client Wise All Cases (testing)")
    ap.add_argument("--out", default=str(BASE / "output" / f"Tracker_{CLIENT_EXTERNAL_ID}.xlsx"))
    args = ap.parse_args()

    conn = connect()
    try:
        row = query(conn, "SELECT client_id FROM ec_client WHERE client_external_id=%s", (args.client,))
        if row.empty:
            raise SystemExit(f"No client with external id {args.client}")
        client_id = int(row.client_id[0])
        print(f"Client {args.client} -> client_id {client_id}; fetching ...")
        ars_list = fetch_ars(conn, args.client, args)
        print(f"{len(ars_list)} ARS numbers from Client Wise All Cases")
        df = fetch_cases(conn, client_id, ars_list)
    finally:
        conn.close()

    if df.empty:
        raise SystemExit("No rows matched.")
    print(f"{df.case_id.nunique()} cases, {len(df)} check rows")
    out, extra = build_tracker(df)
    if extra:
        print("Check slots not in the requested layout (appended at the end):")
        for s in sorted(extra):
            print("  ", s)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    write_excel(out, args.out)
    print("Saved", args.out)


if __name__ == "__main__":
    main()
