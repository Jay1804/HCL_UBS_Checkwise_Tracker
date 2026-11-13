import io
from types import SimpleNamespace

import streamlit as st

import build_tracker as bt

st.set_page_config(page_title="Check-wise Tracker", layout="wide")
st.title("Check-wise Tracker")


def df_empty():
    import pandas as pd
    return pd.DataFrame()


with st.sidebar:
    client = st.number_input("Client external ID", value=bt.CLIENT_EXTERNAL_ID, step=1)
    process = st.text_input("Process name contains", value="UBS")
    use_dates = st.checkbox("Filter by received date")
    d_from = d_to = None
    if use_dates:
        d_from = st.date_input("From").isoformat()
        d_to = st.date_input("To").isoformat()
    go = st.button("Generate tracker", type="primary")

if go:
    args = SimpleNamespace(date_from=d_from, date_to=d_to, ars=None, process=process.strip() or None,
                           limit=None)
    with st.spinner("Querying database..."):
        conn = bt.connect()
        try:
            row = bt.query(conn, "SELECT client_id FROM ec_client WHERE client_external_id=%s", (int(client),))
            if row.empty:
                st.error(f"No client with external ID {int(client)}")
                st.stop()
            ars_list = bt.fetch_ars(conn, int(client), args)
            st.info(f"{len(ars_list)} ARS numbers from Client Wise All Cases")
            df = bt.fetch_cases(conn, int(row.client_id[0]), ars_list) if ars_list else df_empty()
        finally:
            conn.close()
    if df.empty:
        st.warning("No rows matched.")
        st.stop()
    out, extra = bt.build_tracker(df)
    st.session_state["out"] = out
    st.session_state["extra"] = extra
    st.session_state["stats"] = (df.case_id.nunique(), len(df))

if "out" in st.session_state:
    out = st.session_state["out"]
    cases, rows = st.session_state["stats"]
    st.success(f"{cases} cases, {rows} check rows, {out.shape[1]} columns")
    if st.session_state["extra"]:
        with st.expander(f"{len(st.session_state['extra'])} check slots not in the requested layout (appended at end)"):
            st.write(sorted(st.session_state["extra"]))

    # hide all-empty check blocks in the preview only
    flat = out.copy()
    flat.columns = [c if not isinstance(c, tuple) else f"{c[0]} | {c[1]}" for c in flat.columns]
    hide = st.checkbox("Hide empty columns in preview", value=True)
    st.dataframe(flat.dropna(axis=1, how="all") if hide else flat, use_container_width=True, height=500)

    buf = io.BytesIO()
    bt.write_excel(out, buf)
    st.download_button("Download Excel", buf.getvalue(), file_name="Tracker.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
