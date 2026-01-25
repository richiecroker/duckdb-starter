# pages/duckdb.py
import re
import streamlit as st
import duckdb
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import itertools

st.title("Main app (app.py)")


# pages/query_practices.py (consumer)
import streamlit as st
import duckdb

conn = duckdb.connect("app.duckdb")  # same file

# optional: show tables
st.write(conn.execute("SHOW TABLES").fetchdf())

try:
    result = conn.execute(
        """
        SELECT
        stps.name AS icb_name,
        stps.code AS icb_code,
        pcns.name AS pcn_name, 
        pcns.code AS pcn_code,
        practices.name AS practice_name,
        practices.code AS practice_code
        FROM practices
        INNER JOIN
        pcns
        ON
        pcns.code = practices.pcn_id
        INNER JOIN
        ccgs
        ON
        ccgs.code = practices.ccg_id
        INNER JOIN
        stps
        ON
        ccgs.stp_id = stps.code
        WHERE practices.close_date IS NULL 
        AND setting = 4
        """).fetchdf()
    st.dataframe(result)
    st.caption(f"Returned {len(result):,} rows")
except Exception as e:
    st.error(f"Query failed: {e}")
    st.info("If the table is missing, run the loader page first to create `practices`.")


import streamlit as st
import pandas as pd

# ---------------------------
# Config: the column names in your DataFrame
# Adjust these if your columns are named differently.
# ---------------------------
icb_col = "stps"           # column that contains STP codes/names
PCN_COL = "PCN"            # column that contains PCN codes/names
PRACTICE_COL = "practice_code"  # column that contains practice code (the final key)

# ---------------------------
# Guard: make sure 'result' exists
# ---------------------------
if 'result' not in globals():
    st.error("No `result` DataFrame found. This code expects a pandas DataFrame named `result`.")
else:
    # work on a copy with string-casted columns for robust comparisons
    df = result.copy()
    for c in (STP_COL, PCN_COL, PRACTICE_COL):
        if c in df.columns:
            df[c] = df[c].astype(str).fillna("").str.strip()
        else:
            # create empty column to avoid KeyError and let user see missing column message
            df[c] = ""

    st.header("Cascading filters: STP → PCN → Practice")

    # ---------------------------
    # Build STP options
    # ---------------------------
    stp_options = sorted(df[STP_COL].replace("", pd.NA).dropna().unique().tolist())
    # session_state defaults so selection persists
    if "selected_stps" not in st.session_state:
        st.session_state.selected_stps = stp_options.copy()  # default: all selected
    selected_stps = st.multiselect("Filter by STP", options=stp_options, default=st.session_state.selected_stps)
    st.session_state.selected_stps = selected_stps

    # apply STP filter to produce PCN options
    df_after_stp = df[df[STP_COL].isin([str(x) for x in selected_stps])] if selected_stps else df.iloc[0:0]

    # ---------------------------
    # Build PCN options (dependent on STP)
    # ---------------------------
    pcn_options = sorted(df_after_stp[PCN_COL].replace("", pd.NA).dropna().unique().tolist())
    if "selected_pcns" not in st.session_state:
        st.session_state.selected_pcns = pcn_options.copy()
    selected_pcns = st.multiselect("Filter by PCN", options=pcn_options, default=st.session_state.selected_pcns)
    st.session_state.selected_pcns = selected_pcns

    # apply PCN filter to produce Practice options
    df_after_pcn = df_after_stp[df_after_stp[PCN_COL].isin([str(x) for x in selected_pcns])] if selected_pcns else df_after_stp.iloc[0:0]

    # ---------------------------
    # Build Practice options (dependent on PCN)
    # ---------------------------
    practice_options = sorted(df_after_pcn[PRACTICE_COL].replace("", pd.NA).dropna().unique().tolist())
    if "selected_practices_ui" not in st.session_state:
        st.session_state.selected_practices_ui = practice_options.copy()
    selected_practices_ui = st.multiselect("Filter by Practice (final)", options=practice_options, default=st.session_state.selected_practices_ui)
    st.session_state.selected_practices_ui = selected_practices_ui

    # ---------------------------
    # Final list of practice codes
    # ---------------------------
    practice_codes = [str(x) for x in selected_practices_ui]

    st.subheader("Selected practice codes")
    if practice_codes:
        st.write(practice_codes)
        st.caption(f"{len(practice_codes):,} selected")
    else:
        st.info("No practice codes selected (or no options available after previous filters).")

    # ---------------------------
    # Example: use these practice_codes in a DuckDB query (safe)
    # We'll register the list as a tiny DataFrame and join in DuckDB.
    # Requires `conn` to be the same DuckDB connection used by your app.
    # ---------------------------
    if 'conn' in globals() and practice_codes:
        if st.button("Run DuckDB query for selected practices"):
            # create a tiny DF and register as a temp view name that won't conflict
            sel_df = pd.DataFrame({PRACTICE_COL: practice_codes})
            tmp_name = "__selected_practices"
            try:
                # drop previous view/table if it exists
                try:
                    conn.execute(f"DROP VIEW IF EXISTS {tmp_name}")
                except Exception:
                    pass
                conn.register(tmp_name, sel_df)

                # Example query: replace other_table with the real table you want to filter
                sql = f"""
                SELECT t.*
                FROM other_table AS t
                JOIN {tmp_name} AS s
                  ON CAST(t.{PRACTICE_COL} AS VARCHAR) = s.{PRACTICE_COL}
                """
                # execute and show
                out = conn.execute(sql).fetchdf()
                st.dataframe(out)
                st.caption(f"Returned {len(out):,} rows")
            finally:
                # unregister the temporary relation
                try:
                    conn.unregister(tmp_name)
                except Exception:
                    pass
    else:
        if 'conn' not in globals():
            st.info("DuckDB connection `conn` not found in scope — can't run the example query.")

