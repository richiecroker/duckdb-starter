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


df = result.copy()
for c in ["icb_name","icb_code","pcn_name","pcn_code","practice_name","practice_code"]:
    df[c] = df[c].astype(str).fillna("").str.strip()

ALL = "ALL"

# ICB
icb_pairs = df[["icb_code","icb_name"]].drop_duplicates().sort_values("icb_name")
icb_opts = [ALL] + [f"{r.icb_name} ({r.icb_code})" for r in icb_pairs.itertuples()]
icb_map = {opt: opt.split(" (")[-1][:-1] for opt in icb_opts if opt != ALL}
sel_icb = st.selectbox("ICB", icb_opts, index=0)
sel_icb_codes = df["icb_code"].unique().tolist() if sel_icb == ALL else [icb_map[sel_icb]]
df_icb = df[df["icb_code"].isin(sel_icb_codes)]

# PCN (dependent)
pcn_pairs = df_icb[["pcn_code","pcn_name"]].drop_duplicates().sort_values("pcn_name")
pcn_opts = [ALL] + [f"{r.pcn_name} ({r.pcn_code})" for r in pcn_pairs.itertuples()]
pcn_map = {opt: opt.split(" (")[-1][:-1] for opt in pcn_opts if opt != ALL}
sel_pcn = st.selectbox("PCN", pcn_opts, index=0)
sel_pcn_codes = df_icb["pcn_code"].unique().tolist() if sel_pcn == ALL else [pcn_map[sel_pcn]]
df_pcn = df_icb[df_icb["pcn_code"].isin(sel_pcn_codes)]

# Practice (final)
pr_pairs = df_pcn[["practice_code","practice_name"]].drop_duplicates().sort_values("practice_name")
pr_opts = [ALL] + [f"{r.practice_name} ({r.practice_code})" for r in pr_pairs.itertuples()]
pr_map = {opt: opt.split(" (")[-1][:-1] for opt in pr_opts if opt != ALL}
sel_pr = st.selectbox("Practice", pr_opts, index=0)
practice_codes = df_pcn["practice_code"].unique().tolist() if sel_pr == ALL else [pr_map[sel_pr]]

st.write("practice_codes:", practice_codes)