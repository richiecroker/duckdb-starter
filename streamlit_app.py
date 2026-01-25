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


# assume `result` exists and has the exact columns you listed
df = result.copy()
for c in ["icb_name","icb_code","pcn_name","pcn_code","practice_name","practice_code"]:
    df[c] = df[c].astype(str).fillna("").str.strip()

st.header("Filters: ICB → PCN → Practice")

# ICB: show name but map to code
icb_pairs = df[["icb_code","icb_name"]].drop_duplicates().sort_values("icb_name")
icb_labels = [f"{r.icb_name} ({r.icb_code})" if r.icb_name else r.icb_code for r in icb_pairs.itertuples()]
icb_map = {lab: row.icb_code for lab,row in zip(icb_labels, icb_pairs.itertuples())}

selected_icbs = st.multiselect("ICB", icb_labels, default=icb_labels)
selected_icb_codes = [icb_map[l] for l in selected_icbs]

df_icb = df[df["icb_code"].isin(selected_icb_codes)] if selected_icb_codes else df.iloc[0:0]

# PCN (dependent)
pcn_pairs = df_icb[["pcn_code","pcn_name"]].drop_duplicates().sort_values("pcn_name")
pcn_labels = [f"{r.pcn_name} ({r.pcn_code})" if r.pcn_name else r.pcn_code for r in pcn_pairs.itertuples()]
pcn_map = {lab: row.pcn_code for lab,row in zip(pcn_labels, pcn_pairs.itertuples())}

selected_pcns = st.multiselect("PCN", pcn_labels, default=pcn_labels)
selected_pcn_codes = [pcn_map[l] for l in selected_pcns]

df_pcn = df_icb[df_icb["pcn_code"].isin(selected_pcn_codes)] if selected_pcn_codes else df_icb.iloc[0:0]

# Practices (final)
practice_pairs = df_pcn[["practice_code","practice_name"]].drop_duplicates().sort_values("practice_name")
practice_labels = [f"{r.practice_name} ({r.practice_code})" if r.practice_name else r.practice_code for r in practice_pairs.itertuples()]
practice_map = {lab: row.practice_code for lab,row in zip(practice_labels, practice_pairs.itertuples())}

selected_practices = st.multiselect("Practice", practice_labels, default=practice_labels)
practice_codes = [practice_map[l] for l in selected_practices]

st.write("Selected practice_codes:", practice_codes)