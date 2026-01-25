# pages/duckdb.py
import re
import streamlit as st
import duckdb
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import itertools
import plotly.graph_objects as go

st.title("Main app (app.py)")


# pages/query_practices.py (consumer)


conn = duckdb.connect("app.duckdb")  # same file

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


codes_df = pd.DataFrame({"practice_code": practice_codes})

conn.register("_selected_practices", codes_df)

ome_result = conn.execute("""
    SELECT COALESCE(bs_subid, ing) AS bs_ing, bs_nm, SUM(ome_dose) AS ome_dose
    FROM ome_data t
    JOIN _selected_practices s
      ON t.practice = s.practice_code
    GROUP BY bs_ing, bs_nm
""").fetchdf()

conn.unregister("_selected_practices")

# ---- Calculate total and percentages ----
total = ome_result["ome_dose"].sum()
ome_result["percentage"] = (ome_result["ome_dose"] / total * 100).round(1)

# ---- Create display name: original name or "Other" if < 1% ----
threshold = 1.0
ome_result["display_name"] = ome_result.apply(
    lambda row: row["bs_nm"] if row["percentage"] >= threshold else "Other",
    axis=1
)

# ---- Group by display name (this combines all "Other" rows) ----
ome_grouped = ome_result.groupby("display_name", as_index=False).agg({
    "ome_dose": "sum",
    "bs_ing": "first",  # Keep the first bs_ing for each group
    "bs_nm": "first"    # Keep the first bs_nm for each group
})

# ---- Recalculate percentages after grouping ----
ome_grouped["percentage"] = (ome_grouped["ome_dose"] / total * 100).round(1)

# ---- Create custom labels ----
ome_grouped["label"] = ome_grouped.apply(
    lambda row: f"{row['display_name']}<br>{row['ome_dose']:.1f} ({row['percentage']:.1f}%)", 
    axis=1
)

# ---- Create donut chart ----
fig = go.Figure(data=[go.Pie(
    labels=ome_grouped["display_name"],
    values=ome_grouped["ome_dose"],
    hole=0.5,
    textposition='outside',
    textinfo='label+percent',
    hovertemplate='<b>%{label}</b><br>Amount: %{value:.1f}<br>Percentage: %{percent}<extra></extra>',
    marker=dict(
        line=dict(width=1, color="white")
    ),
    pull=[0.05] * len(ome_grouped),
    textfont=dict(size=14),
    insidetextorientation='radial',
    automargin=True
)])

# ---- update layout ----
fig.update_layout(
    showlegend=False,
    margin=dict(l=150, r=150, t=100, b=100),  # More balanced margins
    height=700,  # Taller to accommodate labels
)

# ---- render ----
st.plotly_chart(fig, use_container_width=True)

# Substance (bs_ing and bs_nm)
bs_pairs = ome_grouped[["bs_ing","bs_nm"]].drop_duplicates().sort_values("bs_nm")
bs_opts = [ALL] + [r.bs_nm for r in bs_pairs.itertuples()]
bs_map = dict(zip(bs_pairs["bs_nm"], bs_pairs["bs_ing"]))  # Map name to code
sel_bs = st.selectbox("Substance", bs_opts, index=0, key="substance_select")
sel_bs_codes = ome_grouped["bs_ing"].unique().tolist() if sel_bs == ALL else [bs_map[sel_bs]]
df_bs = ome_grouped[ome_grouped["bs_ing"].isin(sel_bs_codes)]

# Create a temporary table with selected substance codes
bs_codes_df = pd.DataFrame({"bs_ing": sel_bs_codes})
conn.register("_selected_substances", bs_codes_df)
conn.register("_selected_practices", codes_df)
# Query with both practice and substance filters
detail_result = conn.execute("""
    SELECT 
        t.bnf_name,
        SUM(t.ome_dose) as total_ome
    FROM ome_data t
    JOIN _selected_practices s
      ON t.practice = s.practice_code
    JOIN _selected_substances b
      ON COALESCE(t.bs_subid, t.ing) = b.bs_ing
    GROUP BY t.bnf_name
    ORDER BY total_ome DESC
""").fetchdf()

conn.unregister("_selected_substances")
conn.unregister("_selected_practices")
st.dataframe(detail_result)