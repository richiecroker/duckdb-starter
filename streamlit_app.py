import re
import streamlit as st
import duckdb
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import pandas as pd
import itertools
import plotly.graph_objects as go
import os
from datetime import datetime

@st.cache_resource
def get_duckdb_connection():
    """Get DuckDB connection with smart caching"""
    
    local_db = "/tmp/app.duckdb"
    bucket_name = "ebmdatalab"
    gcs_db_path = "RC_tests/app.duckdb"
    gcs_metadata_path = "RC_tests/last_updated.txt"
    
    # Set up credentials (shared by both clients)
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    storage_client = storage.Client(credentials=credentials)
    bq_client = bigquery.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    
    # Step 1: Check if local DuckDB has data
    needs_data = True
    if os.path.exists(local_db):
        conn = duckdb.connect(local_db)
        try:
            result = conn.execute("SELECT COUNT(*) FROM ome_data").fetchone()
            if result[0] > 0:
                needs_data = False
        except:
            pass
        if needs_data:
            conn.close()
    
    if needs_data:
        with st.spinner("Checking data freshness..."):
            # Step 2: Get latest month from BigQuery
            bq_query = "SELECT MAX(month) as max_month FROM measures.vw__opioids_total_dmd_bs"
            bq_max_month = bq_client.query(bq_query).to_dataframe()['max_month'][0]
            
            # Step 3: Check cached month in GCS
            metadata_blob = bucket.blob(gcs_metadata_path)
            use_cache = False
            
            if metadata_blob.exists():
                cached_month_str = metadata_blob.download_as_text().strip()
                try:
                    cached_month = datetime.fromisoformat(cached_month_str).date()
                except:
                    cached_month = cached_month_str
                
                if str(bq_max_month) == str(cached_month):
                    use_cache = True
        
        # Step 4: Either download from GCS or rebuild from BQ
        if use_cache:
            with st.spinner("Loading data from cache..."):
                db_blob = bucket.blob(gcs_db_path)
                db_blob.download_to_filename(local_db)
        else:
            with st.spinner(f"Loading data from BigQuery (this may take a few minutes)..."):
                build_duckdb_from_bigquery(local_db, bq_client)
                
                # Upload to GCS
                db_blob = bucket.blob(gcs_db_path)
                db_blob.upload_from_filename(local_db)
                
                # Update metadata
                metadata_blob.upload_from_string(str(bq_max_month))
        
        conn = duckdb.connect(local_db)
    
    return conn

def build_duckdb_from_bigquery(db_path, bq_client):
    """Build DuckDB from BigQuery data"""
    conn = duckdb.connect(db_path)
    
    # Define tables and their corresponding SQL files
    tables = {
        "ome_data": "queries/vw__opioids_total_dmd_bs.sql",
        "practices": "queries/practices.sql",
        "pcns": "queries/pcns.sql",
        "ccgs": "queries/ccgs.sql",
        "stps": "queries/stps.sql"
    }
    
    for table_name, sql_file in tables.items():
        # Read SQL query from file
        with open(sql_file, 'r') as f:
            query = f.read()
        
        # Execute query and load into DuckDB
        df = bq_client.query(query).to_dataframe()
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    
    conn.close()

# Use it:
conn = get_duckdb_connection()

# Get max date and show header
try:
    max_date = conn.execute("SELECT MAX(month) FROM ome_data").fetchone()[0]
    if max_date:
        end_date = pd.to_datetime(max_date)
        start_date = end_date - pd.DateOffset(months=2)
        date_range = f"{start_date.strftime('%B')} - {end_date.strftime('%B %Y')}"
        st.title("Opioid Prescribing Dashboard")
        st.markdown(f"### Data period: {date_range}")
    else:
        st.title("Opioid Prescribing Dashboard")
except Exception as e:
    st.title("Opioid Prescribing Dashboard")
    st.warning(f"Could not load date range: {e}")

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
    st.info("If the table is missing, run the loader page first.")


df = result.copy()

# creates cascading filters for data

ALL = "ALL"

st.markdown(f"#### Select Organisation")

# Level 1: ICB
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

# create codes_df for filtering data
codes_df = pd.DataFrame({"practice_code": practice_codes})

#register as virtual table with duckdb
conn.register("_selected_practices", codes_df)

#get OME summary data from duckdb for selected practices
ome_result = conn.execute("""
    SELECT COALESCE(bs_subid, ing) AS bs_ing, bs_nm, SUM(ome_dose) AS ome_dose
    FROM ome_data t
    JOIN _selected_practices s
      ON t.practice = s.practice_code
    GROUP BY bs_ing, bs_nm
""").fetchdf()

#unregister virtual table
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
st.dataframe(detail_result, use_container_width=True, hide_index=True)
