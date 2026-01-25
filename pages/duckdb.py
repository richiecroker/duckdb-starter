# pages/duckdb.py

import re
import itertools
import streamlit as st
import duckdb
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

# ------------------------------------------------------------
# Page config
# ------------------------------------------------------------
st.set_page_config(page_title="DuckDB · BigQuery", page_icon="🦆")
st.title("🦆 DuckDB + Streamlit + BigQuery")

# ------------------------------------------------------------
# Step 1: DuckDB connection (PERSISTENT)
# ------------------------------------------------------------
st.header("Step 1: DuckDB Connection")

# File-backed DB so tables survive reruns
conn = duckdb.connect("app.duckdb")
st.success("✅ DuckDB connected (file-backed: app.duckdb)")

def list_duckdb_tables(conn):
    try:
        return conn.execute("SHOW TABLES").fetchdf()
    except Exception:
        return pd.DataFrame()

# ------------------------------------------------------------
# Step 2: BigQuery connection
# ------------------------------------------------------------
st.header("Step 2: Load Data from BigQuery")

try:
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    bq_client = bigquery.Client(credentials=credentials)
    st.success("✅ Connected to BigQuery")
except Exception as e:
    st.error(f"BigQuery connection failed: {e}")
    bq_client = None

# ------------------------------------------------------------
# Step 3: Run BigQuery query
# ------------------------------------------------------------
if bq_client:
    with st.form("bq_form"):
        query = st.text_area(
            "BigQuery SQL",
            value="SELECT * FROM `your-project.your-dataset.your-table` LIMIT 1000",
            height=120,
        )

        table_name = st.text_input(
            "DuckDB table name",
            value="practices",
        )

        overwrite = st.checkbox("Overwrite if table exists", value=True)
        submit = st.form_submit_button("Load into DuckDB")

    if submit:
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name):
            st.error("Invalid DuckDB table name")
        else:
            with st.spinner("Querying BigQuery..."):
                arrow = bq_client.query(query).to_arrow()
                df = arrow.to_pandas()

            st.success(f"Loaded {len(df):,} rows")

            # ------------------------------------------------------------
            # Create REAL DuckDB table
            # ------------------------------------------------------------
            tmp_view = "__tmp_bq_load"

            conn.execute(f"DROP VIEW IF EXISTS {tmp_view}")
            conn.register(tmp_view, df)

            final_name = table_name

            tables = list_duckdb_tables(conn)
            existing = tables["name"].tolist() if not tables.empty else []

            if final_name in existing:
                if overwrite:
                    conn.execute(f"DROP TABLE IF EXISTS {final_name}")
                else:
                    for i in itertools.count(1):
                        candidate = f"{table_name}_{i}"
                        if candidate not in existing:
                            final_name = candidate
                            st.warning(f"Using table name `{final_name}`")
                            break

            conn.execute(
                f"CREATE TABLE {final_name} AS SELECT * FROM {tmp_view}"
            )
            conn.unregister(tmp_view)

            st.session_state["last_table"] = final_name
            st.success(f"✅ Created DuckDB table `{final_name}`")

# ------------------------------------------------------------
# Step 4: Inspect DuckDB
# ------------------------------------------------------------
st.header("Step 3: DuckDB Tables")

tables_df = list_duckdb_tables(conn)
if tables_df.empty:
    st.info("No tables in DuckDB")
else:
    st.dataframe(tables_df)

# ------------------------------------------------------------
# Step 5: Query DuckDB
# ------------------------------------------------------------
if not tables_df.empty:
    table_names = tables_df["name"].tolist()
    default = st.session_state.get("last_table", table_names[0])

    table = st.selectbox(
        "Table to query",
        options=table_names,
        index=table_names.index(default) if default in table_names else 0,
    )

    duckdb_query = st.text_area(
        "DuckDB SQL",
        value=f"SELECT * FROM {table} LIMIT 10",
        height=120,
    )

    if st.button("Run DuckDB Query"):
        result = conn.execute(duckdb_query).fetchdf()
        st.dataframe(result)
        st.caption(f"Returned {len(result):,} rows")
