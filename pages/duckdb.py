# pages/duckdb.py
import re
import streamlit as st
import duckdb
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import itertools

st.set_page_config(page_title="DuckDB · BigQuery", page_icon="🦆")
st.title("🦆 DuckDB + Streamlit + BigQuery (page)")

# Step 1: Initialize DuckDB
st.header("Step 1: DuckDB Connection")
conn = duckdb.connect(":memory:")
st.success("✅ DuckDB connected (in-memory)")

# helper: show existing DuckDB tables
def list_duckdb_tables(conn):
    try:
        df = conn.execute("SHOW TABLES").fetchdf()
        # DuckDB returns columns like 'name' (depends on version); show whatever exists
        if df.empty:
            return None
        return df
    except Exception:
        return None

# Step 2: Connect to BigQuery
st.header("Step 2: Load Data from BigQuery")

# Set up BigQuery credentials from Streamlit secrets
try:
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = bigquery.Client(credentials=credentials)
    st.success("✅ Connected to BigQuery using Streamlit secrets")
except Exception as e:
    st.error(f"Error loading credentials: {e}")
    st.info("Make sure you've added 'gcp_service_account' to your Streamlit secrets")
    client = None

if client:
    with st.form("bq_form"):
        st.subheader("BigQuery Query")
        query = st.text_area(
            "SQL Query",
            value="SELECT * FROM `your-project.your-dataset.your-table` LIMIT 1000",
            height=120,
            help="Write your BigQuery SQL query here"
        )

        # NEW: let user choose the DuckDB table name
        table_name = st.text_input(
            "DuckDB table name",
            value="bq_data",
            help="Name to register the DataFrame as inside DuckDB (letters, numbers, underscores)."
        )

        overwrite = st.checkbox("Overwrite if table exists", value=True)
        submit = st.form_submit_button("Load Data from BigQuery")

    if submit:
        # validate table name (simple: letters, numbers, underscores, cannot start with number)
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name):
            st.error("Invalid table name. Use only letters, numbers and underscores, and don't start with a number.")
        else:
            try:
                with st.spinner("Loading data from BigQuery..."):
                    st.info("Executing query...")
                    query_job = client.query(query)
                    arrow_table = query_job.to_arrow()
                    df = arrow_table.to_pandas()
                    st.success(f"✅ Loaded {len(df):,} rows from BigQuery")

                    # Store in session state
                    st.session_state['df'] = df
                    st.session_state['arrow_table'] = arrow_table
                    st.session_state['duck_table_name'] = table_name
                    st.session_state['duck_overwrite'] = overwrite

            except Exception as e:
                st.error(f"Error querying BigQuery: {e}")

# Step 3: Load into DuckDB and Query
if 'df' in st.session_state:
    st.header("Step 3: Data in DuckDB")
    df = st.session_state['df']
    chosen_name = st.session_state.get('duck_table_name', 'bq_data')
    overwrite = st.session_state.get('duck_overwrite', True)

    # If a table with that name already exists and overwrite requested, drop it first
    try:
        existing = list_duckdb_tables(conn)
        if existing is not None and chosen_name in existing.values:
            if overwrite:
                # try dropping table / view safely
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {chosen_name}")
                except Exception:
                    pass
                try:
                    conn.execute(f"DROP VIEW IF EXISTS {chosen_name}")
                except Exception:
                    pass
            else:
                st.warning(f"A table named '{chosen_name}' already exists in DuckDB. Enable overwrite to replace it.")
    
        # register the dataframe with the user-chosen name
        conn.register(chosen_name, df)
        st.success(f"✅ Registered dataframe as DuckDB table: `{chosen_name}`")
    except Exception as e:
        st.error(f"Error registering DataFrame with DuckDB: {e}")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Rows", f"{len(df):,}")
    with col2:
        st.metric("Columns", len(df.columns))
    with col3:
        st.metric("Size", f"{df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    # List tables
    st.subheader("DuckDB: Tables")
    tables_df = list_duckdb_tables(conn)
    if tables_df is None or tables_df.empty:
        st.info("No tables found in DuckDB.")
    else:
        st.dataframe(tables_df)

    # Show preview
    with st.expander("Preview Data"):
        st.dataframe(df.head(20))

    # Step 4: Query with DuckDB
    st.header("Step 4: Query with DuckDB")
    st.info(f"Now you can query the `{chosen_name}` table using DuckDB SQL")

    duckdb_query = st.text_area(
        "DuckDB SQL Query",
        value=f"SELECT * FROM {chosen_name} LIMIT 10",
        height=120
    )

    if st.button("Run DuckDB Query"):
        try:
            result = conn.execute(duckdb_query).fetchdf()
            st.dataframe(result)
            st.caption(f"Returned {len(result):,} rows")
        except Exception as e:
            st.error(f"Query error: {e}")

else:
    st.info("👆 Add your BigQuery credentials to Streamlit secrets and reload the page")
    st.markdown("---")
    st.subheader("📋 Setup: Streamlit Secrets")
    st.markdown("""
    Create a file `.streamlit/secrets.toml` with your service account details:
    (same instructions as before)
    """)