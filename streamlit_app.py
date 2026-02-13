import streamlit as st
import duckdb
from google.cloud import storage, bigquery
from google.oauth2 import service_account
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
            # Check if tables exist and have data
            result = conn.execute("SELECT COUNT(*) FROM ome_data").fetchone()
            if result[0] > 0:
                needs_data = False
        except:
            pass
        if needs_data:
            conn.close()
    
    if needs_data:
        st.info("Checking data freshness...")
        
        # Step 2: Get latest month from BigQuery
        bq_query = "SELECT MAX(month) as max_month FROM measures.vw__opioids_total_dmd_bs"
        bq_max_month = bq_client.query(bq_query).to_dataframe()['max_month'][0]
        
        # Step 3: Check cached month in GCS
        metadata_blob = bucket.blob(gcs_metadata_path)
        use_cache = False
        
        if metadata_blob.exists():
            cached_month_str = metadata_blob.download_as_text().strip()
            # Handle both date and string formats
            try:
                cached_month = datetime.fromisoformat(cached_month_str).date()
            except:
                cached_month = cached_month_str
            
            if str(bq_max_month) == str(cached_month):
                use_cache = True
                st.success(f"Cache is up to date (month: {bq_max_month})")
        
        # Step 4: Either download from GCS or rebuild from BQ
        if use_cache:
            st.info("Loading from cache...")
            db_blob = bucket.blob(gcs_db_path)
            db_blob.download_to_filename(local_db)
        else:
            st.info(f"Rebuilding database from BigQuery (latest month: {bq_max_month})...")
            build_duckdb_from_bigquery(local_db, bq_client)
            
            # Upload to GCS
            st.info("Saving to cache...")
            db_blob = bucket.blob(gcs_db_path)
            db_blob.upload_from_filename(local_db)
            
            # Update metadata
            metadata_blob.upload_from_string(str(bq_max_month))
            st.success("Cache updated!")
        
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
        st.info(f"Loading {table_name}...")
        
        # Read SQL query from file
        with open(sql_file, 'r') as f:
            query = f.read()
        
        # Execute query and load into DuckDB
        df = bq_client.query(query).to_dataframe()
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    
    conn.close()
    st.success("All tables loaded successfully!")

# Use it:
conn = get_duckdb_connection()
