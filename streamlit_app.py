import streamlit as st
import duckdb
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

st.title("🦆 DuckDB + Streamlit + BigQuery")

# Step 1: Initialize DuckDB
st.header("Step 1: DuckDB Connection")
conn = duckdb.connect(':memory:')
st.success("✅ DuckDB connected (in-memory)")

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
            height=100,
            help="Write your BigQuery SQL query here"
        )
        
        submit = st.form_submit_button("Load Data from BigQuery")

    if submit:
        try:
            with st.spinner("Loading data from BigQuery..."):
                # Execute query and convert to PyArrow
                st.info("Executing query...")
                query_job = client.query(query)
                
                # Get results as PyArrow table (efficient for large datasets)
                arrow_table = query_job.to_arrow()
                
                # Convert to pandas for display
                df = arrow_table.to_pandas()
                
                st.success(f"✅ Loaded {len(df):,} rows from BigQuery")
                
                # Store in session state
                st.session_state['df'] = df
                st.session_state['arrow_table'] = arrow_table
                
        except Exception as e:
            st.error(f"Error querying BigQuery: {e}")

# Step 3: Load into DuckDB and Query
if 'df' in st.session_state:
    st.header("Step 3: Data in DuckDB")
    
    df = st.session_state['df']
    
    # Register the dataframe with DuckDB
    conn.register('bq_data', df)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Rows", f"{len(df):,}")
    with col2:
        st.metric("Columns", len(df.columns))
    with col3:
        st.metric("Size", f"{df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    # Show preview
    with st.expander("Preview Data"):
        st.dataframe(df.head(20))
    
    # Step 4: Query with DuckDB
    st.header("Step 4: Query with DuckDB")
    
    st.info("Now you can query the 'bq_data' table using DuckDB SQL")
    
    duckdb_query = st.text_area(
        "DuckDB SQL Query",
        value="SELECT * FROM bq_data LIMIT 10",
        height=100
    )
    
    if st.button("Run DuckDB Query"):
        try:
            result = conn.execute(duckdb_query).fetchdf()
            st.dataframe(result)
            
            # Show query stats
            st.caption(f"Returned {len(result):,} rows")
            
        except Exception as e:
            st.error(f"Query error: {e}")

else:
    st.info("👆 Add your BigQuery credentials to Streamlit secrets and reload the page")
    
    st.markdown("---")
    st.subheader("📋 Setup: Streamlit Secrets")
    st.markdown("""
    Create a file `.streamlit/secrets.toml` with your service account details:
    
    ```toml
    [gcp_service_account]
    type = "service_account"
    project_id = "your-project-id"
    private_key_id = "your-private-key-id"
    private_key = "-----BEGIN PRIVATE KEY-----\nYour-Private-Key\n-----END PRIVATE KEY-----\n"
    client_email = "your-service-account@your-project.iam.gserviceaccount.com"
    client_id = "your-client-id"
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com"
    ```
    
    **Or** if deploying to Streamlit Cloud, add these in the app settings under "Secrets".
    """)