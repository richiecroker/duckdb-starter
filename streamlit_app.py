import streamlit as st
from google.cloud import storage, bigquery
from google.oauth2 import service_account

st.title("GCP Access Test")

# Set up credentials
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

# Test BigQuery READ
st.subheader("BigQuery Read Test")
try:
    bq_client = bigquery.Client(credentials=credentials)
    query = "SELECT MAX(month) as max_month FROM measures.global_data_lpzomnibus"
    result = bq_client.query(query).to_dataframe()
    st.success("✅ BigQuery READ successful!")
    st.write(f"Latest month: {result['max_month'][0]}")
except Exception as e:
    st.error(f"❌ BigQuery READ failed: {e}")

# Test GCS READ
st.subheader("GCS Read Test")
try:
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket("ebmdatalab")
    
    # Try to list files in RC_tests folder
    blobs = list(bucket.list_blobs(prefix="RC_tests/", max_results=5))
    st.success("✅ GCS READ successful!")
    st.write(f"Found {len(blobs)} files in RC_tests/:")
    for blob in blobs:
        st.write(f"  - {blob.name}")
except Exception as e:
    st.error(f"❌ GCS READ failed: {e}")

# Test GCS WRITE
st.subheader("GCS Write Test")
try:
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket("ebmdatalab")
    test_blob = bucket.blob("RC_tests/test_write.txt")
    
    test_content = f"Test write at {st.session_state.get('test_time', 'unknown')}"
    if 'test_time' not in st.session_state:
        from datetime import datetime
        st.session_state.test_time = datetime.now().isoformat()
        test_content = f"Test write at {st.session_state.test_time}"
    
    test_blob.upload_from_string(test_content)
    st.success("✅ GCS WRITE successful!")
    
    # Read it back to verify
    read_back = test_blob.download_as_text()
    st.write(f"Content written and verified: {read_back}")
except Exception as e:
    st.error(f"❌ GCS WRITE failed: {e}")
