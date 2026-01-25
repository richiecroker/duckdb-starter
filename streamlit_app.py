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
    result = conn.execute("SELECT * FROM practices WHERE close_date IS NULL AND setting = 4").fetchdf()
    st.dataframe(result)
    st.caption(f"Returned {len(result):,} rows")
except Exception as e:
    st.error(f"Query failed: {e}")
    st.info("If the table is missing, run the loader page first to create `practices`.")
