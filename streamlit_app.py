# pages/duckdb.py
import re
import streamlit as st
import duckdb
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import itertools

st.title("Main app (app.py)")


duckdb_query = """
SELECT * from practices
"""

result = conn.execute(duckdb_query).fetchdf()
st.dataframe(result)
st.caption(f"Returned {len(result):,} rows")
