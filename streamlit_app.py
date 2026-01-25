import re
import streamlit as st
import duckdb
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

st.title("🦆 DuckDB + Streamlit + BigQuery")


