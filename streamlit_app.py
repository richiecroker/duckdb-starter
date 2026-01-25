import streamlit as st
import duckdb

st.title("🦆 DuckDB + Streamlit Starter")

# Step 1: Create a DuckDB connection
st.header("Step 1: Initialize DuckDB")

# Create an in-memory database
conn = duckdb.connect(':memory:')

st.success("✅ DuckDB connected (in-memory)")

# Step 2: Create some sample data to test
st.header("Step 2: Test with Sample Data")

# Create a simple table
conn.execute("""
    CREATE TABLE IF NOT EXISTS test_data AS 
    SELECT 
        'Item ' || i as name,
        i as id,
        (i * 10.5)::DECIMAL(10,2) as price
    FROM range(1, 11) as t(i)
""")

st.code("""
CREATE TABLE test_data AS 
SELECT 
    'Item ' || i as name,
    i as id,
    (i * 10.5) as price
FROM range(1, 101111) as t(i)
""", language="sql")

# Query the data
result = conn.execute("SELECT * FROM test_data").fetchdf()

st.dataframe(result)

# Step 3: Show some DuckDB features
st.header("Step 3: Try Some Queries")

query = st.text_area("Write a SQL query:", 
                     value="SELECT * FROM test_data WHERE price > 50",
                     height=100)

if st.button("Run Query"):
    try:
        result = conn.execute(query).fetchdf()
        st.dataframe(result)
    except Exception as e:
        st.error(f"Error: {e}")