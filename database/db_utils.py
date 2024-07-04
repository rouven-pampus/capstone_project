import psycopg2
import pandas as pd
import streamlit as st

def get_data_from_db(sql_string):
    """
    Connects to the PostgreSQL database and returns a DataFrame based on the given SQL query.

    Parameters:
    sql_query (str): The SQL query to execute.

    Returns:
    DataFrame: A pandas DataFrame containing the results of the query.
    """
    try:
        # Get the secrets
        postgres_secrets = st.secrets["postgres"]

        # Establish the connection
        conn = psycopg2.connect(
            host=postgres_secrets["host"],
            port=postgres_secrets["port"],
            dbname=postgres_secrets["dbname"],
            user=postgres_secrets["user"],
            password=postgres_secrets["password"]
        )
        
        # Execute the query and fetch the data into a DataFrame
        df = pd.read_sql_query(sql_string, conn)
        
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return None