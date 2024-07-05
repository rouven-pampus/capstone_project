import psycopg2
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def get_data_from_db_st(sql_string):
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
            host = postgres_secrets["host"],
            port = postgres_secrets["port"],
            dbname = postgres_secrets["dbname"],
            user = postgres_secrets["user"],
            password = postgres_secrets["password"]
        )
        
        # Execute the query and fetch the data into a DataFrame
        df = pd.read_sql_query(sql_string, conn)
        
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
    
def get_data_from_db(sql_string):
    """
    Connects to the PostgreSQL database and returns a DataFrame based on the given SQL query.

    Parameters:
    sql_query (str): The SQL query to execute.

    Returns:
    DataFrame: A pandas DataFrame containing the results of the query.
    """
    # Load login data from .env file
    load_dotenv()
    
    DB_NAME = os.getenv('DB_NAME')
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')    
    
    # Create SQLAlchemy engine
    DB_STRING = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(DB_STRING)    

    # Establish the connection
    conn = psycopg2.connect(
        database=DB_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )    
    
    try:                
        cursor = conn.cursor()
                        
        # Execute the query and fetch the data into a DataFrame
        df = pd.read_sql_query(sql_string, conn)                
        return df
        
    except Exception as error:
        print(f"An error occurred: {error}")
        return None
    
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")