import psycopg2
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
   
    
def get_data_from_db(sql_string):
    """
    Connects to the PostgreSQL database and returns a DataFrame based on the given SQL query.

    Parameters:
    sql_query (str): The SQL query to execute.

    Returns:
    DataFrame: A pandas DataFrame containing the results of the query.
    """
    try:
        # Load login data from .env file
        load_dotenv()
        
        DB_NAME = os.getenv('DB_NAME')
        DB_USERNAME = os.getenv('DB_USERNAME')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_HOST = os.getenv('DB_HOST')
        DB_PORT = os.getenv('DB_PORT')    
        
        # Create SQLAlchemy engine
        DB_STRING = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        
        # Create an SQLAlchemy engine
        engine = create_engine(DB_STRING)    
        
        # Use the engine to connect to the database and read the SQL query into a DataFrame
        with engine.connect() as conn:
            df = pd.read_sql_query(sql_string, conn)
        
        return df
    
    except Exception as e:
        print(f"Error: {e}")
        return None   
    
    
def get_engine():   
    # Load login data from .env file
    load_dotenv()
    
    DB_NAME = os.getenv('DB_NAME')
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')    
    
    # Create SQLAlchemy engine
    DB_STRING = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'    
    
    # Create an SQLAlchemy engine
    engine = create_engine(DB_STRING)
    return engine
    
    
def st_get_engine():
    # Read secrets
    host = st.secrets["postgres"]["host"]
    port = st.secrets["postgres"]["port"]
    dbname = st.secrets["postgres"]["dbname"]
    user = st.secrets["postgres"]["user"]
    password = st.secrets["postgres"]["password"]

    # Create connection string
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    return engine