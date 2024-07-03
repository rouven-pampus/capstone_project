import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, DateTime, Float, String, Integer, Column
from dotenv import load_dotenv
import os

# Load login data from .env file
load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

DB_STRING = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Create SQLAlchemy engine
engine = create_engine(DB_STRING)

# Create a new connection using psycopg2 for non-pandas operations
conn = psycopg2.connect(
    database=DB_NAME,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
try:
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to -", record, "\n")
    
    query_string1 = 'select * from "01_bronze".raw_energy_charts_day_ahead_prices_germany'
    df_prices = pd.read_sql(query_string1, engine)
    
    df_prices.columns = [col.strip().lower().replace(' ', '_') for col in df_prices.columns]
    df_prices.columns = [col.strip().lower().replace('-', '_') for col in df_prices.columns]
    
    df_prices.to_sql('fact_day_ahead_prices_germany', engine, schema='02_silver', if_exists='replace', index=False)
    
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")