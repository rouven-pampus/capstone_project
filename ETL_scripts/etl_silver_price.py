import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd

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
    
    # Load data from the database using SQLAlchemy engine
    print("Price data loading!")
    query_string1 = 'SELECT * FROM "01_bronze"."raw_market_day_ahead_prices"'
    fact_market_day_ahead_price = pd.read_sql(query_string1, engine)
    print("Loading finished!")
    
    print("Loading complete. Starting transformation!")
    
    # Perform data transformation
    fact_market_day_ahead_price.rename(columns={
        'germany/luxembourg_[€/mwh]_original_resolutions': 'germany_luxembourg_eur_mwh', 
        '∅_de/lu_neighbours_[€/mwh]_original_resolutions': 'avg_de_lu_neighbours_eur_mwh', 
        'belgium_[€/mwh]_original_resolutions': 'belgium_eur_mwh', 
        'denmark_1_[€/mwh]_original_resolutions': 'denmark_1_eur_mwh', 
        'denmark_2_[€/mwh]_original_resolutions': 'denmark_2_eur_mwh', 
        'france_[€/mwh]_original_resolutions': 'france_eur_mwh', 
        'netherlands_[€/mwh]_original_resolutions': 'netherlands_eur_mwh', 
        'norway_2_[€/mwh]_original_resolutions': 'norway_2_eur_mwh', 
        'austria_[€/mwh]_original_resolutions': 'austria_eur_mwh', 
        'poland_[€/mwh]_original_resolutions': 'poland_eur_mwh', 
        'sweden_4_[€/mwh]_original_resolutions': 'sweden_4_eur_mwh', 
        'switzerland_[€/mwh]_original_resolutions': 'switzerland_eur_mwh', 
        'czech_republic_[€/mwh]_original_resolutions': 'czech_republic_eur_mwh', 
        'de/at/lu_[€/mwh]_original_resolutions': 'de_at_lu_eur_mwh', 
        'northern_italy_[€/mwh]_original_resolutions': 'northern_italy_eur_mwh', 
        'slovenia_[€/mwh]_original_resolutions': 'slovenia_eur_mwh', 
        'hungary_[€/mwh]_original_resolutions': 'hungary_eur_mwh'
    }, inplace=True)
    
    fact_market_day_ahead_price.replace(-999, np.nan, inplace=True)

    date_format = "%b %d, %Y %I:%M %p"
    fact_market_day_ahead_price['start_date'] = pd.to_datetime(fact_market_day_ahead_price['start_date'], format=date_format, errors='coerce')
    fact_market_day_ahead_price['end_date'] = pd.to_datetime(fact_market_day_ahead_price['end_date'], format=date_format, errors='coerce')
    
    # Convert to UTC for consistency
    fact_market_day_ahead_price['start_date_utc'] = fact_market_day_ahead_price['start_date'].dt.tz_convert('UTC')
    fact_market_day_ahead_price['end_date_utc'] = fact_market_day_ahead_price['end_date'].dt.tz_convert('UTC')

    # Create schema and table in the database
    new_table_command = """
       CREATE SCHEMA IF NOT EXISTS "02_silver";
       DROP TABLE IF EXISTS "02_silver".fact_market_day_ahead_price;
       CREATE TABLE IF NOT EXISTS "02_silver".fact_market_day_ahead_price (
           start_date TIMESTAMP,
           end_date TIMESTAMP,
           start_date_utc TIMESTAMP,
           end_date_utc TIMESTAMP,
           germany_luxembourg_eur_mwh FLOAT,
           avg_de_lu_neighbours_eur_mwh FLOAT,
           belgium_eur_mwh FLOAT,
           denmark_1_eur_mwh FLOAT,
           denmark_2_eur_mwh FLOAT,
           france_eur_mwh FLOAT,
           netherlands_eur_mwh FLOAT,
           norway_2_eur_mwh FLOAT,
           austria_eur_mwh FLOAT,
           poland_eur_mwh FLOAT,
           sweden_4_eur_mwh FLOAT,
           switzerland_eur_mwh FLOAT,
           czech_republic_eur_mwh FLOAT,
           de_at_lu_eur_mwh FLOAT,
           northern_italy_eur_mwh FLOAT,
           slovenia_eur_mwh FLOAT,
           hungary_eur_mwh FLOAT
       );
    """
    cursor.execute(new_table_command)
    conn.commit()
        
    # Insert the transformed data into the new table
    fact_market_day_ahead_price.to_sql('fact_market_day_ahead_price', engine, schema='02_silver', if_exists='replace', index=False)

    print("Data inserted into the database!")
    
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")