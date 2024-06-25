import psycopg2
from sqlalchemy import create_engine, DateTime, Float, String, Integer, Column
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
    print("Price data loading...")
    query_string1 = 'SELECT * FROM "01_bronze"."raw_market_day_ahead_prices"'
    fact_market_day_ahead_price = pd.read_sql(query_string1, engine)
    print("Loading finished!")
    
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
    
    # Localize to the specific time zone (e.g., Europe/Berlin) handling DST transitions
    fact_market_day_ahead_price['start_date'] = fact_market_day_ahead_price['start_date'].dt.tz_localize('Europe/Berlin', ambiguous='NaT', nonexistent='NaT')
    fact_market_day_ahead_price['end_date'] = fact_market_day_ahead_price['end_date'].dt.tz_localize('Europe/Berlin', ambiguous='NaT', nonexistent='NaT')

    # Create schema and table in the database
    columns = fact_market_day_ahead_price.columns.values.tolist()
       
    datatypes = {columns[0]: DateTime(timezone=True), 
                 columns[1]: DateTime(timezone=True), 
                 columns[2]: Float,
                 columns[3]: Float,
                 columns[4]: Float,
                 columns[5]: Float,
                 columns[6]: Float,
                 columns[7]: Float,
                 columns[8]: Float,
                 columns[9]: Float,
                 columns[10]: Float,
                 columns[11]: Float,
                 columns[12]: Float,
                 columns[13]: Float,
                 columns[14]: Float,
                 columns[15]: Float,
                 columns[16]: Float,
                 columns[17]: Float,
                 columns[18]: Float
                 }
        
    # Insert the transformed data into the new table
    print("Inserting data...")
    fact_market_day_ahead_price.to_sql('fact_market_day_ahead_price', engine, schema='02_silver', if_exists='replace', dtype=datatypes, chunksize=100000, index=False)
    print("Data inserted!")
    
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")