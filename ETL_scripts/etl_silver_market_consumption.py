####################################### Setup #######################################

import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd
import pytz

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
    
    ####################################### EXTRACT #######################################
    
    # Load data from the database using SQLAlchemy engine
    print("Consumption data loading!")
    query_string1 = 'SELECT * FROM "01_bronze"."raw_market_consumption"'
    raw_consumption = pd.read_sql(query_string1, engine)
    print("Loading finished!")

    ####################################### Transform #######################################
    
    
    ####################################### Consumption data
    
    # Convert 'start_date' and 'end_date' to datetime
    date_format = "%b %d, %Y %I:%M %p"
    raw_consumption['start_date'] = pd.to_datetime(raw_consumption['start_date'], errors='coerce', format=date_format)
    raw_consumption['end_date'] = pd.to_datetime(raw_consumption['end_date'], errors='coerce', format=date_format)

    # Localize to the specific time zone (e.g., Europe/Berlin) handling DST transitions
    raw_consumption['start_date'] = raw_consumption['start_date'].dt.tz_localize('Europe/Berlin', ambiguous='NaT', nonexistent='NaT')
    raw_consumption['end_date'] = raw_consumption['end_date'].dt.tz_localize('Europe/Berlin', ambiguous='NaT', nonexistent='NaT')
    
    # Generate complete date range with hourly frequency
    full_time_range = pd.date_range(start=raw_consumption['start_date'].min(), end=raw_consumption['start_date'].max(), freq='H', tz='Europe/Berlin')

    # Create a DataFrame with the complete time range
    complete_time_consumption = pd.DataFrame(full_time_range, columns=['start_date'])

    # Merge the original DataFrame with the complete time range DataFrame
    consumption_complete = pd.merge(complete_time_consumption, raw_consumption, on='start_date', how='left')

    # Recalculate 'end_date' for missing values by adding one hour to 'start_date'
    consumption_complete['end_date'] = consumption_complete.apply(
        lambda row: row['start_date'] + pd.Timedelta(hours=1) 
        if pd.isna(row['end_date'])
        else row['end_date'], axis=1)

    # Drop rows where 'start_date' or 'end_date' is NaT
    consumption_complete.dropna(subset=['start_date', 'end_date'], inplace=True)
    
    # List of columns to be cleaned
    numeric_columns = ['total_(grid_load)_[mwh]_calculated_resolutions',
                       'residual_load_[mwh]_calculated_resolutions',
                       'hydro_pumped_storage_[mwh]_calculated_resolutions']

    # Function to clean numeric columns
    def clean_numeric_columns(raw_consumption, columns):
        for col in columns:
            # Convert all values to string to use .str accessor
            raw_consumption[col] = raw_consumption[col].astype(str).str.replace(',', '', regex=False)
            # Convert to numeric values
            raw_consumption[col] = pd.to_numeric(raw_consumption[col], errors='coerce')
        return raw_consumption
        
    # Clean numeric columns
    consumption_complete = clean_numeric_columns(consumption_complete, numeric_columns)

    # Drop consumption NAs
    consumption_complete.dropna(inplace=True)

    # Rename columns
    consumption_complete.rename(columns=lambda x: x.replace('_calculated_resolutions', '_consumption'), inplace=True)


    ####################################### LOAD #######################################
    
    # Create table in the database
    cursor = conn.cursor()
    new_table_command = """
        CREATE SCHEMA if not exists "02_silver";
        DROP TABLE IF EXISTS "02_silver".fact_market_consumption_germany;
        CREATE TABLE IF NOT EXISTS "02_silver".fact_market_consumption_germany(
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        total_grid_load_consumption FLOAT,
        residual_load_consumption FLOAT,
        hydro_pumped_storage_consumption FLOAT            
    );
    """
    cursor.execute(new_table_command)
    conn.commit()
        
    consumption_complete.to_sql('fact_market_consumption_germany', engine, schema='02_silver', if_exists='replace', index=False)
      
    print("Data inserted!")
    
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")