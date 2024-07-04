####################################### Setup #######################################

import psycopg2
from sqlalchemy import create_engine, DateTime, Float, String, Integer, Column
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
    print("Energy generation data loading!")
    query_string2 = 'SELECT * FROM "01_bronze"."raw_market_energy_generation"'
    raw_generation = pd.read_sql(query_string2, engine)
    print("Loading finished!")

    ####################################### Transform #######################################
    
    
    ####################################### Energy generation data 
    
    # Convert 'start_date' and 'end_date' to datetime
    date_format = "%b %d, %Y %I:%M %p"
    raw_generation['start_date'] = pd.to_datetime(raw_generation['start_date'], errors='coerce', format=date_format)
    raw_generation['end_date'] = pd.to_datetime(raw_generation['end_date'], errors='coerce', format=date_format)

    # Localize to the specific time zone (e.g., Europe/Berlin) handling DST transitions
    raw_generation['start_date'] = raw_generation['start_date'].dt.tz_localize('Europe/Berlin', ambiguous='NaT', nonexistent='NaT')
    raw_generation['end_date'] = raw_generation['end_date'].dt.tz_localize('Europe/Berlin', ambiguous='NaT', nonexistent='NaT')
        
    # Generate complete date range with hourly frequency
    full_time_range = pd.date_range(start=raw_generation['start_date'].min(), end=raw_generation['start_date'].max(), freq='H', tz='Europe/Berlin')

    # Create a DataFrame with the complete time range
    complete_time_generation = pd.DataFrame(full_time_range, columns=['start_date'])

    # Merge the original DataFrame with the complete time range DataFrame
    generation_complete = pd.merge(complete_time_generation, raw_generation, on='start_date', how='left')

    # Recalculate 'end_date' for missing values by adding one hour to 'start_date'
    generation_complete['end_date'] = generation_complete.apply(
        lambda row: row['start_date'] + pd.Timedelta(hours=1) if pd.isna(row['end_date']) else row['end_date'],
        axis=1
    )

    # Drop rows where 'start_date' or 'end_date' is NaT
    generation_complete.dropna(subset=['start_date', 'end_date'], inplace=True)

    # List of columns to be cleaned
    numeric_columns = [
        'biomass_[mwh]_calculated_resolutions', 
        'hydropower_[mwh]_calculated_resolutions', 
        'wind_offshore_[mwh]_calculated_resolutions',
        'wind_onshore_[mwh]_calculated_resolutions', 
        'photovoltaics_[mwh]_calculated_resolutions', 
        'other_renewable_[mwh]_calculated_resolutions',
        'nuclear_[mwh]_calculated_resolutions', 
        'lignite_[mwh]_calculated_resolutions', 
        'hard_coal_[mwh]_calculated_resolutions',
        'fossil_gas_[mwh]_calculated_resolutions', 
        'hydro_pumped_storage_[mwh]_calculated_resolutions', 
        'other_conventional_[mwh]_calculated_resolutions'
    ]

    # Function to clean numeric columns
    def clean_numeric_columns(raw_generation, columns):
        for col in columns:
            # Convert all values to string to use .str accessor
            raw_generation[col] = raw_generation[col].astype(str).str.replace(',', '', regex=False)
            # Convert to numeric values
            raw_generation[col] = pd.to_numeric(raw_generation[col], errors='coerce')
        return raw_generation

    # Clean numeric columns
    generation_complete = clean_numeric_columns(generation_complete, numeric_columns)
    
    # create a wind total feature
    generation_complete['wind_total_[mwh]'] = generation_complete['wind_offshore_[mwh]_calculated_resolutions'] + generation_complete['wind_onshore_[mwh]_calculated_resolutions']
    
    # Calculate conventional_total_[mwh]
    conventional_columns = [
        'nuclear_[mwh]_calculated_resolutions',
        'lignite_[mwh]_calculated_resolutions',
        'hard_coal_[mwh]_calculated_resolutions',
        'fossil_gas_[mwh]_calculated_resolutions',
        'hydro_pumped_storage_[mwh]_calculated_resolutions',
        'other_conventional_[mwh]_calculated_resolutions'
    ]
    generation_complete['conventional_total_[mwh]'] = generation_complete[conventional_columns].sum(axis=1)

    # Calculate renewable_total_[mwh]
    renewable_columns = [
        'biomass_[mwh]_calculated_resolutions',
        'hydropower_[mwh]_calculated_resolutions',
        'wind_offshore_[mwh]_calculated_resolutions',
        'wind_onshore_[mwh]_calculated_resolutions',
        'photovoltaics_[mwh]_calculated_resolutions',
        'other_renewable_[mwh]_calculated_resolutions'
    ]
    generation_complete['renewable_total_[mwh]'] = generation_complete[renewable_columns].sum(axis=1)
    
    # Calculate total production
    generation_complete['total_mwh'] = generation_complete['conventional_total_[mwh]'] + generation_complete['renewable_total_[mwh]']

    # Convert trailing NAs in nuclear to 0, since there's no more nuclear power production 
    generation_complete.loc[generation_complete['start_date'] > '2024-01-01', 'nuclear_[mwh]_calculated_resolutions'] = 0
    
    # Drop remaining NAs that stem from time zone conversion
    generation_complete.dropna(inplace=True)

    # Rename columns
    generation_complete.rename(columns=lambda x: x.replace('_calculated_resolutions', '_generation'), inplace=True)

    ####################################### LOAD #######################################
    
    # Create table in the database
    columns = generation_complete.columns.values.tolist()
       
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
                 columns[17]: Float
                 }
    
    print("Inserting data...!")    
    generation_complete.to_sql('fact_market_generation_germany', engine, schema='02_silver', if_exists='replace', dtype=datatypes, chunksize=100000, index=False)
    
    print("Data inserted!")
        
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
