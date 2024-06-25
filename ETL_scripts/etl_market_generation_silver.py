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
    print("Energy generation data loading!")
    query_string2 = 'SELECT * FROM "01_bronze"."raw_market_energy_generation"'
    raw_generation = pd.read_sql(query_string2, engine)
    print("Loading finished!")

    ####################################### Transform #######################################
    
    
    ####################################### Energy generation data 
    
    # Convert 'start_date' and 'end_date' to datetime
    raw_generation['start_date'] = pd.to_datetime(raw_generation['start_date'], errors='coerce')
    raw_generation['end_date'] = pd.to_datetime(raw_generation['end_date'], errors='coerce')

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
    generation_complete[generation_complete['start_date'] > '2024-01-01']['nuclear_[mwh]_calculated_resolutions'] = 0
    
    # Drop remaining NAs that stem from time zone conversion
    generation_complete.dropna(inplace=True)

    # Rename columns
    generation_complete.rename(columns=lambda x: x.replace('_calculated_resolutions', '_generation'), inplace=True)

    ####################################### LOAD #######################################
    
    # Create table in the database
    cursor = conn.cursor()
    new_table_command = """
       CREATE TABLE IF NOT EXISTS "02_silver".fact_market_generation_germany(
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        biomass_generation FLOAT,
        hydropower_generation FLOAT,
        wind_offshore_generation FLOAT,
        wind_onshore_generation FLOAT,
        photovoltaics_generation FLOAT,
        other_renewable_generation FLOAT,
        nuclear_generation FLOAT,
        lignite_generation FLOAT,
        hard_coal_generation FLOAT,
        fossil_gas_generation FLOAT,
        hydro_pumped_storage_generation FLOAT,
        other_conventional_generation FLOAT,
        wind_total FLOAT,
        conventional_total FLOAT,
        renewable_total FLOAT,
        total_mwh FLOAT
    );
    """
    cursor.execute(new_table_command)
    conn.commit()
        
    # Insert the transformed data into the new table
    insert_query = """
        INSERT INTO "02_silver".fact_market_generation_germany (start_date, end_date, biomass_generation, hydropower_generation, wind_offshore_generation, wind_onshore_generation, photovoltaics_generation, other_renewable_generation, nuclear_generation, lignite_generation, hard_coal_generation, fossil_gas_generation, hydro_pumped_storage_generation, other_conventional_generation, wind_total, conventional_total, renewable_total, total_mwh)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for index, row in generation_complete.iterrows():
        # Data to insert
        data = (
            row['start_date'],
            row['end_date'],
            row['biomass_generation'],
            row['hydropower_generation'],
            row['wind_offshore_generation'],
            row['wind_onshore_generation'],
            row['photovoltaics_generation'],
            row['other_renewable_generation'],
            row['nuclear_generation'],
            row['lignite_generation'],
            row['hard_coal_generation'],
            row['fossil_gas_generation'],
            row['hydro_pumped_storage_generation'],
            row['other_conventional_generation'],
            row['wind_total'],
            row['conventional_total'],
            row['renewable_total'],
            row['total_mwh']
        )
        cursor.execute(insert_query, data)
        
    conn.commit()    
    print("Data inserted!")
    
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")