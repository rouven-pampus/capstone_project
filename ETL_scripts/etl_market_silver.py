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
    
    print("Energy generation data loading!")
    query_string2 = 'SELECT * FROM "01_bronze"."raw_market_energy_generation"'
    raw_generation = pd.read_sql(query_string2, engine)
    print("Loading finished!")
    
    print("Energy price data loading!")
    query_string3 = 'SELECT * FROM "01_bronze"."raw_market_day_ahead_prices"'
    raw_prices = pd.read_sql(query_string3, engine)
    print("Loading finished!")
    
    print("Loading complete. Starting transformation!")
    
    
    
    ####################################### Transform #######################################
    
    
    ####################################### Consumption data
    
    # Convert 'start_date' and 'end_date' to datetime
    raw_consumption['start_date'] = pd.to_datetime(raw_consumption['start_date'], errors='coerce')
    raw_consumption['end_date'] = pd.to_datetime(raw_consumption['end_date'], errors='coerce')

    # Localize to the specific time zone (e.g., Europe/Berlin) handling DST transitions
    raw_consumption['start_date'] = raw_consumption['start_date'].dt.tz_localize('Europe/Berlin', ambiguous='NaT', nonexistent='NaT')
    raw_consumption['end_date'] = raw_consumption['end_date'].dt.tz_localize('Europe/Berlin', ambiguous='NaT', nonexistent='NaT')
    
    # Generate complete date range with hourly frequency
    full_time_range = pd.date_range(start=raw_consumption['start_date'].min(), end=raw_consumption['start_date'].max(), freq='H', tz='Europe/Berlin')

    # Create a DataFrame with the complete time range
    complete_time_consumption = raw_consumption.DataFrame(full_time_range, columns=['start_date'])

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
    
    # Calculate total production
    generation_complete['total_mwh'] = generation_complete.iloc[:, 2:].sum(axis=1)
    
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
    
    
    ####################################### Price data
    
    # Filter for Germany
    raw_price_germany = raw_prices.iloc[:, 0:3]

    # Rename column
    raw_price_germany.rename(columns={'germany/luxembourg_[€/mwh]_original_resolutions' : 'Price_Germany[€/MWh]'}, inplace=True)
    raw_price_germany.head()
    
    # transform date to datetime
    raw_price_germany['start_date'] = pd.to_datetime(raw_price_germany['start_date'], format='%b %d, %Y %I:%M %p')
    raw_price_germany['end_date'] = pd.to_datetime(raw_price_germany['end_date'], format='%b %d, %Y %I:%M %p')

    # Separate string by dots
    def convert_to_numeric(x):
        try:
            return pd.to_numeric(x.split('.')[0])  # Takes the first value before the point
        except:
            return pd.np.nan  # SSets NaN if there is a problem

    raw_price_germany['Price_Germany[€/MWh]'] = raw_price_germany['Price_Germany[€/MWh]'].apply(convert_to_numeric)
        
    
    ####################################### Merging
    
    merged_market = ...
    
    
    
    ####################################### LOAD #######################################
    
    # Create table in the database
    cursor = conn.cursor()
    new_table_command = """
       CREATE TABLE IF NOT EXISTS "02_silver".fact_market_germany(
       weather_station_id TEXT,
       ...
                    
    );
    """
    cursor.execute(new_table_command)
    conn.commit()
        
    # Insert the transformed data into the new table
    insert_query = """
        INSERT INTO "02_silver".fact_market_germany (...)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for index, row in merged_market.iterrows():
        cursor.execute(insert_query, (row['...']))
        
    conn.commit()    
    print("Data inserted!")
    
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")