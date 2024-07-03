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
    
    print("loading power data...")
    query_string1 = 'select * from "02_silver".fact_total_power_germany'
    df_power = pd.read_sql(query_string1, engine)
    
    print("loading price data...")
    query_string2 = 'select * from "02_silver".fact_day_ahead_prices_germany'
    df_prices = pd.read_sql(query_string2, engine)
    
    print("loading weather data...")
    query_string3 = 'select * from "02_silver".fact_full_weather'
    df_weather = pd.read_sql(query_string3, engine)
    
    
    ############################ 1. Preperation of tables ############################
    
    ############################ Total Power Table
    
    print("Transforming power data...")
    #define aggregation method for aggregating to full hour 
    power_aggregation ={
    'hydro_pumped_storage_consumption': 'sum',
    'cross_border_electricity_trading': 'sum',
    'nuclear': 'sum',
    'hydro_run_of_river': 'sum',
    'biomass': 'sum',
    'fossil_brown_coal_lignite': 'sum',
    'fossil_hard_coal': 'sum',
    'fossil_oil': 'sum',
    'fossil_gas': 'sum',
    'geothermal': 'sum',
    'hydro_water_reservoir': 'sum',
    'hydro_pumped_storage': 'sum',
    'others': 'sum',
    'waste': 'sum',
    'wind_offshore': 'sum',
    'wind_onshore': 'sum',
    'solar': 'sum',
    'load_incl_self_consumption': 'sum',
    'residual_load': 'sum',
    'renewable_share_of_generation': 'mean',
    'renewable_share_of_load': 'mean'
    }
    
    #aggregating to full hour
    df_power.groupby(df_power.timestamp.dt.floor('H')).agg(power_aggregation).reset_index()
    
    # Define the desired order of columns
    new_column_order = [
        'timestamp',   
        'hydro_run_of_river',     
        'hydro_water_reservoir', 
        'hydro_pumped_storage',     
        'biomass',
        'geothermal',     
        'wind_offshore', 
        'wind_onshore', 
        'solar',     
        'fossil_brown_coal_lignite', 
        'fossil_hard_coal', 
        'fossil_oil', 
        'fossil_gas',
        'nuclear',
        'others', 
        'waste',        
        'hydro_pumped_storage_consumption', 
        'load_incl_self_consumption',    
        'cross_border_electricity_trading',
        'residual_load',
        'renewable_share_of_generation', 
        'renewable_share_of_load'    
    ]

    # Reorder the columns
    df_power = df_power[new_column_order]
    
    #adding totals
    df_power["total_renewable_production"] = df_power.iloc[:,1:9].sum(axis=1)
    df_power["total_production"] = df_power.iloc[:,1:16].sum(axis=1)
    df_power["total_consumption"] = df_power.iloc[:,17:19].sum(axis=1)
    
    
    ############################ Prices Table
    
    print("Transforming price data...")
    df_prices.drop('unit', axis=1, inplace=True)
    df_prices.rename(columns={'de_lu': 'price_eur_mwh'}, inplace=True)
    
    ############################ Weather Table
    
    print("Transforming weather data...")
    #Define aggregation
    weather_aggregation = {
        'temperature_2m': 'mean',
        'relative_humidity_2m': 'mean',
        'apparent_temperature': 'mean',
        'precipitation': 'mean',
        'cloud_cover': 'mean',
        'wind_speed_10m': 'mean',
        'wind_direction_10m': 'mean',
        'direct_radiation': 'mean',
        'diffuse_radiation': 'mean',
        'sunshine_duration': 'mean'
    }

    #drop columns
    df_weather.drop(['station_id','source_table'], axis=1, inplace=True)

    #aggregate
    df_weather.groupby(['timestamp', 'is_forecast']).agg(weather_aggregation).reset_index()
    
    
    ############################ 2. Merge of tables ############################
   
    print("Merging tables...")
    # Merge df_weather and df_power on 'timestamp'
    combined_df = pd.merge(df_weather, df_power, on='timestamp')

    # Merge the resulting DataFrame with df_prices on 'timestamp'
    combined_df = pd.merge(combined_df, df_prices, on='timestamp')
        
    ############################ 3. Load to database ############################
    print("Loading new table...")
    combined_df.to_sql('fact_electricity_market_germany', engine, schema='03_gold', if_exists='replace', index=False)
    
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")