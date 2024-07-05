import psycopg2
from sqlalchemy import create_engine, DateTime, Float, String, Integer, Column
from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd
from packages.db_utils import get_data_from_db, get_engine
    
#define query to retreive data from database
query = """
    SELECT
        "Stations_ID"::text AS "station_id",
        "Stationsname"::text AS "station_name",
        "Breite"::float as "latitude",
        "Länge"::float as "longitude",
        "Bundesland"::text as "state"
    FROM
        "01_bronze".raw_dwd_weather_stations_full rwsf
    group BY"Stations_ID", "Stationsname", "Breite", "Länge", "Bundesland"
    ORDER BY 
    ("Stations_ID"::numeric) ASC;   
"""

# Load data from the database using SQLAlchemy engine
df_weather_stations = get_data_from_db(query)

#Filter to needed weather stations
station_id_list =["183","662","691","853","1048","1358","5856","1684","1975","2290","2712","3015","3631","3668","3987","4271","4336","4393","4466","4928","5100","5404","5705","5792"]    
df_weather_stations.query("station_id == @station_id_list", inplace=True)

# Mapping of states to regions
state_to_region = {
    'SH': 'North',
    'HB': 'North',
    'NI': 'North',
    'MV': 'North',
    'HH': 'North',
    'HE': 'West',
    'NW': 'West',
    'RP': 'West',
    'SL': 'West',
    'SN': 'East',
    'ST': 'East',
    'BB': 'East',
    'TH': 'East',
    'BE': 'East',
    'BY': 'South',
    'BW': 'South',
    'T': 'South'
    }
df_weather_stations['region'] = df_weather_stations['state'].map(state_to_region) # Create a new column 'region' based on the mapping

#retrie engine and write to database
engine = get_engine()
df_weather_stations.to_sql('dim_weather_stations', engine, schema='02_silver', if_exists='replace', index=False)
