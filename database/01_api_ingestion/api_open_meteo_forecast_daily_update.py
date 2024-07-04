####################### get packages #######################

import psycopg2
from sqlalchemy import create_engine, DateTime, Float, String, Integer, Column
from dotenv import load_dotenv
import os
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import timedelta
import numpy as np

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

####################### 1. Get list of coordinates of used weather stations from dim_weather_stations  #######################

try:
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to -", record, "\n")
    
    print("Retreiving list of weather stations...")
    # Load coordinates of used weather stations from database for fetching data from open-meteo api
    query_string1 = 'SELECT * FROM "02_silver"."dim_weather_stations"'
    weather_stations = pd.read_sql(query_string1, engine)    
    station_id = weather_stations.station_id.to_list()    
    stations_latitude = weather_stations.latitude.to_list()
    stations_longitude = weather_stations.longitude.to_list() 
 
        
    ####################### get weather forecast from api and push it into new table #######################

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

    timezone = "Europe/Berlin"
    weather_variables = [
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "precipitation",
                "cloud_cover",
                "wind_speed_10m",
                "wind_direction_10m",
                "direct_radiation",
                "diffuse_radiation",
                "sunshine_duration"
            ]

    timestamp_fetched = pd.to_datetime('today').tz_localize(timezone).floor('h')

    # Function to fetch weather data for a specific station
    def fetch_weather_data(station_id, latitude, longitude):
        url = "https://api.open-meteo.com/v1/dwd-icon"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": weather_variables,
            "timezone": timezone,
            "past_days": 2
        }
        response = retry_session.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        hourly = data['hourly']
        dates = pd.date_range(
            start=pd.to_datetime(hourly['time'][0], utc=False),
            periods=len(hourly['time']),
            freq=pd.Timedelta(hours=1)
        )
        
        dates = dates.tz_localize(timezone, ambiguous='NaT', nonexistent='shift_forward')
            
        hourly_data = pd.DataFrame({
            'timestamp': dates,
            'timestamp_fetched': timestamp_fetched,
            'station_id': station_id,
            'temperature_2m': hourly['temperature_2m'],
            'relative_humidity_2m': hourly['relative_humidity_2m'],
            'apparent_temperature': hourly['apparent_temperature'],
            'precipitation': hourly['precipitation'],
            'cloud_cover': hourly['cloud_cover'],
            'wind_speed_10m': hourly['wind_speed_10m'],
            'wind_direction_10m': hourly['wind_direction_10m'],
            'direct_radiation': hourly['direct_radiation'],
            'diffuse_radiation': hourly['diffuse_radiation'],
            'sunshine_duration': hourly['sunshine_duration']               
        })
        
        return hourly_data

    all_data = []

    print("Fetching data from API...")
    for i in range(len(station_id)):
        station_data = fetch_weather_data(station_id[i], stations_latitude[i], stations_longitude[i])
        all_data.append(station_data)    
        
    # Combine all data into a single DataFrame
    forecast_weather_data = pd.concat(all_data, ignore_index=True)

    #add difference between timestamp of observation and fetched time
    forecast_weather_data["is_forecast"] = np.where(forecast_weather_data.timestamp > timestamp_fetched, "yes","no" )

    # inserting data into database
    print("Inserting data into database...")  
    forecast_weather_data.to_sql('raw_open_meteo_weather_forecast', engine, schema='01_bronze', if_exists='replace', index=False)     

except Exception as error:
    print("Error while connecting to PostgreSQL:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")