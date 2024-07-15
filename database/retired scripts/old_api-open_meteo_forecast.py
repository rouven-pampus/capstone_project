####################### get packages #######################

import requests_cache
import pandas as pd
from retry_requests import retry
from packages.db_utils import get_engine
    
# Load data from the database  
query_string1 = 'SELECT * FROM "02_silver"."dim_active_weather_stations"' #for coordinates
active_stations = pd.read_sql(query_string1, get_engine())

query_string2 = 'SELECT DISTINCT station_id FROM "01_bronze".raw_open_meteo_weather_history;' #for ids of station_ids used in data
used_stations = pd.read_sql(query_string2, get_engine())

used_ids = used_stations.station_id.unique() #create list for filtering

# Filter for data of used weather stations
weather_stations = active_stations[active_stations['station_id'].isin(used_ids)]
      
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
    timestamp_fetched = pd.to_datetime('today').tz_localize(timezone).floor('h')
        
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
final_weather_data = pd.concat(all_data, ignore_index=True).sort_values(by='timestamp', ascending=False)

#add difference between timestamp of observation and fetched time 
final_weather_data["forecast_hours"] = ((final_weather_data['timestamp'] - final_weather_data['timestamp_fetched']).dt.total_seconds() / 3600).astype(int)

print("Inserting data into database...")  
final_weather_data.to_sql('raw_open_meteo_weather_forecast', get_engine(), schema='01_bronze', if_exists='replace', index=False)