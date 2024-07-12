####################### get packages #######################

import requests_cache
import pandas as pd
from retry_requests import retry
from packages.db_utils import get_engine
import time
    
# Load data from the database using SQLAlchemy engine
query_string1 = 'SELECT * FROM "02_silver"."dim_active_weather_stations"'
active_stations = pd.read_sql(query_string1, get_engine())

query_string2 = 'SELECT DISTINCT station_id FROM "01_bronze".raw_open_meteo_weather_history;'
used_stations = pd.read_sql(query_string2, get_engine())

used_ids = used_stations.station_id.unique()

# Removing rows where station_id is in the list
stations_filtered = active_stations[~active_stations['station_id'].isin(used_ids)]     

# Function to sample up to the available number of station IDs in each state
def sample_stations(group, n=5):
    return group.sample(min(len(group), n))

# Group by state and sample station_ids from each state
sampled_stations = stations_filtered.groupby('state').apply(sample_stations).reset_index(drop=True)
    
station_id = sampled_stations.station_id.to_list()    
stations_latitude = sampled_stations.latitude.to_list()
stations_longitude = sampled_stations.longitude.to_list()

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
    url = "https://archive-api.open-meteo.com/v1/archive"
    timezone = "Europe/Berlin"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": weather_variables,
        "timezone": timezone,
        "start_date": "2018-10-01",
	    "end_date": "2024-07-10",
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
    print(f"current import:{station_id[i]}")
    time.sleep(30)
    all_data.append(station_data)

# Combine all data into a single DataFrame
final_weather_data = pd.concat(all_data, ignore_index=True).sort_values(by='timestamp', ascending=False)

final_weather_data.dropna(axis=0, inplace=True)

print("Inserting data into database...")
final_weather_data.to_sql('raw_open_meteo_weather_history_new', get_engine(), schema='01_bronze', if_exists='replace', index=False)