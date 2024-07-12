####################### get packages #######################
import pandas as pd
from packages.db_utils import get_engine
    
# Load data from the database using SQLAlchemy engine
query_string1 = 'SELECT * FROM "02_silver"."fact_full_weather"'
df_weather = pd.read_sql(query_string1, get_engine())

# Load data from the database using SQLAlchemy engine
query_string2 = 'SELECT * FROM "02_silver"."dim_weather_stations"'
df_stations = pd.read_sql(query_string2, get_engine())

df = pd.merge(df_weather, df_stations, on="station_id").sort_values(by=['timestamp','station_id'], ascending=False, ignore_index=True)
df.timestamp = df.timestamp.dt.tz_convert("Europe/Berlin")

aggregation = {
    'temperature_2m': 'mean',
    'relative_humidity_2m': 'mean',
    'apparent_temperature': 'mean',
    'precipitation': 'mean',
    'cloud_cover': 'mean',
    'wind_speed_10m': 'mean',
    'wind_direction_10m': 'mean',
    'direct_radiation': 'mean',
    'diffuse_radiation': 'mean',
    'sunshine_duration': 'mean',
    'is_forecast':'first'
}

df_weather_region = df.groupby(['timestamp','region']).agg(aggregation).sort_values(by='timestamp', ascending=False).reset_index()

df_weather_region.to_sql('fact_full_weather_region', get_engine(), schema='02_silver', if_exists='replace', index=False)