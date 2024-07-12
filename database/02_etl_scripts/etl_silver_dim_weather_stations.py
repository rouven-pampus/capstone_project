import pandas as pd
from packages.db_utils import get_engine
    
#Get all active weather stations data
query_string1 = 'SELECT * FROM "02_silver"."dim_active_weather_stations"'
active_stations = pd.read_sql(query_string1,get_engine())

#Get all used weather stations ids
query_string2 = 'SELECT DISTINCT station_id FROM "01_bronze".raw_open_meteo_weather_history'
used_stations = pd.read_sql(query_string2,get_engine())

#Make unique to be sure
used_ids = used_stations.station_id.unique()

#Filter where station_id is in the list
stations_filtered = active_stations[active_stations['station_id'].isin(used_ids)]

#Write to database
stations_filtered.to_sql('dim_weather_stations', get_engine(), schema='02_silver', if_exists='replace', index=False)