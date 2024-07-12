import pandas as pd
from packages.db_utils import get_engine

#create engine
engine = get_engine()

#get data
query_string1 = 'SELECT * FROM "01_bronze"."raw_dwd_weather_stations_full"'
df_weather_stations = pd.read_sql(query_string1, engine)

#set dates to select active weather stations
active_date = "2024-07-10 00:00:00.000" #should be automated to today - 2 days
min_date = "2018-10-01 00:00:00.000"

#make selection of data depending on active weather stations
df_weather_stations.query('Ende >= @active_date and Beginn <= @min_date',inplace=True)
df_weather_stations = df_weather_stations[["Stations_ID", "Stationsname","Breite", "Länge", "Bundesland","Beginn" ,"Ende"]]

#Group entries by max date
df_weather_stations = df_weather_stations.groupby("Stations_ID").max().reset_index()

# Renaming the columns
df_weather_stations.rename(columns={
    'Stations_ID': 'station_id',
    'Stationsname': 'station_name',
    'Breite': 'latitude',
    'Länge': 'longitude',
    'Bundesland': 'state',
    'Beginn': 'begin',
    'Ende': 'end'
}, inplace=True)

# Mapping of states to regions
state_to_region = {
    'SH': 'north',
    'HB': 'north',
    'NI': 'north',
    'MV': 'north',
    'HH': 'north',
    'HE': 'west',
    'NW': 'west',
    'RP': 'west',
    'SL': 'west',
    'SN': 'east',
    'ST': 'east',
    'BB': 'east',
    'TH': 'east',
    'BE': 'east',
    'BY': 'south',
    'BW': 'south',
    'T': 'south'
    }
df_weather_stations['region'] = df_weather_stations['state'].map(state_to_region) # Create a new column 'region' based on the mapping

#reorder columns
col_order = ["station_id","station_name","latitude","longitude","state","region","begin","end"]
df_weather_stations = df_weather_stations[col_order]

#load to database
df_weather_stations.to_sql('dim_active_weather_stations', engine, schema='02_silver', if_exists='replace', index=False)
