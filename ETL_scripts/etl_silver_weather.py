import psycopg2
from sqlalchemy import create_engine, DateTime, Float, String, Integer, Column
from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd

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
    
    # Load data from the database using SQLAlchemy engine
    print("Solar data loading...")
    query_string1 = 'SELECT * FROM "01_bronze"."raw_weather_solar"'
    raw_solar = pd.read_sql(query_string1, engine)
    print("Loading finished!")
    
    print("Wind data loading...")
    query_string2 = 'SELECT * FROM "01_bronze"."raw_weather_wind"'
    raw_wind = pd.read_sql(query_string2, engine)
    print("Loading finished!")

    print("Temperature data loading...")
    query_string3 = 'SELECT * FROM "01_bronze"."raw_weather_temp"'
    raw_temp = pd.read_sql(query_string3, engine)
    print("Loading finished!")
    
    print("Loading complete. Starting transformation...")
    
    # Perform data transformation
    raw_wind.drop(['eor'], axis=1, inplace=True)
    raw_solar.drop(['eor', 'mess_datum'], axis=1, inplace=True)
    raw_temp.drop(['eor'], axis=1, inplace=True)

    raw_wind.rename(columns={'stations_id': 'id', 'mess_datum': 'date', 'f': 'w_force', 'd': 'w_direc', 'qn_3': 'qual_w'}, inplace=True)
    raw_solar.rename(columns={'stations_id': 'id', 'mess_datum_woz': 'date', 'atmo_lberg': 'atm_rad', 'fd_lberg': 'diff_rad', 'fg_lberg': 'glob_rad', 'sd_lberg': 'sun', 'zenit': 'zenith', 'qn_592': 'qual_s'}, inplace=True)
    raw_temp.rename(columns={'stations_id': 'id', 'mess_datum': 'date', 'tt_tu': 'temp', 'rf_tu': 'humid', 'qn_9': 'qual_t'}, inplace=True)

    raw_wind.replace(-999, np.nan, inplace=True)
    raw_solar.replace(-999, np.nan, inplace=True)
    raw_temp.replace(-999, np.nan, inplace=True)

    raw_wind['date'] = pd.to_datetime(raw_wind['date'], format='%Y-%m-%d %H:%M:%S')
    raw_solar['date'] = pd.to_datetime(raw_solar['date'], format='%Y%m%d%H:%M')
    raw_temp['date'] = pd.to_datetime(raw_temp['date'], format='%Y-%m-%d %H:%M:%S')

    raw_solar['date'] = raw_solar['date'] - pd.to_timedelta(1, unit='h')

    ids_to_drop = ['5779', '5906']
    raw_wind = raw_wind[~raw_wind['id'].isin(ids_to_drop)]
    raw_solar = raw_solar[~raw_solar['id'].isin(ids_to_drop)]
    raw_temp = raw_temp[~raw_temp['id'].isin(ids_to_drop)]

    start_date = raw_solar['date'].min()
    end_date = raw_solar['date'].max()
    all_hours = pd.date_range(start=start_date, end=end_date, freq='h')

    unique_ids = raw_solar['id'].unique()
    complete_index = pd.MultiIndex.from_product([unique_ids, all_hours], names=['id', 'date'])
    complete_df = pd.DataFrame(index=complete_index).reset_index()

    raw_solar_full = pd.merge(complete_df, raw_solar, on=['id', 'date'], how='left')

    df_691 = raw_solar_full[raw_solar_full['id'] == '691']
    df_691_unique = df_691.drop_duplicates(subset='date', keep='first')
    df_rest = raw_solar_full[raw_solar_full['id'] != '691']
    raw_solar_fixed = pd.concat([df_rest, df_691_unique])

    start_date = raw_temp['date'].min()
    end_date = pd.Timestamp('2024-05-31 23:00:00')
    all_hours = pd.date_range(start=start_date, end=end_date, freq='h')

    unique_ids = raw_temp['id'].unique()
    complete_index = pd.MultiIndex.from_product([unique_ids, all_hours], names=['id', 'date'])
    complete_df = pd.DataFrame(index=complete_index).reset_index()

    raw_temp_full = pd.merge(complete_df, raw_temp, on=['id', 'date'], how='left')

    start_date = raw_wind['date'].min()
    end_date = pd.Timestamp('2024-05-31 23:00:00')
    all_hours = pd.date_range(start=start_date, end=end_date, freq='h')

    unique_ids = raw_wind['id'].unique()
    complete_index = pd.MultiIndex.from_product([unique_ids, all_hours], names=['id', 'date'])
    complete_df = pd.DataFrame(index=complete_index).reset_index()

    raw_wind_full = pd.merge(complete_df, raw_wind, on=['id', 'date'], how='left')

    merged_weather = raw_wind_full.merge(raw_solar_fixed, on=['id', 'date'], how='outer') \
                                  .merge(raw_temp_full, on=['id', 'date'], how='outer')

    merged_weather = merged_weather.drop(merged_weather.index[0])
    
    # Localize to the specific time zone (e.g., Europe/Berlin) handling DST transitions
    merged_weather['date'] = merged_weather['date'].dt.tz_localize('Europe/Berlin', ambiguous='NaT', nonexistent='NaT')
    
    merged_weather.rename(columns={
        'id': 'weather_station_id',
        'date': 'timestamp'
    }, inplace=True)

    # # Fill NAs through interpolation
    merged_weather = merged_weather.interpolate()
    
    #Drop unnecessary columns
    merged_weather = merged_weather.drop(columns=['qual_s', 'qual_w', 'qual_t', 'atm_rad'])
    
    # Create table in the database
    columns = merged_weather.columns.values.tolist()
       
    datatypes = {columns[0]: String, 
                 columns[1]: DateTime(timezone=True), 
                 columns[2]: Float,
                 columns[3]: Float,
                 columns[4]: Float,
                 columns[5]: Float,
                 columns[6]: Float,
                 columns[7]: Float,
                 columns[8]: Float,
                 columns[9]: Float
                 }
    
        
    # Insert the transformed data into the new table
    print("Inserting data...")
    merged_weather.to_sql('fact_weather_data', engine, schema='02_silver', if_exists='replace', dtype=datatypes, chunksize=100000, index=False)
    print("Data inserted! Rouven is great!")

except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")