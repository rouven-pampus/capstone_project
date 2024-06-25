import psycopg2
from sqlalchemy import create_engine
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
    print("Solar data loading!")
    query_string1 = 'SELECT * FROM "01_bronze"."raw_weather_solar"'
    raw_solar = pd.read_sql(query_string1, engine)
    print("Loading finished!")
    
    print("Wind data loading!")
    query_string2 = 'SELECT * FROM "01_bronze"."raw_weather_wind"'
    raw_wind = pd.read_sql(query_string2, engine)
    print("Loading finished!")

    print("Temperature data loading!")
    query_string3 = 'SELECT * FROM "01_bronze"."raw_weather_temp"'
    raw_temp = pd.read_sql(query_string3, engine)
    print("Loading finished!")
    
    print("Loading complete. Starting transformation!")
    
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

    merged_weather.rename(columns={
        'id': 'weather_station_id',
        'date': 'timestamp'
    }, inplace=True)

    # # Fill NAs through interpolation
    merged_weather = merged_weather.interpolate()
    
    # Create table in the database
    cursor = conn.cursor()
    new_table_command = """
       CREATE TABLE IF NOT EXISTS "02_silver".fact_weather_data(
       weather_station_id TEXT,
       timestamp TIMESTAMP,
       w_force FLOAT,
       w_direc INT,
       diff_rad INT,
       glob_rad INT,
       sun INT,
       zenith FLOAT,
       temp FLOAT,
       humid INT             
    );
    """
    cursor.execute(new_table_command)
    conn.commit()
        
    # Insert the transformed data into the new table
    insert_query = """
        INSERT INTO "02_silver".fact_weather_data (weather_station_id, timestamp, w_force, w_direc, diff_rad, glob_rad, sun, zenith, temp, humid)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for index, row in merged_weather.iterrows():
        cursor.execute(insert_query, (row['weather_station_id'], row['timestamp'], row['w_force'], row['w_direc'], 
                                      row['diff_rad'], row['glob_rad'], row['sun'], row['zenith'], row['temp'], row['humid']))
        
    conn.commit()    
    print("Data inserted!")
    
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")