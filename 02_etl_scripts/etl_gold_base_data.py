# Packages
import psycopg2
from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# Load login data from .env file
load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

DB_STRING = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(DB_STRING)

# Create a new connection using psycopg2 for non-pandas operations
conn = psycopg2.connect(
    database=DB_NAME,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Read weather data
query_string1 = 'SELECT * FROM "02_silver"."fact_weather_data"'
weather = pd.read_sql(query_string1, engine)

# Group by the 'timestamp' column and calculate the mean for each group
aggregated_weather = weather.groupby('timestamp').mean().reset_index()
aggregated_weather = aggregated_weather.drop(columns=['weather_station_id'])

# Read market data
query_string2 = 'SELECT * FROM "02_silver"."fact_market_consumption_germany"'
consumption = pd.read_sql(query_string2, engine)

query_string3 = 'SELECT * FROM "02_silver"."fact_market_day_ahead_price"'
price = pd.read_sql(query_string3, engine)

query_string4 = 'SELECT * FROM "02_silver"."fact_market_generation_germany"'
generation = pd.read_sql(query_string4, engine)

# Cut data into correct time frame
start_date = '2018-10-01'
end_date = '2024-05-31'

weather_cut = aggregated_weather[(aggregated_weather['timestamp'] >= start_date) & (aggregated_weather['timestamp'] <= end_date)]
consumption_cut = consumption[(consumption['start_date'] >= start_date) & (consumption['start_date'] <= end_date)]
price_cut = price[(price['start_date'] >= start_date) & (price['start_date'] <= end_date)]
generation_cut = generation[(generation['start_date'] >= start_date) & (generation['start_date'] <= end_date)]

# Drop half-hour instances from weather
weather_cut_30 = weather_cut[weather_cut['timestamp'].dt.minute != 30]

# Drop end date columns
price_cut = price_cut.drop(columns='end_date')
consumption_cut = consumption_cut.drop(columns='end_date')
generation_cut = generation_cut.drop(columns='end_date')

# Merge full dataset
merged_df = pd.merge(weather_cut_30, price_cut, left_on='timestamp', right_on='start_date', how='inner')
merged_df = pd.merge(merged_df, consumption_cut, left_on='timestamp', right_on='start_date', how='inner')
merged_df = pd.merge(merged_df, generation_cut, left_on='timestamp', right_on='start_date', how='inner')

# Drop redundant columns (start_date from price_cut, consumption_cut, generation_cut)
merged_df.drop(['start_date_x', 'start_date_y', 'start_date'], axis=1, inplace=True)

# Insert the transformed data into the new table
merged_df.to_sql('master_data', engine, schema='03_gold', if_exists='replace', index=False)
print("Data inserted! Rouven is still great!")