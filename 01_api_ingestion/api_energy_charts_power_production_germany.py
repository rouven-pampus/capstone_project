import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, DateTime, Float, String, Integer, Column
from dotenv import load_dotenv
import os

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

country = "de"
start = "2018-10-01"
end = "2024-07-05"

url = f"https://api.energy-charts.info/total_power?country={country}&start={start}&end={end}"

response = requests.get(url)
json_data = response.json()

# Extract the unix_seconds
timestamps = json_data['unix_seconds']

# Create a dictionary to hold the data for the DataFrame
data_dict = {'timestamp': pd.to_datetime(timestamps, unit='s')}

# Extract production type data and add to the dictionary
for production_type in json_data['production_types']:
    data_dict[production_type['name']] = production_type['data']

# Convert the dictionary to a DataFrame
df_power = pd.DataFrame(data_dict)

# Convert timestamp to datetime and correct timezone
df_power['timestamp'] = pd.to_datetime(df_power['timestamp'], unit='s')
df_power["timestamp"] = df_power.timestamp.dt.tz_localize("UTC").dt.tz_convert("Europe/Berlin")

df_power.to_sql('raw_energy_charts_power_germany', engine, schema='01_bronze', if_exists='replace', index=False)