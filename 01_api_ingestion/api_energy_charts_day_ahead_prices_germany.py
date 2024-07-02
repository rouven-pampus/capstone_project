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

bzn = "DE-LU"
start = "2018-10-01"
end = "2024-07-05"

url = f"https://api.energy-charts.info/price?bzn={bzn}&start={start}&end={end}"

response = requests.get(url)
json_data = response.json()

# Extract relevant data
unix_seconds = json_data['unix_seconds']
prices = json_data['price']
units = json_data['unit']

# Combine into a DataFrame
df_prices = pd.DataFrame({
    'timestamp': unix_seconds,
     bzn : prices,
    'unit': units
})

# Convert timestamp to datetime and correct timezone
df_prices['timestamp'] = pd.to_datetime(df_prices['timestamp'], unit='s')
df_prices["timestamp"] = df_prices.timestamp.dt.tz_localize("UTC").dt.tz_convert("Europe/Berlin")

df_prices.to_sql('raw_energy_charts_day_ahead_prices_germany', engine, schema='01_bronze', if_exists='replace', index=False)