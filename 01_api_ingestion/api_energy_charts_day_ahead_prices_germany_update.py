import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, DateTime, Float, String, Integer, Column
from dotenv import load_dotenv
import os
from datetime import timedelta

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

#Define window for updated data
start_date = (pd.to_datetime("today") - timedelta(days=7)).strftime("%Y-%m-%d") #lookback window of 7 days to limit load of retrieved data 
end_date = (pd.to_datetime("today") + timedelta(days=1)).strftime("%Y-%m-%d") #cut-off date to avoid null values

bzn = "DE-LU"
start = start_date
end = end_date

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

df_prices.to_sql('raw_energy_charts_day_ahead_prices_germany_temp', engine, schema='01_bronze', if_exists='replace', index=False)

try:
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to -", record, "\n")
    
    # Insert new data into old history table
    query_string1 = """
    INSERT INTO "01_bronze".raw_energy_charts_day_ahead_prices_germany (
        timestamp,
        "DE-LU",
        unit        
    )
    SELECT
        timestamp,
        "DE-LU",
        unit
    FROM "01_bronze".raw_energy_charts_day_ahead_prices_germany_temp 
    ON CONFLICT (timestamp) 
    DO UPDATE SET 
        "DE-LU" = EXCLUDED."DE-LU",
        unit = EXCLUDED.unit;
    """
    #Statement to drop temporary table
    query_string2 = """Drop table "01_bronze".raw_energy_charts_day_ahead_prices_germany_temp;"""
    
    print("Insert new price data...")
    cursor.execute(query_string1)
    
    print("Droping temp table...")
    cursor.execute(query_string2)
    conn.commit()
    
    print("Update done!")
    
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")