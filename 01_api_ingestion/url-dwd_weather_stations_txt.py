import psycopg2
from sqlalchemy import create_engine, DateTime, Float, String, Integer
from dotenv import load_dotenv
import os
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

url = "https://www.dwd.de/DE/leistungen/klimadatendeutschland/statliste/statlex_html.html;jsessionid=59BB3C5A492B7E29D3CFDF5723D5A5A3.live31092?view=nasPublication&nn=16102"
file = pd.read_html(url)
df = file[0]

# Rename columns
columns = df.columns = [
    "Stationsname", 
    "Stations_ID", 
    "Kennung",
    "Stationskennung",
    "Breite",
    "Länge",
    "Stationshöhe",
    "Flussgebiet",
    "Bundesland",
    "Beginn",
    "Ende"
]	
									
# reorder columns
order = [
    "Stations_ID",
    "Stationsname",     
    "Kennung",
    "Stationskennung",
    "Breite",
    "Länge",
    "Stationshöhe",
    "Flussgebiet",
    "Bundesland",
    "Beginn",
    "Ende"
    ]

df = df[order].sort_values(by="Stations_ID", ascending=True)

# Convert date columns to datetime format
df['Beginn'] = pd.to_datetime(df['Beginn'], format='%d.%m.%Y', errors='coerce')
df['Ende'] = pd.to_datetime(df['Ende'], format='%d.%m.%Y', errors='coerce')

# Define data types
datatypes = {
    columns[0]: String, 
    columns[1]: String, 
    columns[2]: String,
    columns[3]: String,
    columns[4]: Float,
    columns[5]: Float,
    columns[6]: Integer,
    columns[7]: String,
    columns[8]: String,
    columns[9]: DateTime,
    columns[10]: DateTime
    }

# Ensure the DataFrame does not have a MultiIndex
if isinstance(df.columns, pd.MultiIndex):
    df.columns = ['_'.join(col).strip() for col in df.columns.values]

if isinstance(df.index, pd.MultiIndex):
    df.reset_index(inplace=True)

# Write the DataFrame to the database
df.to_sql('raw_dwd_weather_stations_full', engine, schema='01_bronze', if_exists='replace', dtype=datatypes, index=False)

# Close the connection
conn.close()