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
    query = """
    SELECT
        "Stations_ID"::text AS "stations_id",
        "Stationsname"::text AS "station_name",
        "Breite"::float as "latitude",
        "Länge"::float as "longitude",
        "Bundesland"::text as "state"
    FROM
        "01_bronze".raw_weather_stations_full rwsf
    WHERE
        "Stations_ID" IN (
            SELECT 
                weather_station_id
            FROM 
                "02_silver".fact_weather_data fwd
        )
    GROUP BY 
        "Stations_ID", "Stationsname", "Breite", "Länge", "Bundesland"
    ORDER BY 
       "Stationsname";    
    """
    df_weather_stations = pd.read_sql(query, engine)
    
    df_weather_stations.to_sql('dim_weather_stations', engine, schema='02_silver', if_exists='replace', index=False)
        
except Exception as error:  
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")