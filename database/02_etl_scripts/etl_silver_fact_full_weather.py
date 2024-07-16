####################### get packages #######################

import psycopg2
from sqlalchemy import create_engine
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

####################### 1. Get list of coordinates of used weather stations from dim_weather_stations  #######################

try:
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to -", record, "\n")
   
    #recreate view
    query_string1 = """
        DROP TABLE IF EXISTS "02_silver".fact_full_weather;
        
        CREATE TABLE "02_silver".fact_full_weather AS
        SELECT DISTINCT ON (timestamp, station_id)
            timestamp,
            station_id,
            temperature_2m,
            relative_humidity_2m,
            apparent_temperature,
            precipitation,
            cloud_cover,
            wind_speed_10m,
            wind_direction_10m,
            direct_radiation,
            diffuse_radiation,
            sunshine_duration,
            is_forecast,
            source_table
        FROM ( 
            SELECT
                timestamp,
                station_id,
                temperature_2m,
                relative_humidity_2m,
                apparent_temperature,
                precipitation,
                cloud_cover,
                wind_speed_10m,
                wind_direction_10m,
                direct_radiation,
                diffuse_radiation,
                sunshine_duration,
                'no' AS is_forecast,
                'hist' AS source_table
            FROM "01_bronze".raw_open_meteo_weather_history romwh
            UNION
            SELECT
                timestamp,
                station_id,
                temperature_2m,
                relative_humidity_2m,
                apparent_temperature,
                precipitation,
                cloud_cover,
                wind_speed_10m,
                wind_direction_10m,
                direct_radiation,
                diffuse_radiation,
                sunshine_duration,
                is_forecast,
                'forecast' AS source_table
            FROM "01_bronze".raw_open_meteo_weather_forecast romwf
        ) AS combined_weather
        ORDER BY station_id ASC, timestamp DESC, source_table DESC;       
    """
    cursor.execute(query_string1)
    conn.commit()   

except Exception as error:
    print("Error while connecting to PostgreSQL:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")