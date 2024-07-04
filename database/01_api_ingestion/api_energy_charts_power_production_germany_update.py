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
end_date = (pd.to_datetime("today") - timedelta(days=1)).strftime("%Y-%m-%d") #cut-off date to avoid null values

country = "de"
start = start_date
end = end_date

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

df_power.to_sql('raw_energy_charts_total_power_germany_temp', engine, schema='01_bronze', if_exists='replace', index=False)

try:
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to -", record, "\n")
    
    # Insert new data into old history table
    query_string1 = """
    INSERT INTO "01_bronze".raw_energy_charts_total_power_germany (
        timestamp,
        "Hydro pumped storage consumption" ,
        "Cross border electricity trading" ,
        "Hydro Run-of-River" ,
        "Biomass" ,
        "Fossil brown coal / lignite" ,
        "Fossil hard coal" ,
        "Fossil oil" ,
        "Fossil gas" ,
        "Geothermal" ,
        "Hydro water reservoir" ,
        "Hydro pumped storage" ,
        "Others" ,
        "Waste" ,
        "Wind offshore" ,
        "Wind onshore" ,
        "Solar" ,
        "Load (incl. self-consumption)" ,
        "Residual load" ,
        "Renewable share of generation" ,
        "Renewable share of load"         
    )
    select
	    timestamp,
        "Hydro pumped storage consumption" ,
        "Cross border electricity trading" ,
        "Hydro Run-of-River" ,
        "Biomass" ,
        "Fossil brown coal / lignite" ,
        "Fossil hard coal" ,
        "Fossil oil" ,
        "Fossil gas" ,
        "Geothermal" ,
        "Hydro water reservoir" ,
        "Hydro pumped storage" ,
        "Others" ,
        "Waste" ,
        "Wind offshore" ,
        "Wind onshore" ,
        "Solar" ,
        "Load (incl. self-consumption)" ,
        "Residual load" ,
        "Renewable share of generation" ,
        "Renewable share of load" 
    FROM "01_bronze".raw_energy_charts_total_power_germany_temp 
    ON CONFLICT (timestamp) 
    DO UPDATE SET 
   		"Hydro pumped storage consumption" = EXCLUDED."Hydro pumped storage consumption",
        "Cross border electricity trading" = EXCLUDED."Cross border electricity trading",
        "Hydro Run-of-River" = EXCLUDED."Hydro Run-of-River",
        "Biomass" = EXCLUDED."Biomass",
        "Fossil brown coal / lignite" = EXCLUDED."Fossil brown coal / lignite",
        "Fossil hard coal" = EXCLUDED."Fossil hard coal",
        "Fossil oil" = EXCLUDED."Fossil oil",
        "Fossil gas" = EXCLUDED."Fossil gas",
        "Geothermal" = EXCLUDED."Geothermal",
        "Hydro water reservoir" = EXCLUDED."Hydro water reservoir",
        "Hydro pumped storage" = EXCLUDED."Hydro pumped storage",
        "Others" = EXCLUDED."Others",
        "Waste" = EXCLUDED."Waste",
        "Wind offshore" = EXCLUDED."Wind offshore",
        "Wind onshore" = EXCLUDED."Wind onshore",
        "Solar" = EXCLUDED."Solar",
        "Load (incl. self-consumption)" = EXCLUDED."Load (incl. self-consumption)",
        "Residual load" = EXCLUDED."Residual load",
        "Renewable share of generation" = EXCLUDED."Renewable share of generation",
        "Renewable share of load" = EXCLUDED."Renewable share of load"   
    """
    #Statement to drop temporary table
    query_string2 = """Drop table "01_bronze".raw_energy_charts_total_power_germany_temp;"""
    
    print("Insert new power data...")
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
