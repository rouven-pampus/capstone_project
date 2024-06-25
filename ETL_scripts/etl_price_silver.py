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
    print("Price data loading!")
    query_string1 = 'SELECT * FROM "01_bronze"."raw_market_day_ahead_prices"'
    fact_market_day_ahead_price = pd.read_sql(query_string1, engine)
    print("Loading finished!")
    
    print("Loading complete. Starting transformation!")
    
    # Perform data transformation  

    fact_market_day_ahead_price.rename(columns={'germany/luxembourg_[€/mwh]_original_resolutions': 'germany/luxembourg_[€/mwh]', 
                              '∅_de/lu_neighbours_[€/mwh]_original_resolutions': '∅_de/lu_neighbours_[€/mwh]', 
                              'belgium_[€/mwh]_original_resolutions': 'belgium_[€/mwh]', 
                              'denmark_1_[€/mwh]_original_resolutions': 'denmark_1_[€/mwh]', 
                              'denmark_2_[€/mwh]_original_resolutions': 'denmark_2_[€/mwh]', 
                              'france_[€/mwh]_original_resolutions': 'france_[€/mwh]', 
                              'netherlands_[€/mwh]_original_resolutions': 'netherlands_[€/mwh]', 
                              'norway_2_[€/mwh]_original_resolutions': 'norway_2_[€/mwh]', 
                              'austria_[€/mwh]_original_resolutions': 'austria_[€/mwh]', 
                              'poland_[€/mwh]_original_resolutions': 'poland_[€/mwh]', 
                              'sweden_4_[€/mwh]_original_resolutions': 'sweden_4_[€/mwh]', 
                              'switzerland_[€/mwh]_original_resolutions': 'switzerland_[€/mwh]', 
                              'czech_republic_[€/mwh]_original_resolutions': 'czech_republic_[€/mwh]', 
                              'de/at/lu_[€/mwh]_original_resolutions': 'de/at/lu_[€/mwh]', 
                              'northern_italy_[€/mwh]_original_resolutions': 'northern_italy_[€/mwh]', 
                              'slovenia_[€/mwh]_original_resolutions': 'slovenia_[€/mwh]', 
                              'hungary_[€/mwh]_original_resolutions': 'hungary_[€/mwh]'
                              }, inplace=True)
    
    fact_market_day_ahead_price.replace(-999, np.nan, inplace=True)


    fact_market_day_ahead_price['start_date'] = pd.to_datetime(fact_market_day_ahead_price['start_date'], format='%Y-%m-%d %H:%M:%S')
    fact_market_day_ahead_price['end_date'] = pd.to_datetime(fact_market_day_ahead_price['end_date'], format='%Y-%m-%d %H:%M:%S')

    start_date = raw_price['start_date'].min()
    end_date = raw_price['end_date'].max()
    all_hours = pd.date_range(start=start_date, end=end_date, freq='h')

    
    # Create table in the database
    cursor = conn.cursor()
    new_table_command = """
       CREATE TABLE IF NOT EXISTS "02_silver".fact_market_day_ahead_price(
       start_date TIMESTAMP,
       end_date TIMESTAMP,
       germany/luxembourg_[€/mwh] FLOAT,
       ∅_de/lu_neighbours_[€/mwh] FLOAT,
       belgium_[€/mwh] FLOAT,
       denmark_1_[€/mwh] FLOAT,
       denmark_2_[€/mwh] FLOAT,
       france_[€/mwh] FLOAT,
       netherlands_[€/mwh] FLOAT,
       norway_2_[€/mwh] FLOAT,
       austria_[€/mwh] FLOAT,
       poland_[€/mwh] FLOAT,
       sweden_4_[€/mwh] FLOAT,
       switzerland_[€/mwh] FLOAT,
       czech_republic_[€/mwh] FLOAT,
       de/at/lu_[€/mwh] FLOAT,
       northern_italy_[€/mwh] FLOAT,
       slovenia_[€/mwh] FLOAT,  
       hungary_[€/mwh] FLOAT,           
    );
    """
    cursor.execute(new_table_command)
    conn.commit()
        
    # Insert the transformed data into the new table
    insert_query = """
        INSERT INTO "02_silver".fact_market_day_ahead_price (
        start_date, end_date, germany/luxembourg_[€/mwh], ∅_de/lu_neighbours_[€/mwh], 
        belgium_[€/mwh], denmark_1_[€/mwh], denmark_2_[€/mwh], france_[€/mwh], 
        netherlands_[€/mwh], norway_2_[€/mwh], austria_[€/mwh], poland_[€/mwh], 
        sweden_4_[€/mwh], switzerland_[€/mwh], czech_republic_[€/mwh], de/at/lu_[€/mwh], 
        northern_italy_[€/mwh], slovenia_[€/mwh], hungary_[€/mwh]
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for index, row in fact_market_day_ahead_price.iterrows():
        cursor.execute(insert_query, (row['...']))

    
except Exception as error:
    print("Error while connecting to PostgreSQL:", error)
    
finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
