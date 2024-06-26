import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
api_urls = [
    "https://api1.example.com/data",
    "https://api2.example.com/data",
    # Add more APIs here
]

db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def fetch_data_from_api(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def insert_data_to_postgresql(data, table_name):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    # Assuming data is a list of dictionaries
    columns = data[0].keys()
    query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s"
    values = [[value for value in item.values()] for item in data]

    execute_values(cursor, query, values)
    conn.commit()
    cursor.close()
    conn.close()

def main():
    for api_url in api_urls:
        data = fetch_data_from_api(api_url)
        table_name = api_url.split('/')[-1]  # Customize this as needed
        insert_data_to_postgresql(data, table_name)

if __name__ == "__main__":
    main()
