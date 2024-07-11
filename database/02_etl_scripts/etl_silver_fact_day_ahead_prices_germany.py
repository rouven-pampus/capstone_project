import pandas as pd
from packages.db_utils import get_engine

# Create engine
engine = get_engine()

#get data    
query_string1 = 'select * from "01_bronze".raw_energy_charts_day_ahead_prices_germany'
df_prices = pd.read_sql(query_string1, engine)

#Change column names
df_prices.columns = [col.strip().lower().replace(' ', '_') for col in df_prices.columns]
df_prices.columns = [col.strip().lower().replace('-', '_') for col in df_prices.columns]

#timezone conversion
df_prices['timestamp'] = df_prices['timestamp'].dt.tz_convert("Europe/Berlin") #timezone

#Add features
df_prices['time'] = df_prices.timestamp.dt.strftime('%H:%M') #add time column
df_prices['date'] = df_prices.timestamp.dt.strftime('%Y-%m-%d') #add date column

# Define the desired order of columns
new_column_order = [
    'timestamp',
    'date',
    'time',
    'de_lu',
    'unit'
]

# Reorder the columns
df_prices = df_prices[new_column_order]

#Sort Values
df_prices = df_prices.sort_values(by='timestamp', ascending=False, ignore_index=True)
    
df_prices.to_sql('fact_day_ahead_prices_germany', engine, schema='02_silver', if_exists='replace', index=False)
