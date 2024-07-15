import pandas as pd
from packages.db_utils import get_engine

print("loading power data...")
query_string1 = 'select * from "02_silver".fact_total_power_germany'
df_power = pd.read_sql(query_string1, get_engine())

print("loading price data...")
query_string2 = 'select * from "02_silver".fact_day_ahead_prices_germany'
df_prices = pd.read_sql(query_string2, get_engine())

print("loading weather data...")
query_string3 = 'select * from "02_silver".fact_full_weather'
df_weather = pd.read_sql(query_string3, get_engine())


############################ 1. Preperation of tables ############################

############################ Total Power Table

print("Transforming power data...")
#define aggregation method for aggregating to full hour 
power_aggregation ={
    'hydro_pumped_storage_consumption': 'sum',
    'cross_border_electricity_trading': 'sum',
    'nuclear': 'sum',
    'hydro_run_of_river': 'sum',
    'biomass': 'sum',
    'fossil_brown_coal_lignite': 'sum',
    'fossil_hard_coal': 'sum',
    'fossil_oil': 'sum',
    'fossil_gas': 'sum',
    'geothermal': 'sum',
    'hydro_water_reservoir': 'sum',
    'hydro_pumped_storage': 'sum',
    'others': 'sum',
    'waste': 'sum',
    'wind_offshore': 'sum',
    'wind_onshore': 'sum',
    'solar': 'sum',
    'load_incl_self_consumption': 'sum',
    'residual_load': 'sum',
    'renewable_share_of_generation': 'mean',
    'renewable_share_of_load': 'mean',
    'total_production': 'sum',
    'renewable_production': 'sum',
    'fossil_production': 'sum'
}

#aggregating to full hour
df_power = df_power.groupby(df_power.timestamp.dt.floor('H')).agg(power_aggregation).reset_index()

############################ Prices Table

print("Transforming price data...")
df_prices.drop('unit', axis=1, inplace=True)
df_prices.rename(columns={'de_lu': 'price_eur_mwh'}, inplace=True)

############################ Weather Table

print("Transforming weather data...")
#Define aggregation
weather_aggregation = {
    'temperature_2m': 'mean',
    'relative_humidity_2m': 'mean',
    'apparent_temperature': 'mean',
    'precipitation': 'mean',
    'cloud_cover': 'mean',
    'wind_speed_10m': 'mean',
    'wind_direction_10m': 'mean',
    'direct_radiation': 'mean',
    'diffuse_radiation': 'mean',
    'sunshine_duration': 'mean'
}

#drop columns
df_weather.drop(['station_id','source_table', 'is_forecast'], axis=1, inplace=True)

#aggregate
df_weather = df_weather.groupby(['timestamp']).agg(weather_aggregation).reset_index().sort_values(by='timestamp', ascending=True)


############################ 2. Merge of tables ############################

print("Merging tables...")
# Merge df_weather and df_power on 'timestamp'
combined_df = pd.merge(df_weather, df_power, on='timestamp')

# Merge the resulting DataFrame with df_prices on 'timestamp'
combined_df = pd.merge(combined_df, df_prices, on='timestamp')

#correct timezone
combined_df['timestamp'] = combined_df['timestamp'].dt.tz_convert('Europe/Berlin')

# Define the desired order of columns
new_column_order = [
    'timestamp',
    'date',
    'time',   
    'hydro_run_of_river',     
    'hydro_water_reservoir', 
    'hydro_pumped_storage',     
    'biomass',
    'geothermal',     
    'wind_offshore', 
    'wind_onshore', 
    'solar',     
    'fossil_brown_coal_lignite', 
    'fossil_hard_coal', 
    'fossil_oil', 
    'fossil_gas',
    'nuclear',
    'others', 
    'waste',        
    'hydro_pumped_storage_consumption', 
    'load_incl_self_consumption',    
    'cross_border_electricity_trading',
    'residual_load',
    'renewable_share_of_generation', 
    'renewable_share_of_load',
    'total_production',
    'renewable_production',
    'fossil_production',
    'temperature_2m',
    'relative_humidity_2m',
    'apparent_temperature',
    'precipitation',
    'cloud_cover',
    'wind_speed_10m',
    'wind_direction_10m',
    'direct_radiation',
    'diffuse_radiation',
    'sunshine_duration',
    'price_eur_mwh'    
]

#new column order
combined_df = combined_df[new_column_order]

#Sort by timestamp
combined_df.sort_values(by='timestamp', ascending=False, ignore_index=True, inplace=True)
    
############################ 3. Load to database ############################
print("Loading new table...")
combined_df.to_sql('fact_electricity_market_germany', get_engine(), schema='03_gold', if_exists='replace', index=False)