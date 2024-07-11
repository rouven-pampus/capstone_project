import pandas as pd
from packages.db_utils import get_engine

#create engine
engine = get_engine()

#get data
query_string1 = 'select * from "01_bronze".raw_energy_charts_total_power_germany'
df_power = pd.read_sql(query_string1, engine)

#Change column names
df_power.columns = [col.strip().lower().replace(' / ', '_').replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_').replace('.', '') for col in df_power.columns]

#timezone conversion
df_power['timestamp'] = df_power['timestamp'].dt.tz_convert("Europe/Berlin") #timezone

#data cleaning
df_power.drop('fossil_coal_derived_gas', axis=1, inplace=True)
df_power['nuclear'] = df_power['nuclear'].fillna(0)
df_power.dropna(inplace=True)

renewable_columns = [
    'hydro_run_of_river',     
    'hydro_water_reservoir', 
    'hydro_pumped_storage',     
    'biomass',
    'geothermal',     
    'wind_offshore', 
    'wind_onshore', 
    'solar']

production_columns = [
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
    'waste']

#Add features
df_power['time'] = df_power.timestamp.dt.strftime('%H:%M') #add time column
df_power['date'] = df_power.timestamp.dt.strftime('%Y-%m-%d') #add date column
df_power['total_production'] = df_power[production_columns].sum(axis=1) #sum for total production
df_power['renewable_production'] = df_power[renewable_columns].sum(axis=1) #sum for renewable production

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
    'renewable_production'
]

# Reorder the columns
df_power = df_power[new_column_order]

#Sort Values
df_power = df_power.sort_values(by='timestamp', ascending=False, ignore_index=True)

#Export to database
df_power.to_sql('fact_total_power_germany', engine, schema='02_silver', if_exists='replace', index=False)