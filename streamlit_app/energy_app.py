import streamlit as st
import pandas as pd
from packages.st_app_utils import get_timeframe, st_get_engine
import plotly.graph_objects as go
from datetime import datetime,timedelta


################## data extraction energy app ##################

@st.cache_data
def load_data(query):
    df = pd.read_sql(query, st_get_engine())
    df["timestamp"] = df["timestamp"].dt.tz_convert("Europe/Berlin")  # timezone
    df["timeframe"] = df["timestamp"].apply(get_timeframe)
    return df

#Place for queries
query_string1 = """ SELECT timestamp, date, time, de_lu as price, unit
FROM "02_silver".fact_day_ahead_prices_germany
WHERE date_trunc('day', "timestamp") >= (
    SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '7 days'
    FROM "02_silver".fact_day_ahead_prices_germany
        );
"""

query_string2 = """
select 
    timestamp,
    TO_CHAR(timestamp, 'HH24:MI') AS time,
    TO_CHAR(timestamp, 'YYYY-MM-DD') AS date,
    avg(temperature_2m) as "temp",
    avg(sunshine_duration) as "sun",
    avg(wind_speed_10m) as wind
from "02_silver".fact_full_weather ffw 
left join "02_silver".dim_weather_stations dws on ffw.station_id = dws.station_id
    WHERE date_trunc('day', "timestamp") >= (
            SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '7 days'
            FROM "02_silver".fact_day_ahead_prices_germany        
        )
        group by timestamp
        order by 1 desc;
"""

df_prices = load_data(query_string1)
df_weather = load_data(query_string2)

################## data transformation ##################

#get timefram entries
timeframe_entries = df_prices.timeframe.unique().tolist()

# Ensure "today" is in the list
if "today" in timeframe_entries:
    today_index = timeframe_entries.index("today")
else:
    today_index = 0  # Fallback index if "today" is not in the list

#Add current time metrics
timestamp_now = pd.Timestamp.now()
datetime_now = timestamp_now.floor('s')
time_now = timestamp_now.strftime('%H:%M')
hour_now = pd.Timestamp.now().floor('H').strftime('%H:%M')
hour_before = (pd.Timestamp.now() - timedelta(hours=1)).floor('H').strftime('%H:%M')
today_date = datetime_now.strftime('%Y-%m-%d')

#Define dataframes for current metrics
df_current_price = df_prices.query("time == @hour_now and date == @today_date")
df_before_price = df_prices.query("time == @hour_before and date == @today_date")
df_current_weather = df_weather.query("time == @hour_now and date == @today_date")
df_before_weather = df_weather.query("time == @hour_before and date == @today_date")

#Add price metrics
current_price = round(df_current_price.price.values[0],1)
before_price = round(df_before_price.price.values[0],1)
delta_price = round((current_price - before_price),1)

#Add weather metrics
current_temp = round(df_current_weather.temp.values[0],1)
before_temp = round(df_before_weather.temp.values[0],1)
delta_temp = round((current_temp - before_temp),1)

current_sun = round(df_current_weather.sun.values[0]/60,1)
before_sun = round(df_before_weather.sun.values[0]/60,1)
delta_sun = round((current_sun - before_sun),1)

current_wind = round(df_current_weather.wind.values[0],1)
before_wind = round(df_before_weather.wind.values[0],1)
delta_wind = round((current_wind - before_wind),1)



################## definition of charts ##################

def create_bar_chart(x_data, y_data, title, x_title, y_title):
    """
    Create a bar chart using Plotly.

    Parameters:
    x_data (list): Data for the x-axis.
    y_data (list): Data for the y-axis.
    title (str): Title of the bar chart.
    x_title (str): Label for the x-axis.
    y_title (str): Label for the y-axis.

    Returns:
    fig: A Plotly figure object representing the bar chart.
    """
    fig = go.Figure()

    # Add bar trace
    fig.add_trace(go.Bar(
        x=x_data,
        y=y_data,
        marker=dict(
        color=y_data,  # Use y_data for coloring
        colorscale=[
            [0.0, '#61d3b7'],  # Start of the scale --> 0.0 ersetzen mit min(y), wobei y deine y-achse darstellt
            [1.0, '#26909b']   # End of the scale --> 1.0 ersetzen mit max(y)
            ]
        )
    ))
    
    # Update layout
    fig.update_layout(
        title=title,
        xaxis_title=x_title,
        yaxis_title=y_title,
        
    )
    return fig

################## Design elements ##################

#Example text
st.title('Welcome home 	:house_with_garden:')
    
col1, col2, col3  = st.columns ([1,1,2], vertical_alignment="center")
with col1:
    st.write(f"### Current time {time_now}")
with col2:
    timeframe_radio = st.selectbox("Daily prices for:", timeframe_entries, index=today_index)
with col3:
    st.write()
    
st.divider()

st.write(f"__Values at {hour_now}__")

col1, col2,col3,col4 = st.columns([1,1,1,1])
with col1:
    st.metric(label =f"Price", value = f"{current_price} €/MWh", delta= f"{delta_price} €/MWh", delta_color="inverse")
with col2:
    st.metric(label="Temperature", value=f"{current_temp} C°", delta=f"{delta_temp} C°", delta_color="off")
with col3:
    st.metric(label="Sunshine duration", value=f"{current_sun} min", delta=f"{delta_sun} min", delta_color="off")    
with col4:
    st.metric(label="Wind speed", value=f"{current_wind} km/h", delta=f"{delta_wind} km/h", delta_color="off")
    
df_sel = df_prices.query('timeframe == @timeframe_radio')

# Creating and calling the price bar chart
fig = create_bar_chart(df_sel["timestamp"], df_sel["price"], title="Day-ahead-price per hour", x_title="Hour", y_title="€/MWh")
st.plotly_chart(fig, theme="streamlit", use_container_width=True)
