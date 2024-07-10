import streamlit as st
import pandas as pd
import numpy as np
from packages.db_utils import get_data_from_db_st, get_engine
from packages.streamlit_app import get_timeframe
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime,timedelta

st.set_page_config(
    page_title="Ex-stream-ly Cool App",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

################## data extraction ##################

#Place for queries
query_prices = """ SELECT timestamp, de_lu as price, unit
    FROM "02_silver".fact_day_ahead_prices_germany
    WHERE date_trunc('day', "timestamp") >= (
        SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '7 days'
        FROM "02_silver".fact_day_ahead_prices_germany
    );
"""

#Cache data for faster loading times
@st.cache_data
def load_data(query):
    data = get_data_from_db_st(query)
    return data

#Load price data
df_prices = load_data(query_prices)

################## data transformation ##################

#Do transformations to price dataframe
df_prices["timestamp"] = df_prices["timestamp"].dt.tz_convert("Europe/Berlin") #timezone
df_prices["hour"] = df_prices.timestamp.dt.strftime('%H:%M') #add hour column
df_prices['date'] = df_prices.timestamp.dt.strftime('%Y-%m-%d') #add date column
df_prices["timeframe"] = df_prices["timestamp"].apply(get_timeframe)

#get timefram entries
timeframe_entries = df_prices.timeframe.unique()

#Add current time metrics
timestamp_now = pd.Timestamp.now()
datetime_now = timestamp_now.floor('s')
time_now = timestamp_now.strftime('%H:%M')
hour_now = timestamp_now.floor('H').strftime('%H:%M')
hour_before = format(F"{(datetime.now() - timedelta(hours=1)).hour}:00")
today_date = datetime_now.strftime('%Y-%m-%d')

#Add current price metrics
current_price = df_prices.query("hour == @hour_now and date == @today_date").price.values[0]
before_price = df_prices.query("hour == @hour_before and date == @today_date").price.values[0]
price_delta = round((current_price - before_price),2)

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
        colorscale='Blues'
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
st.title('Insights into insights into insights :sunglasses:')

col1, col2, col3 = st.columns(spec=3, gap="large")
with col1:
    st.metric("Date", today_date)
with col2:
    st.metric("Time", time_now)

    
col1, col2, col3 = st.columns(spec=3, gap="large")
with col1 :
    timeframe_radio = st.selectbox("Daily prices for:", timeframe_entries, index=2)
with col2:
    unit_radio = st.selectbox("select unit",["€/MWh","ct€/KWh"])
    if unit_radio == "€/MWh":
        x = 1
    elif unit_radio == "ct€/KWh":
        x = 10          
with col3:
    st.metric(label ="Current price", value = f"{round(current_price/x,2)} {unit_radio}", delta= f"{round(price_delta/x,2)} {unit_radio}", delta_color="inverse")

df_sel = df_prices.query('timeframe == @timeframe_radio')

# Creating and calling the price bar chart
fig = create_bar_chart(df_sel["timestamp"], df_sel["price"]/x, title="Day-ahead-price", x_title="Hour", y_title=unit_radio)

st.plotly_chart(fig, theme="streamlit", use_container_width=True)
