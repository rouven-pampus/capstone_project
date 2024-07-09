import streamlit as st
import pandas as pd
import numpy as np
from packages.db_utils import get_data_from_db_st, get_engine
from packages.streamlit_app import classify_timestamp
import plotly.express as px
import plotly.graph_objects as go

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

#Example text
st.title('Our amazing, world-changing app :sunglasses:')

#Place for queries
query_prices = """
    SELECT * 
    FROM "02_silver".fact_day_ahead_prices_germany
    WHERE date_trunc('day', "timestamp") >= (
        SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '7 days'
        FROM "02_silver".fact_day_ahead_prices_germany
    );
"""
#Load price data
df_prices = get_data_from_db_st(query_prices)

#Do transformations to price dataframe
df_prices["timestamp"] = df_prices["timestamp"].dt.tz_convert("Europe/Berlin")
df_prices['timeframe'] = df_prices['timestamp'].apply(classify_timestamp)
df_prices["hour"] = df_prices.timestamp.dt.strftime('%H:%M')
df_prices

timeframe_list = df_prices.timeframe.unique()

now = pd.Timestamp.now()

current_price = df_prices[df_prices["timestamp"].dt.hour == pd.Timestamp.now().hour].max().de_lu
current_price

#Metric bar
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("current time", now.strftime('%H:%M'))
with col2:
    st.metric("current price", current_price)
with col3:
    st.metric("current time", now.strftime('%H:%M'))

timeframe_radio = st.selectbox("Daily prices for:", timeframe_list, )
    
df = df_prices.query('timeframe == @timeframe_radio')
    
# Create stepped line chart using Plotly
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df['timestamp'],
    y=df['de_lu'],
    mode='lines',
    line_shape='hv'
))

# Update layout
fig.update_layout(
    title='Stepped Line Chart',
    xaxis_title='Timestamp',
    yaxis_title='EUR/MWh'        
)

# You can call any Streamlit command, including custom components:
st.plotly_chart(fig, theme="streamlit", use_container_width=True)
