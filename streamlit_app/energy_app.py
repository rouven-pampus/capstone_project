import streamlit as st
import pandas as pd
from packages.db_utils import st_get_engine
from packages.st_app_utils import get_timeframe
import plotly.graph_objects as go
from datetime import datetime,timedelta


################## data extraction energy app ##################

@st.cache_data
def load_data(query):
    engine = st_get_engine()
    df = pd.read_sql(query, engine)
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

df_prices = load_data(query_string1)

################## data transformation ##################

#get timefram entries
timeframe_entries = df_prices.timeframe.unique()

#Add current time metrics
timestamp_now = pd.Timestamp.now()
datetime_now = timestamp_now.floor('s')
time_now = timestamp_now.strftime('%H:%M')
hour_now = pd.Timestamp.now().floor('H').strftime('%H:%M')
hour_before = (pd.Timestamp.now() - timedelta(hours=1)).floor('H').strftime('%H:%M')
today_date = datetime_now.strftime('%Y-%m-%d')

#Add current price metrics
current_price = df_prices.query("time == @hour_now and date == @today_date").price.values[0]
before_price = df_prices.query("time == @hour_before and date == @today_date").price.values[0]
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
st.title('Fancy Insights :sunglasses:')

col1, col2, col3  = st.columns ([1,1,1])
with col1:
    timeframe_radio = st.selectbox("Daily prices for:", timeframe_entries, index=0,)
    df_sel = df_prices.query('timeframe == @timeframe_radio')
with col2:
    unit_radio = st.selectbox("Select unit",["€/MWh","ct€/KWh"])
    if unit_radio == "€/MWh":
        x = 1
    elif unit_radio == "ct€/KWh":
        x = 10
    # Creating and calling the price bar chart
    fig = create_bar_chart(df_sel["timestamp"], df_sel["price"]/x, title="Day-ahead-price", x_title="Hour", y_title=unit_radio)
with col3:
    st.metric(label =f"Price at {hour_now}", value = f"{round(current_price/x,2)} {unit_radio}", delta= f"{round(price_delta/x,2)} {unit_radio}", delta_color="inverse")

st.plotly_chart(fig, theme="streamlit", use_container_width=True)
