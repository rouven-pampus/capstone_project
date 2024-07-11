import streamlit as st
import pandas as pd
import numpy as np
from packages.db_utils import st_get_engine
from packages.streamlit_app import get_timeframe
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime,timedelta

query_market =""" select * from "02_silver".fact_total_power_germany ftpg
    WHERE date_trunc('day', "timestamp") >= (
    SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '365 days'
    FROM "02_silver".fact_total_power_germany)  """

df_power = pd.read_sql(query_market, st_get_engine())

#Do transformations to price dataframe
df_power["timestamp"] = df_power["timestamp"].dt.tz_convert("Europe/Berlin") #timezone
df_power["hour"] = df_power.timestamp.dt.strftime('%H:%M') #add hour column
df_power['date'] = df_power.timestamp.dt.strftime('%Y-%m-%d') #add date column
df_power["timeframe"] = df_power["timestamp"].apply(get_timeframe)

#get timefram entries
timeframe_entries = df_power.timeframe.unique()

#Add current time metrics
timestamp_now = pd.Timestamp.now()
datetime_now = timestamp_now.floor('s')
time_now = timestamp_now.strftime('%H:%M')
hour_now = timestamp_now.floor('H').strftime('%H:%M')
hour_before = format(F"{(datetime.now() - timedelta(hours=1)).hour}:00")
today_date = datetime_now.strftime('%Y-%m-%d')


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
        yaxis_title=y_title        
    )
    return fig


def create_line_chart(x_data, y_data, title, x_title, y_title):
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
    fig.add_trace(go.Line(
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
        yaxis_title=y_title        
    )
    return fig

#get timefram entries
timeframe_entries = df_power.timeframe.unique()

timeframe_radio = st.selectbox("Total Production for:", timeframe_entries)

df_sel = df_power.query('timeframe == @timeframe_radio')

fig_prod = create_line_chart(df_sel["timestamp"], df_sel["total_production"],"Total Electricity Production Germany", "Time", "MWh" )
fig_rens = create_line_chart(df_sel["timestamp"], df_sel["renewable_share_of_generation"],"Share of Renewable Production Germany", "Time", "% Share of total" )
fig_ren = create_line_chart(df_sel["timestamp"], df_sel["renewable_production"],"Renewable Production Germany", "Time", "MWh" )


st.plotly_chart(fig_prod, theme="streamlit", use_container_width=True)
st.plotly_chart(fig_ren, theme="streamlit", use_container_width=True)
st.plotly_chart(fig_rens, theme="streamlit", use_container_width=True)