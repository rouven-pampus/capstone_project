import streamlit as st
import pandas as pd
import numpy as np
from packages.db_utils import st_get_engine
from packages.st_app_utils import get_timeframe, get_data
import plotly.graph_objects as go
from datetime import datetime, timedelta

query_market = """select * from "02_silver".fact_total_power_germany ftpg
    WHERE date_trunc('day', "timestamp") >= (
    SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '365 days'
    FROM "02_silver".fact_total_power_germany)"""

df_power = get_data(query_market)

# Do transformations to price dataframe
df_power["timestamp"] = df_power["timestamp"].dt.tz_convert("Europe/Berlin")  # timezone
df_power["hour"] = df_power.timestamp.dt.strftime('%H:%M')  # add hour column
df_power['date'] = df_power.timestamp.dt.strftime('%Y-%m-%d')  # add date column
df_power["timeframe"] = df_power["timestamp"].apply(get_timeframe)

# Get timeframe entries
timeframe_entries = df_power.timeframe.unique()

# Add current time metrics
timestamp_now = pd.Timestamp.now()
datetime_now = timestamp_now.floor('s')
time_now = timestamp_now.strftime('%H:%M')
hour_now = timestamp_now.floor('H').strftime('%H:%M')
hour_before = format(f"{(datetime.now() - timedelta(hours=1)).hour}:00")
today_date = datetime_now.strftime('%Y-%m-%d')


def create_combined_line_chart(df, metrics, title, x_title, y_title_left, y_title_right):
    """
    Create a combined line chart using Plotly with a secondary y-axis.

    Parameters:
    df (DataFrame): DataFrame containing the data.
    metrics (list): List of metrics to display on the y-axis.
    title (str): Title of the line chart.
    x_title (str): Label for the x-axis.
    y_title_left (str): Label for the left y-axis.
    y_title_right (str): Label for the right y-axis.

    Returns:
    fig: A Plotly figure object representing the line chart.
    """
    fig = go.Figure()

    # Add line traces for each metric
    for metric in metrics:
        if metric == "renewable_share_of_generation":
            fig.add_trace(go.Scatter(
                x=df["timestamp"],
                y=df[metric],
                mode='lines',
                name=metric,
                yaxis="y2"
            ))
        else:
            fig.add_trace(go.Scatter(
                x=df["timestamp"],
                y=df[metric],
                mode='lines',
                name=metric
            ))

    # Update layout
    fig.update_layout(
        title=title,
        xaxis_title=x_title,
        yaxis=dict(
            title=y_title_left,
            side="left"
        ),
        yaxis2=dict(
            title=y_title_right,
            overlaying='y',
            side='right'
        ),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )
    return fig

# Get timeframe entries
timeframe_entries = df_power.timeframe.unique()

timeframe_radio = st.selectbox("Total Production for:", timeframe_entries)
metrics_multiselect = st.multiselect(
    "Select Metrics",
    ["total_production", "renewable_share_of_generation", "renewable_production"],
    default=["total_production"]
)

df_sel = df_power.query('timeframe == @timeframe_radio').sort_values(by='timestamp')

fig_combined = create_combined_line_chart(df_sel, metrics_multiselect, "Electricity Production Metrics Germany", "Time", "Values (MWh)", "Renewable Share (%)")

st.plotly_chart(fig_combined, theme="streamlit", use_container_width=True)
