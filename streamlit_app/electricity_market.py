import streamlit as st
import pandas as pd
from packages.db_utils import st_get_engine
from packages.st_app_utils import get_timeframe, get_data
import plotly.graph_objects as go
from datetime import datetime, timedelta

@st.cache_data
def load_data(query):
    engine = st_get_engine()
    df = pd.read_sql(query, engine)
    df["timestamp"] = df["timestamp"].dt.tz_convert("Europe/Berlin")  # timezone
    df["hour"] = df.timestamp.dt.strftime('%H:%M')  # add hour column
    df['date'] = df.timestamp.dt.strftime('%Y-%m-%d')  # add date column
    df["timeframe"] = df["timestamp"].apply(get_timeframe)
    return df

query_string1 = """select * from "03_gold".fact_electricity_market_germany
    WHERE date_trunc('day', "timestamp") >= (
    SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '365 days'
    FROM "02_silver".fact_total_power_germany)"""

df_power = load_data(query_string1)

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
            y=-0.3,
            xanchor="center",
            x=0.5
        )
    )
    return fig

st.title('Whats going in the energy market')

col1, col2 = st.columns([3,1])
with col1:
    # Create multiselection for chart
    metrics_multiselect = st.multiselect(
        label="Select Metrics",
        options=["Total Production","Consumption", "Renewable Production", "Fossil Production", "Renewable Share of Generation"],
        default="Total Production"
    )
    
    multiselect_options = {
        "Total Production": "total_production",
        "Renewable Share of Generation": "renewable_share_of_generation",
        "Renewable Production": "renewable_production",
        "Fossil Production": "fossil_production",
        "Consumption": 'load_incl_self_consumption'
    }
    
    selected_metrics = [multiselect_options[metric] for metric in metrics_multiselect]

with col2:
    # Create timeframe selection
    timeframe_entries = df_power.timeframe.unique()
    timeframe_radio = st.selectbox("Please choose your metrics:", ["today","yesterday"])


# Filter dataframe
df_sel = df_power.query('timeframe == @timeframe_radio').sort_values(by='timestamp')

# Create chart
fig_combined = create_combined_line_chart(df_sel, selected_metrics, "Electricity Production Metrics Germany", "Time", "MWh", "Share (%)")

# Show chart
st.plotly_chart(fig_combined, theme="streamlit", use_container_width=True)