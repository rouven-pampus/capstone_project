import streamlit as st
import pandas as pd
from packages.st_app_utils import st_get_engine
import plotly.graph_objects as go


################## data extraction energy app ##################

@st.cache_data
def load_data(query):
    df = pd.read_sql(query, st_get_engine())
    df["timestamp"] = df["timestamp"].dt.tz_convert("Europe/Berlin")  # timezone
    return df

#Place for queries
query_string1 = """ SELECT timestamp, date, time, de_lu as price, unit
    FROM "02_silver".fact_day_ahead_prices_germany
    WHERE date_trunc('day', "timestamp") >= (
        SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '7 days'
        FROM "02_silver".fact_day_ahead_prices_germany
    );
"""
query_string2 = """ SELECT * FROM "02_silver".fact_predicted_values;"""

df_prices = load_data(query_string1)
df_pred = load_data(query_string2)

df_prices.drop(["date","time","unit"], axis=1, inplace=True)

df_pivot = df_pred.pivot(index ="timestamp", columns="source", values="prediction")
df_pivot.reset_index(inplace=True)

# Define the start and end date for the hourly timestamps
start_date = df_pred["timestamp"].min()
end_date = df_pred["timestamp"].max()

# Create a date range with hourly frequency
hourly_timestamps = pd.date_range(start=start_date, end=end_date, freq='H')

# Create a DataFrame from the hourly timestamps
df_time = pd.DataFrame(hourly_timestamps, columns=['timestamp'])

#Merge dataframe together
df_combined = df_time.merge(df_pivot, on="timestamp", how="left")
df = df_combined.merge(df_prices, on="timestamp", how="left")

#Define chart
def create_combined_chart(df, metrics, title, x_title, y_title):
    """
    Create a combined chart using Plotly with a single y-axis.

    Parameters:
    df (DataFrame): DataFrame containing the data.
    metrics (list): List of metrics to display on the chart.
    title (str): Title of the chart.
    x_title (str): Label for the x-axis.
    y_title (str): Label for the y-axis.

    Returns:
    fig: A Plotly figure object representing the combined chart.
    """
    fig = go.Figure()

    # Add line traces for metrics other than "price"
    for metric in metrics:
        if metric != "price":
            fig.add_trace(go.Scatter(
                x=df["timestamp"],
                y=df[metric],
                mode='lines',
                name=metric
            ))

    # Add bar chart for "price"
    if "price" in metrics:
        fig.add_trace(go.Bar(
            x=df["timestamp"],
            y=df["price"],
            name="Day-Ahead-Price",
            marker=dict(
            color=df["price"],  # Use y_data for coloring
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
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="center",
            x=0.5
        ),
        height=500  # set the height of the chart in pixels
    )
    return fig

# Streamlit app
st.title('Predicting future prices :bulb:')

# Create multiselection for chart
metrics_multiselect = st.multiselect(
    label="Select Metrics",
    options=["Day-Ahead-Price", "Prediction", "Prediction 24h", "Prediction 48h", "Prediction 72h"],
    default=["Day-Ahead-Price"]
)

multiselect_options = {
    "Day-Ahead-Price": "price",
    "Prediction": "comb.",
    "Prediction 24h": "24h",
    "Prediction 48h": "48h",
    "Prediction 72h": "72h"
}

selected_metrics = [multiselect_options[metric] for metric in metrics_multiselect]

# Create chart
fig = create_combined_chart(df, selected_metrics, "Electrictiy price per hour", "Time", "€/MWh")

# Show chart
st.plotly_chart(fig, theme="streamlit", use_container_width=True)