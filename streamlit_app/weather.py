import streamlit as st
import pandas as pd
from packages.st_app_utils import get_timeframe, st_get_engine
import plotly.graph_objects as go
from datetime import datetime, timedelta

@st.cache_data
def load_data(query):
    df = pd.read_sql(query, st_get_engine())
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)  # Ensure timestamp is in correct format
    df["timestamp"] = df["timestamp"].dt.tz_convert("Europe/Berlin")  # timezone
    df["hour"] = df.timestamp.dt.strftime('%H:%M')  # add hour column
    df['date'] = df.timestamp.dt.strftime('%Y-%m-%d')  # add date column
    df["timeframe"] = df["timestamp"].apply(get_timeframe)
    df["wind"] = df["wind_speed_10m"]
    df["sunshine"] = df["sunshine_duration"] / 3600  # Convert to hours per day
    df["temperature"] = df["temperature_2m"] 
    return df

query_string2 = """select * from "03_gold".fact_electricity_market_germany
    WHERE date_trunc('day', "timestamp") >= (
    SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '30 days'
    FROM "02_silver".fact_total_power_germany)"""

df_weather = load_data(query_string2)

# Aggregate sunshine data over daily intervals to smooth out night values
df_weather['timestamp'] = pd.to_datetime(df_weather['timestamp'])
df_weather.set_index('timestamp', inplace=True)
daily_sunshine = df_weather['sunshine'].resample('D').sum().reset_index()  # Sum sunshine per day
df_weather.reset_index(inplace=True)

def create_plot(df, metric):
    # Set colors and axis labels for different metrics
    metrics_info = {
        'temperature': {
            'line': '#40c088',  
            'fill': 'rgba(64, 192, 136, 0.0)',  
            'yaxis_title': 'Temperature [°C]'
        },
        'wind': {
            'line': '#26909b',  
            'fill': 'rgba(38, 144, 155, 0.0)',  
            'yaxis_title': 'Wind Speed [m/s]'
        },
        'sunshine': {
            'line': '#fac500',  
            'fill': 'rgba(250, 197, 0, 0.0)',
            'yaxis_title': 'Sunshine [h/d]'  # Adjusted label for daily aggregated sunshine
        }
    }

    # Default colours and labels if metric is not defined
    default_info = {
        'line': '#26909b',
        'fill': 'rgba(38, 144, 155, 0.2)',
        'yaxis_title': 'Value'
    }

    # Choose the colors and labels based on the metric
    selected_info = metrics_info.get(metric, default_info)

    fig = go.Figure(data=[go.Scatter(
        x=df['timestamp'],
        y=df[metric],
        mode='lines',
        line=dict(color=selected_info['line'], width=3),
        fill='tozeroy',
        fillcolor=selected_info['fill']
    )])
    fig.update_layout(
        title=f"{metric.capitalize()} over time",
        xaxis_title='',
        yaxis_title=selected_info['yaxis_title'],  # Dynamic y-axis title based on the metric
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(
            family="Helvetica, Arial, sans-serif",
            size=14  # Changed to black for better visibility in light mode
        ),
        xaxis=dict(
            showline=True,
            showgrid=False,
            linecolor='black',  # Changed to black for better visibility in light mode
            linewidth=2,
            ticks='outside',
            tickfont=dict(
                family='Arial',
                size=12  # Changed to black for better visibility in light mode
            ),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='gray',
            gridwidth=0.5,
            showline=True,
            linecolor='black',  # Changed to black for better visibility in light mode
            linewidth=2,
            ticks='outside',
            tickfont=dict(
                family='Arial',
                size=12  # Changed to black for better visibility in light mode
            ),
        )
    )
    return fig


# Title of the Streamlit app
st.title('Overview of weather data')

# Add custom CSS for button colours
st.markdown("""
<style>
/* Buttons-Stil anpassen */
div.stButton > button:first-child {
    border: 1px solid #d4d4d4; /* Standardgraue Umrandung */
    background-color: transparent;
    width: 150px;  /* Festgelegte Breite */
    height: 50px;  /* Festgelegte Höhe */
}
div.stButton > button:first-child:hover {
    border: 2px solid #26909b; /* Blaue Umrandung beim Hover */
    background-color: transparent;
    color: #26909b;  /* Textfarbe beim Hover auf Blau setzen */
}
div.stButton > button:first-child:focus {
    border: 2px solid #26909b; /* Blaue Umrandung beim Fokus */
    background-color: transparent;
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.5);
    color: #26909b;  /* Textfarbe beim Fokus auf Blau setzen */
}

/* Dropdown-Stil anpassen */
div[data-baseweb="select"] > div {
    border: 1px solid #d4d4d4 !important; /* Standardgraue Umrandung */
    cursor: default !important; /* Standard-Cursor statt Text-Cursor */
    margin-top: -30px; /* Reduzierter Abstand nach oben */
    margin-left: 0; /* Abstand nach links */
}
div[data-baseweb="select"] > div:hover {
    border-color: #26909b !important; /* Blaue Umrandung beim Hover */
}
div[data-baseweb="select"] > div:focus-within {
    border-color: #26909b !important; /* Blaue Umrandung beim Fokus */
    outline: none !important;
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.5) !important;
}

/* Label-Stil anpassen */
div[data-testid="stFormLabel"] > label {
    margin-top: 0px; /* Reduzierter Abstand nach oben */
    margin-left: 0; /* Abstand nach links */
    display: block; /* Block-Anzeige für den Abstand */
    text-align: left; /* Text nach links ausrichten */
}
</style>
""", unsafe_allow_html=True)

# Layout for buttons
temp_emoji = "🌡️"
wind_emoji = "💨"
sun_emoji = "☀️"
button_col1, button_col2, button_col3 = st.columns([1, 1, 1])
with button_col1:
    if st.button(f'{temp_emoji} Temperature'):
        st.session_state.active_metric = 'temperature'
with button_col2:
    if st.button(f'{wind_emoji} Wind'):
        st.session_state.active_metric = 'wind'
with button_col3:
    if st.button(f'{sun_emoji} Sunshine'):
        st.session_state.active_metric = 'sunshine'

# Dropdown for the period selection
st.markdown("##### Select Time Period")
options = {
    "Last 7 days": datetime.now() - timedelta(days=7),
    "Last 14 days": datetime.now() - timedelta(days=14),
    "Last 21 days": datetime.now() - timedelta(days=21)
}
selected_option = st.selectbox(" ", list(options.keys()))
selected_date = pd.Timestamp(options[selected_option], tz='Europe/Berlin')

# Filter the data by selected time period
df_weather['timestamp'] = pd.to_datetime(df_weather['timestamp'], utc=True)
df_weather = df_weather[df_weather['timestamp'] >= selected_date]

# Display the graph based on the current selection
selected_metric = st.session_state.get('active_metric', 'temperature')
if selected_metric == 'sunshine':
    daily_sunshine = df_weather[df_weather['timestamp'] >= selected_date]
    daily_sunshine = daily_sunshine.set_index('timestamp')['sunshine'].resample('D').sum().reset_index()
    df_weather = daily_sunshine  # Use daily aggregated data for sunshine
    selected_metric = 'sunshine'  # Ensure the metric name matches the aggregated data

# Display the graph based on the current selection
st.plotly_chart(create_plot(df_weather, selected_metric), use_container_width=True)


