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
    df["wind"] = df[["wind_onshore","wind_offshore"]].sum(axis=1)
    df["sunshine"] = df["sunshine_duration"] 
    df["temperature"] = df["temperature_2m"] 
    return df

query_string2 = """select * from "03_gold".fact_electricity_market_germany
    WHERE date_trunc('day', "timestamp") >= (
    SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '30 days'
    FROM "02_silver".fact_total_power_germany)"""

df_weather = load_data(query_string2)


def create_plot(df, metric):
    # Set colours for different metrics
    colors = {
        'temperature': {
            'line': '#40c088',  
            'fill': 'rgba(255, 127, 14, 0.0)'  
        },
        'wind': {
            'line': '#26909b',  
            'fill': 'rgba(44, 160, 44, 0.0)'  
        },
        'sunshine': {
            'line': '#fac500',  
            'fill': 'rgba(214, 39, 40, 0.0)'  
        }
    }

    # Default colours if metric is not defined
    default_color = {
        'line': '#26909b',
        'fill': 'rgba(38, 144, 155, 0.2)'
    }

    # Choose the colours based on the metric
    selected_colors = colors.get(metric, default_color)

    fig = go.Figure(data=[go.Scatter(
        x=df['timestamp'],
        y=df[metric],
        mode='lines',
        line=dict(color=selected_colors['line'], width=3),
        fill='tozeroy',
        fillcolor=selected_colors['fill']
    )])
    fig.update_layout(
        title=f"{metric.capitalize()} over time",
        xaxis_title='Time',
        yaxis_title=metric,
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(
            family="Helvetica, Arial, sans-serif",
            size=14,
            color="white"
        ),
        xaxis=dict(
            showline=True,
            showgrid=False,
            linecolor='white',
            linewidth=2,
            ticks='outside',
            tickfont=dict(
                family='Arial',
                size=12,
                color='white'
            ),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='gray',
            gridwidth=0.5,
            showline=True,
            linecolor='white',
            linewidth=2,
            ticks='outside',
            tickfont=dict(
                family='Arial',
                size=12,
                color='white'
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
    color: white;
    background-color: transparent;
}
div.stButton > button:first-child:hover {
    border: 2px solid #26909b; /* Blaue Umrandung beim Hover */
    color: white;
    background-color: transparent;
}
div.stButton > button:first-child:focus {
    border: 2px solid #26909b; /* Blaue Umrandung beim Fokus */
    color: white;
    background-color: transparent;
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.5);
}

/* Dropdown-Stil anpassen */
div[data-baseweb="select"] > div {
    border: 1px solid #d4d4d4 !important; /* Standardgraue Umrandung */
    cursor: default !important; /* Standard-Cursor statt Text-Cursor */
}
div[data-baseweb="select"] > div:hover {
    border-color: #26909b !important; /* Blaue Umrandung beim Hover */
}
div[data-baseweb="select"] > div:focus-within {
    border-color: #26909b !important; /* Blaue Umrandung beim Fokus */
    outline: none !important;
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.5) !important;
}
</style>
""", unsafe_allow_html=True)

# Layout for buttons and drop-down menu
col1, col2 = st.columns([3, 1])

with col1:
    temp_emoji = "🌡️"  
    wind_emoji = "💨"  
    sun_emoji = "☀️"  
    if st.button(f'{temp_emoji} Temperature'):
        st.session_state.active_metric = 'temperature'
    if st.button(f'{wind_emoji} Wind'):
        st.session_state.active_metric = 'wind'
    if st.button(f'{sun_emoji} Sunshine'):
        st.session_state.active_metric = 'sunshine'

with col2:
    # Dropdown for the period selection
    options = {
        "Last 7 days": datetime.now() - timedelta(days=7),
        "Last 14 days": datetime.now() - timedelta(days=14),
        "Last 21 days": datetime.now() - timedelta(days=21)
    }
    selected_option = st.selectbox("Select period:", list(options.keys()))
    selected_date = pd.Timestamp(options[selected_option], tz='Europe/Berlin')  

# Filter the data by selected time period
df_weather['timestamp'] = pd.to_datetime(df_weather['timestamp'], utc=True)
df_weather = df_weather[df_weather['timestamp'] >= selected_date]

# Display the graph based on the current selection
selected_metric = st.session_state.get('active_metric', 'temperature')
st.plotly_chart(create_plot(df_weather, selected_metric), use_container_width=True)






