# Importing Modules
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from packages.db_utils import st_get_engine
from packages.st_app_utils import get_timeframe, get_data
from packages.calculator_css import custom_css

# set up style.
st.markdown(custom_css, unsafe_allow_html=True)

# Load consumption data
query_consumption = """SELECT * FROM "01_bronze".raw_consumption_pattern"""
hourly_consumption_data = get_data(query_consumption)
consumption_sum = hourly_consumption_data['total'].sum()
hourly_consumption_data['usage'] = hourly_consumption_data['total'] / consumption_sum

# Page title
st.title("Electricity Savings Calculator")
option = st.radio("Do you currently have a fixed or flexible electricity usage plan?", ('Fix', 'Flexible'))

# User Input for Annual Consumption
col1, col2, col3= st.columns([1,0.5,1.5])
with col1:
    if option == 'Fix':
        st.subheader("Fixed Plan Details")
        annual_consumption = st.number_input("Enter your annual electricity consumption (kWh)", min_value=0, value=2500)
        fix_price = st.number_input("Enter your fixed price (€/month)", min_value=0.0)
        working_price = st.number_input("Enter your working price (cents/kWh)", min_value=0.0)
        bundesland = st.selectbox("Select your State", [
        "Baden-Württemberg", "Bayern", "Berlin", "Brandenburg", "Bremen",
        "Hamburg", "Hessen", "Niedersachsen", "Mecklenburg-Vorpommern",
        "Nordrhein-Westfalen", "Rheinland-Pfalz", "Saarland",
        "Sachsen", "Sachsen-Anhalt", "Schleswig-Holstein", "Thüringen"
        ])
    elif option == 'Flexible':
        st.subheader("Flexible Plan Details")
        annual_consumption = st.number_input("Enter your annual electricity consumption (kWh)", min_value=0, value=2500)
        bundesland = st.selectbox("Select your State", [
        "Baden-Württemberg", "Bayern", "Berlin", "Brandenburg", "Bremen",
        "Hamburg", "Hessen", "Niedersachsen", "Mecklenburg-Vorpommern",
        "Nordrhein-Westfalen", "Rheinland-Pfalz", "Saarland",
        "Sachsen", "Sachsen-Anhalt", "Schleswig-Holstein", "Thüringen"
        ])

with col3:
    if option == 'Fix':
        st.subheader("Flexibility Details")
        flexibility_00_08 = st.slider("Flexibility (00:00-08:00)", min_value=0, max_value=100, value=50)
        flexibility_08_20 = st.slider("Flexibility (08:00-20:00)", min_value=0, max_value=100, value=50)
        flexibility_20_24 = st.slider("Flexibility (20:00-24:00)", min_value=0, max_value=100, value=50)
    elif option == 'Flexible':
        st.subheader("Flexibility Details")
        flexibility_00_08 = st.slider("Flexibility (00:00-08:00)", min_value=0, max_value=100, value=50)
        flexibility_08_20 = st.slider("Flexibility (08:00-20:00)", min_value=0, max_value=100, value=50)
        flexibility_20_24 = st.slider("Flexibility (20:00-24:00)", min_value=0, max_value=100, value=50)


# State Data
netzentgelte_2024 = {
    "Baden-Württemberg": 7.01,
    "Bayern": 6.97,
    "Berlin": 5.59,
    "Brandenburg": 8.45,
    "Bremen": 5.56,
    "Hamburg": 8.17,
    "Hessen": 6.92,
    "Mecklenburg-Vorpommern": 8.13,
    "Niedersachsen": 7.17,
    "Nordrhein-Westfalen": 6.72,
    "Rheinland-Pfalz": 6.79,
    "Saarland": 7.39,
    "Sachsen": 7.16,
    "Sachsen-Anhalt": 7.52,
    "Schleswig-Holstein": 9.63,
    "Thüringen": 7.27
}

konzessionsabgaben_2024 = {
    "Baden-Württemberg": 1.50,
    "Bayern": 1.60,
    "Berlin": 2.20,
    "Brandenburg": 1.45,
    "Bremen": 1.80,
    "Hamburg": 2.10,
    "Hessen": 1.55,
    "Mecklenburg-Vorpommern": 1.40,
    "Niedersachsen": 1.70,
    "Nordrhein-Westfalen": 1.65,
    "Rheinland-Pfalz": 1.60,
    "Saarland": 1.60,
    "Sachsen": 1.35,
    "Sachsen-Anhalt": 1.35,
    "Schleswig-Holstein": 1.90,
    "Thüringen": 1.35
}

# Functions
def fetch_market_prices():
    query_prices = """SELECT timestamp, de_lu as price, unit
        FROM "02_silver".fact_day_ahead_prices_germany
        WHERE date_trunc('day', "timestamp") >= (
            SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '365 days'
            FROM "02_silver".fact_day_ahead_prices_germany
        );
    """

    @st.cache_data
    def get_data(query):
        df = pd.read_sql(query, st_get_engine())
        return df

    df_prices = get_data(query_prices)
    df_prices["timestamp"] = df_prices["timestamp"].dt.tz_convert("Europe/Berlin")
    df_prices["hour"] = df_prices["timestamp"].dt.hour
    df_prices["date"] = df_prices["timestamp"].dt.strftime('%Y-%m-%d')
    return df_prices

def get_taxes_for_bundesland(bundesland):
    netzentgelt = netzentgelte_2024[bundesland]
    eeg_umlage = 6.5
    kwkg_umlage = 0.5
    strom_nev_umlage = 0.4
    offshore_umlage = 0.2
    ablav_umlage = 0.1
    stromsteuer = 2.05
    konzessions_abgabe = konzessionsabgaben_2024[bundesland]
    taxes = netzentgelt + eeg_umlage + kwkg_umlage + strom_nev_umlage + offshore_umlage + ablav_umlage + stromsteuer + konzessions_abgabe
    return taxes

def get_flexibility(hour):
    if hour < 8:
        return flexibility_00_08 / 100
    elif hour < 20:
        return flexibility_08_20 / 100
    else:
        return flexibility_20_24 / 100
    
def get_flexibility_group(hour):
    if hour < 8:
        return 'A'
    elif hour < 20:
        return 'B'
    else:
        return 'C'

def calculate_savings_fix(bundesland, annual_consumption, fix_price, working_price, flexibility_00_08, flexibility_08_20, flexibility_20_24, mehrwertsteuer=0.19):
    market_prices = fetch_market_prices()
    taxes = get_taxes_for_bundesland(bundesland)
    market_prices['price_full'] = ((market_prices['price'] / 100 + taxes) * (1 + mehrwertsteuer))/100

    # get flexibility and flexibility groups based on time
    market_prices['hour'] = market_prices['timestamp'].dt.hour
    market_prices = market_prices.merge(hourly_consumption_data, on='hour', how='left')
    market_prices['flexibility'] = market_prices['hour'].apply(get_flexibility)
    market_prices['flexibility_group'] = market_prices['hour'].apply(get_flexibility_group)

    # Calculate the fix cost, so the usage paying the standard prize
    market_prices['fixed_cost'] = market_prices['hourly_consumption'] * (1 - market_prices['flexibility']) * market_prices['price_full']

    # Calculate flexible cost
    min_prices = market_prices.groupby([market_prices['date'], market_prices['flexibility_group']])['price_full'].min().reset_index()
    min_prices.columns = ['date', 'flexibility_group', 'min_price']
    market_prices = market_prices.merge(min_prices, on=['date', 'flexibility_group'], how='left')
    market_prices['flexible_cost'] = market_prices['hourly_consumption'] * market_prices['flexibility'] * market_prices['min_price']

    # Calculate -potential- daily and annual costs
    daily_cost = market_prices.groupby('date')[['flexible_cost', 'fixed_cost']].sum().reset_index(drop=False)
    annual_potential_cost = daily_cost[['flexible_cost', 'fixed_cost']].sum().sum()
    potential_cost = annual_potential_cost

    # Calculate current paid costs
    annual_current_cost = annual_consumption * working_price / 100
    annual_fixed_price = 12 * fix_price
    current_cost = annual_current_cost + annual_fixed_price

    # Calculate savings
    potential_savings = max(0, current_cost - potential_cost)
    saving_ratio = (potential_savings / current_cost) * 100 if current_cost != 0 else 0

    return potential_savings, saving_ratio, market_prices, daily_cost, current_cost, potential_cost

def calculate_savings_flexible(bundesland, annual_consumption, flexibility_00_08, flexibility_08_20, flexibility_20_24, mehrwertsteuer=0.19):
    market_prices = fetch_market_prices()
    taxes = get_taxes_for_bundesland(bundesland)
    market_prices['price_full'] = (market_prices['price'] / 100 + taxes) * (1 + mehrwertsteuer)/100

    # Calculate regular current costs
    date_col = 'date'
    hour_col = 'hour'
    hourly_consumption_data['hourly_consumption'] = hourly_consumption_data['usage'] * annual_consumption / 365
    merged_data = pd.merge(market_prices, hourly_consumption_data, on=[hour_col], how='left')
    merged_data['current_cost'] = merged_data['hourly_consumption'] * merged_data['price_full']
    daily_current_cost = merged_data.groupby(date_col)['current_cost'].sum().reset_index(drop=False)
    hourly_current_cost = daily_current_cost['current_cost'].values # hourly_current_cost is in fact daily.

    # Get flexibility
    market_prices['hour'] = market_prices['timestamp'].dt.hour
    market_prices = market_prices.merge(hourly_consumption_data, on='hour', how='left')
    market_prices['flexibility'] = market_prices['hour'].apply(get_flexibility)
    market_prices['flexibility_group'] = market_prices['hour'].apply(get_flexibility_group)

    # Calculate flexible and fix costs
    min_prices = market_prices.groupby(['date', 'flexibility_group']).apply(lambda x: x.loc[x['price_full'].idxmin()]).reset_index(drop=True)
    min_prices = min_prices[['date', 'flexibility_group', 'price_full']].rename(columns={'price_full': 'min_price'})
    market_prices = market_prices.merge(min_prices, on=['date', 'flexibility_group'], how='left')
    market_prices['flexible_cost'] = market_prices['hourly_consumption'] * market_prices['flexibility'] * (market_prices['min_price'])
    market_prices['fixed_cost'] = market_prices['hourly_consumption'] * (1 - market_prices['flexibility']) * (market_prices['price_full'])
    
    # Calculate final costs
    daily_cost = market_prices.groupby('date')[['flexible_cost', 'fixed_cost']].sum().reset_index(drop=False)
    annual_potential_cost = daily_cost[['flexible_cost', 'fixed_cost']].sum().sum()
    potential_cost = annual_potential_cost
    current_cost = daily_current_cost['current_cost'].sum()

    # Calculate savings
    potential_savings = max(0, current_cost - potential_cost)
    saving_ratio = (potential_savings / (current_cost)) * 100 if current_cost != 0 else 0

    return potential_savings, saving_ratio, market_prices, daily_cost, current_cost, potential_cost, hourly_current_cost

def plot_savings(daily_cost, annual_consumption, fix_price=0, working_price=0, hourly_current_cost=None, y_min=None, y_max=None):
    daily_usage = annual_consumption / 365
    daily_fixed_cost = (12 * fix_price) / 365 if fix_price else 0

    if hourly_current_cost is not None:
        daily_cost['current_cost'] = hourly_current_cost
    else:
        daily_cost['current_cost'] = daily_usage * (working_price / 100) + daily_fixed_cost

    daily_cost['potential_cost'] = daily_cost['flexible_cost'] + daily_cost['fixed_cost']
    daily_cost['savings'] = daily_cost['current_cost'] - daily_cost['potential_cost']
    daily_cost['average_savings'] = daily_cost['savings'].expanding().mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily_cost['date'], y=daily_cost['current_cost'], mode='lines', name='Current Payment Method Cost', line=dict(color='#FAC500')))
    fig.add_trace(go.Scatter(x=daily_cost['date'], y=daily_cost['potential_cost'], mode='lines', name='Potential Solution Cost', line=dict(color='#26909b')))
    
    fig.update_layout(
        title='Daily Costs: Potential Solution vs. Current Payment Method',
        yaxis_title="Cost [€]",
        xaxis_title=None,
        legend=dict(
            x=1,
            y=0,
            xanchor='right',
            yanchor='bottom',
            bordercolor='lightgrey',
            borderwidth=1
        )
    )

    fig.update_traces(
        hovertemplate='Date: %{x|%Y-%m-%d}<br>Cost: %{y:.2f} €'
    )

    fig.update_yaxes(range=[0, daily_cost['current_cost'].max() + .25])

    st.plotly_chart(fig)



# Main Execution Block
if st.button("Calculate Savings"):
    if option == 'Fix':
        # Calculate hourly consumption based on user input
        hourly_consumption_data['hourly_consumption'] = hourly_consumption_data['usage'] * annual_consumption/365

        potential_savings, saving_ratio, market_prices, daily_cost, current_cost, potential_cost = calculate_savings_fix(
            bundesland, annual_consumption, fix_price, working_price, flexibility_00_08, flexibility_08_20, flexibility_20_24)

    elif option == 'Flexible':
        # Calculate hourly consumption based on user input
        hourly_consumption_data['hourly_consumption'] = hourly_consumption_data['usage'] * annual_consumption/365

        potential_savings, saving_ratio, market_prices, daily_cost, current_cost, potential_cost, hourly_current_cost = calculate_savings_flexible(
            bundesland, annual_consumption, flexibility_00_08, flexibility_08_20, flexibility_20_24)

    st.write('___' * 10)
    col1, col2 = st.columns([1,4])
    with col1:
        st.write("")
        st.write("")
        st.write(f"Current Cost: {current_cost:.2f} €")
        st.write(f"Potential Cost: {potential_cost:.2f} €")
        st.write(f"Estimated annual savings: {potential_savings:.2f} Euro ({saving_ratio:.2f} %)")
    with col2:
        if option == 'Fix':
            plot_savings(daily_cost, annual_consumption, fix_price, working_price)
        elif option == 'Flexible':
            # Ensure hourly_current_cost is of appropriate shape
            hourly_current_cost = hourly_current_cost[:]
            plot_savings(daily_cost, annual_consumption, hourly_current_cost=hourly_current_cost)

