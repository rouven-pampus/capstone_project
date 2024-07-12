# Load modules
import streamlit as st
import pandas as pd
from packages.db_utils import st_get_engine
from packages.st_app_utils import get_timeframe
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Page title
st.title("Electricity Savings Calculator")

# Step 1: Question with two possible answers
option = st.radio("Do you have a fixed or flexible electricity usage plan?", ('Fix', 'Flexible'))

# Step 2: Show different inputs based on the selected option
if option == 'Fix':
    st.subheader("Fixed Plan Details")
    annual_consumption = st.number_input("Enter your annual electricity consumption (kWh)", min_value=0, value=2500)
    fix_price = st.number_input("Enter your fixed price (€/month)", min_value=0.0)
    working_price = st.number_input("Enter your working price (cents/kWh)", min_value=0.0)
    st.subheader("Flexibility Details")
    flexibility_00_08 = st.slider("Flexibility (00:00-08:00)", min_value=0, max_value=100, value=50)
    flexibility_08_20 = st.slider("Flexibility (08:00-20:00)", min_value=0, max_value=100, value=50)
    flexibility_20_24 = st.slider("Flexibility (20:00-24:00)", min_value=0, max_value=100, value=50)
elif option == 'Flexible':
    st.subheader("Flexible Plan Details")
    annual_consumption = st.number_input("Enter your annual electricity consumption (kWh)", min_value=0, value=2500)
    st.subheader("Flexibility Details")
    flexibility_00_08 = st.slider("Flexibility (00:00-08:00)", min_value=0, max_value=100, value=50)
    flexibility_08_20 = st.slider("Flexibility (08:00-20:00)", min_value=0, max_value=100, value=50)
    flexibility_20_24 = st.slider("Flexibility (20:00-24:00)", min_value=0, max_value=100, value=50)


# Step 3: Select Bundesland
bundesland = st.selectbox("Select your State", [
    "Baden-Württemberg", "Bayern", "Berlin", "Brandenburg", "Bremen",
    "Hamburg", "Hessen", "Niedersachsen", "Mecklenburg-Vorpommern",
    "Nordrhein-Westfalen", "Rheinland-Pfalz", "Saarland",
    "Sachsen", "Sachsen-Anhalt", "Schleswig-Holstein", "Thüringen"
])

########## STATE DATA ##########
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

########## FUNCTIONS ###########
def fetch_market_prices():
    query_prices = """ SELECT timestamp, de_lu as price, unit
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
    df_prices["hour"] = df_prices.timestamp.dt.strftime('%H:%M')
    df_prices['date'] = df_prices.timestamp.dt.strftime('%Y-%m-%d')
    df_prices["timeframe"] = df_prices["timestamp"].apply(get_timeframe)
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

def calculate_savings_fix(bundesland, annual_consumption, fix_price, working_price, flexibility_00_08, flexibility_08_20, flexibility_20_24, mehrwertsteuer=0.19):
    market_prices = fetch_market_prices()
    taxes = get_taxes_for_bundesland(bundesland)
    market_prices['price_full'] = (market_prices['price'] / 100 + taxes) * (1 + mehrwertsteuer)

    hourly_consumption = annual_consumption / 8760

    def get_flexibility(hour):
        if hour < 8:
            return flexibility_00_08 / 100
        elif hour < 20:
            return flexibility_08_20 / 100
        else:
            return flexibility_20_24 / 100

    market_prices['flexibility'] = market_prices['timestamp'].dt.hour.apply(get_flexibility)
    market_prices['flexible_cost'] = hourly_consumption * market_prices['flexibility'] * market_prices['price_full'] / 100
    market_prices['fixed_cost'] = hourly_consumption * (1 - market_prices['flexibility']) * market_prices['price_full'] / 100

    daily_cost = market_prices.groupby('date')[['flexible_cost', 'fixed_cost']].sum()
    potential_cost = daily_cost.sum().sum() + (12 * fix_price)
    current_cost = hourly_consumption * 8760 * working_price / 100 + (12 * fix_price)
    daily_cost['current_cost'] = hourly_consumption * 24 * working_price / 100 + (fix_price / 30)  # Daily fixed cost

    potential_savings = max(0, current_cost - potential_cost)
    saving_ratio = (potential_savings / current_cost) * 100 if current_cost != 0 else 0

    return potential_savings, saving_ratio, market_prices, daily_cost

def calculate_savings_flexible(bundesland, annual_consumption, flexibility_00_08, flexibility_08_20, flexibility_20_24, mehrwertsteuer=0.19):
    # Fetch market prices and taxes
    market_prices = fetch_market_prices()
    taxes = get_taxes_for_bundesland(bundesland)
    
    # Adjust market prices
    market_prices['price_full'] = (market_prices['price'] / 100 + taxes) * (1 + mehrwertsteuer)
    
    # Calculate hourly consumption
    hourly_consumption = annual_consumption / 8760

    # Define flexibility function
    def get_flexibility(hour):
        if hour < 8:
            return flexibility_00_08 / 100
        elif hour < 20:
            return flexibility_08_20 / 100
        else:
            return flexibility_20_24 / 100

    # Apply flexibility function to market prices
    market_prices['flexibility'] = market_prices['timestamp'].dt.hour.apply(get_flexibility)
    
    # Calculate flexible and fixed costs
    market_prices['flexible_cost'] = hourly_consumption * market_prices['flexibility'] * market_prices['price_full'] / 100
    market_prices['fixed_cost'] = hourly_consumption * (1 - market_prices['flexibility']) * market_prices['price_full'] / 100
    
    # Calculate daily costs
    daily_cost = market_prices.groupby('date')[['flexible_cost', 'fixed_cost']].sum()
    
    # Calculate potential cost
    potential_cost = daily_cost['flexible_cost'].sum() + daily_cost['fixed_cost'].sum()
    
    # Calculate current cost
    avg_market_price = market_prices['price_full'].mean()
    current_cost = hourly_consumption * 8760 * avg_market_price / 100
    
    # Add current_cost to daily_cost for plotting
    daily_cost['current_cost'] = hourly_consumption * 24 * avg_market_price / 100
    
    # Calculate potential savings
    potential_savings = max(0, current_cost - potential_cost)
    saving_ratio = (potential_savings / current_cost) * 100 if current_cost != 0 else 0

    return potential_savings, saving_ratio, market_prices, daily_cost





def plot_savings(daily_cost):
    daily_cost['savings'] = daily_cost['current_cost'] - (daily_cost['flexible_cost'] + daily_cost['fixed_cost'])
    daily_cost.reset_index(inplace=True)  # Ensure the date column is accessible

    fig = px.line(daily_cost, x='date', y='savings', title='Potential Savings Over Time')
    fig.update_layout(
        yaxis_title="Savings [ct€]"
    )
    fig.update_traces(
        hovertemplate='Date: %{x}<br>Savings: %{y:.2f} ct'
    )

    st.plotly_chart(fig)

###### EXECUTE ######
if st.button("Calculate Savings"):
    st.write("Calculating savings...")
    st.write(f"Selected Bundesland: {bundesland}")
    st.write(f"Annual Consumption: {annual_consumption} kWh")
    st.write(f"Flexibility (00:00-08:00): {flexibility_00_08} %")
    st.write(f"Flexibility (08:00-20:00): {flexibility_08_20} %")
    st.write(f"Flexibility (20:00-24:00): {flexibility_20_24} %")

    if option == 'Fix':
        st.write(f"Fixed Price: {fix_price} €/month")
        st.write(f"Working Price: {working_price} cents/kWh")
        potential_savings, saving_ratio, market_prices, daily_cost = calculate_savings_fix(bundesland, annual_consumption, fix_price, working_price, flexibility_00_08, flexibility_08_20, flexibility_20_24)
        st.markdown("<hr style='width:50%;border:1px solid lightgrey;'>", unsafe_allow_html=True)
        st.write(f"Estimated annual savings: {potential_savings:.2f} Euro")
        st.write(f"Estimated annual savings: {saving_ratio:.2f} %")
        plot_savings(daily_cost)

    elif option == 'Flexible':
        potential_savings, saving_ratio, market_prices, daily_cost = calculate_savings_flexible(bundesland, annual_consumption, flexibility_00_08, flexibility_08_20, flexibility_20_24)
        st.markdown("<hr style='width:50%;border:1px solid lightgrey;'>", unsafe_allow_html=True)
        st.write(f"Estimated annual savings: {potential_savings:.2f} Euro")
        st.write(f"Estimated annual savings: {saving_ratio:.2f} %")
        plot_savings(daily_cost)
