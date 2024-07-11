import streamlit as st
import pandas as pd
from packages.db_utils import st_get_engine
from packages.st_app_utils import get_timeframe
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime,timedelta

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
    flexibility = st.slider("Estimate your flexibility (0-100%)", min_value=0, max_value=100)
elif option == 'Flexible':
    st.subheader("Flexible Plan Details")
    annual_consumption = st.number_input("Enter your annual electricity consumption (kWh)", min_value=0, value = 2500)
    flexibility = st.slider("Estimate your flexibility (0-100%)", min_value=0, max_value=100)


# Step 1: Select Bundesland
bundesland = st.selectbox("Select your State", [
    "Baden-Württemberg", "Bayern", "Berlin", "Brandenburg", "Bremen",
    "Hamburg", "Hessen", "Niedersachsen", "Mecklenburg-Vorpommern",
    "Nordrhein-Westfalen", "Rheinland-Pfalz", "Saarland",
    "Sachsen", "Sachsen-Anhalt", "Schleswig-Holstein", "Thüringen"
])

# Data per Bundesland
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
    # The "market_prices" should return a dataset with hourly data, at the best for the last year from the current date.
    # But for that, I also have a filter built in.
    # Dataset should contain:
    #   - Hourly prices (I later add taxes etc.)
    #   - Hourly Timestamps (so we can be sure we have one year)
    ################## data extraction energy app ##################

    #Place for queries
    query_prices = """ SELECT timestamp, de_lu as price, unit
        FROM "02_silver".fact_day_ahead_prices_germany
        WHERE date_trunc('day', "timestamp") >= (
            SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '365 days'
            FROM "02_silver".fact_day_ahead_prices_germany
        );
    """

    @st.cache_data
    def get_data(query):
        df =pd.read_sql(query, st_get_engine())
        return df

    df_prices = get_data(query_prices)


    ################## data transformation ##################

    #Do transformations to price dataframe
    df_prices["timestamp"] = df_prices["timestamp"].dt.tz_convert("Europe/Berlin") #timezone
    df_prices["hour"] = df_prices.timestamp.dt.strftime('%H:%M') #add hour column
    df_prices['date'] = df_prices.timestamp.dt.strftime('%Y-%m-%d') #add date column
    df_prices["timeframe"] = df_prices["timestamp"].apply(get_timeframe)
    # Placeholder return statement
    return df_prices

def get_taxes_for_bundesland(bundesland):
    # This function should return the tax amount for the given Bundesland
    netzentgelt = netzentgelte_2024[bundesland]
    eeg_umlage = 6.5
    kwkg_umlage = 0.5
    strom_nev_umlage = 0.4
    offshore_umlage = 0.2
    ablav_umlage = 0.1
    stromsteuer = 2.05
    konzessions_abgabe = konzessionsabgaben_2024[bundesland] # Abhängig von Gemeinde, i.d.R. zwischen 1.5 und 2.5 ct/kWh

    taxes = netzentgelt + eeg_umlage + kwkg_umlage + strom_nev_umlage + offshore_umlage + ablav_umlage + stromsteuer + konzessions_abgabe
    
    return taxes


def calculate_savings_fix(bundesland, annual_consumption, fix_price, working_price, flexibility, mehrwertsteuer=0.19):
    # Fetch the official market price data
    market_prices = fetch_market_prices()

    # Fetch the taxes for the selected Bundesland
    taxes = get_taxes_for_bundesland(bundesland)

    # Add taxes to price
    # ADJUST €/MWh to ct/kWh
    market_prices['price_full'] = (market_prices['price']/100 + taxes) * (1 + mehrwertsteuer)

    # Calculate the cost with the user's current plan
    current_cost = (annual_consumption * working_price / 100) + (12 * fix_price)

    # Calculate the average and lowest market prices
    avg_market_price = market_prices.groupby('date').price_full.mean().mean()
    lowest_market_price = market_prices.groupby('date').price_full.min().mean()

    # Calculate the flexible and fixed portions of the consumption
    flexible_consumption = annual_consumption * (flexibility / 100)
    fixed_consumption = annual_consumption * (1 - flexibility / 100)

    # Calculate the potential cost for flexible and fixed portions
    flexible_cost = flexible_consumption * lowest_market_price / 100
    fixed_cost = fixed_consumption * working_price / 100  # Fixed portion uses the working price

    # Total potential cost including the fixed monthly price
    potential_cost = flexible_cost + fixed_cost + (12 * fix_price)

    # Calculate potential savings
    potential_savings = max(0, current_cost - potential_cost)

    # Calculate saving ratio
    saving_ratio = (potential_savings / current_cost) * 100 if current_cost != 0 else 0

    return potential_savings, saving_ratio

def calculate_savings_flexible(bundesland, annual_consumption, flexibility, mehrwertsteuer=0.19):
    # Fetch the official market price data
    market_prices = fetch_market_prices()

    # Fetch the taxes for the selected Bundesland
    taxes = get_taxes_for_bundesland(bundesland)

    # Add taxes to price
    market_prices['price_full'] = (market_prices['price']/100 + taxes) * (1 + mehrwertsteuer)

    # Calculate the average and lowest market prices
    avg_market_price = market_prices.groupby('date').price_full.mean().mean()
    lowest_market_price = market_prices.groupby('date').price_full.min().mean()

    # Calculate the flexible and fixed portions of the consumption
    flexible_consumption = annual_consumption * (flexibility / 100)
    fixed_consumption = annual_consumption * (1 - flexibility / 100)

    # Calculate the potential cost for flexible and fixed portions
    flexible_cost = flexible_consumption * lowest_market_price / 100
    fixed_cost = fixed_consumption * avg_market_price / 100

    # Total potential cost
    potential_cost = flexible_cost + fixed_cost
    current_cost = annual_consumption * avg_market_price / 100
    # Calculate potential savings
    potential_savings = max(0, current_cost - potential_cost)

    # Calculate saving ratio
    saving_ratio = (potential_savings / current_cost) * 100 if current_cost != 0 else 0

    return potential_savings, saving_ratio


# Button to calculate savings
if st.button("Calculate Savings"):
    # Perform calculations here
    st.write("Calculating savings...")

    
    # Placeholder for the results
    st.write(f"Selected Bundesland: {bundesland}")
    st.write(f"Annual Consumption: {annual_consumption} kWh")
    st.write(f"Flexibility: {flexibility} %")

    if option == 'Fix':
        st.write(f"Fixed Price: {fix_price} €/month")
        st.write(f"Working Price: {working_price} cents/kWh")
        potential_savings, saving_ratio = calculate_savings_fix(bundesland, annual_consumption, fix_price, working_price, flexibility)
        st.markdown("<hr style='width:50%;border:1px solid lightgrey;'>", unsafe_allow_html=True)
        st.write(f"Estimated annual savings: {potential_savings:.2f} Euro")
        st.write(f"Estimated annual savings: {saving_ratio:.2f} %")
    elif option == 'Flexible':
        potential_savings, saving_ratio = calculate_savings_flexible(bundesland, annual_consumption, flexibility, mehrwertsteuer = 0.19)
        st.markdown("<hr style='width:50%;border:1px solid lightgrey;'>", unsafe_allow_html=True)
        st.write(f"Estimated annual savings: {potential_savings:.2f} Euro")
        st.write(f"Estimated annual savings: {saving_ratio:.2f} %")
    