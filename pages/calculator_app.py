import streamlit as st

# Page title
st.title("Electricity Savings Calculator")

# Step 1: Select Bundesland
bundesland = st.selectbox("Select your Bundesland", [
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

########## CATCH USER ENTRIES ###########
# Step 2: Input annual electricity consumption
annual_consumption = st.number_input("Enter your annual electricity consumption (kWh)", min_value=0)

# Step 3: Input fix-price for the electricity price
fix_price = st.number_input("Enter your fixed price (Euro/month)", min_value=0.0)

# Step 4: Input working price
working_price = st.number_input("Enter your working price (cents/kWh)", min_value=0.0)

# Step 5: Flexibility estimate
flexibility = st.slider("Estimate your flexibility (0-100%)", min_value=0, max_value=100)

########## FUNCTIONS ###########
def fetch_market_prices():
    # This function should fetch the current market prices from a reliable source
    # Placeholder return statement
    return [30.0, 28.0, 27.5, 29.0, 32.0]

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

# You will need to implement the actual savings calculation based on market prices and taxes
def calculate_savings(bundesland, annual_consumption, fix_price, working_price, flexibility, mehrwertsteuer = 0.19):
    # Fetch the official market price data
    market_prices = fetch_market_prices()

    # Fetch the taxes for the selected Bundesland
    taxes = get_taxes_for_bundesland(bundesland)

    # Calculate the cost with the user's current plan
    current_cost = (annual_consumption * fix_price / 100) + taxes
    current_cost_mws = current_cost + (current_cost * mehrwertsteuer)

    # Calculate the potential cost with the lowest market prices
    lowest_market_price = min(market_prices)
    potential_savings = annual_consumption * ((working_price - lowest_market_price) / 100) * (flexibility / 100)

    # Calculate the total savings
    total_savings = current_cost_mws - (annual_consumption * lowest_market_price / 100 + taxes)

    return total_savings

# Button to calculate savings
if st.button("Calculate Savings"):
    # Perform calculations here
    st.write("Calculating savings...")
    total_savings = calculate_savings(bundesland, annual_consumption, fix_price, working_price, flexibility)
    # Placeholder for the results
    st.write(f"Selected Bundesland: {bundesland}")
    st.write(f"Annual Consumption: {annual_consumption} kWh")
    st.write(f"Fixed Price: {fix_price} cents/kWh")
    st.write(f"Working Price: {working_price} cents/kWh")
    st.write(f"Flexibility: {flexibility} %")
    st.write(f"Estimated annual savings: {total_savings:.2f} Euros")