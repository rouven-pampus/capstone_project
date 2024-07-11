import streamlit as st

home_page = st.Page("pages/energy_app.py", title="Energy App", icon=":material/calculate:")
market_page = st.Page("pages/electricity_market.py", title="Electricity Market", icon=":material/bolt:")
weather_page = st.Page("pages/weather.py", title="Weather Data", icon=":material/sunny:")
calculator_page = st.Page("pages/calculator_app.py", title="Calculator App", icon=":material/sunny:")

pg = st.navigation([home_page, market_page, weather_page, calculator_page])
pg.run()

select_timeframe = st.sidebar.selectbox("Timeframe",["yesterday","today", "tomorrow"])
select_unit = st.sidebar.selectbox("Unit",["€/MWh","ct€/KWh"])