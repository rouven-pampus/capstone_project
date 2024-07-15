import streamlit as st
import pandas as pd
from datetime import datetime,timedelta
import sys
import os

def add_path():
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if root_dir not in sys.path:
        sys.path.append(root_dir)

add_path()

print(sys.path)

st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)


home_page = st.Page("energy_app.py", title="Energy App", icon=":material/home:", default=True)
market_page = st.Page("electricity_market.py", title="Electricity Market", icon=":material/bolt:")
weather_page = st.Page("weather.py", title="Weather Data", icon=":material/sunny:")
calculator_page = st.Page("calculator_app.py", title="Calculator App", icon=":material/calculate:")

st.logo('images/logo.png')

pg = st.navigation(
    {
        "Home": [home_page],
        "Tools": [calculator_page],
        "Info": [market_page, weather_page]     
    }
)
pg.run()


