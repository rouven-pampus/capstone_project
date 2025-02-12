import streamlit as st
import pandas as pd
from datetime import datetime,timedelta
import sys
import os

# Function to set correct path for accessing modules etc.
def add_path():
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if root_dir not in sys.path:
        sys.path.append(root_dir)

# call function
add_path()

#Configuration of all pages
st.set_page_config(
    page_title="Energizing your world",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': "https://eloncodereview.com/",
        'Report a bug': 'https://docs.streamlit.io/',
        'About': """
        # Energy Insights App
        *Get all the information you need and push the green transformation of the energy market forward!*        
        
        __Created with love by:__
        Marina, Jakob, Pascal, and Rouven
        
        Thanks to energy-charts.de, open-meteo.de, dwd.de, and neuefische Hamburg
        
        Check out our Github Repo: [Link](https://github.com/rouven-pampus/capstone_project)
        """
    }
)

#Define all pages
home_page = st.Page("energy_app.py", title="Overview", icon=":material/home:", default=True)
prediction_page = st.Page("prediction_page.py", title="Prediction", icon=":material/science:")
market_page = st.Page("electricity_market.py", title="Electricity Market", icon=":material/bolt:")
weather_page = st.Page("weather.py", title="Weather", icon=":material/sunny:")
calculator_page = st.Page("calculator_app.py", title="Savings Calculator", icon=":material/calculate:")


st.logo('images/logo.png', link="https://github.com/rouven-pampus/capstone_project")

pg = st.navigation(
    {
        "Home": [home_page],
        "Tools": [prediction_page, calculator_page],
        "Info": [market_page, weather_page]     
    }    
)
pg.run()