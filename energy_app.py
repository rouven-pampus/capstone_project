import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from PIL import Image
from database.db_utils import get_data_from_db

#Example text
st.title('Our amazing, world-changing app :sunglasses:')
st.header('One Ring to rule them all, One Ring to find them, One Ring to bring them all and in the darkness bind them.')

logo = Image.open('images/logo.png')
st.logo(logo, icon_image=logo)

query_string= """
    SELECT * 
    FROM "02_silver".fact_day_ahead_prices_germany
    WHERE date_trunc('day', "timestamp") >= (
        SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '7 days'
        FROM "02_silver".fact_day_ahead_prices_germany
    );
"""
df_prices = get_data_from_db(query_string)

df_prices["timestamp"] = df_prices["timestamp"].dt.tz_convert("Europe/Berlin")

st.sidebar.markdown("Hi, here could be our exiting life story!")  
        
st.line_chart(df_prices, x="timestamp", y="de_lu")

st.caption('This is a string that explains something above.')
st.caption('A caption with _italics_ :blue[colors] and emojis :sunglasses:')