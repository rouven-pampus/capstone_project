
import streamlit as st
import pandas as pd
import numpy as np
from packages.db_utils import get_data_from_db_st, get_engine
from packages.streamlit_app import get_timeframe
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime,timedelta

query_market =""" select * from "02_silver".fact_total_power_germany ftpg
    WHERE date_trunc('day', "timestamp") >= (
    SELECT MAX(date_trunc('day', "timestamp")) - INTERVAL '365 days'
    FROM "02_silver".fact_total_power_germany)  """

@st.cache_data
def load_data(query):
    data = get_data_from_db_st(query)
    return data

#Load market data
df_market = load_data(query_market)

df_market