import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import streamlit as st

def get_timeframe(ts):
    today = pd.to_datetime(datetime.now().date())
    tomorrow = today + timedelta(days=1)
    yesterday = today - timedelta(days=1)
    if ts.date() == today.date():
        return 'today'
    elif ts.date() == tomorrow.date():
        return 'tomorrow'
    elif ts.date() == yesterday.date():
        return 'yesterday'
    elif ts.date() < yesterday.date():
        return 'history'
    else:
        return 'other'
    
def st_get_engine():
    # Read secrets
    host = st.secrets["postgres"]["host"]
    port = st.secrets["postgres"]["port"]
    dbname = st.secrets["postgres"]["dbname"]
    user = st.secrets["postgres"]["user"]
    password = st.secrets["postgres"]["password"]

    # Create connection string
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    return engine