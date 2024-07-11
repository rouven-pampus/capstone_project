import pandas as pd
from datetime import datetime, timedelta
import pytz

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