from datetime import datetime, timedelta

def classify_timestamp(ts):
    """function to create column for timestamp classification into 
    historic, today, tomorrow, future for easy filtering in app.

    Args:
        None

    Returns:
        str: historic, today, tomorrow, future
    """
    now = datetime.now(ts.tz)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start = today_start + timedelta(days=1)
    day_after_tomorrow_start = tomorrow_start + timedelta(days=1)
    
    if ts < today_start:
        return "historic"
    elif today_start <= ts < tomorrow_start:
        return "today"
    elif tomorrow_start <= ts < day_after_tomorrow_start:
        return "tomorrow"
    else:
        return "future"