from datetime import datetime, date, timedelta

def truncate_timestamp(timestamp_ms):
    dt = datetime.fromtimestamp(timestamp_ms/1000)
    return date(dt.year, dt.month, dt.day)