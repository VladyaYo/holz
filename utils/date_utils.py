from datetime import datetime

def format_date_for_binotel(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())

def format_date_for_bitrix(date_str, end_of_day=False):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    else:
        dt = dt.replace(hour=0, minute=0, second=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")
