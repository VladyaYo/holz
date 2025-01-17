import os
from datetime import datetime
from binotelApi import BinotelApi
import requests
from dotenv import load_dotenv

load_dotenv()

BITRIX_DOMAIN = os.getenv("BITRIX_DOMAIN")
BITRIX_REGION = os.getenv("BITRIX_REGION")
BITRIX_AUTH_KEY = os.getenv("BITRIX_AUTH_KEY")

BINOTEL_API_KEY = os.getenv("BINOTEL_API_KEY")
BINOTEL_API_SECRET = os.getenv("BINOTEL_API_SECRET")

api = BinotelApi(BINOTEL_API_KEY, BINOTEL_API_SECRET)
api.debug = True  # Включить отладку

def fetch_incoming_calls(start_time: datetime, stop_time: datetime):
    """Запрос статистики входящих звонков за указанный период."""
    start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
    stop_time = stop_time.replace(hour=23, minute=59, second=59, microsecond=0)

    start_timestamp = int(start_time.timestamp())
    stop_timestamp = int(stop_time.timestamp())
    #TO_DO добавить время

    result = api.send_request('stats/incoming-calls-for-period', {
        'startTime': start_timestamp,
        'stopTime': stop_timestamp,
    })

    if result.get('status') == 'success':
        return result.get("callDetails", {})
    else:
        raise Exception(f"REST API ошибка {result.get('code', 'unknown')}: {result.get('message')}")

def fetch_getcalls_for_period(start_time: datetime, stop_time: datetime):
    """Запрос списка звонков за указанный период."""
    start_timestamp = int(start_time.timestamp())
    stop_timestamp = int(stop_time.timestamp())

    result = api.send_request('getcall/list-of-getcalls-for-period', {
        'startTime': start_timestamp,
        'stopTime': stop_timestamp,
    })

    if result.get('status') == 'success':
        return result.get("listOfGetCalls", {})
    else:
        raise Exception(f"REST API ошибка {result.get('code', 'unknown')}: {result.get('message')}")

def fetch_bitrix_leads(start_date_str, end_date_str):
    method = "crm.lead.list"
    BITRIX_WEBHOOK_URL = f"https://{BITRIX_DOMAIN}.{BITRIX_REGION}/rest/28/{BITRIX_AUTH_KEY}/"

    start_date = f"{start_date_str}T00:00:00Z"
    end_date = f"{end_date_str}T23:59:59Z"

    params = {
        "filter": {
            ">=DATE_CREATE": start_date,
            "<=DATE_CREATE": end_date,
        },
        "select": ["ID", "TITLE", "DATE_CREATE", "STATUS_ID", "SOURCE_DESCRIPTION", "PHONE"],
    }

    all_leads = []
    start = 0

    while True:
        params["start"] = start
        response = requests.post(f"{BITRIX_WEBHOOK_URL}{method}", json=params)

        if response.ok:
            leads = response.json().get("result", [])
            all_leads.extend(leads)

            if len(leads) < 50:
                break

            start += 50
        else:
            raise Exception(f"Ошибка {response.status_code}: {response.text}")

    return all_leads
