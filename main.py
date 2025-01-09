import asyncio
import os
from dotenv import load_dotenv
from api_clients.binotel import BinotelClient
from api_clients.bitrix import BitrixClient
from utils.date_utils import format_date_for_binotel, format_date_for_bitrix

load_dotenv()


BINOTEL_API_KEY = os.getenv("BINOTEL_API_KEY")
BINOTEL_API_SECRET = os.getenv("BINOTEL_API_SECRET")
BITRIX_DOMAIN = os.getenv("BITRIX_DOMAIN")
BITRIX_REGION = os.getenv("BITRIX_REGION")
BITRIX_AUTH_KEY = os.getenv("BITRIX_AUTH_KEY")

async def main():
    # Инициализация клиентов
    binotel_client = BinotelClient(api_key=BINOTEL_API_KEY, api_secret=BINOTEL_API_SECRET)
    bitrix_client = BitrixClient(domain=BITRIX_DOMAIN, region=BITRIX_REGION, auth_key=BITRIX_AUTH_KEY)

    # Даты для запросов
    start_date = "2024-12-01"
    end_date = "2024-12-31"

    # Форматирование дат
    binotel_start = format_date_for_binotel(start_date)
    binotel_end = format_date_for_binotel(end_date)
    bitrix_start = format_date_for_bitrix(start_date)
    bitrix_end = format_date_for_bitrix(end_date, end_of_day=True)

    # Выполнение запросов
    binotel_incoming = await binotel_client.send_request("stats/incoming-calls-for-period", {
        "startTime": binotel_start,
        "stopTime": binotel_end
    })
    binotel_getcalls = await binotel_client.send_request("getcall/list-of-getcalls-for-period", {
        "startTime": binotel_start,
        "stopTime": binotel_end
    })

    bitrix_response = await bitrix_client.send_request("crm.lead.list", {
        "filter": {
            ">DATE_CREATE": bitrix_start,
            "<DATE_CREATE": bitrix_end
        },
        "select": ["ID", "TITLE", "PHONE"]
    })

    # Обработка ответов
    # ... (ваш код обработки данных)

if __name__ == "__main__":
    asyncio.run(main())
