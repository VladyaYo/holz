import requests
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BITRIX_DOMAIN = os.getenv("BITRIX_DOMAIN")
BITRIX_REGION = os.getenv("BITRIX_REGION")
BITRIX_AUTH_KEY = os.getenv("BITRIX_AUTH_KEY")

# Базовые данные
method = "crm.lead.list"  # Пример метода Bitrix24
domain = BITRIX_DOMAIN
region = BITRIX_REGION
auth_key = BITRIX_AUTH_KEY

BITRIX_WEBHOOK_URL = f"https://{domain}.{region}/rest/28/{auth_key}/"

start_date_str = "2024-12-01"  # Начальная дата (в формате YYYY-MM-DD)
end_date_str = "2024-12-31"  # Конечная дата (в формате YYYY-MM-DD)

start_date = f"{start_date_str}T00:00:00Z"
end_date = f"{end_date_str}T23:59:59Z"

# Параметры фильтрации
params = {
    "filter": {
        ">=DATE_CREATE": start_date,
        "<=DATE_CREATE": end_date,
    },
    "select": ["ID", "TITLE", "DATE_CREATE", "STATUS_ID", "SOURCE_DESCRIPTION", "PHONE"],  # Запросим поле "PHONE"
}

# Список для хранения всех лидов
all_leads = []
start = 0  # Начальный индекс для пагинации

while True:
    # Добавляем параметр для пагинации
    params["start"] = start

    # Выполнение запроса
    response = requests.post(f"{BITRIX_WEBHOOK_URL}{method}", json=params)

    if response.ok:
        # Получаем лидов из ответа
        leads = response.json().get("result", [])
        all_leads.extend(leads)  # Добавляем новых лидов в общий список

        # Если количество лидов меньше 50, значит это последняя страница
        if len(leads) < 50:
            break

        # Увеличиваем start для следующей страницы
        start += 50
    else:
        print("Ошибка:", response.status_code, response.text)
        break

# Обработка данных: для каждого лида сохраняем телефоны в отдельные колонки
processed_leads = []
for lead in all_leads:
    # Извлекаем телефоны и формируем отдельные колонки
    phones = [phone['VALUE'] for phone in lead.get('PHONE', [])]

    # Добавляем данные о лидах и их телефонах в новую структуру
    lead_data = {
        'ID': lead['ID'],
        'TITLE': lead['TITLE'],
        'DATE_CREATE': lead['DATE_CREATE'],
        'STATUS_ID': lead['STATUS_ID']
    }

    # Добавляем телефоны в отдельные колонки
    for i, phone in enumerate(phones):
        lead_data[f'PHONE_{i + 1}'] = phone

    processed_leads.append(lead_data)

# Преобразуем обработанные данные в DataFrame
df = pd.DataFrame(processed_leads)

# Запись в CSV файл
output_file = f"received_data/bitrix_leads_data_{start_date_str}-{end_date_str}.csv"
df.to_csv(output_file, index=False, encoding="utf-8")

print(f"Данные успешно записаны в {output_file}")
