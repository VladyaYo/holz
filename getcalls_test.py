import os
from datetime import datetime
from binotelApi import BinotelApi
from dotenv import load_dotenv
import json
import psycopg2


# # Загружаем переменные окружения
# load_dotenv()
#
# # Инициализация API
# BINOTEL_API_KEY = os.getenv("BINOTEL_API_KEY")
# BINOTEL_API_SECRET = os.getenv("BINOTEL_API_SECRET")
#
# api = BinotelApi(BINOTEL_API_KEY, BINOTEL_API_SECRET)
# api.debug = True  # Включить отладку для просмотра деталей запроса
#
# # Указываем временные промежутки
# start_time_str = "2024-12-01 00:00:00"
# stop_time_str = "2024-12-31 23:59:59"
#
# start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
# stop_time = datetime.strptime(stop_time_str, "%Y-%m-%d %H:%M:%S")
#
# # Перевод в Unix Timestamp
# start_timestamp = int(start_time.timestamp())
# stop_timestamp = int(stop_time.timestamp())
#
# # Отправка запроса
# result = api.send_request('getcall/list-of-getcalls-for-period', {
#     'startTime': start_timestamp,
#     'stopTime': stop_timestamp,
# })
# # result = api.send_request('stats/incoming-calls-for-period', {
# #     'startTime': start_timestamp,
# #     'stopTime': stop_timestamp,
# # })
#
# # Проверка результата
# if result.get('status') == 'success':
#     print("Запрос выполнен успешно. Вот данные:")
#     # Преобразуем ответ в читаемый JSON и выводим
#     print(json.dumps(result, indent=4, ensure_ascii=False))
# else:
#     print(f"REST API ошибка {result.get('code', 'unknown')}: {result.get('message')}")

try:
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="sugG7LCG",
        host="localhost",
        port="5432"
    )
    print("Подключение успешно!")
    conn.close()
except Exception as e:
    print(f"Ошибка: {e}")