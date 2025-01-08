import os
import pandas as pd

from datetime import datetime
from binotelApi import BinotelApi
from dotenv import load_dotenv

load_dotenv()

BINOTEL_API_KEY = os.getenv("BINOTEL_API_KEY")
BINOTEL_API_SECRET = os.getenv("BINOTEL_API_SECRET")

api = BinotelApi(BINOTEL_API_KEY, BINOTEL_API_SECRET)
api.debug = True  # Включить отладку

start_time_str = "2024-12-01 00:00:00"
stop_time_str = "2024-12-31 23:59:59"

# Конвертация строки в объект datetime
start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
stop_time = datetime.strptime(stop_time_str, "%Y-%m-%d %H:%M:%S")

# Перевод в Unix Timestamp
start_timestamp = int(start_time.timestamp())
stop_timestamp = int(stop_time.timestamp())


print(start_timestamp)
print(stop_timestamp)

result = api.send_request('stats/incoming-calls-for-period', {
    # 'startTime': 1370034000,
    # 'stopTime': 1370048400
    'startTime': start_timestamp,
    'stopTime': stop_timestamp

})

if result.get('status') == 'success':
    call_details = result.get("callDetails", {})

    # Преобразуем данные в список словарей для pandas
    rows = []

    for call_id, details in call_details.items():
        row = {
            "CompanyID": details.get("companyID"),
            "GeneralCallID": details.get("generalCallID"),
            "StartTime": datetime.fromtimestamp(
                int(details.get("startTime", 0))
            ).strftime("%Y-%m-%d %H:%M:%S")
            if details.get("startTime")
            else None,
            "CallType": details.get("callType"),
            "InternalNumber": details.get("internalNumber"),
            "InternalAdditionalData": details.get("internalAdditionalData"),
            "ExternalNumber": details.get("externalNumber"),
            "WaitSec": details.get("waitsec"),
            "BillSec": details.get("billsec"),
            "Disposition": details.get("disposition"),
            "IsNewCall": details.get("isNewCall"),
            "CustomerName": details.get("customerData").get("name")
            if isinstance(details.get("customerData"), dict)
            else None,
            "CustomerEmail": details.get("customerData").get("email")
            if isinstance(details.get("customerData"), dict)
            else None,
            "EmployeeName": details.get("employeeData").get("name")
            if isinstance(details.get("employeeData"), dict)
            else None,
            "EmployeeEmail": details.get("employeeData").get("email")
            if isinstance(details.get("employeeData"), dict)
            else None,
            "PBXNumberNumber": details.get("pbxNumberData").get("number")
            if isinstance(details.get("pbxNumberData"), dict)
            else None,
            "PBXNumberName": details.get("pbxNumberData").get("name")
            if isinstance(details.get("pbxNumberData"), dict)
            else None,
            "HistoryData": details.get("historyData", []),
            "CallTrackingData": details.get("callTrackingData", []),
            "GetCallData": details.get("getCallData", []),
            "SMSContent": details.get("smsContent"),
            "LinkToCrmUrl": details.get("customerDataFromOutside").get("linkToCrmUrl")
            if isinstance(details.get("customerDataFromOutside"), dict)
            else None,
        }
        rows.append(row)

    # Создаем DataFrame
    df = pd.DataFrame(rows)

    # Определяем имя файла с учетом периода
    file_name = f"Incoming_calls_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"

    # Сохраняем в CSV
    df.to_csv(f"received_data/{file_name}", index=False, encoding="utf-8-sig")
    print(f"Данные успешно сохранены в файл {file_name}")
else:
    print(f"REST API ошибка {result.get('code', 'unknown')}: {result.get('message')}")