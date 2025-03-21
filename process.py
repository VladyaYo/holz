import os
import pandas as pd
from datetime import datetime

# Обработка входящих звонков
def process_call_data(call_details, start_time, stop_time):
    if not call_details:
        print("Нет данных для входящих звонков за указанный период.")
        return

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

    df = pd.DataFrame(rows)

    os.makedirs("received_data", exist_ok=True)
    file_name = f"Incoming_calls_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"
    df.to_csv(f"received_data/{file_name}", index=False, encoding="utf-8-sig")
    print(f"Данные входящих звонков сохранены в файл {file_name}")

# Обработка данных из get calls
def process_getcalls_data(listOfGetCalls, start_time, stop_time):
    if not listOfGetCalls:
        print("Нет данных для списка звонков за указанный период.")
        return

    rows = []
    for call_id, details in listOfGetCalls.items():
        row = {
            "CallID": details.get("id"),
            "WidgetID": details.get("widgetID"),
            "ExternalNumber": details.get("externalNumber"),
            "IsNewNumber": details.get("isNewNumber"),
            "CreatedAt": datetime.fromtimestamp(int(details.get("createdAt", 0))).strftime("%Y-%m-%d %H:%M:%S")
            if details.get("createdAt") else None,
            "CallAt": datetime.fromtimestamp(int(details.get("callAt", 0))).strftime("%Y-%m-%d %H:%M:%S")
            if details.get("callAt") else None,
            "ProcessedAt": datetime.fromtimestamp(int(details.get("processedAt", 0))).strftime("%Y-%m-%d %H:%M:%S")
            if details.get("processedAt") else None,
            "IsProcessed": details.get("isProcessed"),
            "GeneralCallID": details.get("generalCallID"),
            "RequestsCounter": details.get("requestsCounter"),
            "AttemptsCounter": details.get("attemptsCounter"),
            "EmployeesDontAnswerCounter": details.get("employeesDontAnswerCounter"),
            "ClientDontAnswerCounter": details.get("clientDontAnswerCounter"),
            "FullUrl": details.get("fullUrl"),
            "Description": details.get("description"),
            "GATrackingID": details.get("gaTrackingId"),
            "GAClientID": details.get("gaClientId"),
            "UTMSource": details.get("utm_source"),
            "UTMMedium": details.get("utm_medium"),
            "UTMCampaign": details.get("utm_campaign"),
            "UTMContent": details.get("utm_content"),
            "UTMTerm": details.get("utm_term"),
            "IPAddress": details.get("ipAddress"),
            "GeoIPCountry": details.get("geoipCountry"),
            "GeoIPRegion": details.get("geoipRegion"),
            "GeoIPCity": details.get("geoipCity"),
            "GeoIPOrg": details.get("geoipOrg"),
        }
        rows.append(row)

    # Создаем DataFrame
    df = pd.DataFrame(rows)

    # Убедимся, что папка для сохранения данных существует
    os.makedirs("received_data", exist_ok=True)

    # Сохраняем данные в файл CSV
    file_name = f"Getcalls_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"
    df.to_csv(f"received_data/{file_name}", index=False, encoding="utf-8-sig")
    print(f"Данные списка звонков сохранены в файл {file_name}")

# Обработка данных из Bitrix
def process_bitrix_data(bitrix_leads, start_time, stop_time):
    if not bitrix_leads:
        print("Нет данных для лидов из Bitrix за указанный период.")
        return

    print("Пример данных перед обработкой:", bitrix_leads[:2])
    rows = []
    for lead in bitrix_leads:
        phones = lead.get('PHONE', [])
        row = {
            "ID": lead.get("ID"),
            "TITLE": lead.get("TITLE"),
            "SOURCE_ID":lead.get("SOURCE_ID"),
            "DATE_CREATE": lead.get("DATE_CREATE")
            if lead.get("DATE_CREATE") else None,
            "STATUS_ID": lead.get("STATUS_ID"),
            "SOURCE_DESCRIPTION": lead.get("SOURCE_DESCRIPTION")
        }
        if phones and isinstance(phones, list):
            for i, phone in enumerate(phones):
                if phone.get('isMultiple', False):
                    row[f'PHONE_{i + 1}'] = phone['VALUE']
                else:
                    row['PHONE'] = phone['VALUE']
                    break
        rows.append(row)

    # Создаем DataFrame
    df = pd.DataFrame(rows)

    # Убедимся, что папка для сохранения данных существует
    os.makedirs("received_data", exist_ok=True)

    # Сохраняем данные в файл CSV
    file_name = f"bitrix_leads_data_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"
    df.to_csv(f"received_data/{file_name}", index=False, encoding="utf-8-sig")
    print(f"Данные из Bitrix сохранены в файл {file_name}")


