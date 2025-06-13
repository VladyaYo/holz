import pandas as pd
import os

def save_to_csv(df: pd.DataFrame, start_time, stop_time, file_prefix: str) -> str | None:
    """
    Сохраняет итоговую таблицу в файл CSV и возвращает путь к файлу.

    :param df: DataFrame для сохранения.
    :param start_time: Начальное время (объект datetime или строка).
    :param stop_time: Конечное время (объект datetime или строка).
    :param file_prefix: Префикс для имени файла.
    :return: Путь к сохраненному файлу.
    """
    try:
        # Создаем папку, если она не существует
        os.makedirs("processed_data", exist_ok=True)

        # Формируем имя файла
        if hasattr(start_time, 'strftime') and hasattr(stop_time, 'strftime'):
            # Если start_time и stop_time — объекты datetime
            file_name = f"processed_{file_prefix}_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"
        else:
            # Если start_time и stop_time — строки
            file_name = f"processed_{file_prefix}_{start_time}-{stop_time}.csv"

        # Полный путь к файлу
        file_path = os.path.join("processed_data", file_name)

        # Сохраняем DataFrame в CSV
        df.to_csv(file_path, index=False, encoding="utf-8-sig")

        print(f"Данные успешно сохранены в файл {file_path}")
        return file_path

    except Exception as e:
        print(f"Ошибка при сохранении CSV-файла: {e}")
        return None

def format_phone(phone):
    """Форматирует номер телефона (оставляет последние 9 цифр, удаляет '.0')."""
    try:
        phone = str(phone).strip()
        if phone.endswith('.0'):
            phone = phone[:-2]
        clean_phone = ''.join(filter(str.isdigit, phone))
        if len(clean_phone) < 9:
            return None
        return clean_phone[-9:]  # Оставляем последние 9 цифр
    except Exception:
        return None

def format_phone_columns(df, column_names):
    """Форматирует номера в указанных колонках DataFrame."""
    for col in column_names:
        if col in df.columns:
            df[col] = df[col].apply(format_phone)
    return df

def format_all_phone_numbers(calltracking_calls_df, incoming_calls_df, getcalls_df, bitrix_df):
    """Форматирует номера телефонов во всех таблицах."""
    calltracking_calls_df = format_phone_columns(calltracking_calls_df, ['ExternalNumber'])
    incoming_calls_df = format_phone_columns(incoming_calls_df, ['ExternalNumber', 'PBXNumberNumber'])
    getcalls_df = format_phone_columns(getcalls_df, ['ExternalNumber'])
    bitrix_df = format_phone_columns(bitrix_df, ['PHONE'])
    return calltracking_calls_df, incoming_calls_df, getcalls_df, bitrix_df


def merge_tables(bitrix_df, incoming_calls_df, getcalls_df):
    """Объединяет таблицы по совпадению номеров телефонов, игнорируя пустые и некорректные номера (менее 9 цифр)."""

    # Оставляем только номера, содержащие 9 и более цифр, остальные игнорируем при объединении
    # bitrix_df["Match number"] = bitrix_df["PHONE"].where(bitrix_df["PHONE"].notna(), None)
    valid_numbers = bitrix_df["PHONE"].astype(str).str.replace(r"\D", "", regex=True)
    bitrix_df["Match number"] = bitrix_df["PHONE"].where(valid_numbers.str.len() >= 9, None)

    # Объединяем только строки, где есть корректный номер
    merged_df = bitrix_df.merge(
        incoming_calls_df,
        left_on="Match number",
        right_on="ExternalNumber",
        how="left",
        suffixes=("", "_IncomingCalls")
    )

    merged_df = merged_df.merge(
        getcalls_df,
        left_on="Match number",
        right_on="ExternalNumber",
        how="left",
        suffixes=("", "_GetCalls")
    )

    return merged_df

# def process_and_count_rows(merged_df):
#     """Обрабатывает таблицу: удаляет пустые и дубли, считает нужные строки и сохраняет результат."""
#
#     # Удаляем строки, где 'Match number' пустой
#     filtered_df = merged_df.dropna(subset=['Match number']).copy()
#
#     # Удаляем дубли по 'Match number', оставляя строки с большим числом ненулевых значений
#     filtered_df['non_null_count'] = filtered_df.notna().sum(axis=1)
#     filtered_df = filtered_df.sort_values(by=['Match number', 'non_null_count'], ascending=[True, False])
#     filtered_df = filtered_df.drop_duplicates(subset=['Match number'], keep='first')
#     filtered_df = filtered_df.drop(columns=['non_null_count'])
#
#     # Считаем количество строк, содержащих PBXNumberNumber и 'Match number'
#     total_valid_rows = filtered_df.dropna(subset=['PBXNumberNumber', 'Match number']).shape[0]
#
#     # Считаем количество строк с PBXNumberNumber, но без PBXNumberName
#     pbx_number_without_name = filtered_df.dropna(subset=['PBXNumberNumber']).loc[filtered_df['PBXNumberName'].isna()].shape[0]
#
#     # Считаем количество строк, где есть значение в колонках 'Match number' и 'GeneralCallID_GetCalls'
#     match_and_general_call_id = filtered_df.dropna(subset=['Match number', 'GeneralCallID_GetCalls']).shape[0]
#
#     # Считаем количество строк, где есть значение в колонках 'Match number' и 'GeneralCallID_GetCalls', но нет значения в 'PBXNumberNumber'
#     match_and_general_call_id_without_pbx = filtered_df.dropna(subset=['Match number', 'GeneralCallID_GetCalls']).loc[filtered_df['PBXNumberNumber'].isna()].shape[0]
#
#     # Группируем по 'PBXNumberName' и считаем количество строк
#     pbx_name_counts = filtered_df['PBXNumberName'].fillna('Пусто').value_counts().reset_index()
#     pbx_name_counts.columns = ['PBXNumberName', 'Count']
#
#     # Добавляем строки с дополнительными расчетами
#     summary_rows = [
#         {'PBXNumberName': 'Ліди зі всіх дзвінків', 'Count': total_valid_rows},
#         {'PBXNumberName': 'Ліди з інших номерів', 'Count': pbx_number_without_name},
#         {'PBXNumberName': 'Строки с Match number и GeneralCallID_GetCalls', 'Count': match_and_general_call_id},
#         {'PBXNumberName': 'Ліди з GetCall', 'Count': match_and_general_call_id_without_pbx}
#     ]
#
#     # Добавляем дополнительные строки в итоговую таблицу
#     pbx_name_counts = pd.concat([pd.DataFrame(summary_rows), pbx_name_counts], ignore_index=True)
#     return pbx_name_counts

def process_and_count_rows(merged_df):
    """Обрабатывает таблицу: удаляет пустые и дубли, считает нужные строки и сохраняет результат."""

    # Удаляем строки, где 'Match number' пустой
    filtered_df = merged_df.dropna(subset=['Match number']).copy()

    # Удаляем дубли по 'Match number', оставляя строки с большим числом ненулевых значений
    filtered_df['non_null_count'] = filtered_df.notna().sum(axis=1)
    filtered_df = filtered_df.sort_values(by=['Match number', 'non_null_count'], ascending=[True, False])
    filtered_df = filtered_df.drop_duplicates(subset=['Match number'], keep='first')
    filtered_df = filtered_df.drop(columns=['non_null_count'])

    # Считаем количество строк, содержащих PBXNumberNumber и 'Match number'
    total_valid_rows = filtered_df.dropna(subset=['PBXNumberNumber', 'Match number']).shape[0]

    # Считаем количество строк с PBXNumberNumber, но без PBXNumberName
    pbx_number_without_name = filtered_df.dropna(subset=['PBXNumberNumber']).loc[filtered_df['PBXNumberName'].isna()].shape[0]

    # Считаем количество строк, где есть значение в колонках 'Match number' и 'GeneralCallID_GetCalls'
    match_and_general_call_id = filtered_df.dropna(subset=['Match number', 'GeneralCallID_GetCalls']).shape[0]

    # Считаем количество строк, где есть значение в колонках 'Match number' и 'GeneralCallID_GetCalls', но нет значения в 'PBXNumberNumber'
    match_and_general_call_id_without_pbx = filtered_df.dropna(subset=['Match number', 'GeneralCallID_GetCalls']).loc[filtered_df['PBXNumberNumber'].isna()].shape[0]

    # 👉 Считаем количество строк, где PBXNumberNumber = 504239741
    specific_pbx_number_count = filtered_df[filtered_df['PBXNumberNumber'] == 504239741].shape[0]

    # Группируем по 'PBXNumberName' и считаем количество строк
    pbx_name_counts = filtered_df['PBXNumberName'].fillna('Пусто').value_counts().reset_index()
    pbx_name_counts.columns = ['PBXNumberName', 'Count']

    # Добавляем строки с дополнительными расчетами
    summary_rows = [
        {'PBXNumberName': 'Ліди зі всіх дзвінків', 'Count': total_valid_rows},
        {'PBXNumberName': 'Ліди з інших номерів', 'Count': pbx_number_without_name},
        {'PBXNumberName': 'Строки с Match number и GeneralCallID_GetCalls', 'Count': match_and_general_call_id},
        {'PBXNumberName': 'Ліди з GetCall', 'Count': match_and_general_call_id_without_pbx},
        {'PBXNumberName': 'Ліди з номеру 504239741', 'Count': specific_pbx_number_count},
    ]

    # Добавляем дополнительные строки в итоговую таблицу
    pbx_name_counts = pd.concat([pd.DataFrame(summary_rows), pbx_name_counts], ignore_index=True)
    return pbx_name_counts


def calculate_incoming_calls_stats(incoming_calls_df, pbx_summary_df):
    """
    Удаляет строки с 'SMS-SUCCESS' в колонке Disposition и считает:
    - Общее количество оставшихся строк
    - Количество уникальных значений в колонке ExternalNumber
    - Количество строк, где PBXNumberNumber = 985405115
    - Количество уникальных ExternalNumber, где PBXNumberNumber = 985405115
    - Количество строк, где PBXNumberName = 'CallTracking holz.ua'
    - Количество уникальных ExternalNumber, где PBXNumberName = 'CallTracking holz.ua'
    - Добавляет данные в итоговую таблицу pbx_summary_df
    """
    # Фильтруем данные (исключаем SMS-SUCCESS)
    filtered_df = incoming_calls_df[incoming_calls_df["Disposition"] != "SMS-SUCCESS"]

    # Основные расчёты
    total_rows = len(filtered_df)
    unique_numbers = filtered_df["ExternalNumber"].nunique()

    # Расчёты для PBXNumberNumber = 985405115
    pbx_filtered = filtered_df[filtered_df["PBXNumberNumber"] == "985405115"]
    pbx_total = len(pbx_filtered)
    pbx_unique = pbx_filtered["ExternalNumber"].nunique()

    # Расчёты для PBXNumberNumber = 985405115
    pbx_gmb_filtered = filtered_df[filtered_df["PBXNumberNumber"] == "504239741"]
    pbx_gmb_total = len(pbx_gmb_filtered)
    pbx_gmb_unique = pbx_gmb_filtered["ExternalNumber"].nunique()

    # Расчёты для PBXNumberName = 'CallTracking holz.ua'
    calltracking_filtered = filtered_df[filtered_df["PBXNumberName"] == "CallTracking holz.ua"]
    calltracking_total = len(calltracking_filtered)
    calltracking_unique = calltracking_filtered["ExternalNumber"].nunique()

    # Создаем DataFrame с результатами
    stats_df = pd.DataFrame({
        "Метрика": [
            "Бінотел (вхідні дзвінки): Всього дзвінків",
            "Бінотел (вхідні дзвінки): Всього дзвінків унікальних",
            "Бінотел (вхідні дзвінки): PBXNumberNumber = 985405115",
            "Бінотел (вхідні дзвінки): PBXNumberNumber = 985405115 (унікальні ExternalNumber)",
            "Бінотел (вхідні дзвінки): PBXNumberNumber = 504239741",
            "Бінотел (вхідні дзвінки): PBXNumberNumber = 504239741 (унікальні ExternalNumber)",
            "Бінотел (вхідні дзвінки): PBXNumberName = CallTracking holz.ua",
            "Бінотел (вхідні дзвінки): PBXNumberName = CallTracking holz.ua (унікальні ExternalNumber)"
        ],
        "Значение": [
            total_rows, unique_numbers,
            pbx_total, pbx_unique,
            pbx_gmb_total, pbx_gmb_unique,
            calltracking_total, calltracking_unique
        ]
    })

    # Добавляем результаты в итоговую таблицу
    pbx_summary_df = pd.concat([pbx_summary_df, stats_df], ignore_index=True)

    return pbx_summary_df


def calculate_getcalls_stats(getcalls_df, pbx_summary_df):
    """
    Считает:
    - Общее количество строк в getcalls_df
    - Количество уникальных значений в колонке ExternalNumber
    - Добавляет данные в итоговую таблицу pbx_summary_df
    """
    # Подсчитываем значения
    total_rows = len(getcalls_df)
    unique_numbers = getcalls_df["ExternalNumber"].nunique()

    # Создаем DataFrame с результатами
    stats_df = pd.DataFrame({
        "Метрика": ["Всього разів залишили номер (GetCalls)", "Всього унікальних номерів (GetCalls)"],
        "Значение": [total_rows, unique_numbers]
    })

    # Добавляем результаты в итоговую таблицу
    pbx_summary_df = pd.concat([pbx_summary_df, stats_df], ignore_index=True)

    return pbx_summary_df

def calculate_bitrix_stats(bitrix_df, pbx_summary_df):
    """
    Считает:
    - Общее количество строк в bitrix_df
    - Количество строк, где SOURCE_ID содержит "8", "11" или "STORE"
    - Добавляет данные в итоговую таблицу pbx_summary_df
    """
    # Подсчитываем общее количество строк
    total_rows = len(bitrix_df)

    # Подсчитываем строки с нужными значениями в SOURCE_ID
    filtered_rows = bitrix_df["SOURCE_ID"].astype(str).isin(["8", "11", "STORE"]).sum()

    # Создаем DataFrame с результатами
    stats_df = pd.DataFrame({
        "Метрика": ["Всего лидов (Bitrix)", "Ліди Бізнес з інтернета"],
        "Значение": [total_rows, filtered_rows]
    })

    # Добавляем результаты в итоговую таблицу
    pbx_summary_df = pd.concat([pbx_summary_df, stats_df], ignore_index=True)

    return pbx_summary_df

# filter_facebook_calls.py


def filter_facebook_calls(dfs_with_names: list, start_time: str, stop_time: str, filename_prefix: str = "facebook_combined_calls"):
    """
    Фильтрует звонки из нескольких таблиц по источникам 'facebook_ads', 'fb_catalog', 'fb' и добавляет пометку источника.

    :param dfs_with_names: список кортежей (имя_источника, датафрейм)
    :param start_time: дата начала (для имени файла)
    :param stop_time: дата конца (для имени файла)
    :param filename_prefix: префикс для имени файла
    :return: объединённый датафрейм с фильтрованными строками
    """
    facebook_sources = ["facebook_ads", "fb_catalog", "fb"]
    facebook_rows = []

    for source_name, df in dfs_with_names:
        df.columns = df.columns.str.strip()

        if "UTMSource" not in df.columns:
            print(f"⚠️ В таблице {source_name} нет колонки 'UTMSource'")
            continue

        df["UTMSource"] = df["UTMSource"].astype(str).str.strip().str.lower()
        filtered_df = df[df["UTMSource"].isin(facebook_sources)].copy()
        filtered_df["SourceTable"] = source_name
        facebook_rows.append(filtered_df)

    if facebook_rows:
        combined_df = pd.concat(facebook_rows, ignore_index=True)
        save_to_csv(combined_df, start_time, stop_time, filename_prefix)
        return combined_df
    else:
        print("❌ Не найдено строк с источником 'facebook_ads', 'fb_catalog' или 'fb'")
        return pd.DataFrame()