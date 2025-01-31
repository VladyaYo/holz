import pandas as pd
import os

def save_to_csv(df, start_time, stop_time, file_prefix):
    """Сохраняем итоговую таблицу в файл CSV"""
    os.makedirs("processed_data", exist_ok=True)
    file_name = f"processed_{file_prefix}_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"
    df.to_csv(f"processed_data/{file_name}", index=False, encoding="utf-8-sig")
    print(f"Данные успешно сохранены в файл {file_name}")

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

def format_all_phone_numbers(incoming_calls_df, getcalls_df, bitrix_df):
    """Форматирует номера телефонов во всех таблицах."""
    incoming_calls_df = format_phone_columns(incoming_calls_df, ['ExternalNumber', 'PBXNumberNumber'])
    getcalls_df = format_phone_columns(getcalls_df, ['ExternalNumber'])
    bitrix_df = format_phone_columns(bitrix_df, ['PHONE'])
    return incoming_calls_df, getcalls_df, bitrix_df


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

    # Группируем по 'PBXNumberName' и считаем количество строк
    pbx_name_counts = filtered_df['PBXNumberName'].fillna('Пусто').value_counts().reset_index()
    pbx_name_counts.columns = ['PBXNumberName', 'Count']

    # Добавляем строки с дополнительными расчетами
    summary_rows = [
        {'PBXNumberName': 'Всего строк (с PBXNumberNumber и Match number)', 'Count': total_valid_rows},
        {'PBXNumberName': 'Строки с PBXNumberNumber, но без PBXNumberName', 'Count': pbx_number_without_name},
        {'PBXNumberName': 'Строки с Match number и GeneralCallID_GetCalls', 'Count': match_and_general_call_id},
        {'PBXNumberName': 'Строки с Match number и GeneralCallID_GetCalls, но без PBXNumberNumber', 'Count': match_and_general_call_id_without_pbx}
    ]

    # Добавляем дополнительные строки в итоговую таблицу
    pbx_name_counts = pd.concat([pd.DataFrame(summary_rows), pbx_name_counts], ignore_index=True)
    return pbx_name_counts