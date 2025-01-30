import pandas as pd
import os
import phonenumbers


# def format_phone(phone, default_region="UA"):
#     """
#     Преобразует телефонный номер в стандартный формат E.164.
#     Учитывает номера, начинающиеся с 380, '+' или с кода оператора (например, 66XXXXXXX).
#     """
#     try:
#         # Удаляем все символы, кроме цифр и '+'
#         clean_phone = ''.join(filter(lambda x: x.isdigit() or x == '+', phone))
#
#         # Если номер начинается с кода оператора (например, 66XXXXXXX)
#         if len(clean_phone) == 9 and clean_phone[:2] in ['39', '50', '66', '67', '68', '73', '91', '92', '93', '94']:
#             clean_phone = '+380' + clean_phone
#
#         # Если номер начинается с 380, но без '+', добавляем '+'
#         elif clean_phone.startswith('380') and not clean_phone.startswith('+'):
#             clean_phone = '+' + clean_phone
#
#         # Парсим номер
#         parsed_phone = phonenumbers.parse(clean_phone, default_region)
#
#         # Проверяем, что номер валидный, и форматируем в E.164
#         if phonenumbers.is_valid_number(parsed_phone):
#             return phonenumbers.format_number(parsed_phone, phonenumbers.PhoneNumberFormat.E164)
#     except phonenumbers.NumberParseException:
#         pass
#
#     return None


# def merge_tables_by_phone(incoming_calls_df, getcalls_df, bitrix_df, default_region="UA"):
#     """Соединяем таблицы, используя bitrix_df в качестве главной.
#
#     Преобразуем все указанные колонки с номерами телефонов в стандартный формат E.164.
#     """
#     # Преобразование всех указанных колонок в incoming_calls_df
#     for column in ['PBXNumberNumber', 'ExternalNumber']:
#         if column in incoming_calls_df.columns:
#             incoming_calls_df[column] = incoming_calls_df[column].astype(str).apply(
#                 lambda x: format_phone(x, default_region)
#             )
#
#     # Создание единой колонки 'Phone' в incoming_calls_df
#     incoming_calls_df['Phone'] = incoming_calls_df[['PBXNumberNumber', 'ExternalNumber']].bfill(axis=1).iloc[:, 0]
#
#     # Преобразование указанной колонки в getcalls_df
#     if 'ExternalNumber' in getcalls_df.columns:
#         getcalls_df['Phone'] = getcalls_df['ExternalNumber'].astype(str).apply(
#             lambda x: format_phone(x, default_region)
#         )
#
#     # Преобразование указанной колонки в bitrix_df
#     if 'PHONE' in bitrix_df.columns:
#         bitrix_df['Phone'] = bitrix_df['PHONE'].astype(str).apply(
#             lambda x: format_phone(x, default_region)
#         )
#
#     # Удаление некорректных номеров и дубликатов
#     incoming_calls_df = incoming_calls_df.dropna(subset=['Phone']).drop_duplicates(subset=['Phone'])
#     getcalls_df = getcalls_df.dropna(subset=['Phone']).drop_duplicates(subset=['Phone'])
#     bitrix_df = bitrix_df.dropna(subset=['Phone']).drop_duplicates(subset=['Phone'])
#
#     # Объединение таблиц
#     merged_df = pd.merge(bitrix_df, incoming_calls_df, how='inner', on='Phone', suffixes=('_bitrix', '_incoming'))
#     merged_df = pd.merge(merged_df, getcalls_df, how='inner', on='Phone', suffixes=('', '_getcalls'))
#
#     # Преобразование всех колонок с номерами в финальной таблице
#     for column in merged_df.columns:
#         if 'Number' in column or column == 'PHONE':
#             merged_df[column] = merged_df[column].astype(str).apply(
#                 lambda x: format_phone(x, default_region) if pd.notna(x) else x
#             )
#
#     return merged_df

def format_phone(phone, default_region="UA"):
    """
    Преобразует телефонный номер в стандартный формат E.164.
    Учитывает только последние 9 цифр номера (без кода страны).
    """
    try:
        # Преобразуем номер в строку, если он не строка
        phone = str(phone).strip()

        # Удаляем .0, если номер в формате float
        if phone.endswith('.0'):
            phone = phone[:-2]

        # Удаляем все символы, кроме цифр
        clean_phone = ''.join(filter(str.isdigit, phone))

        # Если номер содержит менее 9 цифр, возвращаем None
        if len(clean_phone) < 9:
            return None

        # Оставляем последние 9 цифр номера
        clean_phone = clean_phone[-9:]

        # Добавляем код страны Украины для стандартного формата
        # clean_phone = '+380' + clean_phone

        # Парсим номер
        parsed_phone = phonenumbers.parse(clean_phone, default_region)

        # Проверяем, что номер валидный, и форматируем в E.164
        if phonenumbers.is_valid_number(parsed_phone):
            return phonenumbers.format_number(parsed_phone, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass

    return None


# def combine_tables_with_phone_formatting(incoming_calls_df, getcalls_df, bitrix_df):
#     """
#     Объединяет три таблицы в одну, приводит номера телефонов к единому формату,
#     сохраняет оригинальные названия колонок и указывает источник данных.
#
#     Args:
#         incoming_calls_df (pd.DataFrame): Таблица входящих звонков.
#         getcalls_df (pd.DataFrame): Таблица звонков.
#         bitrix_df (pd.DataFrame): Основная таблица.
#
#     Returns:
#         pd.DataFrame: Объединенная таблица с форматированными номерами телефонов.
#     """
#     # Список колонок с номерами телефонов и их источников
#     phone_columns = {
#         'IncomingCalls_ExternalNumber': incoming_calls_df,
#         'IncomingCalls_PBXNumberNumber': incoming_calls_df,
#         'GetCalls_ExternalNumber': getcalls_df,
#         'Bitrix_PHONE': bitrix_df
#     }
#
#     # Приводим номера к единому формату
#     for col, df in phone_columns.items():
#         if col in df.columns:
#             df[col] = df[col].apply(format_phone).astype(str)  # Приводим к строковому типу
#
#     # Добавляем префиксы к колонкам для указания источника
#     incoming_calls_df = incoming_calls_df.add_prefix('IncomingCalls_')
#     getcalls_df = getcalls_df.add_prefix('GetCalls_')
#     bitrix_df = bitrix_df.add_prefix('Bitrix_')
#
#     # Добавляем источник данных для идентификации
#     incoming_calls_df['Source'] = 'IncomingCalls'
#     getcalls_df['Source'] = 'GetCalls'
#     bitrix_df['Source'] = 'Bitrix'
#
#     # Объединяем таблицы
#     combined_df = pd.concat([incoming_calls_df, getcalls_df, bitrix_df], ignore_index=True, sort=False)
#
#     return combined_df

def combine_tables_with_phone_formatting(incoming_calls_df, getcalls_df, bitrix_df):
    """
    Объединяет три таблицы, предварительно форматируя номера телефонов в указанных колонках.
    """
    def format_phone_numbers(df, columns):
        """
        Применяет преобразование номера телефона ко всем указанным колонкам DataFrame.
        """
        for col in columns:
            if col in df.columns:
                df[col] = df[col].apply(format_phone)
        return df

    # Преобразование номеров в каждой таблице
    incoming_calls_df = format_phone_numbers(
        incoming_calls_df, ['ExternalNumber', 'PBXNumberNumber']
    )
    getcalls_df = format_phone_numbers(getcalls_df, ['ExternalNumber'])
    bitrix_df = format_phone_numbers(bitrix_df, ['PHONE'])

    # Добавляем источник данных в название колонок
    incoming_calls_df = incoming_calls_df.add_prefix("IncomingCalls_")
    getcalls_df = getcalls_df.add_prefix("GetCalls_")
    bitrix_df = bitrix_df.add_prefix("Bitrix_")

    # Объединяем все три таблицы
    merged_df = pd.concat([incoming_calls_df, getcalls_df, bitrix_df], ignore_index=True)

    return merged_df



def save_to_csv(df, start_time, stop_time, file_prefix):
    """Сохраняем итоговую таблицу в файл CSV"""
    os.makedirs("processed_data", exist_ok=True)
    file_name = f"processed_{file_prefix}_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"
    df.to_csv(f"processed_data/{file_name}", index=False, encoding="utf-8-sig")
    print(f"Данные успешно сохранены в файл {file_name}")


# def merge_tables_by_phone(incoming_calls_df, getcalls_df, bitrix_df):
#     """
#     Форматирует телефонные номера и мержит incoming_calls_df и getcalls_df в bitrix_df.
#
#     Args:
#         incoming_calls_df (pd.DataFrame): Таблица входящих звонков.
#         getcalls_df (pd.DataFrame): Таблица звонков.
#         bitrix_df (pd.DataFrame): Основная таблица.
#
#     Returns:
#         pd.DataFrame: Объединенная таблица.
#     """
#
#     def format_column(df, column):
#         """Форматирует указанную колонку с номерами телефонов."""
#         df[column] = df[column].apply(lambda x: format_phone(str(x)) if pd.notna(x) else None)
#         return df
#
#     # Форматируем номера телефонов
#     incoming_calls_df = format_column(incoming_calls_df, 'PBXNumberNumber')
#     incoming_calls_df = format_column(incoming_calls_df, 'ExternalNumber')
#     getcalls_df = format_column(getcalls_df, 'ExternalNumber')
#     bitrix_df = format_column(bitrix_df, 'PHONE')
#
#     # Удаляем дубликаты по ключевым колонкам
#     incoming_calls_df = incoming_calls_df.drop_duplicates(subset=['ExternalNumber'])
#     getcalls_df = getcalls_df.drop_duplicates(subset=['ExternalNumber'])
#     bitrix_df = bitrix_df.drop_duplicates(subset=['PHONE'])
#
#     print(f"Размер bitrix_df после удаления дубликатов: {bitrix_df.shape}")
#     print(f"Размер incoming_calls_df после удаления дубликатов: {incoming_calls_df.shape}")
#     print(f"Размер getcalls_df после удаления дубликатов: {getcalls_df.shape}")
#
#     # Мержим incoming_calls_df в bitrix_df
#     merged_df = pd.merge(
#         bitrix_df,
#         incoming_calls_df[['ExternalNumber', 'PBXNumberNumber']],
#         left_on='PHONE',
#         right_on='ExternalNumber',
#         how='left',
#         suffixes=('_bitrix', '_incoming')
#     )
#
#     print(f"Размер merged_df после первого мержa: {merged_df.shape}")
#
#     # Мержим getcalls_df в merged_df
#     merged_df = pd.merge(
#         merged_df,
#         getcalls_df[['ExternalNumber']],  # Добавьте нужные колонки
#         left_on='PHONE',
#         right_on='ExternalNumber',
#         how='left',
#         suffixes=('', '_getcalls')
#     )
#
#     print(f"Размер merged_df после второго мержa: {merged_df.shape}")
#
#     return merged_df

def merge_and_filter_columns(merged_df):
    """
    Объединяет строки внутри `merged_df` по совпадению `Bitrix_PHONE` и `IncomingCalls_ExternalNumber`.
    - Создаёт колонку 'Phone' для объединения.
    - Объединяет строки, заменяя пропущенные значения.
    - Оставляет только нужные колонки.
    """

    # Создаём новую колонку 'Phone' для объединения
    merged_df['Phone'] = merged_df['Bitrix_PHONE'].fillna(merged_df['IncomingCalls_ExternalNumber'])

    # Удаляем дубликаты по 'Phone' (оставляем первую встреченную строку)
    merged_df = merged_df.drop_duplicates(subset=['Phone'])

    # Объединяем таблицу саму с собой по 'Phone'
    combined_df = merged_df.merge(merged_df, on='Phone', how='outer')

    # Заполняем пропущенные значения (NaN) из другой строки
    combined_df = combined_df.combine_first(merged_df)

    # Выбираем только нужные колонки
    required_columns = [
        'IncomingCalls_GeneralCallID',
        'IncomingCalls_StartTime',
        'IncomingCalls_InternalNumber',
        'IncomingCalls_PBXNumberNumber',
        'IncomingCalls_PBXNumberName',
        'IncomingCalls_LinkToCrmUrl',
        'IncomingCalls_ExternalNumber'
    ] + [col for col in combined_df.columns if col.startswith('Bitrix_')]

    # Оставляем только нужные колонки
    filtered_df = combined_df[required_columns]

    return filtered_df


def process_pbx_column_data(processed_merged_data):
    """
    Подсчитывает количество строк с непустыми значениями в PBXNumberNumber,
    уникальные значения в PBXNumberName, и сохраняет результаты в новую таблицу.
    Удаляет дублирующиеся строки в колонке Phone перед расчетами.
    Также подсчитывает строки с непустыми значениями в CallID и PBXNumberName.
    """
    # Удаляем дубликаты по колонке Phone
    # processed_merged_data = processed_merged_data.drop_duplicates(subset=['Phone'])

    # Убираем пропуски (NaN) из колонок перед подсчетом
    pbx_number_count = processed_merged_data['PBXNumberNumber'].notna().sum()

    # Подсчитываем количество уникальных значений в колонке PBXNumberName
    unique_pbx_names_count = processed_merged_data['PBXNumberName'].nunique()

    # Подсчитываем количество каждого уникального значения в колонке PBXNumberName
    pbx_name_value_counts = processed_merged_data['PBXNumberName'].value_counts().reset_index()
    pbx_name_value_counts.columns = ['PBXNumberName', 'Count']

    # Подсчитываем количество строк с непустыми значениями в колонке CallID
    call_id_count = processed_merged_data['CallID'].notna().sum()

    # Подсчитываем строки, где есть и CallID, и PBXNumberName
    filtered_data = processed_merged_data.dropna(subset=['CallID', 'PBXNumberName'])
    callid_pbx_name_value_counts = filtered_data['PBXNumberName'].value_counts().reset_index()
    callid_pbx_name_value_counts.columns = ['PBXNumberName', 'CountWithCallID']

    # Создаем новый DataFrame с результатами подсчета
    summary_data = {
        'PBXNumberNumberCount': [pbx_number_count],
        'UniquePBXNumberNameCount': [unique_pbx_names_count],
        'CallIDCount': [call_id_count]
    }

    summary_df = pd.DataFrame(summary_data)

    # Используем pd.concat для объединения всех таблиц
    result_df = pd.concat([summary_df, pbx_name_value_counts, callid_pbx_name_value_counts], ignore_index=True, axis=1)

    return result_df
