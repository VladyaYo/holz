import asyncio
from datetime import datetime
import pandas as pd
import os

# Измененные импорты
from ..api_requests import fetch_calltracking_calls, fetch_incoming_calls, fetch_getcalls_for_period, fetch_bitrix_leads
from ..process import process_calltracking_data, process_call_data, process_getcalls_data, process_bitrix_data
from ..table_processing import save_to_csv, format_all_phone_numbers, merge_tables, process_and_count_rows, \
    calculate_incoming_calls_stats, calculate_getcalls_stats, calculate_bitrix_stats, filter_facebook_calls

# Define the directory for generated reports
GENERATED_REPORTS_DIR = "generated_reports"

async def fetch_all_data(start_time, stop_time, start_date_str, end_date_str):
    """Асинхронный запуск запросов"""
    calltracking_calls_task = asyncio.to_thread(fetch_calltracking_calls, start_time, stop_time)
    incoming_calls_task = asyncio.to_thread(fetch_incoming_calls, start_time, stop_time)
    getcalls_task = asyncio.to_thread(fetch_getcalls_for_period, start_time, stop_time)
    bitrix_task = asyncio.to_thread(fetch_bitrix_leads, start_date_str, end_date_str)

    calltracking_calls, incoming_calls, getcalls, bitrix_leads = await asyncio.gather(calltracking_calls_task, incoming_calls_task, getcalls_task, bitrix_task)

    return calltracking_calls, incoming_calls, getcalls, bitrix_leads


async def generate_report(start_date_str: str, end_date_str: str) -> str:
    """
    Генерирует отчет на основе данных из различных источников.
    Возвращает путь к сгенерированному файлу отчета.
    """
    start_time = datetime.strptime(start_date_str, "%Y-%m-%d")
    stop_time = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Генерация имен файлов
    received_data_dir = "received_data"
    if not os.path.exists(received_data_dir):
        os.makedirs(received_data_dir)
    if not os.path.exists(GENERATED_REPORTS_DIR):
        os.makedirs(GENERATED_REPORTS_DIR)

    calltracking_calls_file = os.path.join(received_data_dir, f"calltracking_calls_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv")
    incoming_calls_file = os.path.join(received_data_dir, f"Incoming_calls_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv")
    getcalls_file = os.path.join(received_data_dir, f"Getcalls_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv")
    bitrix_file = os.path.join(received_data_dir, f"bitrix_leads_data_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv")
    merged_file = os.path.join(received_data_dir, f"merged_data_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv")

    # Define the final report file path early
    final_report_name = f"processed_pbx_summary_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"
    final_file_path = os.path.join(GENERATED_REPORTS_DIR, final_report_name)

    # Проверка на существование итогового файла
    if os.path.exists(final_file_path): # Check for the final report file
        print(f"Итоговый файл уже существует: {final_file_path}")
        return final_file_path

    # Проверка на существование промежуточных файлов
    files_exist = all(os.path.exists(f) for f in [calltracking_calls_file, incoming_calls_file, getcalls_file, bitrix_file])

    if not files_exist:
        # Получаем данные параллельно
        calltracking_calls, incoming_calls, getcalls, bitrix_leads = await fetch_all_data(start_time, stop_time, start_date_str, end_date_str)

        # Обрабатываем данные
        if calltracking_calls:
            process_calltracking_data(calltracking_calls, start_time, stop_time)

        if incoming_calls:
            process_call_data(incoming_calls, start_time, stop_time)

        if getcalls:
            process_getcalls_data(getcalls, start_time, stop_time)

        if bitrix_leads:
            process_bitrix_data(bitrix_leads, start_time, stop_time)
    else:
        print("Все файлы уже существуют. Пропускаем запросы.")

    # Загружаем обработанные данные в DataFrame
    calltracking_calls_df = pd.read_csv(calltracking_calls_file)
    incoming_calls_df = pd.read_csv(incoming_calls_file)
    getcalls_df = pd.read_csv(getcalls_file)
    bitrix_df = pd.read_csv(bitrix_file)

    calltracking_calls_df, incoming_calls_df, getcalls_df, bitrix_df = format_all_phone_numbers(calltracking_calls_df, incoming_calls_df, getcalls_df, bitrix_df)

    # Update save_to_csv calls for intermediate files
    save_to_csv(incoming_calls_df, start_time, stop_time, "incoming_calls_ph_formatting", output_dir=received_data_dir)
    save_to_csv(getcalls_df, start_time, stop_time, "getcalls_ph_formatting", output_dir=received_data_dir)
    save_to_csv(bitrix_df, start_time, stop_time, "bitrix_ph_formatting", output_dir=received_data_dir)

    facebook_combined_df = filter_facebook_calls(
        dfs_with_names=[
            ("calltracking_calls", calltracking_calls_df),
            ("getcalls", getcalls_df),
        ],
        start_time=start_time.strftime('%Y.%m.%d'),
        stop_time=stop_time.strftime('%Y.%m.%d'),
        output_dir=GENERATED_REPORTS_DIR # Pass output_dir to filter_facebook_calls
    )

    # Объединяем таблицы
    merged_df = merge_tables(bitrix_df, incoming_calls_df, getcalls_df)

    # Сохраняем финальный результат
    save_to_csv(merged_df, start_time, stop_time, "final_merged", output_dir=received_data_dir)

    # Фильтруем строки, где UTMSource — facebook_ads или fb_catalog
    merged_df["UTMSource"] = merged_df["UTMSource"].str.strip().str.lower()
    facebook_df = merged_df[
        merged_df["UTMSource"].isin(["facebook_ads", "fb"])
    ]

    # Сохраняем отфильтрованные данные
    save_to_csv(facebook_df, start_time, stop_time, "facebook_only", output_dir=GENERATED_REPORTS_DIR)

    pbx_summary_df = process_and_count_rows(merged_df)

    pbx_summary_df = calculate_incoming_calls_stats(incoming_calls_df, pbx_summary_df)

    pbx_summary_df = calculate_getcalls_stats(getcalls_df, pbx_summary_df)

    pbx_summary_df = calculate_bitrix_stats(bitrix_df, pbx_summary_df)

    if 'TITLE' in bitrix_df.columns:
        # Приводим к строке и к нижнему регистру, на всякий случай обрабатываем NaN
        titles = bitrix_df['TITLE'].astype(str).str.strip().str.lower()

        # Ищем те, что начинаются с нужных фраз
        site_orders_mask = titles.str.startswith('замовлення на сайті') | titles.str.startswith('заказ на сайте')
        site_orders_count = site_orders_mask.sum()

        # Добавляем в сводную таблицу новую строку
        new_row = pd.DataFrame([{
            'Источник': 'Замовлення на сайті',
            'Всего звонков/лидов': site_orders_count,
            # 'Уникальные': site_orders_count,  # можно уточнить логику уникальности, если нужно
            # 'Принято': site_orders_count,
            # 'Пропущено': 0,
            # 'Целевые': site_orders_count,
            # 'Нецелевые': 0,
            # 'Конверсия': 100.0 if site_orders_count > 0 else 0.0
        }])

        # Вставляем строку в нужное место (например, после строки с Битриксом или в конец)
        pbx_summary_df = pd.concat([pbx_summary_df, new_row], ignore_index=True)
    else:
        print("Внимание: колонка 'TITLE' не найдена в bitrix_df — пропущен расчёт 'Замовлення на сайті'")

    # Сохраняем итоговую таблицу
    final_file_path = save_to_csv(pbx_summary_df, start_time, stop_time, "pbx_summary", output_dir=GENERATED_REPORTS_DIR)

    print("Обработка завершена.")
    return final_file_path
