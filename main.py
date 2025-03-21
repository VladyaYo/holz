import asyncio
from datetime import datetime
import pandas as pd
import os

from api_requests import fetch_incoming_calls, fetch_getcalls_for_period, fetch_bitrix_leads
from process import process_call_data, process_getcalls_data, process_bitrix_data
from table_processing import  save_to_csv, format_all_phone_numbers, merge_tables, process_and_count_rows, calculate_incoming_calls_stats, calculate_getcalls_stats, calculate_bitrix_stats

async def fetch_data(start_time, stop_time, start_date_str, end_date_str):
    """Асинхронный запуск запросов"""
    incoming_calls_task = asyncio.to_thread(fetch_incoming_calls, start_time, stop_time)
    getcalls_task = asyncio.to_thread(fetch_getcalls_for_period, start_time, stop_time)
    bitrix_task = asyncio.to_thread(fetch_bitrix_leads, start_date_str, end_date_str)

    incoming_calls, getcalls, bitrix_leads = await asyncio.gather(incoming_calls_task, getcalls_task, bitrix_task)

    return incoming_calls, getcalls, bitrix_leads


async def main(start_date_str: str, end_date_str: str):

    start_time = datetime.strptime(start_date_str, "%Y-%m-%d")
    stop_time = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Генерация имен файлов
    incoming_calls_file = f"received_data/Incoming_calls_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"
    getcalls_file = f"received_data/Getcalls_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"
    bitrix_file = f"received_data/bitrix_leads_data_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"
    merged_file = f"received_data/merged_data_{start_time.strftime('%Y.%m.%d')}-{stop_time.strftime('%Y.%m.%d')}.csv"

    # Проверка на существование итогового файла
    if os.path.exists(merged_file):
        print(f"Итоговый файл уже существует: {merged_file}")
        return

    # Проверка на существование промежуточных файлов
    files_exist = all(os.path.exists(f) for f in [incoming_calls_file, getcalls_file, bitrix_file])

    if not files_exist:
        # Получаем данные параллельно
        incoming_calls, getcalls, bitrix_leads = await fetch_data(start_time, stop_time, start_date_str, end_date_str)

        # Обрабатываем данные
        if incoming_calls:
            process_call_data(incoming_calls, start_time, stop_time)

        if getcalls:
            process_getcalls_data(getcalls, start_time, stop_time)

        if bitrix_leads:
            process_bitrix_data(bitrix_leads, start_time, stop_time)
    else:
        print("Все файлы уже существуют. Пропускаем запросы.")

    # Загружаем обработанные данные в DataFrame
    incoming_calls_df = pd.read_csv(incoming_calls_file)
    getcalls_df = pd.read_csv(getcalls_file)
    bitrix_df = pd.read_csv(bitrix_file)

    incoming_calls_df, getcalls_df, bitrix_df = format_all_phone_numbers(incoming_calls_df, getcalls_df, bitrix_df)

    save_to_csv(incoming_calls_df, start_time, stop_time, "incoming_calls_ph_formatting")
    save_to_csv(getcalls_df, start_time, stop_time, "getcalls_ph_formatting")
    save_to_csv(bitrix_df, start_time, stop_time, "bitrix_ph_formatting")

    # Объединяем таблицы
    merged_df = merge_tables(bitrix_df, incoming_calls_df, getcalls_df)

    # Сохраняем финальный результат
    save_to_csv(merged_df, start_time, stop_time, "final_merged")

    pbx_summary_df = process_and_count_rows(merged_df)

    pbx_summary_df = calculate_incoming_calls_stats(incoming_calls_df, pbx_summary_df)

    pbx_summary_df = calculate_getcalls_stats(getcalls_df, pbx_summary_df)

    pbx_summary_df = calculate_bitrix_stats(bitrix_df, pbx_summary_df)

    # Сохраняем итоговую таблицу
    final_file_path = save_to_csv(pbx_summary_df, start_time, stop_time, "pbx_summary")


    print("Обработка завершена.")
    return final_file_path

if __name__ == "__main__":
    asyncio.run(main("2025-03-08", "2025-03-14"))
