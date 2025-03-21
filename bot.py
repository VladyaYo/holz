import logging
import asyncio
import datetime
import re
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from dotenv import load_dotenv
import os
from main import main

# Загружаем переменные окружения
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

# Инициализация бота и диспетчера
bot = Bot(token=TOKEN)
dp = Dispatcher()

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Обработчик команды /start
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Введите период в формате ГГГГ-ММ-ДД:ГГГГ-ММ-ДД")

# Обработчик текстовых сообщений
@dp.message()
async def process_text(message: types.Message):
    try:
        # Парсим введенный текст
        start_date, end_date = map(str.strip, re.split(r":", message.text))
        datetime.datetime.strptime(start_date, "%Y-%m-%d")
        datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except (ValueError, IndexError):
        await message.answer("Некорректный формат! Введите даты в формате ГГГГ-ММ-ДД:ГГГГ-ММ-ДД")
        return

    # Отправляем подтверждение выбора периода
    await message.answer(f"Вы выбрали период: {start_date} - {end_date}")

    # Генерируем файл и отправляем его пользователю
    final_file_path = await main(start_date, end_date)
    await bot.send_document(message.chat.id, types.FSInputFile(final_file_path))

    # Сообщаем, что бот готов к новому запросу
    await message.answer("Готов к следующему запросу! Введите новый период.")

# Функция для запуска бота
async def start_bot():
    await dp.start_polling(bot)

# Основная точка входа
if __name__ == "__main__":
    asyncio.run(start_bot())