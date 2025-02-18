import logging
import asyncio
import datetime
import re
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from dotenv import load_dotenv
import os
from fastapi import FastAPI
from main import main  # Импортируем функцию main из main.py

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(token=TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

app = FastAPI()  # Создаем FastAPI приложение


@app.get("/")
async def root():
    return {"status": "бот работает"}


@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Введите период в формате ГГГГ-ММ-ДД:ГГГГ-ММ-ДД")


@dp.message()
async def process_text(message: types.Message):
    try:
        start_date, end_date = map(str.strip, re.split(r":", message.text))
        datetime.datetime.strptime(start_date, "%Y-%m-%d")
        datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except (ValueError, IndexError):
        await message.answer("Некорректный формат! Введите даты в формате ГГГГ-ММ-ДД:ГГГГ-ММ-ДД")
        return

    await message.answer(f"Вы выбрали период: {start_date} - {end_date}")

    final_file_path = await main(start_date, end_date)
    await bot.send_document(message.chat.id, types.FSInputFile(final_file_path))

    await message.answer("Готов к следующему запросу! Введите новый период.")


async def start_bot():
    await dp.start_polling(bot)


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(start_bot())  # Запускаем бота в фоне
