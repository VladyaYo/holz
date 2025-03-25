# Используем официальный образ Python
FROM python:3.12-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файл с зависимостями
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем переменные окружения из .env файла
ENV $(cat .env | xargs)

# Копируем остальные файлы проекта
COPY . .

# Команда для запуска бота
CMD ["python3", "bot.py"]