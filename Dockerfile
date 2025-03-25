FROM python:3.12-slim

# Устанавливаем зависимости для PostgreSQL
RUN apt-get update && apt-get install -y libpq-dev

# Создаем рабочую директорию
WORKDIR /app

# Копируем файлы проекта
COPY . .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "bot.py"]