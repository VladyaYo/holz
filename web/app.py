import asyncio
from flask import Flask, request, render_template, send_file, url_for, redirect
import os
from datetime import datetime

# Импортируем наш генератор отчетов
from services.report_generator import generate_report

app = Flask(__name__)

# Убедимся, что директория для отчетов существует
REPORT_DIR = "generated_reports"
if not os.path.exists(REPORT_DIR):
    os.makedirs(REPORT_DIR)

@app.route('/', methods=['GET', 'POST'])
async def index():
    if request.method == 'POST':
        start_date_str = request.form.get('start_date')
        end_date_str = request.form.get('end_date')

        if not start_date_str or not end_date_str:
            return render_template('index.html', error="Пожалуйста, выберите обе даты.")

        try:
            # Проверяем формат даты
            datetime.strptime(start_date_str, "%Y-%m-%d")
            datetime.strptime(end_date_str, "%Y-%m-%d")
        except ValueError:
            return render_template('index.html', error="Неверный формат даты. Используйте YYYY-MM-DD.")

        # Запускаем генерацию отчета
        report_file_path = await generate_report(start_date_str, end_date_str)

        if report_file_path:
            # Перенаправляем на страницу с отчетом или предлагаем скачать
            # Для простоты пока просто перенаправим на скачивание
            # В реальном приложении можно было бы отобразить отчет в таблице на странице
            return redirect(url_for('download_report', filename=os.path.basename(report_file_path)))
        else:
            return render_template('index.html', error="Не удалось сгенерировать отчет.")

    return render_template('index.html')

@app.route('/download/<filename>')
def download_report(filename):
    file_path = os.path.join(REPORT_DIR, filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        return "Файл не найден.", 404

if __name__ == '__main__':
    # Для разработки, в продакшене будет использоваться Gunicorn
    app.run(debug=True, host='0.0.0.0', port=5000)
