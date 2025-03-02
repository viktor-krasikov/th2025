import datetime
import sqlite3

import telebot

# Замените 'YOUR_BOT_TOKEN' на токен вашего бота
API_TOKEN = '7741259661:AAEl82efwtIKDyAbCVw-Xb-HQLeFklxEUwk'
bot = telebot.TeleBot(API_TOKEN)


# Создание базы данных и таблицы
conn = sqlite3.connect('tender.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS inn_telegram (
    inn TEXT,
    tg_user_id INTEGER,
    tg_username TEXT,
    period INTEGER,
    last_time TEXT,
    PRIMARY KEY (inn, tg_user_id)
)
''')

conn.commit()
conn.close()


# Функция для добавления ИНН и периода в базу данных
def add_inn_to_db(inn, user_id, username, period):
    conn = sqlite3.connect('tender.db')
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR REPLACE INTO inn_telegram (inn, tg_user_id, tg_username, period, last_time) VALUES (?, ?, ?, ?, ?)',
        (inn, user_id, username, period, None))
    conn.commit()
    conn.close()


# Обработчик команды /start
@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, "Добро пожаловать! Пожалуйста, отправьте свой ИНН для подписки.")


# Обработчик текстовых сообщений
@bot.message_handler(func=lambda message: True)
def handle_inn(message):
    inn = message.text.strip()
    user_id = message.from_user.id
    username = message.from_user.username

    # Запрос периода
    bot.reply_to(message,
                 "Пожалуйста, укажите период отправки отчетов (1 - раз в день, 7 - раз в неделю, 30 - раз в месяц):")
    bot.register_next_step_handler(message, process_period, inn, user_id, username)


def process_period(message, inn, user_id, username):
    try:
        period = int(message.text.strip())
        if period not in [1, 7, 30]:
            raise ValueError("Неверный период. Пожалуйста, введите 1, 7 или 30.")

        # Добавление ИНН и периода в базу данных
        add_inn_to_db(inn, user_id, username, period)
        bot.reply_to(message, f"Вы успешно подписались на обновления для ИНН: {inn} с периодом {period} дней.")
    except ValueError as e:
        bot.reply_to(message, str(e))
        bot.register_next_step_handler(message, process_period, inn, user_id, username)


# Функция для обновления времени последней отправки отчета
def update_last_time(user_id):
    conn = sqlite3.connect('tender.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE inn_telegram SET last_time = ? WHERE tg_user_id = ?', (datetime.now().isoformat(), user_id))
    conn.commit()
    conn.close()


# Отправка отчетов по расписанию
def send_reports():
    conn = sqlite3.connect('tender.db')
    cursor = conn.cursor()
    cursor.execute('SELECT tg_user_id, inn, period, last_time FROM inn_telegram')
    users = cursor.fetchall()
    conn.close()

    for user in users:
        user_id, inn, period, last_time = user
        report_content = f"Ваш отчет для ИНН: {inn}"

        # Проверка, когда был последний отчет
        if last_time is None:
            last_time = datetime.now() - timedelta(days=period)  # Если отчета не было, отправляем сразу
        else:
            last_time = datetime.fromisoformat(last_time)

        # Проверка, нужно ли отправлять отчет
        if (datetime.now() - last_time).days >= period:
            try:
                bot.send_message(user_id, report_content)
                update_last_time(user_id)  # Обновление времени последней отправки
            except Exception as e:
                print(f"Не удалось отправить сообщение пользователю {user_id}: {e}")


# Настройка расписания
import schedule
import time

schedule.every(1).day.at("09:00").do(send_reports)  # Отправка каждый день в 09:00
schedule.every(7).days.at("09:00").do(send_reports)  # Отправка каждую неделю в 09:00
schedule.every(30).days.at("09:00").do(send_reports)  # Отправка каждый месяц в 09:00

# Запуск планировщика в отдельном потоке
def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(1)

import threading
threading.Thread(target=run_schedule).start()

# Запуск бота
bot.polling()