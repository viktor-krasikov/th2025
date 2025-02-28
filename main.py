from fastapi import FastAPI
import pandas as pd

app = FastAPI()

data = pd.read_excel('./data/TenderHack_20250228_1900.xlsx', sheet_name=0)  # sheet_name=0 для первого листа

@app.get("/")
def hello():
    return {"message": "Hello World"}

@app.get("/winners")
def get_winners():
    global data
    # Загрузка данных из файла Excel (замените 'data.xlsx' на ваш файл)
    # Группируем данные по ИНН и подсчитываем количество побед
    result = data.groupby(['ИНН победителя КС', 'Наименование победителя КС', 'Регион победителя КС']).size().reset_index(name='Количество побед')

    # Сортируем по количеству побед в порядке убывания и выбираем топ-100
    top_winners = result.sort_values(by='Количество побед', ascending=False).head(100)

    # Преобразуем результат в формат JSON
    return top_winners.to_dict(orient='records')

@app.get("/unique_inns")
def get_unique_inns():
    global data

    # Подсчитываем количество уникальных значений ИНН и их упоминания
    unique_inns = data['ИНН победителя КС'].value_counts().reset_index()
    unique_inns.columns = ['ИНН победителя КС', 'Количество упоминаний']

    # Преобразуем результат в формат JSON
    return unique_inns.to_dict(orient='records')

# Запуск приложения
# Используйте команду: uvicorn имя_файла:app --reload
