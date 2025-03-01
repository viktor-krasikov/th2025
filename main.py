from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
from fastapi.responses import StreamingResponse
import matplotlib.pyplot as plt
import io
import sqlite3

app = FastAPI()

print("XLS started")
df = pd.read_excel('./data/TenderHack_20250228_1900.xlsx', sheet_name=0)  # sheet_name=0 для первого листа
print("XLS loaded")

conn = sqlite3.connect("tender.db")
@app.get("/")
def hello():
    return {"message": "Hello World2"}


@app.get("/winners")
def get_winners():
    global df
    # Загрузка данных из файла Excel (замените 'data.xlsx' на ваш файл)
    # Группируем данные по ИНН и подсчитываем количество побед
    result = df.groupby(['ИНН победителя КС', 'Наименование победителя КС', 'Регион победителя КС']).size().reset_index(
        name='Количество побед')

    # Сортируем по количеству побед в порядке убывания и выбираем топ-100
    top_winners = result.sort_values(by='Количество побед', ascending=False).head(100)

    # Преобразуем результат в формат JSON
    return top_winners.to_dict(orient='records')


@app.get("/unique_inns")
def get_unique_inns():
    global df

    # Подсчитываем количество уникальных значений ИНН и их упоминания
    unique_inns = df['ИНН победителя КС'].value_counts().reset_index()
    unique_inns.columns = ['ИНН победителя КС', 'Количество упоминаний']

    # Преобразуем результат в формат JSON
    return unique_inns.to_dict(orient='records')


@app.get("/inns")
def get_inns(q: str = Query(None)):
    global df
    # Извлечение необходимых колонок
    suppliers = df[['ИНН победителя КС', 'Наименование победителя КС']].drop_duplicates()

    # Фильтрация по параметру q, если он задан
    if q:
        suppliers = suppliers[
            suppliers['ИНН победителя КС'].astype(str).str.contains(q, case=False) |
            suppliers['Наименование победителя КС'].str.contains(q, case=False)
            ]

    # Переименование колонок
    suppliers = suppliers.rename(columns={
        'ИНН победителя КС': 'inn',
        'Наименование победителя КС': 'name'
    })

    # Сортировка по колонке 'inn'
    suppliers = suppliers.sort_values(by='inn')

    # Преобразование в список словарей
    suppliers_list = suppliers.to_dict(orient='records')

    return JSONResponse(content=suppliers_list)


@app.get("/top_customers_plot.jpg")
async def get_top_customers_plot(count: int = Query(10, ge=1)):
    # Топ-N заказчиков по количеству КС
    top_customers = df['Наименование заказчика'].value_counts().nlargest(count)

    # Создание графика
    plt.figure(figsize=(10, 6))
    top_customers.plot(kind='bar', color='skyblue')
    plt.title(f'Топ-{count} заказчиков по количеству КС', fontsize=16)
    plt.xlabel('Заказчики', fontsize=14)
    plt.ylabel('Количество КС', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Сохранение графика в буфер
    buf = io.BytesIO()
    plt.savefig(buf, format='jpg')
    buf.seek(0)
    plt.close()  # Закрываем график, чтобы избежать утечек памяти

    return StreamingResponse(buf, media_type="image/jpeg")


@app.get("/top_suppliers_plot.jpg")
async def get_top_suppliers_plot(count: int = Query(10, ge=1)):
    # Топ-N поставщиков по общей сумме выигранных КС
    top_suppliers = df.groupby('Наименование победителя КС')['Конечная цена КС (победителя в КС)'].sum().nlargest(count)

    # Создание графика
    plt.figure(figsize=(10, 6))
    top_suppliers.plot(kind='bar', color='lightgreen')
    plt.title(f'Топ-{count} поставщиков по общей сумме выигранных КС', fontsize=16)
    plt.xlabel('Поставщики', fontsize=14)
    plt.ylabel('Сумма выигранных КС', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Сохранение графика в буфер
    buf = io.BytesIO()
    plt.savefig(buf, format='jpg')
    buf.seek(0)
    plt.close()  # Закрываем график, чтобы избежать утечек памяти

    return StreamingResponse(buf, media_type="image/jpeg")