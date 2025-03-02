import io
import sqlite3
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import pandas as pd
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse

app = FastAPI()

print("XLS started")
df = pd.read_excel('./data/TenderHack_20250228_1900.xlsx', sheet_name=0)  # sheet_name=0 для первого листа
# Вывод типов всех колонок
print(df.dtypes)
df['Окончание КС'] = pd.to_datetime(df['Окончание КС'], errors='coerce')
print(df.dtypes)
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
def get_top_customers_plot(count: int = Query(10, ge=1)):
    global df
    # Топ-N заказчиков по количеству КС
    count = 15
    top_customers = df['Наименование заказчика'].value_counts().nlargest(count)

    # Проверка, если количество запрашиваемых значений больше доступных
    if len(top_customers) < count:
        return {"error": f"Запрашиваемое количество {count} превышает доступные значения: {len(top_customers)}"}

    # Создание графика
    plt.figure(figsize=(20, 6))
    top_customers.plot(kind='bar', color='skyblue')
    plt.title(f'Топ-{count} заказчиков по количеству КС', fontsize=16)
    plt.xlabel('Заказчики', fontsize=14)
    plt.ylabel('Количество КС', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Использование tight_layout для автоматической настройки отступов
    plt.tight_layout()

    # Сохранение графика в буфер
    buf = io.BytesIO()
    plt.savefig(buf, format='jpg', bbox_inches='tight')
    buf.seek(0)
    plt.close()  # Закрываем график, чтобы избежать утечек памяти

    return StreamingResponse(buf, media_type="image/jpeg")


@app.get("/top_suppliers_plot.jpg")
def get_top_suppliers_plot(count: int = Query(10, ge=1)):
    global df
    # Топ-N поставщиков по общей сумме выигранных КС
    top_suppliers = df.groupby('Наименование победителя КС')['Конечная цена КС (победителя в КС)'].sum().nlargest(count)

    # Проверка, если количество запрашиваемых значений больше доступных
    if len(top_suppliers) < count:
        return {"error": f"Запрашиваемое количество {count} превышает доступные значения: {len(top_suppliers)}"}

    # Создание графика
    plt.figure(figsize=(10, 6))
    top_suppliers.plot(kind='bar', color='lightgreen')
    plt.title(f'Топ-{count} поставщиков по общей сумме выигранных КС', fontsize=16)
    plt.xlabel('Поставщики', fontsize=14)
    plt.ylabel('Сумма выигранных КС', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Использование tight_layout для автоматической настройки отступов
    plt.tight_layout()

    # Сохранение графика в буфер
    buf = io.BytesIO()
    plt.savefig(buf, format='jpg', bbox_inches='tight')
    buf.seek(0)
    plt.close()  # Закрываем график, чтобы избежать утечек памяти

    return StreamingResponse(buf, media_type="image/jpeg")


@app.get("/sessions")
async def get_sessions(inn: str = Query(...)):
    global df
    # Фильтрация по ИНН в колонке "Участники КС - поставщики"
    filtered_sessions = df[df['Участники КС - поставщики'].str.contains(inn, na=False)]

    # Выбор необходимых колонок
    selected_columns = filtered_sessions[['Id КС', 'Ссылка на КС', 'ИНН заказчика',
                                          'Наименование заказчика', 'Регион заказчика',
                                          'Закон-основание', 'Начало КС', 'Окончание КС',
                                          'Начальная цена КС', 'Конечная цена КС (победителя в КС)',
                                          'ИНН победителя КС', 'Код КПГЗ', 'Наименование КПГЗ']]

    # Дедупликация
    deduplicated_sessions = selected_columns.drop_duplicates()

    # Добавление колонки "Победитель"
    deduplicated_sessions['Победитель'] = deduplicated_sessions['ИНН победителя КС'] == inn

    # Добавление вычисляемой колонки "Размер уступки"
    # deduplicated_sessions['Размер уступки'] = deduplicated_sessions['Начальная цена КС'] - deduplicated_sessions[
    #     'Конечная цена КС (победителя в КС)']

    # Преобразование в список словарей
    result_list = deduplicated_sessions.to_dict(orient='records')
    print(result_list)
    return JSONResponse(content=result_list)


@app.get("/wins_plot.jpg")
async def get_wins_plot(inn: str = Query(...)):
    global df
    print(df)
    # Фильтрация данных по ИНН, где ИНН является победителем
    filtered_data = df[df['ИНН победителя КС'].astype(str).str.contains(inn)]
    print(filtered_data)

    # Проверка, есть ли данные для построения графика
    if filtered_data.empty:
        return {"message": "Нет данных для указанного ИНН."}

    # Создание графика
    plt.figure(figsize=(10, 6))
    plt.scatter(filtered_data['Окончание КС'], filtered_data['Конечная цена КС (победителя в КС)'], color='blue')
    plt.title(f'Победы в КС: {inn}')
    plt.xlabel('Дата окончания КС')
    plt.ylabel('Конечная цена КС')
    plt.xticks(rotation=45)
    plt.grid()

    buf = io.BytesIO()
    plt.savefig(buf, format='jpg', bbox_inches='tight')
    buf.seek(0)
    plt.close()  # Закрываем график, чтобы избежать утечек памяти

    return StreamingResponse(buf, media_type="image/jpeg")

    #
    # # Сохранение графика в формате JPG
    # plot_filename = 'plot.jpg'
    # plt.savefig(plot_filename)
    # plt.close()  # Закрываем график, чтобы освободить память
    #
    # # Возврат графика
    # return FileResponse(plot_filename, media_type='image/jpeg', filename=plot_filename)


@app.get("/wins_dots")
async def get_wins_dots(inn: str = Query(...)):
    global df
    print(df)
    # Фильтрация данных по ИНН, где ИНН является победителем
    filtered_data = df[df['ИНН победителя КС'].astype(str).str.contains(inn)]
    print(filtered_data)

    # Проверка, есть ли данные для построения графика
    if filtered_data.empty:
        return {"message": "Нет данных для указанного ИНН."}

    # Получение текущей даты и даты два года назад
    current_date = datetime.now()
    two_years_ago = current_date - timedelta(days=730)  # 2 года = 730 дней

    # Фильтрация данных за последние 2 года
    filtered_data = filtered_data[filtered_data['Окончание КС'] >= two_years_ago]

    # Создание графика
    points = filtered_data[
        ['Окончание КС', 'Начальная цена КС', 'Конечная цена КС (победителя в КС)']].drop_duplicates()
    points = points.rename(columns={'Окончание КС': 'ks_date', 'Начальная цена КС': 'ks_start_price',
                                    'Конечная цена КС (победителя в КС)': 'ks_end_price'})

    # Преобразование колонки даты в формат datetime и обрезка времени
    points['ks_date'] = pd.to_datetime(points['ks_date']).dt.date

    # Вычисление суммы уступки
    #points['Сумма уступки'] = points['ks_start_price'] - points['ks_end_price']
    summa_ustupki = points['ks_start_price'] - points['ks_end_price']
    print(points)
    print(points.dtypes)
    #points['Сумма уступки'] = pd.to_numeric(points['Сумма уступки'], errors='coerce')

    # Вычисление процента уступки
    procent_ustupki = (summa_ustupki / points['ks_start_price']) * 100
#    points['Процент уступки'] = pd.to_numeric(points['Процент уступки'], errors='coerce')

    # # Группировка по дате и суммирование цен
    points = points.groupby('ks_date', as_index=False).sum()

    # Сортировка по возрастанию даты
    points = points.sort_values(by='ks_date').astype(str)

    # Подсчет общей суммы уступок и среднего процента уступки
    #total_discount_sum = points['Сумма уступки'].sum()
#    average_discount_percentage = points['Процент уступки'].dropna().mean()  #???

    summary = {
        "total_discount_sum": 12000.0,
        "average_discount_percentage": 12.3
    }
    # Преобразование в список словарей
    result_list = points.to_dict(orient='records')
    return JSONResponse(content={"summary": summary, "data": result_list})
    # return JSONResponse(content=result_list)

    #
    # # Сохранение графика в формате JPG
    # plot_filename = 'plot.jpg'
    # plt.savefig(plot_filename)
    # plt.close()  # Закрываем график, чтобы освободить память
    #
    # # Возврат графика
    # return FileResponse(plot_filename, media_type='image/jpeg', filename=plot_filename)


@app.get("/wins_plot_sql.jpg")
async def get_wins_plot_sql(inn: str = Query(...)):
    global conn
    # ГРАФИК ПОЛУЧЕНИЯ ПОБЕД В РАЗРЕЗЕ ДАТА И КОНЕЧНАЯ ЦЕНА
    GET_WINNS_COST_AND_DATA = f'''SELECT DISTINCT data."id КС", data."Конечная цена КС (победителя в КС)", data."Окончание КС"
    from data
    WHERE data."ИНН победителя КС" = \'{inn}\''''
    postavshik_df = pd.read_sql_query(GET_WINNS_COST_AND_DATA, conn)
    print(postavshik_df)

    # Создание графика
    plt.figure(figsize=(10, 6))
    plt.scatter(postavshik_df['Окончание КС'], postavshik_df['Конечная цена КС (победителя в КС)'], color='blue')
    plt.title(f'Победы в КС: {inn}')
    plt.xlabel('Дата окончания КС')
    plt.ylabel('Конечная цена КС')
    plt.xticks(rotation=45)
    plt.grid()

    buf = io.BytesIO()
    plt.savefig(buf, format='jpg', bbox_inches='tight')
    buf.seek(0)
    plt.close()  # Закрываем график, чтобы избежать утечек памяти

    return StreamingResponse(buf, media_type="image/jpeg")

    #
    # # Сохранение графика в формате JPG
    # plot_filename = 'plot.jpg'
    # plt.savefig(plot_filename)
    # plt.close()  # Закрываем график, чтобы освободить память
    #
    # # Возврат графика
    # return FileResponse(plot_filename, media_type='image/jpeg', filename=plot_filename)


@app.get("/kpi")
async def get_kpi(inn: str = Query(...)):
    global df
    # Фильтрация данных по ИНН, где ИНН является победителем
    filtered_data = df[df['ИНН победителя КС'].astype(str).str.contains(inn)]
    print(filtered_data)

    # Проверка, есть ли данные для построения графика
    if filtered_data.empty:
        return {"message": "Нет данных для указанного ИНН."}

    # Создание графика
    points = filtered_data[
        ['Окончание КС', 'Начальная цена КС', 'Конечная цена КС (победителя в КС)']].drop_duplicates()
    points = points.rename(columns={'Окончание КС': 'ks_date', 'Начальная цена КС': 'ks_start_price',
                                    'Конечная цена КС (победителя в КС)': 'ks_end_price'})

    # Преобразование колонки даты в формат datetime и обрезка времени
    points['ks_date'] = pd.to_datetime(points['ks_date']).dt.date
    # # Группировка по дате и суммирование цен
    points = points.groupby('ks_date', as_index=False).sum()

    # Сортировка по возрастанию даты
    points = points.sort_values(by='ks_date').astype(str)

    # Преобразование в список словарей
    result_list = points.to_dict(orient='records')
    return JSONResponse(content=result_list)
    # return JSONResponse(content=result_list)

    #
    # # Сохранение графика в формате JPG
    # plot_filename = 'plot.jpg'
    # plt.savefig(plot_filename)
    # plt.close()  # Закрываем график, чтобы освободить память
    #
    # # Возврат графика
    # return FileResponse(plot_filename, media_type='image/jpeg', filename=plot_filename)
