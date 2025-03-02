import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd

app = FastAPI()


# Модель для фирмы
class Firma(BaseModel):
    inn: str
    name: str
    region: Optional[str]


# Модель для котировочной сессии
class Session(BaseModel):
    id_ks: int
    ks_url: str
    customer_inn: str
    customer_name: str
    customer_region: Optional[str]
    fz: str
    start_time: str
    end_time: str
    start_price: float
    end_price: float
    winner_inn: str
    kpgz_code: str
    kpgz_name: str
    discount_size: float  # Размер уступки


conn2 = sqlite3.connect('tender.db', check_same_thread=False)


# Функция для получения соединения с базой данных
def get_db_connection():
    conn = sqlite3.connect('Tender7.db')
    conn.row_factory = sqlite3.Row  # Позволяет обращаться к столбцам по имени
    return conn


# Эндпоинт для получения списка фирм
@app.get("/inns", response_model=List[Firma])
def get_firms():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT inn, firma_name AS name, firma_region AS region FROM firma")
    firms = cursor.fetchall()
    conn.close()
    return [Firma(inn=row['inn'], name=row['name'], region=row['region']) for row in firms]


# Эндпоинт для получения списка котировочных сессий
@app.get("/sessions")
def get_sessions():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ks.ks_id AS id_ks, ks.ks_url, ks.customer_inn, f1.firma_name AS customer_name, 
               f1.firma_region AS customer_region, ks.fz, ks.start_time, ks.end_time, 
               ks.start_price, ks.end_price, ks.winner_inn, kpgz.kpgz_code, kpgz.kpgz_name,
               ks.end_price - ks.start_price as discount_size
        FROM ks
        LEFT JOIN firma AS f1 ON ks.customer_inn = f1.inn
        LEFT JOIN firma AS f2 ON ks.winner_inn = f2.inn
        LEFT JOIN kpgz ON ks.kpgz_code = kpgz.kpgz_code
    ''')
    sessions = cursor.fetchall()
    conn.close()
    return JSONResponse([{
        'Id КС': row['id_ks'],
        'Ссылка на КС': row['ks_url'],
        'ИНН заказчика': row['customer_inn'],
        'Наименование заказчика': row['customer_name'],
        'Регион заказчика': row['customer_region'],
        'Закон-основание': row['fz'],
        'Начало КС': row['start_time'],
        'Окончание КС': row['end_time'],
        'Начальная цена КС': row['start_price'],
        'Конечная цена КС (победителя в КС)': row['end_price'],
        'ИНН победителя КС': row['winner_inn'],
        'Код КПГЗ': row['kpgz_code'],
        'Наименование КПГЗ': row['kpgz_name'],
        'Размер уступки': row['discount_size']
    } for row in sessions])


@app.get("/wins_dots")
async def get_wins_dots(inn: str = Query(...)):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Фильтрация данных по ИНН, где ИНН является победителем
    cursor.execute('''
        SELECT ks.end_time AS ks_date, ks.start_price AS ks_start_price, ks.end_price AS ks_end_price
        FROM ks
        WHERE ks.winner_inn = ?
    ''', (inn,))
    filtered_data = cursor.fetchall()

    # Проверка, есть ли данные для построения графика
    if not filtered_data:
        return {"message": "Нет данных для указанного ИНН."}

    # Получение текущей даты и даты два года назад
    current_date = datetime.now()
    two_years_ago = current_date - timedelta(days=730)  # 2 года = 730 дней

    # Фильтрация данных за последние 2 года
    filtered_data = [row for row in filtered_data if
                     datetime.strptime(row['ks_date'][:19], '%Y-%m-%d %H:%M:%S') >= two_years_ago]

    # Создание графика
    points = []
    for row in filtered_data:
        points.append({
            'ks_date': row['ks_date'],
            'ks_start_price': round(row['ks_start_price'], 2),  # Округление до сотых
            'ks_end_price': round(row['ks_end_price'], 2)  # Округление до сотых
        })

    # Преобразование в DataFrame для дальнейших вычислений
    if not points:
        return {"message": "Нет данных за последние 2 года."}

    # Вычисление суммы уступки и процента уступки
    for point in points:
        point['summa_ustupki'] = round(point['ks_start_price'] - point['ks_end_price'], 2)

    # Группировка по дате и суммирование цен
    grouped_points = {}
    for point in points:
        date = point['ks_date'].split(' ')[0]  # Берем только дату
        if date not in grouped_points:
            grouped_points[date] = {
                'ks_start_price': 0,
                'ks_end_price': 0,
                'summa_ustupki': 0,
            }
        grouped_points[date]['ks_start_price'] += point['ks_start_price']
        grouped_points[date]['ks_end_price'] += point['ks_end_price']
        grouped_points[date]['summa_ustupki'] += point['summa_ustupki']

    # Подсчет общей суммы уступок и среднего процента уступки
    total_discount_sum = round(sum(item['summa_ustupki'] for item in grouped_points.values()), 2)
    average_discount_percentage = round(
        (total_discount_sum / sum(item['ks_start_price'] for item in grouped_points.values())) * 100, 2) if sum(
        item['ks_start_price'] for item in grouped_points.values()) != 0 else 0

    # Преобразование в список словарей без лишних полей
    result_list = [{'ks_date': date,
                    'ks_start_price': round(values['ks_start_price'], 2),
                    'ks_end_price': round(values['ks_end_price'], 2)}
                   for date, values in grouped_points.items()]

    summary = {
        "total_discount_sum": total_discount_sum,
        "average_discount_percentage": average_discount_percentage
    }

    return JSONResponse(content={"summary": summary, "data": result_list})


@app.get("/contracts_by_years")
async def get_contracts_by_years(inn: str = Query(...)):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Запрос для получения количества контрактов по годам
    cursor.execute('''
        SELECT strftime('%Y', ks.start_time) AS year, 
               COUNT(DISTINCT p.ks_id) AS parts_count, 
               SUM(CASE WHEN ks.winner_inn = ? THEN 1 ELSE 0 END) AS wins_count
        FROM ks
        JOIN participant AS p ON ks.ks_id = p.ks_id
        WHERE p.inn = ?
        GROUP BY year
    ''', (inn, inn))

    results = cursor.fetchall()

    # Словарь для хранения количества контрактов по годам
    contracts_by_year = {row['year']: {
        'year': row['year'],
        'parts_count': row['parts_count'],
        'wins_count': row['wins_count']
    } for row in results}

    # Подсчет общего числа выигранных контрактов
    all_wins_count = sum(row['wins_count'] for row in contracts_by_year.values())
    all_parts_count = sum(row['parts_count'] for row in contracts_by_year.values())

    # Вычисление процента выигранных контрактов
    all_wins_percentage = (all_wins_count / all_parts_count * 100) if all_parts_count > 0 else 0

    # Преобразование в список словарей
    result_list = list(contracts_by_year.values())

    summary = {
        "all_wins_count": all_wins_count,
        "all_wins_percentage": round(all_wins_percentage, 2)  # Округление до двух знаков
    }

    return JSONResponse(content={"summary": summary, "data": result_list})


@app.get('/customers')
def get_start_customers():
    UNIC_CITY = pd.read_sql_query(
        '''SELECT data."Регион победителя КС"  FROM data GROUP BY data."Регион победителя КС"''', conn2)

    UNIC_KPGZ = pd.read_sql_query('''SELECT data."Код КПГЗ"  FROM data GROUP BY data."Код КПГЗ"''', conn2)

    UNIC_CUSTOMERS = pd.read_sql_query(
        '''SELECT data."ИНН заказчика", data."Наименование заказчика"  FROM data GROUP BY data."ИНН заказчика"''',
        conn2)

    kpgz_code_UPD = 'null'
    winner_region_UPD = 'null'
    start_date_UPD = 'null'
    end_date_UPD = 'null'
    inn_UPD = 'null'

    first100 = pd.read_sql_query(f'''SELECT data."Id КС", data.*  FROM data WHERE 
    (data."Код КПГЗ" = {kpgz_code_UPD} OR {kpgz_code_UPD} IS NULL) AND
    (data."Регион победителя КС" = {winner_region_UPD} OR {winner_region_UPD} IS NULL) AND
    (data."Окончание КС" BETWEEN {start_date_UPD} AND {end_date_UPD} OR ({start_date_UPD} IS NULL AND {end_date_UPD} IS NULL)) AND
    (data."ИНН победителя КС" = {inn_UPD} OR {inn_UPD} IS NULL) GROUP BY data."Id КС" lIMIT 100''', conn2)

    return {"city": UNIC_KPGZ.to_dict(orient='records'),
            "kpgz:": UNIC_CITY.to_dict(orient='records'),
            "customers": UNIC_CUSTOMERS.to_dict(orient='records'),
            "first100": first100.to_dict(orient='records')}


@app.get("/customers/config/")
def get_tenders(request: Request,
                kpgz_code: str = Query(None),
                winner_region: str = Query(None),
                start_date: str = Query(None),
                end_date: str = Query(None),
                inn: str = Query(None),
                do: str = Query(None),
                min_price: str = Query(None),
                max_price: str = Query(None),
                customers: str = Query(None),
                win: str = Query(None)
                ):
    # Подключаемся к базе данных
    conn = sqlite3.connect('tender.db')
    kpgz_code_UPD = '',
    winner_region_UPD = ''
    start_date_UPD = ''
    end_date_UPD = ''
    inn_UPD = ''
    min_price_upd = ''
    max_price_upd = ''
    inn_variant_UPD = ''
    do_upd = ''
    concatQUERY = ''
    customers_upd = ''
    win_upd = ''

    kpgz_code_UPD = 'null' if kpgz_code == None else f'\'{kpgz_code}%\''
    winner_region_UPD = 'null' if winner_region == None else f'\'{winner_region}\''
    start_date_UPD = 'null' if start_date == None else f'\'{start_date}\''
    end_date_UPD = 'null' if end_date == None else f'\'{end_date}\''
    min_price_upd = 'null' if min_price == None else f'\'{min_price}\''
    max_price_upd = 'null' if max_price == None else f'\'{max_price}\''
    customers_upd = 'null' if customers == None else f'\'{customers}\''
    win_upd = 'null' if win == None else f'\'{inn}\''

    do_upd = 'null' if do == None else f'\'{do}\''

    query = f'''SELECT 
        DISTINCT data."Id КС", 
    CAST(TRIM(REPLACE(REPLACE(trim(data."Конечная цена КС (победителя в КС)"), '\t', ''), CHAR(160), '')) AS DECIMAL(10, 2)) AS "Конечная цена КС (победителя в КС)",
    CAST(TRIM(REPLACE(REPLACE(trim(data."Начальная цена КС"), '\t' , ''), CHAR(160), '')) AS DECIMAL(10, 2)) AS "Начальная цена КС",

    data."Ссылка на КС", data."ИНН заказчика", data."Наименование заказчика", data."Регион заказчика", 
    data."Начало КС", data."Окончание КС", data."ИНН победителя КС", data."Наименование победителя КС", data."Регион победителя КС",
    data."Участники КС - поставщики", data."Код КПГЗ", data."Наименование КПГЗ", 
        CASE 
           WHEN "Участники КС - поставщики" LIKE '%{inn}%' THEN 'true'
           ELSE 'false'
        END AS "do" 
        FROM data 
            WHERE 
                (data."Код КПГЗ" LIKE {kpgz_code_UPD} OR {kpgz_code_UPD} IS NULL) AND
                (data."Регион победителя КС" = {winner_region_UPD} OR {winner_region_UPD} IS NULL) AND
                (data."Окончание КС" BETWEEN {start_date_UPD} AND {end_date_UPD} OR ({start_date_UPD} IS NULL AND {end_date_UPD} IS NULL)) AND 
                (data."Регион победителя КС" = {winner_region_UPD} OR {winner_region_UPD} IS NULL) AND
                (data."ИНН победителя КС" = {win_upd} OR {win_upd} IS NULL) AND
                (CAST(TRIM(REPLACE(REPLACE(data."Конечная цена КС (победителя в КС)", CHAR(9), ''), CHAR(160), '')) AS FLOAT) > {min_price_upd} OR {min_price_upd} IS NULL) AND 
                (CAST(TRIM(REPLACE(REPLACE(data."Конечная цена КС (победителя в КС)", CHAR(9), ''), CHAR(160), '')) AS FLOAT) < {max_price_upd} OR {max_price_upd} IS NULL) AND
                (data."ИНН заказчика" = {customers} OR {customers} IS NULL) AND

                (do = {do_upd} OR {do_upd} IS NULL) 


        GROUP BY data."Id КС"'''
    print(query)

    params = [kpgz_code_UPD, winner_region_UPD, start_date_UPD, end_date_UPD, inn_UPD]
    print(params)
    df = pd.read_sql_query(query, conn)

    print(df.columns)

    df['diff'] = df['Конечная цена КС (победителя в КС)'] / df['Начальная цена КС'] * 100 - 100
    df['sum_ystupki'] = df['Конечная цена КС (победителя в КС)'] - df['Начальная цена КС']
    discount = abs(int(df['sum_ystupki'].sum()))

    print(discount)
    total_sum = df['diff'].sum() / df['Id КС'].count()
    total_sum_rounded = round(total_sum, 2)
    print(total_sum)
    conn.close()

    return {"KPI_DIFF_PROC": total_sum_rounded,
            "discount": discount,
            "data": df.to_dict(orient='records')}


@app.get("/competitors")
async def get_competitors(inn: str = Query(...)):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Запрос для получения списка конкурентов
    cursor.execute('''
        SELECT competitor_inn,
            competitor_name,
            competitor_region,
            total_contracts,
            competitor_wins,
            supplier_wins,
            total_contracts - (competitor_wins + supplier_wins) AS other_wins,
            GROUP_CONCAT(DISTINCT kpgz_info) AS kpgz_info
    FROM (
		SELECT 
            p2.inn AS competitor_inn,
            f2.firma_name AS competitor_name,
            f2.firma_region AS competitor_region,
            kpgz.kpgz_code || ' ' || kpgz.kpgz_name AS kpgz_info,
            COUNT(DISTINCT ks.ks_id) AS total_contracts,
            SUM(CASE WHEN p2.inn = ks.winner_inn THEN 1 ELSE 0 END) AS competitor_wins,
            SUM(CASE WHEN p1.inn = ks.winner_inn THEN 1 ELSE 0 END) AS supplier_wins
        FROM 
            participant p1
        JOIN 
            participant p2 ON p1.ks_id = p2.ks_id AND p1.inn != p2.inn
        JOIN 
            ks ON p1.ks_id = ks.ks_id
        JOIN 
            firma f2 ON p2.inn = f2.inn
        JOIN 
            kpgz ON ks.kpgz_code = kpgz.kpgz_code
        WHERE 
            p1.inn = ?
        GROUP BY 
            p2.inn, f2.firma_name, f2.firma_region
        ORDER BY competitor_wins DESC, total_contracts DESC
        LIMIT 100
) AS X
GROUP BY 1, 2, 3
ORDER BY competitor_wins DESC, total_contracts DESC
    ''', (inn,))

    results = cursor.fetchall()

    # Обработка результатов
    competitors = []
    total_kpi = 0  # Сумма KPI по всем конкурентам
    total_contracts_all = 0  # Общее количество контрактов по всем конкурентам

    for row in results:
        total_contracts = row['total_contracts']
        competitor_wins = row['competitor_wins']
        supplier_wins = row['supplier_wins']
        other_wins = row['other_wins']

        competitor_percentage = (competitor_wins / total_contracts * 100) if total_contracts > 0 else 0
        supplier_percentage = (supplier_wins / total_contracts * 100) if total_contracts > 0 else 0
        other_percentage = (other_wins / total_contracts * 100) if total_contracts > 0 else 0

        # Подсчет KPI
        total_kpi += supplier_percentage * total_contracts
        total_contracts_all += total_contracts

        competitors.append({
            "competitor_inn": row['competitor_inn'],
            "competitor_name": row['competitor_name'],
            "competitor_region": row['competitor_region'],
            "kpgz_info": row['kpgz_info'],
            "total_contracts": total_contracts,
            "competitor_wins": competitor_wins,
            "competitor_win_percentage": round(competitor_percentage, 2),
            "supplier_wins": supplier_wins,
            "supplier_win_percentage": round(supplier_percentage, 2),
            "other_wins": other_wins,
            "other_win_percentage": round(other_percentage, 2)
        })
    average_kpi = (total_kpi / total_contracts_all) if total_contracts_all > 0 else 0
    return JSONResponse(content={"kpi": round(average_kpi, 2), "competitors": competitors})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=5009)
