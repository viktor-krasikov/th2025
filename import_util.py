import pandas as pd
import sqlite3

def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS firma (
            inn TEXT PRIMARY KEY,
            firma_name TEXT,
            firma_region TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ks (
            ks_id INTEGER PRIMARY KEY,
            ks_url TEXT,
            customer_inn TEXT,
            winner_inn TEXT,
            fz TEXT,
            start_time TEXT,
            end_time TEXT,
            start_price REAL,
            end_price REAL,
            kpgz_code TEXT,
            offer_start_date TEXT,
            offer_end_date TEXT,
            FOREIGN KEY (customer_inn) REFERENCES firma (inn),
            FOREIGN KEY (winner_inn) REFERENCES firma (inn)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS participant (
            inn TEXT,
            ks_id INTEGER,
            PRIMARY KEY (inn, ks_id),
            FOREIGN KEY (inn) REFERENCES firma (inn),
            FOREIGN KEY (ks_id) REFERENCES ks (ks_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS kpgz (
            kpgz_code TEXT PRIMARY KEY,
            kpgz_name TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sku (
            sku_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ks_id INTEGER,
            sku_link TEXT,
            sku_name TEXT,
            sku_count INTEGER,
            sku_start_price REAL,
            sku_offer_price REAL,
            FOREIGN KEY (ks_id) REFERENCES ks (ks_id)
        )
    ''')

    conn.commit()

def insert_data(conn, excel_file):
    df = pd.read_excel(excel_file)

    cursor = conn.cursor()

    for index, row in df.iterrows():
        # Firma (Заказчик)
        customer_inn = str(row['ИНН заказчика'])
        customer_name = row['Наименование заказчика']
        customer_region = row['Регион заказчика']

        try:
            cursor.execute('''
                INSERT INTO firma (inn, firma_name, firma_region)
                VALUES (?, ?, ?)
            ''', (customer_inn, customer_name, customer_region))
        except sqlite3.IntegrityError:
            pass  # Игнорировать дубликаты

        # Firma (Победитель)
        winner_inn = str(row['ИНН победителя КС'])
        winner_name = row['Наименование победителя КС']
        winner_region = row['Регион победителя КС']
        try:
            cursor.execute('''
                INSERT INTO firma (inn, firma_name, firma_region)
                VALUES (?, ?, ?)
            ''', (winner_inn, winner_name, winner_region))
        except sqlite3.IntegrityError:
            pass

        # ks
        ks_id = int(row['Id КС'])
        ks_url = row['Ссылка на КС']
        fz = row['Закон-основание']
        start_time = str(row['Начало КС'])
        end_time = str(row['Окончание КС'])
        start_price = float(row['Начальная цена КС']) # .replace(' ', '').replace(',', '.'))
        end_price = float(row['Конечная цена КС (победителя в КС)']) # .replace(' ', '').replace(',', '.'))
        kpgz_code = str(row['Код КПГЗ'])
        offer_start_date = str(row['Начало действия оферты'])
        offer_end_date = str(row['Окончание действия оферты'])
        try:
            cursor.execute('''
                INSERT INTO ks (ks_id, ks_url, customer_inn, winner_inn, fz, start_time, end_time, start_price, end_price, kpgz_code, offer_start_date, offer_end_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (ks_id, ks_url, customer_inn, winner_inn, fz, start_time, end_time, start_price, end_price, kpgz_code, offer_start_date, offer_end_date))
        except sqlite3.IntegrityError:
            pass

        # Participant
        participants = str(row['Участники КС - поставщики']).split('; ')
        for participant in participants:
            parts = participant.split('  ')  # Разделяем по двум пробелам
            if len(parts) >= 3:
                inn_participant = parts[0].replace('ИНН:', '').strip()  # Извлекаем ИНН
                firma_name = parts[1].strip()  # Извлекаем название фирмы
                firma_region = parts[2].strip() if len(parts) >= 3 else None  # Извлекаем регион, если он есть

                # Вставляем фирму участника
                try:
                    cursor.execute('''
                        INSERT INTO firma (inn, firma_name, firma_region)
                        VALUES (?, ?, ?)
                    ''', (inn_participant, firma_name, firma_region))
                except sqlite3.IntegrityError:
                    pass  # Игнорировать дубликаты

                # Вставляем участника
                try:
                    cursor.execute('''
                        INSERT INTO participant (inn, ks_id)
                        VALUES (?, ?)
                    ''', (inn_participant, ks_id))
                except sqlite3.IntegrityError:
                    pass

        # kpgz
        kpgz_name = row['Наименование КПГЗ']
        try:
            cursor.execute('''
                INSERT INTO kpgz (kpgz_code, kpgz_name)
                VALUES (?, ?)
            ''', (kpgz_code, kpgz_name))
        except sqlite3.IntegrityError:
            pass

        # sku
        sku_link = row['Ссылка на СТЕ']
        sku_name = row['Наименование СТЕ']
        sku_count = int(row['Количество СТЕ'])
        sku_start_price = float(str(row['Стоимость за единицу СТЕ']).replace(' ', '').replace(',', '.'))
        sku_offer_price = float(str(row['Цена оферты за единицу']).replace(' ', '').replace(',', '.'))
        try:
            cursor.execute('''
                INSERT INTO sku (ks_id, sku_link, sku_name, sku_count, sku_start_price, sku_offer_price)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (ks_id, sku_link, sku_name, sku_count, sku_start_price, sku_offer_price))
        except sqlite3.IntegrityError:
            pass

    conn.commit()

@app.get("/competitors")
async def get_competitors(inn: str = Query(...)):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Запрос для получения списка конкурентов
    cursor.execute('''
        SELECT 
            p2.inn AS competitor_inn,
            f2.firma_name AS competitor_name,
            f2.firma_region AS competitor_region,
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
        WHERE 
            p1.inn = ?
        GROUP BY 
            p2.inn, f2.firma_name, f2.firma_region
    ''', (inn,))

    results = cursor.fetchall()

    # Обработка результатов
    competitors = []
    for row in results:
        total_contracts = row['total_contracts']
        competitor_wins = row['competitor_wins']
        supplier_wins = row['supplier_wins']

        competitor_percentage = (competitor_wins / total_contracts * 100) if total_contracts > 0 else 0
        supplier_percentage = (supplier_wins / total_contracts * 100) if total_contracts > 0 else 0

        competitors.append({
            "competitor_inn": row['competitor_inn'],
            "competitor_name": row['competitor_name'],
            "competitor_region": row['competitor_region'],
            "total_contracts": total_contracts,
            "competitor_wins": competitor_wins,
            "competitor_win_percentage": round(competitor_percentage, 2),
            "supplier_wins": supplier_wins,
            "supplier_win_percentage": round(supplier_percentage, 2)
        })

    return JSONResponse(content={"competitors": competitors})

if __name__ == '__main__':
    db_file = 'Tender7.db'
    excel_file = r'.\data\TenderHack_20250228_1900.xlsx'

    conn = sqlite3.connect(db_file)
    create_tables(conn)
    insert_data(conn, excel_file)
    conn.close()
    print("Data transferred successfully!")