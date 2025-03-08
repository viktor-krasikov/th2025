import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# Настройка подключения к базе данных SQLite
DATABASE_PATH = "Tender7.db"  # Путь к вашей базе данных
conn = sqlite3.connect(DATABASE_PATH)

# Функция для выполнения SQL-запроса и получения данных
def get_data(query):
    return pd.read_sql_query(query, conn)

# Заголовок приложения
st.title("Дашборды на основе SQL-запросов")

# Дашборд 1: Все фирмы
st.header("Дашборд 1: Все фирмы")
query1 = "SELECT * FROM firma;"
data1 = get_data(query1)
st.write(data1)

# Дашборд 2: Закупки с информацией о заказчиках и победителях
st.header("Дашборд 2: Закупки с информацией о заказчиках и победителях")
query2 = '''
SELECT ks.ks_id, ks.ks_url, f1.firma_name AS customer_name, f1.firma_region AS customer_region,
       f2.firma_name AS winner_name, f2.firma_region AS winner_region, ks.start_time, ks.end_time,
       ks.start_price, ks.end_price
FROM ks
JOIN firma f1 ON ks.customer_inn = f1.inn
JOIN firma f2 ON ks.winner_inn = f2.inn;
'''
data2 = get_data(query2)
st.write(data2)

# Дашборд 3: Участники закупок
st.header("Дашборд 3: Участники закупок")
query3 = '''
SELECT p.inn, f.firma_name, ks.ks_id
FROM participant p
JOIN firma f ON p.inn = f.inn
JOIN ks ON p.ks_id = ks.ks_id;
'''
data3 = get_data(query3)
st.write(data3)

# Дашборд 4: Статистика по закупкам
st.header("Дашборд 4: Статистика по закупкам")
query4 = '''
SELECT COUNT(*) AS total_ks, AVG(end_price) AS average_end_price
FROM ks;
'''
data4 = get_data(query4)
st.write(data4)

# График: Средняя цена по закупкам
st.header("График: Средняя цена по закупкам")
avg_price_query = '''
SELECT AVG(end_price) AS average_end_price
FROM ks;
'''
avg_price_data = get_data(avg_price_query)
st.bar_chart(avg_price_data)

# График: Распределение победителей по регионам
st.header("График: Распределение победителей по регионам")
query_winner_region = '''
SELECT f.firma_region, COUNT(*) AS winner_count
FROM ks
JOIN firma f ON ks.winner_inn = f.inn
GROUP BY f.firma_region;
'''
winner_region_data = get_data(query_winner_region)

# Построение графика
plt.figure(figsize=(10, 5))
 plt.bar(winner_region_data['firma_region'][1:], winner_region_data['winner_count'][1:], color='skyblue')
plt.xlabel('Регион')
plt.ylabel('Количество победителей')
plt.title('Распределение победителей по регионам')
plt.xticks(rotation=45)
st.pyplot(plt)

# Закрытие соединения с базой данных
conn.close()

# Запуск приложения
if __name__ == "__main__":
    st.run()
