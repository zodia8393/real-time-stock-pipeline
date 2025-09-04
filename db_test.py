import psycopg2

conn = psycopg2.connect(
    host='localhost',
    database='stock_data',
    user='admin',
    password='password123',
    port=5432
)
cur = conn.cursor()
cur.execute('SELECT * FROM stock_prices ORDER BY timestamp DESC LIMIT 10;')
for row in cur.fetchall():
    print(row)
cur.close()
conn.close()
