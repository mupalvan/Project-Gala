# Read Data on DataBase Server and wirte to Site DataBase (if not exists or change Data)
import pyodbc
import sqlite3

# اتصال به SQL Server
sql_conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-RFH2G51;'
    'DATABASE=KarbinoEMP_Ehsan_1404;'
    'Trusted_Connection=yes;'
)
sql_cursor = sql_conn.cursor()

# اتصال به SQLite
sqlite_conn = sqlite3.connect(r'C:\Users\SiSo\Documents\Projects\Project Gala\ehsanDBproduct.db')
sqlite_cursor = sqlite_conn.cursor()

# === مرحله 1: گرفتن قیمت‌ها از Kalas که SitePrice != 0
sql_cursor.execute("SELECT Code_Kala, SitePrice FROM Kalas WHERE SitePrice != 0")
price_data = {str(row.Code_Kala): row.SitePrice for row in sql_cursor.fetchall()}

# === مرحله 2: گرفتن موجودی از GardeshKala1
sql_cursor.execute("""
    SELECT CodeKala, 
           SUM(ISNULL(TededVorodi, 0)) - SUM(ISNULL(TedadOut, 0)) AS Mojoodi
    FROM GardeshKala1
    GROUP BY CodeKala
""")
stock_data = {str(row.CodeKala): int(row.Mojoodi or 0) for row in sql_cursor.fetchall()}

# === مرحله 3: بررسی و آپدیت در SQLite
# گرفتن همه products از SQLite برای مقایسه
sqlite_cursor.execute("SELECT id, price, stock_quantity FROM products")
products = sqlite_cursor.fetchall()

update_count = 0

for product_id, current_price, current_stock in products:
    # داده‌های جدید قیمت و موجودی (اگر موجود نبود فرض 0)
    new_price = price_data.get(str(product_id), current_price)
    new_stock = stock_data.get(str(product_id), current_stock)

    # فقط اگر حداقل یکی تغییر کرده باشه آپدیت کن
    if new_price != current_price or new_stock != current_stock:
        sqlite_cursor.execute("""
            UPDATE products
            SET price = ?, stock_quantity = ?
            WHERE id = ?
        """, (new_price, new_stock, product_id))
        update_count += 1
        print(f"Updated ID {product_id}: price {current_price} -> {new_price}, stock {current_stock} -> {new_stock}")

sqlite_conn.commit()
sqlite_conn.close()
sql_conn.close()

print(f"Total updated rows: {update_count}")
