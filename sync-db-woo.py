import csv
import datetime
import time
import sqlite3
from woocommerce import API
import concurrent.futures

# تنظیمات اتصال به ووکامرس
wcapi = API(
    url="https://ehsanstore.ir/",
    consumer_key="ck_000fbb9f08fcd924776db7af5e040d179b044f75",
    consumer_secret="cs_61ec024891b4b178006ffcf3199e3ab0178b817b",
    version="wc/v3",
    timeout=120,
    verify_ssl=True,
    headers={"User-Agent": "Mozilla/5.0 (compatible; SyncScript/1.0)"}
)
path = r'C:\Users\SiSo\Documents\Projects\Project Gala'
SQLITE_PATH = path + r'\ehsanDBproduct.db'
LOG_CSV = path + r"\log\update_log.csv"

def load_db_data(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    cursor.execute("SELECT id, price, stock_quantity FROM products")
    data = {}
    for id_, price, stock in cursor.fetchall():
        data[str(id_)] = (price, stock)
    conn.close()
    return data

def write_log_row(row):
    header = ["SKU", "Old Price", "New Price", "Old Stock", "New Stock", "Timestamp", "Status", "Message"]
    write_header = False
    try:
        with open(LOG_CSV, "r", newline='', encoding='utf-8') as f:
            pass
    except FileNotFoundError:
        write_header = True

    with open(LOG_CSV, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        writer.writerow(row)

def update_variant(variant_id, parent_id, db_data):
    try:
        # دریافت اطلاعات واریانت
        response = wcapi.get(f"products/{parent_id}/variations/{variant_id}")
        if response.status_code != 200:
            write_log_row([variant_id, "", "", "", "", datetime.datetime.now(), "Fail", f"HTTP {response.status_code}"])
            return
        variant = response.json()

        sku = variant.get("sku")
        if not sku:
            write_log_row([variant_id, "", "", "", "", datetime.datetime.now(), "Fail", "No SKU"])
            return

        wc_price = float(variant.get("price") or 0)
        wc_stock = int(variant.get("stock_quantity") or 0)

        if sku not in db_data:
            write_log_row([sku, wc_price, "", wc_stock, "", datetime.datetime.now(), "Skip", "SKU not in DB"])
            return

        db_price_raw, db_stock = db_data[sku]
        db_price = db_price_raw / 10  # تبدیل ریال به تومان

        if db_price != wc_price or db_stock != wc_stock:
            payload = {
                "regular_price": str(int(db_price)),
                "stock_quantity": db_stock,
                "manage_stock": True
            }

            for attempt in range(3):
                try:
                    update_resp = wcapi.put(f"products/{parent_id}/variations/{variant_id}", payload)
                    if update_resp.status_code in (200, 201):
                        write_log_row([sku, wc_price, db_price, wc_stock, db_stock, datetime.datetime.now(), "Updated", "Success"])
                        break
                    else:
                        raise Exception(f"HTTP {update_resp.status_code} {update_resp.text}")
                except Exception as e:
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        write_log_row([sku, wc_price, db_price, wc_stock, db_stock, datetime.datetime.now(), "Fail", str(e)])
        else:
            write_log_row([sku, wc_price, db_price, wc_stock, db_stock, datetime.datetime.now(), "No Change", ""])

    except Exception as e:
        write_log_row([variant_id, "", "", "", "", datetime.datetime.now(), "Fail", str(e)])

def main():
    with open(LOG_CSV, "w", newline='', encoding='utf-8') as f:
        pass  
    db_data = load_db_data(SQLITE_PATH)

    products = []
    page = 1
    per_page = 100
    while True:
        resp = wcapi.get("products", params={"type": "variable", "per_page": per_page, "page": page})
        if resp.status_code != 200:
            print(f"Error getting products page {page}: {resp.text}")
            break
        data = resp.json()
        if not data:
            break
        products.extend(data)
        page += 1

    variant_tasks = []
    for product in products:
        parent_id = product.get("id")
        variations = product.get("variations", [])
        for variant_id in variations:
            variant_tasks.append((variant_id, parent_id, db_data))

    max_workers = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda args: update_variant(*args), variant_tasks)

if __name__ == "__main__":
    main()
