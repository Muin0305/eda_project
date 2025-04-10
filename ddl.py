import json
import pandas as pd
import duckdb
import os

DB_FILE = 'my.db'
EXCEL_FILE = 'source/california_mall.xlsx'  # Проверь, что файл существует

# Проверка наличия Excel-файла
if not os.path.exists(EXCEL_FILE):
    raise FileNotFoundError(f"Файл {EXCEL_FILE} не найден!")

# Загружаем словарь с таблицами
with open('tables.json') as f:
    tables_dict = json.load(f)

def create_tables():
    """Создаёт таблицы из SQL-скрипта без foreign key."""
    try:
        tables_query = """
        CREATE TABLE IF NOT EXISTS customer_data (
            customer_key VARCHAR(50) PRIMARY KEY,
            gender VARCHAR(20) NOT NULL,
            customer_age INTEGER NOT NULL,
            payment_type VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS shopping_mall_data (
            mall_name VARCHAR(100) PRIMARY KEY,
            built_year INTEGER,
            total_area_sqm VARCHAR(50),
            city_location VARCHAR(50),
            total_stores INTEGER
        );

        CREATE TABLE IF NOT EXISTS sales_data (
            invoice_number VARCHAR(50) PRIMARY KEY,
            customer_key VARCHAR(50),
            product_category VARCHAR(50),
            quantity_sold INTEGER,
            invoice_date DATE,
            total_price DECIMAL(10, 2),
            mall_name VARCHAR(100)
        );
        """

        with duckdb.connect(DB_FILE) as duck:
            duck.execute(tables_query)

        print("✅ Таблицы успешно созданы")
    except Exception as e:
        print("❌ Ошибка при создании таблиц:", e)

def read_xl(sheet_name, columns_dict):
    """Читает лист из Excel и переименовывает колонки."""
    try:
        temp_df = pd.read_excel(
            EXCEL_FILE,
            sheet_name=sheet_name,
            usecols=columns_dict.keys()
        ).rename(columns=columns_dict)

        if temp_df.empty:
            print(f"⚠️ Внимание! Лист {sheet_name} пуст или содержит некорректные данные.")
            return temp_df

        # ✅ Обработка дат в sales_data
        if sheet_name == 'sales_data' and 'invoice_date' in temp_df.columns:
            try:
                temp_df['invoice_date'] = pd.to_datetime(
                    temp_df['invoice_date'],
                    format="%m/%d/%Y",
                    errors='coerce'
                )
                before = len(temp_df)
                temp_df = temp_df.dropna(subset=['invoice_date'])
                removed = before - len(temp_df)
                if removed > 0:
                    print(f"🧹 Удалено {removed} строк с некорректным invoice_date.")
            except Exception as date_err:
                print(f"⚠️ Ошибка при обработке дат: {date_err}")

        # ✅ Удаление строк с отсутствующим возрастом в customer_data
        if sheet_name == 'customer_data' and 'customer_age' in temp_df.columns:
            original_len = len(temp_df)
            temp_df = temp_df.dropna(subset=['customer_age'])
            removed = original_len - len(temp_df)
            if removed > 0:
                print(f"🧹 Удалено {removed} строк с пустым customer_age.")

        return temp_df

    except Exception as e:
        print(f"❌ Ошибка при чтении Excel-листа {sheet_name}: {e}")
        return None

def insert_to_db(temp_df, tbl_name):
    """Вставляет данные в таблицу DuckDB."""
    if temp_df is None or temp_df.empty:
        print(f"⚠️ Данные для {tbl_name} отсутствуют, пропускаем вставку.")
        return

    try:
        with duckdb.connect(DB_FILE) as duck:
            duck.register('temp_df', temp_df)
            duck.execute(f"""
                INSERT INTO {tbl_name}
                SELECT * FROM temp_df
            """)
            duck.unregister('temp_df')
        print(f"✅ Данные успешно загружены в {tbl_name}")
    except Exception as e:
        print(f"❌ Ошибка при вставке данных в {tbl_name}: {e}")

def xl_etl(sheet_name, columns_dict, tbl_name):
    """Загружает данные из Excel в таблицу."""
    print(f"📥 Загружаем данные в {tbl_name}...")
    temp_df = read_xl(sheet_name, columns_dict)
    insert_to_db(temp_df, tbl_name)

def create_n_insert():
    """Главная функция для создания таблиц и загрузки данных."""
    try:
        print('🔎 Проверяем наличие таблицы sales_data...')
        with duckdb.connect(DB_FILE) as duck:
            duck.execute("SELECT 1 FROM sales_data").fetchone()
        print("✅ Таблицы уже существуют, пропускаем создание.")
    except:
        print("⚠️ Таблицы не найдены, создаём заново...")
        create_tables()

        for sheet_name, table_info in tables_dict.items():
            xl_etl(sheet_name, table_info["columns"], table_info["table_name"])

        print("✅ Все данные успешно загружены!")

create_n_insert()