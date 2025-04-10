import duckdb

DB_FILE = 'my.db'

def fetch_customers(edate):
    with open('queries/tables.sql') as f:
        custs_query = f.read().format(report_date = edate)

    with duckdb.connect(DB_FILE) as duck:
        custs_df = duck.query(custs_query).to_df()
        return custs_df