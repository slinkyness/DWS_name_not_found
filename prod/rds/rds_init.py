import psycopg2

def handler(event, context):
    conn = psycopg2.connect(
        host="REDACTED_DB_HOST",
        port=5432,
        dbname="postgres",
        user="postgres",
        password="REDACTED_MASTER_PASS"
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE DATABASE REDACTED_DB_NAME;")
    print("Datenbank REDACTED_DB_NAME erstellt")
    conn.close()
    return {"status": "ok"}