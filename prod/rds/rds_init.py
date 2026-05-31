import os
import psycopg2

from lambda_utils import get_secret

SECRET_NAME = os.environ["SECRET_NAME"]
REGION      = os.environ["AWS_REGION_NAME"]
S3_BUCKET   = os.environ["S3_BUCKET"]
DB_HOST = get_secret(SECRET_NAME, REGION, "host")
DB_NAME = get_secret(SECRET_NAME, REGION, "dbInstanceIdentifier")
DB_USER = get_secret(SECRET_NAME, REGION, "adminuser")
DB_PASS = get_secret(SECRET_NAME, REGION, "adminpw")
DB_PORT = get_secret(SECRET_NAME, REGION, "port")

def handler(event, context):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname="postgres",
        user=DB_USER,
        password=DB_PASS
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE {DB_NAME};")
    print(f"Database {DB_NAME} created")
    conn.close()
    return {"status": "ok"}