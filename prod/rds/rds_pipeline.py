import os
import boto3
import io
import json
import pg8000
import polars as pl
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timezone

from lambda_utils import get_secret

SECRET_NAME = os.environ["SECRET_NAME"]
REGION      = os.environ["AWS_REGION_NAME"]
S3_BUCKET   = os.environ["S3_BUCKET"]
DB_HOST = get_secret(SECRET_NAME, REGION, "host")
DB_NAME = get_secret(SECRET_NAME, REGION, "dbInstanceIdentifier")
DB_USER = get_secret(SECRET_NAME, REGION, "username")
DB_PASS = get_secret(SECRET_NAME, REGION, "password")


def get_conn():
    conn = pg8000.connect(
        host=DB_HOST, database=DB_NAME,
        user=DB_USER, password=DB_PASS, port=5432
    )
    conn.autocommit = True
    return conn


def read_parquet(bucket, key, columns=None):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    if columns:
        return pl.read_parquet(buf, columns=columns)
    return pl.read_parquet(buf)


def clean_val(v):
    if v is None: return None
    if isinstance(v, float) and v != v: return None
    if isinstance(v, (list, dict)): return json.dumps(v)
    return v


def load_table(df, table, conn):
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {table}")
    cols = df.columns
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
    rows = [tuple(clean_val(v) for v in row) for row in df.iter_rows()]
    for i in range(0, len(rows), 200):
        cur.executemany(sql, rows[i:i + 200])
    print(f"Loaded {len(rows)} rows into {table}")


def update_fact_table(conn):
    cur = conn.cursor()

    # trial_count
    cur.execute("TRUNCATE TABLE staging_trial_tmp")
    cur.execute("""
                INSERT INTO staging_trial_tmp (disease_key, trial_count)
                SELECT d.disease_key, COUNT(DISTINCT t.trial_id)
                FROM dim_disease d
                         JOIN staging_trials t ON t.conditions ILIKE '%' || d.disease_name || '%'
                WHERE t.conditions IS NOT NULL
                GROUP BY d.disease_key
                """)
    cur.execute("""
                UPDATE fact_health_metrics f
                SET trial_count = s.trial_count FROM staging_trial_tmp s
                WHERE f.disease_key = s.disease_key
                """)
    print(f"trial_count updated: {cur.rowcount} rows")

    # media_mention_count
    cur.execute("""
                UPDATE fact_health_metrics f
                SET media_mention_count = sub.cnt FROM (
            SELECT d.disease_key, COUNT(a.url) AS cnt
            FROM dim_disease d
            JOIN staging_articles a
                ON a.title ILIKE '%' || d.disease_name || '%'
                OR a.description ILIKE '%' || d.disease_name || '%'
            GROUP BY d.disease_key
        ) sub
                WHERE f.disease_key = sub.disease_key
                """)
    print(f"media_mention_count updated: {cur.rowcount} rows")


def update_publication_count(conn):
    df = read_parquet(S3_BUCKET, "processed/publication_processed.parquet", columns=["work_id", "title"])
    titles = [str(t).lower() if t else "" for t in df["title"].to_list()]
    cur = conn.cursor()
    cur.execute("SELECT disease_key, disease_name FROM dim_disease")
    diseases = cur.fetchall()
    counts = {}
    for disease_key, disease_name in diseases:
        cnt = sum(1 for t in titles if disease_name.lower() in t)
        if cnt > 0:
            counts[disease_key] = cnt
    for disease_key, cnt in counts.items():
        cur.execute("UPDATE fact_health_metrics SET publication_count = %s WHERE disease_key = %s", (cnt, disease_key))
    print(f"publication_count updated: {len(counts)} diseases")


def update_sentiment_and_norms(conn):
    analyzer = SentimentIntensityAnalyzer()
    cur = conn.cursor()

    # Artikel holen
    cur.execute("SELECT url, title, description FROM staging_articles WHERE title IS NOT NULL")
    articles = cur.fetchall()

    # Krankheiten holen
    cur.execute("SELECT disease_key, disease_name FROM dim_disease")
    diseases = cur.fetchall()

    # Sentiment berechnen
    for disease_key, disease_name in diseases:
        name_lower = disease_name.lower()
        scores = []
        for url, title, desc in articles:
            text = (title or "") + " " + (desc or "")
            if name_lower in text.lower():
                scores.append(analyzer.polarity_scores(text)["compound"])
        if scores:
            avg = round(sum(scores) / len(scores), 4)
            cur.execute(
                "UPDATE fact_health_metrics SET media_sentiment_score = %s WHERE disease_key = %s",
                (avg, disease_key)
            )
    print(f"media_sentiment_score updated: {len([d for d in diseases if d[0]])} diseases processed")

    # norm_media_score (0-1)
    cur.execute("""
                UPDATE fact_health_metrics f
                SET norm_media_score = sub.norm FROM (
            SELECT disease_key,
                ROUND(media_mention_count::numeric / 
                    NULLIF(MAX(media_mention_count) OVER (), 0), 4) AS norm
            FROM fact_health_metrics
            WHERE media_mention_count IS NOT NULL
        ) sub
                WHERE f.disease_key = sub.disease_key
                """)
    print(f"norm_media_score updated: {cur.rowcount} rows")

    # norm_research_score (0-1)
    cur.execute("""
                UPDATE fact_health_metrics f
                SET norm_research_score = sub.norm FROM (
            SELECT disease_key,
                ROUND(publication_count::numeric / 
                    NULLIF(MAX(publication_count) OVER (), 0), 4) AS norm
            FROM fact_health_metrics
            WHERE publication_count IS NOT NULL
        ) sub
                WHERE f.disease_key = sub.disease_key
                """)
    print(f"norm_research_score updated: {cur.rowcount} rows")


def update_is_neglected(conn):
    cur = conn.cursor()
    cur.execute("UPDATE dim_disease SET is_neglected = FALSE")
    cur.execute("""
                UPDATE dim_disease d
                SET is_neglected = TRUE FROM v_bi5_neglected v
                WHERE d.disease_name = v.disease_name
                  AND v.attention_level = 'Low'
                  AND v.norm_daly >= 10
                """)
    print(f"is_neglected updated: {cur.rowcount} neglected diseases")


def lambda_handler(event, context):
    print("Starting rds_pipeline...", datetime.now(timezone.utc).isoformat())
    conn = get_conn()

    print("Loading staging_trials...")
    load_table(read_parquet(S3_BUCKET, "processed/trials_processed.parquet"), "staging_trials", conn)

    print("Loading staging_articles...")
    load_table(read_parquet(S3_BUCKET, "processed/articles_processed.parquet"), "staging_articles", conn)

    print("Updating fact_health_metrics...")
    update_fact_table(conn)
    update_publication_count(conn)
    update_sentiment_and_norms(conn)

    print("Updating is_neglected...")
    update_is_neglected(conn)

    conn.close()
    completed = datetime.now(timezone.utc).isoformat()
    print("rds_pipeline completed!", completed)
    return {"status": "ok", "completed_at": completed}
