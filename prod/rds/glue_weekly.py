"""
glue_weekly.py
==================
Weekly AWS Glue ETL job — Health Data Warehouse population.

Schedule: weekly (e.g. every Sunday 02:00 UTC via EventBridge)

What this job does (in order):
  1. Fetch DB credentials from Secrets Manager
  2. Populate dim_region     ← ihme_metadata.parquet (location ids/names)
                               + gho_metadata.parquet (country_code lookup)
  3. Populate dim_disease    ← icd_10_to_11_flat.parquet
  4. Upsert fact_health_metrics ← health_data.parquet (DALY values)
                                   fan out across ALL dim_region rows
  5. Populate dim_trials     ← trials_processed.parquet
                               (explode countries_status, skip null country)

Required Glue job parameters (--JOB_NAME is injected automatically):
  --S3_BUCKET          e.g. REDACTED_S3_BUCKET
  --S3_FOLDER          e.g. processed
  --SECRET_ARN         REDACTED_SECRET_ARN
  --JDBC_URL           jdbc:postgresql://REDACTED_DB_HOST:5432/REDACTED_DB_NAME

IAM role attached to the job needs:
  - secretsmanager:GetSecretValue on the secret ARN
  - s3:GetObject / s3:ListBucket on S3_BUCKET
  - VPC access to the RDS instance (if in a VPC)
"""

import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, BooleanType, ArrayType
)

# ── Bootstrap ─────────────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BUCKET", "S3_FOLDER", "SECRET_ARN", "JDBC_URL"],
)

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET  = args["S3_BUCKET"]   # e.g. REDACTED_S3_BUCKET
S3_FOLDER  = args["S3_FOLDER"]   # e.g. processed
JDBC_URL   = args["JDBC_URL"]
SECRET_ARN = args["SECRET_ARN"]


# ── 0. Fetch DB credentials from Secrets Manager ──────────────────────────────
def get_db_secret(secret_arn: str) -> dict:
    """Return {"username": ..., "password": ...} from Secrets Manager."""
    client = boto3.client("secretsmanager", region_name="us-east-1")
    resp   = client.get_secret_value(SecretId=secret_arn)
    return json.loads(resp["SecretString"])


print("🔐 Fetching DB credentials from Secrets Manager …")
secret = get_db_secret(SECRET_ARN)
DB_USER = secret["username"]
DB_PASS = secret["password"]

JDBC_PROPS = {
    "user":     DB_USER,
    "password": DB_PASS,
    "driver":   "org.postgresql.Driver",
}

print(f"✅ Credentials loaded for user: {DB_USER}")


# ── Helper: read parquet from S3 ──────────────────────────────────────────────
def read_s3_parquet(filename: str):
    path = f"s3://{S3_BUCKET}/{S3_FOLDER}/{filename}"
    print(f"📂 Reading {path}")
    return spark.read.parquet(path)


# ── Helper: get a JDBC connection via the JVM ─────────────────────────────────
def get_jdbc_conn():
    jvm   = spark._jvm
    jprop = jvm.java.util.Properties()
    jprop.setProperty("user",     DB_USER)
    jprop.setProperty("password", DB_PASS)
    jprop.setProperty("driver",   "org.postgresql.Driver")
    conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, jprop)
    conn.setAutoCommit(False)
    return conn


def execute_sql_steps(steps: list[str], label: str = ""):
    """Execute a list of SQL DML statements in one transaction."""
    conn = get_jdbc_conn()
    stmt = conn.createStatement()
    try:
        for i, sql in enumerate(steps, 1):
            rows = stmt.executeUpdate(sql.strip())
            print(f"  ✅ [{label}] step {i}: {rows} rows affected")
        conn.commit()
        print(f"✅ [{label}] committed")
    except Exception as e:
        conn.rollback()
        print(f"❌ [{label}] rolled back: {e}")
        raise
    finally:
        stmt.close()
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Populate dim_region
# ══════════════════════════════════════════════════════════════════════════════
# Sources:
#   ihme_metadata.parquet  → dimension="location", id (IHME location id), name (country_name)
#   gho_metadata.parquet   → dimension="spatial_dim", id (GHO spatial id), name (country_code 3-char)
#
# Strategy:
#   • inner-join on country_name (ihme) == country_name derived from gho via
#     a country name → ISO-3 normalisation already done in the processing pipeline.
#   • The ihme_metadata "location" dimension already normalises names to ISO-3 codes
#     (see ihme_process.py LOOKUP_MAP) — so ihme name IS the ISO-3 code.
#   • gho_metadata "spatial_dim" gives us the raw GHO id; its name field holds the
#     country_code (3-char) after the gho_metadata processing step.
#   • We use the IHME location id as a stable natural key for upsert.
#   • who_region and income_group are not available in either metadata file,
#     so we default both to NULL (they can be enriched separately).
#   • is_global = FALSE for all country rows.

print("\n══ STEP 1: dim_region ══")

ihme_meta = read_s3_parquet("ihme_metadata.parquet")
gho_meta  = read_s3_parquet("gho_metadata.parquet")

# ihme_metadata schema: dimension (string), id (int64), name (string)
# Filter to the "location" dimension — name is the Country Name, id is the IHME location id
ihme_locations = (
    ihme_meta
    .filter(F.lower(F.col("dimension")) == "location")
    .select(
        F.col("id").cast("integer").alias("ihme_location_id"),
        F.col("name").alias("country_name"),
    )
    .dropDuplicates(["ihme_location_id"])
)

# gho_metadata schema: dimension (string), id (string), name (string)
# The "spatial_dim" entries: name holds the ISO-3 country code
gho_countries = (
    gho_meta
    .filter(F.lower(F.col("dimension")) == "gho_ihme_country")
    .select(
        F.col("name").cast("integer").alias("ihme_location_id"),
        F.col("id").alias("country_code"),
    )
    .dropDuplicates(["country_code"])
)

# The IHME pipeline normalises location names → we can attempt a join on country_code
# where IHME "name" (after normalisation) equals GHO country_code.
# If join fails for a row, country_code remains NULL — acceptable; region still inserted.
dim_region_df = (
    ihme_locations
    .join(gho_countries, on="ihme_location_id", how="inner")
    .select(
        F.col("country_name"),
        F.col("country_code"),
        F.lit(None).cast("string").alias("who_region"),
        F.lit(None).cast("string").alias("income_group"),
        F.lit(False).alias("is_global"),
    )
    .filter(F.col("country_code").isNotNull())
    .filter(F.length(F.col("country_code")) == 3)
    .dropDuplicates(["country_code"])
)

dim_region_count = dim_region_df.count()
print(f"  dim_region candidates: {dim_region_count} rows")

# Write to a temp table then upsert via SQL
dim_region_df.write.jdbc(
    url=JDBC_URL,
    table="staging_dim_region_glue",
    mode="overwrite",
    properties=JDBC_PROPS,
)
print(f"  ✅ staged {dim_region_count} rows → staging_dim_region_glue")

execute_sql_steps([
    """
    INSERT INTO dim_region (country_code, country_name, who_region, income_group, is_global)
    SELECT
        TRIM(s.country_code)::character(3),
        s.country_name,
        s.who_region,
        s.income_group,
        s.is_global
    FROM staging_dim_region_glue s
    ON CONFLICT (country_code) DO UPDATE SET
        country_name = EXCLUDED.country_name,
        who_region   = COALESCE(EXCLUDED.who_region,   dim_region.who_region),
        income_group = COALESCE(EXCLUDED.income_group, dim_region.income_group)
    """
], label="dim_region upsert")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Populate dim_disease from icd_10_to_11_flat.parquet
# ══════════════════════════════════════════════════════════════════════════════
# icd_10_to_11_flat schema (from icd_metadata.py):
#   icd10Code   (string)   — ICD-10 code
#   icd11Code   (string)   — one ICD-11 code per row
#   icd11_chapter (list[string]) — chapter(s)
#
# dim_disease expected columns (inferred from codebase):
#   disease_key   serial PK
#   disease_name  varchar      — we use icd11Code as a placeholder; could be enriched
#   icd10_code    varchar
#   icd11_code    varchar
#   disease_category   varchar
#   is_neglected  boolean (default false, updated by separate pipeline)

print("\n══ STEP 2: dim_disease ══")

icd_flat    = read_s3_parquet("icd_10_to_11_flat.parquet")
icd_catalog = read_s3_parquet("icd_11_catalogue.parquet")

# icd11_chapter is a list column — take the first element
dim_disease_df = (
    icd_flat
    .select(
        F.col("icd10Code").alias("icd10_code"),
        F.col("icd11Code").alias("icd11_code_raw"),
        F.element_at(F.col("icd11_chapter"), 1).alias("disease_category"),
    )
    .filter(F.col("icd11_code_raw").isNotNull() & (F.col("icd11_code_raw") != ""))
    .withColumn("icd11_code", F.explode(F.split(F.col("icd11_code_raw"), "&")))
    .drop("icd11_code_raw")
    .withColumn("icd11_code", F.trim(F.col("icd11_code")))
    .filter(F.col("icd11_code") != "")
    .filter(F.length(F.col("icd11_code")) <= 20)
    .join(
        icd_catalog.select(
            F.col("icd_code"),
            F.col("title").alias("disease_name"),
        ),
        on=F.col("icd11_code") == F.col("icd_code"),
        how="left",
    )
    .drop("icd_code")
    # Fall back to icd11_code if no catalogue entry found
    .withColumn(
        "disease_name",
        F.coalesce(F.col("disease_name"), F.col("icd11_code"))
    )
    .dropDuplicates(["icd10_code", "icd11_code"])
)

disease_count = dim_disease_df.count()
print(f"  dim_disease candidates: {disease_count} rows")

dim_disease_df.write.jdbc(
    url=JDBC_URL,
    table="staging_dim_disease_glue",
    mode="overwrite",
    properties=JDBC_PROPS,
)
print(f"  ✅ staged {disease_count} rows → staging_dim_disease_glue")

execute_sql_steps([
    """
    INSERT INTO dim_disease (disease_name, icd10_code, icd11_code, disease_category, is_neglected)
    SELECT
        s.disease_name,
        s.icd10_code,
        s.icd11_code,
        COALESCE(s.disease_category, 'Unclassified'),
        FALSE
    FROM staging_dim_disease_glue s
    ON CONFLICT (icd10_code, icd11_code) DO UPDATE SET
        disease_name     = EXCLUDED.disease_name,
        disease_category = COALESCE(EXCLUDED.disease_category, dim_disease.disease_category)
    """
], label="dim_disease upsert")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Populate fact_health_metrics (DALY, one row per disease × region)
# ══════════════════════════════════════════════════════════════════════════════
# health_data.parquet key columns (from gho_process.py + ihme_process.py):
#   cause        — IHME cause id (integer) mapped via dim_disease.icd10_code
#   location     — IHME location id (integer) → must map to dim_region via ihme_location_id
#   year         — integer
#   val          — DALY value (float)
#   source_data  — "GHO" or "IHME"
#
# Mapping chain:
#   health_data.cause  → staging_dim_disease_glue.icd10_code ? No — cause is the IHME *cause id*
#   We need: dim_disease.disease_key where icd10_code maps via GHO_TO_IHME_CAUSE (static map).
#   BUT: the icd_10_to_11_flat already gives us icd10_code → icd11_code.
#   The health_data.cause is an IHME integer cause id. The icd_catalogue has ICD-11 codes.
#   The safest join that survives schema variations:
#     health_data.cause (int) JOIN dim_disease via icd10_code cast — this requires
#     the cause column to literally be the ICD-10 code string. In gho_process.py the
#     "cause" column is the GHO indicator code (SA_000...). In ihme_process.py it is
#     the IHME cause int id renamed to "cause".
#
#   Given the actual pipeline, the cleanest approach is:
#     • JOIN health_data → dim_disease ON health_data.cause::text = dim_disease.icd10_code
#       (works when health_data.cause is already an ICD-10 code, e.g. from gho_process)
#     • Fan out: for EACH disease × ALL dim_region rows (i.e. replicate DALY across
#       all regions, aggregating val per disease when source_data has multi-region rows).
#       The fact table fan-out is done in SQL after staging.

print("\n══ STEP 3: fact_health_metrics (DALY) ══")

health_data = read_s3_parquet("health_data.parquet")
ihme_meta   = read_s3_parquet("ihme_metadata.parquet")
gho_meta    = read_s3_parquet("gho_metadata.parquet")

# Aggregate to one val per (cause, year) — average across sex/age/metric groups

ihme_causes = (
    ihme_meta
    .filter(F.col("dimension") == "cause")
    .select(
        F.col("id").cast("integer").alias("cause_id"),
        F.col("name").alias("cause_name"),
    )
    .dropDuplicates(["cause_id"])
)

ihme_locations_bridge = (
    gho_meta
    .filter(F.lower(F.col("dimension")) == "gho_ihme_country")
    .select(
        F.col("name").cast("integer").alias("location_id"),
        F.col("id").alias("country_code"),
    )
    .dropDuplicates(["country_code"])
)

dim_disease_spark = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_disease",
    properties=JDBC_PROPS,
).select("disease_key", "disease_name")

dim_region_spark = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_region",
    properties=JDBC_PROPS,
).select("region_key", F.trim(F.col("country_code")).alias("country_code"))

dim_time_spark = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_time",
    properties=JDBC_PROPS,
).filter(F.col("month") == 1).select("time_key", "year")

health_agg = (
    health_data
    .filter(F.col("val").isNotNull())
    .filter(F.col("cause").isNotNull())
    .filter(F.col("year").isNotNull())
    .groupBy(
        F.col("cause").cast("integer").alias("cause_id"),
        F.col("location").cast("integer").alias("location_id"),
        F.col("year").cast("integer").alias("year"),
        F.col("source_data").alias("source_data"),

    )
    .agg(F.avg("val").alias("avg_daly"))
    .join(ihme_causes, on="cause_id", how="left")
    .join(ihme_locations_bridge, on="location_id", how="left")
    .filter(F.col("cause_name").isNotNull())
    .filter(F.col("country_code").isNotNull())
    .withColumn(
        "source_key",
        F.when(F.col("source_data") == "IHME", F.lit(9))
         .when(F.col("source_data") == "GHO",  F.lit(7))
    )
)

health_with_disease = (
    health_agg
    .join(
        broadcast(dim_disease_spark),
        F.lower(F.col("disease_name")).contains(F.lower(F.col("cause_name"))) |
        F.lower(F.col("cause_name")).contains(F.lower(F.col("disease_name"))),
        how="left",
    )
    .filter(F.col("disease_key").isNotNull())
)

health_final = (
    health_with_disease.alias("h")
    .join(
        dim_region_spark.alias("r"),
        F.trim(F.col("h.country_code")) == F.col["r-country_code"],
        how="left",
    )
    .join(dim_time_spark, on="year", how="left")
    .filter(F.col("region_key").isNotNull())
    .filter(F.col("time_key").isNotNull())
    .select(
        F.col("disease_key"),
        F.col("time_key"),
        F.col("region_key"),
        F.col("source_key"),
        F.col("avg_daly").alias("avg_daly"),
        F.col("cause_id"),
        F.col("year"),
        F.col("country_code"),
        F.col("cause_name"),
    )
    # Deduplicate grain before staging
    .dropDuplicates(["disease_key", "time_key", "region_key", "source_key"])
)


health_count = health_final.count()
print(f"  health_data aggregated: {health_count} (cause, year) pairs")

health_final.write.jdbc(
    url=JDBC_URL,
    table="staging_health_agg_glue",
    mode="overwrite",
    properties=JDBC_PROPS,
)
print(f"  ✅ staged {health_count} rows → staging_health_agg_glue")

# SQL fan-out: for each (disease_key, year) insert one fact row per region_key
# The ON CONFLICT clause updates the DALY values if the row already exists.
execute_sql_steps([
    # 3a: ensure dim_time rows exist for all years in the data
    """
    INSERT INTO dim_time (year, month, quarter, year_month)
    SELECT DISTINCT
        h.year,
        1                                            AS month,
        1                                            AS quarter,
        LPAD(h.year::text, 4, '0') || '-01'          AS year_month
    FROM staging_health_agg_glue h
    ON CONFLICT (year_month) DO NOTHING
    """,

    # 3b: fan-out upsert — one row per (disease × region × year)
    # Joins:
    # staging_health_agg_glue.cause_str  → dim_disease.icd10_code (direct ICD-10 match)
    # match with disease, region and source is handled in spark because ILIKE matching requires full scans for all rows.
    # With 63k rows this is not feasible.
    """
    INSERT INTO fact_health_metrics (
        disease_key, time_key, region_key, source_key, daly_value, daly_per_100k
    )
    SELECT
        disease_key,
        time_key,
        region_key,
        source_key,
        avg_daly AS daly_value,
        avg_daly AS daly_per_100k
    FROM staging_health_agg_glue
    ON CONFLICT (disease_key, time_key, region_key, source_key)
    DO UPDATE SET
        daly_value    = EXCLUDED.daly_value,
        daly_per_100k = EXCLUDED.daly_per_100k,
        loaded_at     = NOW()
    """,
], label="fact_health_metrics")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Populate dim_trials from trials_processed.parquet
# ══════════════════════════════════════════════════════════════════════════════
# trials_processed schema (relevant fields):
#   trial_id          String
#   phases            List(Int8)
#   overall_status    String
#   start_date        Datetime
#   end_date          Datetime
#   countries_status  List(Struct({country: String, approval: String, status: String}))
#   codes             Struct({icd_10: List(String), icd_11: List(Struct({code, chapter}))})
#
# dim_trials PK: (trial_id, region_key)
# Mapping:
#   disease_key  ← dim_disease JOIN ON icd11_code (from codes.icd_11[].code)
#   region_key   ← dim_region JOIN ON country_code (from countries_status[].country)
#   active       ← end_date IS NULL
#   stage        ← phases (serialised as string)
#   status       ← countries_status[].status (per-country)
#   loaded_at    ← NOW()

print("\n══ STEP 4: dim_trials ══")

trials_raw = read_s3_parquet("trials_processed.parquet")

# Explode countries_status to get one row per (trial_id, country)
# and explode icd_11 codes to get one row per (trial_id, icd11_code)
trials_countries = (
    trials_raw
    .select(
        "trial_id",
        "phases",
        "overall_status",
        "start_date",
        "end_date",
        F.explode_outer("countries_status").alias("cs"),
        "codes",
    )
    .filter(F.col("cs").isNotNull())
    .filter(F.col("cs.country").isNotNull())
    .select(
        "trial_id",
        "phases",
        "overall_status",
        "start_date",
        "end_date",
        F.col("cs.country").alias("country"),
        F.col("cs.status").alias("country_status"),
        "codes",
    )
)

# Explode icd_11 codes (struct with .code field)
trials_exploded = (
    trials_countries
    .select(
        "trial_id",
        "phases",
        "overall_status",
        "start_date",
        "end_date",
        "country",
        "country_status",
        F.explode_outer("codes.icd_11").alias("icd11_struct"),
    )
    .select(
        "trial_id",
        # phases: cast list to string for storage
        F.array_join(
            F.transform(
                F.col("phases"),
                lambda x: x.cast("string")
            ),
            ","
        ).alias("stage"),
        F.col("overall_status").alias("overall_status"),
        F.col("start_date"),
        F.col("end_date"),
        F.col("country"),
        F.col("country_status").alias("status"),
        # icd11 code from the struct; null when no codes present
        F.col("icd11_struct.code").alias("icd11_code"),
    )
    # is_active = true when end_date is null
    .withColumn("active", F.col("end_date").isNull())
    .withColumn("loaded_at", F.current_timestamp())
    # Drop rows where country is null (explicit requirement)
    .filter(F.col("country").isNotNull() & (F.trim(F.col("country")) != ""))
)

# Deduplicate: for a given (trial_id, country, icd11_code), keep one row
trials_deduped = trials_exploded.dropDuplicates(["trial_id", "country", "icd11_code"])
trial_count = trials_deduped.count()
print(f"  trials exploded: {trial_count} rows (trial × country × icd11_code)")

# Stage to RDS
trials_deduped.write.jdbc(
    url=JDBC_URL,
    table="staging_trials_glue",
    mode="overwrite",
    properties=JDBC_PROPS,
)
print(f"  ✅ staged {trial_count} rows → staging_trials_glue")

# SQL upsert into dim_trials
execute_sql_steps([
    """
    TRUNCATE TABLE dim_trials
    """,

    """
    INSERT INTO dim_trials (trial_id,
                            region_key,
                            disease_key,
                            stage,
                            status,
                            active,
                            loaded_at)
    SELECT DISTINCT ON (s.trial_id, r.region_key)
        s.trial_id,
        r.region_key,
        d.disease_key,
        s.stage,
        s.status,
        s.active,
        s.loaded_at
    FROM staging_trials_glue s
    JOIN dim_region r
      ON TRIM(r.country_code) = TRIM(s.country)
    LEFT JOIN dim_disease d
      ON d.icd11_code = s.icd11_code
    WHERE s.country IS NOT NULL
    ORDER BY s.trial_id, r.region_key, d.disease_key ASC NULLS LAST
    """,
], label="dim_trials upsert")


# ══════════════════════════════════════════════════════════════════════════════
# Verification
# ══════════════════════════════════════════════════════════════════════════════
print("\n══ Verification ══")

verify = spark.read.jdbc(
    url=JDBC_URL,
    table="""(
        SELECT
            (SELECT COUNT(*) FROM dim_region)                               AS regions,
            (SELECT COUNT(*) FROM dim_disease)                              AS diseases,
            (SELECT COUNT(*) FROM fact_health_metrics WHERE daly_value > 0) AS fact_with_daly,
            (SELECT COUNT(*) FROM dim_trials)                               AS trials
    ) AS v""",
    properties=JDBC_PROPS,
).collect()[0]

print(
    f"📊 dim_region: {verify['regions']} | "
    f"dim_disease: {verify['diseases']} | "
    f"fact_health_metrics (with DALY): {verify['fact_with_daly']} | "
    f"dim_trials: {verify['trials']}"
)

job.commit()
print("\n✅ Weekly ETL job completed successfully.")