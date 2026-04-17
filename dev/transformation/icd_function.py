import os
from datetime import datetime, timezone
from utils.config import data_dir
import polars as pl


NEEDED_COLS = [
    "icd_code", "icd_uri", "browser_url", "class_kind", "title",
    "definition", "long_definition", "fully_specified_name",
    "inclusion", "exclusion", "index_terms", "parent_uris",
    "child_uris", "foundation_uri",
]
CHAPTER_NAMING = {
    "certain_infectious": "1",
    "neoplasms": "2",
    "blood": "3",
    "endocrine": "4",
    "mental": "5",
    "nervous": "6",
    "eye": "7",
    "ear": "8",
    "circulatory": "9",
    "respiratory": "a",
    "digestive": "b",
    "skin": "c",
    "musculoskeletal": "d",
    "genitourinary": "e",
    "pregnancy": "f",
    "perinatal": "g",
    "congenital": "h",
    "symptoms": "j",
    "injury": "k",
    "external": "l",
    "factors_health": "m",
    "purposes": "n",
    "supplementary_factors": "p",
    "complementary_factors": "q",
    "supplementary_conditions": "r",
    "supplementary_populations": "s",
    "supplementary_contexts": "t",
    "supplementary_settings": "u",
}
LETTER_TO_CHAPTER = {v: k for k, v in CHAPTER_NAMING.items()}
ENTITY_ID_RE = r"/mms/(\d+)"

# ---- Lambda Config ----
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_FOLDER = os.environ.get("S3_PROCESSED_FOLDER", "")
S3_FILE_KEY = "icd_11"

def lambda_handler(event: dict, context) -> dict:
    now = datetime.now(timezone.utc)
    input_path = event.get("input", os.environ.get("ICD_PATH", None))
    if input_path is None:
        raise

    df = (
        pl.read_ndjson(input_path, infer_schema_length=None)
        .select(NEEDED_COLS)[:-1]
    )
    id_to_code = dict(
        df.select(
            pl.col("icd_uri")
            .str
            .extract(ENTITY_ID_RE)
            .alias("entity_id"),
            pl.col("icd_code"),
        )
        .filter(pl.col("entity_id").is_not_null())
        .iter_rows()
    )
    excl_resolved = (
        df.select(pl.col("exclusion"))
        .with_row_index("_row")
        .explode("exclusion")
        .filter(pl.col("exclusion").is_not_null())
        .unnest("exclusion")
        .with_columns(
            pl.col("linearization_uri").str.extract(ENTITY_ID_RE)
            .replace_strict(id_to_code, default=None)
            .alias("icd_code")
        )
        .select("_row", "label", "icd_code")
        .group_by("_row", maintain_order=True)
        .agg(
            pl.col("label").alias("exclusion_labels"),
            pl.col("icd_code").alias("exclusion_codes"),
        )
        .sort("_row")
    )
    payload = (
        df.with_row_index("_row")
        .drop("exclusion")
        .join(excl_resolved, on="_row", how="left")
        .with_columns(
            pl.col(
                "title",
                "definition",
                "long_definition",
                "fully_specified_name",
            ).str.replace_all("\u00a0", " "),
            pl.col("exclusion_labels",
                   "index_terms").fill_null([])
            .list.eval(
                pl.element()
                .str.replace_all("\u00a0", " ")
            ),
            pl.col("exclusion_codes").fill_null([]),
            pl.col("parent_uris", "child_uris").list.eval(
                pl.element()
                .str.extract(ENTITY_ID_RE)
                .replace_strict(id_to_code, default=None)
            ).name.map(lambda n: n.replace("_uris", "_codes")),
            pl.col("icd_code")
            .str.slice(0, 1)
            .str.to_lowercase()
            .replace(LETTER_TO_CHAPTER)
            .alias("chapter"),
        )
        .drop("_row")
        .select(
            [c for c in NEEDED_COLS if  c not in ["exclusion", "parent_uris", "child_uris"]]
            + [
                "exclusion_labels",
                "exclusion_codes",
                "parent_codes",
                "child_codes",
                "chapter"
            ]
        )
    )
    # uri = save_to_s3_parquet(payload, bucket=S3_BUCKET, s3_folder=S3_FOLDER, s3_key=S3_FILE_KEY, timestamp=now)
    return {
        "statusCode": 200,
        #"saved_to": uri,
    }

def main():
    input_path = data_dir / "icd11_mms_codes.jsonl"
    df = (
        pl.read_ndjson(input_path, infer_schema_length=None)
        .select(NEEDED_COLS)[:-1]
    )
    id_to_code = dict(
        df.select(
            pl.col("icd_uri")
            .str
            .extract(ENTITY_ID_RE)
            .alias("entity_id"),
            pl.col("icd_code"),
        )
        .filter(pl.col("entity_id").is_not_null())
        .iter_rows()
    )
    excl_resolved = (
        df.select(pl.col("exclusion"))
        .with_row_index("_row")
        .explode("exclusion")
        .filter(pl.col("exclusion").is_not_null())
        .unnest("exclusion")
        .with_columns(
            pl.col("linearization_uri").str.extract(ENTITY_ID_RE)
            .replace_strict(id_to_code, default=None)
            .alias("icd_code")
        )
        .select("_row", "label", "icd_code")
        .group_by("_row", maintain_order=True)
        .agg(
            pl.col("label").alias("exclusion_labels"),
            pl.col("icd_code").alias("exclusion_codes"),
        )
        .sort("_row")
    )
    payload = (
        df.with_row_index("_row")
        .drop("exclusion")
        .join(excl_resolved, on="_row", how="left")
        .with_columns(
            pl.col(
                "title",
                "definition",
                "long_definition",
                "fully_specified_name",
            ).str.replace_all("\u00a0", " "),
            pl.col("exclusion_labels",
                   "index_terms").fill_null([])
            .list.eval(
                pl.element()
                .str.replace_all("\u00a0", " ")
            ),
            pl.col("exclusion_codes").fill_null([]),
            pl.col("parent_uris", "child_uris").list.eval(
                pl.element()
                .str.extract(ENTITY_ID_RE)
                .replace_strict(id_to_code, default=None)
            ).name.map(lambda n: n.replace("_uris", "_codes")),
            pl.col("icd_code")
            .str.slice(0, 1)
            .str.to_lowercase()
            .replace(LETTER_TO_CHAPTER)
            .alias("chapter"),
        )
        .drop("_row")
        .select(
            [c for c in NEEDED_COLS if  c not in ["exclusion", "parent_uris", "child_uris"]]
            + [
                "exclusion_labels",
                "exclusion_codes",
                "parent_codes",
                "child_codes",
                "chapter"
            ]
        )
    )
    output = data_dir / "icd_11.parquet"
    payload.write_parquet(output, compression="zstd", use_pyarrow=False)

if __name__ == "__main__":
    main()