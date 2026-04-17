"""
ihme_function.py — IHME GBD CSV transformer
========================================================
Reads raw IHME CSVs and transforms them into a clean, upsert-ready
DataFrame. The Lambda handler (ihme_lambda.py) owns I/O and upsert.
Metadata (id→name mappings) is maintained as a separate parquet dimension table.

Handles two dataset variants:
  - Cause of Death / Injury
  - Etiology

`ingested_at` is stamped onto every incoming row at transform time so
that upsert_by_date (keyed on the composite row_key) can decide which
version of a row is newer.

Env vars (required): S3_BUCKET, S3_PROCESSED_FOLDER
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

import polars as pl
import pycountry

from lambda_utils import load_s3_parquet

OUT_BUCKET = os.environ["S3_BUCKET"]
S3_FOLDER = os.environ["S3_PROCESSED_FOLDER"]

HEALTH_DATA_URI   = f"s3://{OUT_BUCKET}/{S3_FOLDER}/health_data.parquet"
METADATA_URI    = f"s3://{OUT_BUCKET}/{S3_FOLDER}/ihme_dimension_map.parquet"

MEASURE_COLS    = {"val", "upper", "lower"}
KEY_COLS = ["age", "cause", "etiology", "location", "measure", "metric", "population_group", "sex", "year"]

UPSERT_KEY = "row_key"
DATE_COL   = "last_updated"
SOURCE_COL = "source_data"

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── ISO-3 resolution for the `location` dimension ────────────────────────────
# pycountry fuzzy-resolves 198/204 IHME location names automatically.
# The 6 below are parenthetical variants it cannot match.
_IHME_LOCATION_ISO3_OVERRIDES: dict[str, str] = {
    "Bolivia (Plurinational State of)":         "BOL",
    "Democratic Republic of the Congo":         "COD",
    "Iran (Islamic Republic of)":               "IRN",
    "Micronesia (Federated States of)":         "FSM",
    "United States Virgin Islands":             "VIR",
    "Venezuela (Bolivarian Republic of)":       "VEN",
    "Democratic People's Republic of Korea":    "PRK",
    "Republic of Korea":                        "KOR",
}


def _build_lookup_map():
    lookup = dict(_IHME_LOCATION_ISO3_OVERRIDES)

    # Add standard pycountry entries
    for country in pycountry.countries:
        # You might want to normalize names here if needed
        lookup[country.name] = country.alpha_3
        # Add common aliases if available in your logic
        for alias in getattr(country, 'common_name', []):
            lookup[alias] = country.alpha_3

    # Handle Fuzzy Search (Optional: Pre-calculate for known problematic names)
    # Note: Fuzzy search is expensive. If you have a specific list of problematic names, resolve them here.
    # If you need dynamic fuzzy search for *any* input, you must use a function, but it won't be "vectorized" in the C-sense.

    return lookup


LOOKUP_MAP = _build_lookup_map()

def _load_metadata() -> pl.DataFrame:
    """Load the dimension map, or return an empty frame on first run."""
    result = load_s3_parquet(METADATA_URI)
    if result is None:
        log.info("No existing metadata — starting fresh")
        return pl.DataFrame(
            {"dimension": [], "id": [], "name": []},
            schema={"dimension": pl.String, "id": pl.Int64, "name": pl.String},
        )
    return result if isinstance(result, pl.DataFrame) else result.collect()

def _extract_metadata(df: pl.DataFrame, pairs: list) -> pl.DataFrame:
    return (
        pl.concat([
        df.select(
            pl.lit(base).alias("dimension"),
            pl.col(id_col).cast(pl.Int64).alias("id"),
            pl.col(name_col).alias("name"),
        ).unique(subset=["id"])
        for id_col, name_col, base in pairs
        ])
        .with_columns(
            pl.when(pl.col("dimension") == "location")
            .then(pl.col("name").replace(LOOKUP_MAP, default=pl.col("name")))
            .otherwise(pl.col("name"))
            .alias("name")
        )
    )

def _merge_metadata(
    existing: pl.DataFrame,
    new: pl.DataFrame,
) -> tuple[pl.DataFrame, int]:
    merged = (
        pl.concat([existing, new])
        .unique(subset=["dimension", "id"], keep="first")
        .sort(["dimension", "id"])
    )
    return merged, len(merged) - len(existing)


def transform(df: pl.DataFrame, now: datetime) -> pl.DataFrame:
    """
    Transform a raw IHME CSV DataFrame.

    Steps
    -----
    1. Detect id/name column pairs and extract dimension metadata.
    2. Merge new metadata into the persisted dimension table on S3.
    3. Drop ``*_name`` columns; rename ``*_id`` → base dimension name.
    4. Fill optional variant columns with None when absent.
    5. Synthesise a ``row_key`` from all dimension (non-measure) columns.
    6. Stamp ``ingested_at`` from *now* so upsert_by_date can compare freshness.

    Args:
        df:  Raw DataFrame read directly from the source CSV.
        now: UTC timestamp of the current Lambda invocation.

    Returns:
        Cleaned DataFrame ready to be passed to upsert_by_date.
    """
    cols  = set(df.columns)
    pairs = [
        (col, f"{col[:-3]}_name", col[:-3])
        for col in df.columns
        if col.endswith("_id") and f"{col[:-3]}_name" in cols
    ]

    # -- Metadata --------------------------------------------------------------
    new_metadata   = _extract_metadata(df, pairs)
    existing_meta  = _load_metadata()
    merged_meta, n = _merge_metadata(existing_meta, new_metadata)
    if n:
        merged_meta.write_parquet(METADATA_URI, compression="zstd", use_pyarrow=False)
        log.info("Metadata: %d new id→name pairs added", n)

    # -- Structural cleanup ----------------------------------------------------
    df = (
        df
        .drop(pl.selectors.ends_with("_name"))
        .rename({id_col: base for id_col, _, base in pairs})
        .with_columns(
            pl.lit("IHME").alias(SOURCE_COL),
            pl.when("etiology" not in df.columns)
            .then(pl.lit(None).cast(pl.Int64).alias("etiology")),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias(DATE_COL)
        )
        .with_columns(
            pl.concat_str(
                [pl.col(c).cast(pl.String) for c in KEY_COLS],
                separator="|",
                ignore_nulls=True,
            ).alias("row_key")
        )
    )
    return df