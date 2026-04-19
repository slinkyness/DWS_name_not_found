"""
ihme_process.py — IHME GBD CSV transformer (pure logic, no metadata I/O)
=========================================================================
Reads raw IHME CSVs and transforms them into a clean, upsert-ready
DataFrame. The Lambda handler (ihme_function.py) owns data I/O and upsert.
Metadata extraction is a separate concern handled by ihme_metadata.py.

Public API
----------
transform(df, now)       -> pl.DataFrame   — structural cleanup only
extract_metadata(df)     -> pl.DataFrame   — id→name dimension rows
merge_metadata(existing, new) -> (pl.DataFrame, int)  — deduplicated merge

Handles two dataset variants:
  - Cause of Death / Injury
  - Etiology

`last_updated` is stamped onto every incoming row at transform time so
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

OUT_BUCKET = os.environ["S3_BUCKET"]
S3_FOLDER = os.environ["S3_PROCESSED_FOLDER"]

HEALTH_DATA_URI = f"s3://{OUT_BUCKET}/{S3_FOLDER}/health_data.parquet"
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

    return lookup


LOOKUP_MAP = _build_lookup_map()

def _detect_pairs(df: pl.DataFrame) -> list[tuple[str, str, str]]:
    """Return (id_col, name_col, base_name) tuples for all *_id / *_name pairs."""
    cols = set(df.columns)
    return [
        (col, f"{col[:-3]}_name", col[:-3])
        for col in df.columns
        if col.endswith("_id") and f"{col[:-3]}_name" in cols
    ]


def _extract_metadata_rows(df: pl.DataFrame, pairs: list) -> pl.DataFrame:
    """Build a (dimension, id, name) frame from detected id/name column pairs."""
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


# ── Public API ────────────────────────────────────────────────────────────────

def extract_metadata(df: pl.DataFrame) -> pl.DataFrame:
    """
    Extract id→name dimension rows from a raw IHME DataFrame.

    Returns a DataFrame with columns (dimension, id, name) containing
    unique id→name mappings for every *_id / *_name column pair found.
    Location names are normalised to ISO-3 codes via LOOKUP_MAP.

    This function performs no S3 I/O — callers are responsible for
    loading/saving the dimension table.  See ihme_metadata.py.
    """
    pairs = _detect_pairs(df)
    return _extract_metadata_rows(df, pairs)


def merge_metadata(
    existing: pl.DataFrame,
    new: pl.DataFrame,
) -> tuple[pl.DataFrame, int]:
    """
    Merge *new* metadata rows into *existing*, deduplicating by (dimension, id).

    Returns (merged_df, n_added) where n_added is the net number of new rows.
    """
    merged = (
        pl.concat([existing, new])
        .unique(subset=["dimension", "id"], keep="first")
        .sort(["dimension", "id"])
    )
    return merged, len(merged) - len(existing)


def empty_metadata() -> pl.DataFrame:
    """Return an empty metadata frame with the canonical schema."""
    return pl.DataFrame(
        {"dimension": [], "id": [], "name": []},
        schema={"dimension": pl.String, "id": pl.Int64, "name": pl.String},
    )


def transform(df: pl.DataFrame, now: datetime) -> pl.DataFrame:
    """
    Transform a raw IHME CSV DataFrame (structural cleanup only).

    Steps
    -----
    1. Detect id/name column pairs.
    2. Drop ``*_name`` columns; rename ``*_id`` → base dimension name.
    3. Fill optional variant columns (etiology) with None when absent.
    4. Synthesise a ``row_key`` from all dimension (non-measure) columns.
    5. Stamp ``last_updated`` from *now* so upsert_by_date can compare freshness.

    Note: Metadata extraction and persistence is handled separately by
    ihme_metadata.py.  This function performs no S3 I/O.

    Args:
        df:  Raw DataFrame read directly from the source CSV.
        now: UTC timestamp of the current Lambda invocation.

    Returns:
        Cleaned DataFrame ready to be passed to upsert_by_date.
    """
    pairs = _detect_pairs(df)

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