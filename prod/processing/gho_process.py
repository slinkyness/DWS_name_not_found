"""
=================
Merges GHO records with IHME DALYs on (country_code, year).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import polars as pl
from process_lambda_utils import load_s3_parquet


# ── Paths ──────────────────────────────────────────────────────────────────────
OUT_BUCKET = os.environ["S3_BUCKET"]
S3_FOLDER = os.environ["S3_PROCESSED_FOLDER"]
GHO_META_URI   = f"s3://{OUT_BUCKET}/{S3_FOLDER}/gho_metadata.parquet"
IHME_META_URI    = f"s3://{OUT_BUCKET}/{S3_FOLDER}/ihme_metadata.parquet"
HEALTH_DATA_URI   = f"s3://{OUT_BUCKET}/{S3_FOLDER}/health_data.parquet"


KEY_COLS = ["age", "cause", "etiology", "location", "measure", "metric", "population_group", "sex", "year"]
MEASURE_COLS = ["val", "upper", "lower"]
UPSERT_KEY = "row_key"
DATE_COL   = "last_updated"
SOURCE_COL = "source_data"

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")


def make_lookup(metadata: pl.DataFrame, dimension: str, alias: str) -> pl.DataFrame:
    return (
        metadata
        .filter(pl.col("dimension").str.to_lowercase() == dimension)
        .select([pl.col("id"), pl.col("name").cast(pl.Int64).alias(alias)])
    )

def log_unmapped(df: pl.DataFrame, source_col: str, mapped_col: str, condition: pl.Expr | None = None) -> pl.DataFrame:
    mask = pl.col(mapped_col).is_null()
    if condition is not None:
        mask = mask & condition
    unmapped = (
        df.filter(mask)
        .get_column(source_col)
        .unique()
        .to_list()
    )
    if unmapped:
        print(f"Unmapped {mapped_col} (from '{source_col}'): {unmapped}")
    return df


def _load_metadata() -> pl.DataFrame:
    gho_metadata = load_s3_parquet(GHO_META_URI)
    if gho_metadata.filter(
            pl.col("dimension").str.to_lowercase() == "gho_ihme_country"
    ).is_empty():
        ihme_subset = (
            load_s3_parquet(IHME_META_URI)
            .filter(pl.col("dimension") == "location")
            .with_columns(
                pl.col("id").cast(pl.String).alias("name"),
                pl.col("name").cast(pl.Utf8).alias("id"),
                pl.col("dimension").replace("location", "gho_ihme_country")
            )
        )
        enriched = (
            pl.concat(
            [
                gho_metadata,
                ihme_subset.select(gho_metadata.columns)
             ], how="vertical")
        )
        enriched.write_parquet(GHO_META_URI, compression="zstd", use_pyarrow=False)
        log.info(f"Enriched Metadata, saved to {GHO_META_URI}")
        return enriched
    return gho_metadata


def transform(df: pl.DataFrame) -> pl.DataFrame:
    metadata = _load_metadata()
    cause_lut = make_lookup(metadata, "gho_ihme_cause", "cause")
    country_lut = make_lookup(metadata, "gho_ihme_country", "location")
    sex_lut = make_lookup(metadata, "gho_ihme_sex", "sex")
    df = (
        df
        .join(cause_lut, left_on="indicator_code", right_on="id", how="left")
        .pipe(log_unmapped, "indicator_code", "cause")
        .join(
            country_lut,
            left_on=pl.when(pl.col("spatial_dim_type").str.to_lowercase() == "country")
                      .then(pl.col("country_code")),
            right_on="id",
            how="left"
        )
        .pipe(log_unmapped, "spatial_dim", "location",
              condition=pl.col("spatial_dim_type").str.to_lowercase() == "country")
        .join(
            sex_lut,
            left_on=pl.when(pl.col("dim1_type").str.to_lowercase() == "sex")
                      .then(pl.col("dim1")),
            right_on="id",
            how="left"
        )
        .pipe(log_unmapped, "dim1", "sex",
              condition=pl.col("dim1_type").str.to_lowercase() == "sex")
        .with_columns(
            pl.lit(None).cast(pl.Int64).alias("etiology"),
            pl.lit(2).cast(pl.Int64).alias("measure"),
            pl.lit(27).cast(pl.Int64).alias("age"),
            pl.lit(3).cast(pl.Int64).alias("metric"),
            pl.lit(1).cast(pl.Int64).alias("population_group"),
            pl.lit("GHO").alias(SOURCE_COL),
            pl.col("year").cast(pl.Int64),
            pl.when(pl.col("numeric_value").is_not_null())
            .then(pl.col("numeric_value").cast(pl.Float64))
            .otherwise(pl.col("value_display").cast(pl.Float64))
            .alias("val"),
            pl.col("low").cast(pl.Float64).alias("lower"),
            pl.col("high").cast(pl.Float64).alias("upper"),
            pl.col("last_updated")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%#z", time_unit="us")
            .dt.convert_time_zone("UTC")
            .alias(DATE_COL),
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
