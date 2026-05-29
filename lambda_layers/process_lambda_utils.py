"""
process_lambda_utils.py — Shared helpers for all PROCESS-phase Lambdas
=======================================================================
Keeps the process Lambda layer separate from the fetch layer so each
stays small.  Import this in prod/processing/* instead of lambda_utils.

Provides:
  find_s3_key(bucket, prefix, stem)               → str
  read_s3_csv(bucket, prefix, stem, ...)          → pl.DataFrame
  load_s3_parquet(uri, lazy)                      → DataFrame | LazyFrame | None
  save_to_s3_parquet(df, bucket, s3_folder,       → str
                     s3_key, timestamp)
  upsert_by_date(existing, incoming, ...)         → (DataFrame, int, int, int)
  ok_response(body)                               → dict
  error_response(status_code, message)            → dict

Note: LazyFrame inputs to upsert_by_date are automatically collected.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import boto3
import s3fs
import polars as pl

from botocore.exceptions import ClientError

log = logging.getLogger(__name__)
_s3_client = boto3.client("s3")

# ── S3 helpers ─────────────────────────────────────────────────────────────────

def find_s3_key(bucket: str, prefix: str, stem: str) -> str:
    """
    Find the first S3 key whose lowercase filename contains *stem*.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix to narrow the search, e.g. "raw/icd/".
        stem:   Lowercase substring to match in the filename.

    Returns:
        The matching S3 key (without the bucket name).

    Raises:
        FileNotFoundError: if no object matches.
    """
    prefix_clean = prefix.strip("/") + "/"
    paginator = _s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix_clean):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.split("/")[-1].lower()
            if stem.lower() in filename:
                log.info("Matched  s3://%s/%s  (stem='%s')", bucket, key, stem)
                return key
    raise FileNotFoundError(
        f"No object in s3://{bucket}/{prefix_clean} whose filename contains '{stem}'"
    )


def read_s3_csv(
    bucket: str,
    prefix: str,
    stem: str,
    sep: str = ",",
    **kwargs,
) -> pl.DataFrame:
    key = find_s3_key(bucket, prefix, stem)
    return pl.read_csv(f"s3://{bucket}/{key}", separator=sep, **kwargs)

def _s3_object_exists(bucket: str, key: str) -> bool:
    """Return True if the S3 object exists, using a cheap HeadObject call."""
    try:
        _s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise

def get_s3_info(event):
    # EventBridge format
    if "detail" in event:
        return {
            "bucket": event["detail"]["bucket"]["name"],
            "key": event["detail"]["object"]["key"]
        }
    # S3 direct trigger format
    if "Records" in event:
        record = event["Records"][0]["s3"]
        return {
            "bucket": record["bucket"]["name"],
            "key": record["object"]["key"]
        }
    raise ValueError(f"Unrecognised event format: {event}")

def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` into ``(bucket, key)``."""
    without_scheme = uri[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key

def load_s3_parquet(
    uri: str,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame | None:
    """
    Read a Parquet file from *uri* (s3://… or local path).

    Args:
        uri:  Full URI, e.g. "s3://my-bucket/processed/data.parquet".
        lazy: If True, returns a LazyFrame (no data read until .collect()).

    Returns:
        DataFrame / LazyFrame, or None if the file does not exist yet
        (first-run case — FileNotFoundError is silently converted to None;
        all other exceptions propagate).
    """
    bucket, key = _parse_s3_uri(uri)
    if not _s3_object_exists(bucket, key):
        log.info("No existing data at %s -- starting fresh.", uri)
        return None
    # Object confirmed present; safe to scan/read.
    return pl.scan_parquet(uri) if lazy else pl.read_parquet(uri)

# ── Generic Polars upsert ──────────────────────────────────────────────────────

def upsert_by_date(
    existing:   pl.DataFrame | pl.LazyFrame | None,
    incoming:   pl.DataFrame,
    upsert_key: str,
    date_col:   str,
    source_col: str | None = None,
) -> tuple[pl.DataFrame, int, int, int]:
    """
    Merge *incoming* rows into *existing* using a date-comparison upsert.

    Accepts LazyFrame for *existing* (collected automatically) so callers
    can use load_s3_parquet(lazy=True) without an extra .collect() call.

    Keyed on *upsert_key*, compared on *date_col* (a Datetime column).
    When *source_col* is given (a List[String] column), sources from
    incoming rows are merged into the stored list rather than overwriting.

    Rules
    -----
    NEW          Key not in existing → append, version = 1.
    UPDATED      Same key, incoming date strictly newer → replace content,
                 version += 1, preserve first_seen_at, merge sources.
    SOURCE-ONLY  Same key, incoming date ≤ stored date → keep existing,
                 merge source lists only (no version bump).
    UNTOUCHED    Existing key with no incoming counterpart → unchanged.

    Returns
    -------
    (merged_df, n_new, n_updated, n_source_only)
    """
    # Collect LazyFrame so the join works on a materialised DataFrame
    if isinstance(existing, pl.LazyFrame):
        existing = existing.collect()

    if existing is None:
        return incoming, len(incoming), 0, 0

    inc = "{0}_inc"
    d_inc = inc.format(date_col)
    s_inc = inc.format(source_col) if source_col else None

    joined = existing.join(
        incoming.rename(
            {c: inc.format(c) for c in incoming.columns if c != upsert_key}
        ),
        on=upsert_key, how="full", coalesce=True,
    )

    has_version  = "version" in existing.columns
    version_null = pl.col("version").is_null()     if has_version else pl.lit(False)
    version_live = pl.col("version").is_not_null() if has_version else pl.lit(True)

    is_new      = version_null
    is_updated  = (
        version_live
        & pl.col(d_inc).is_not_null()
        & (pl.col(d_inc) > pl.col(date_col))
    )
    is_src_only = (
        version_live
        & pl.col(d_inc).is_not_null()
        & (pl.col(d_inc) <= pl.col(date_col))
    )
    takes_incoming   = is_new | is_updated
    has_any_incoming = is_new | is_updated | is_src_only

    n_new, n_updated, n_src = joined.select([
        is_new.sum().alias("n_new"),
        is_updated.sum().alias("n_updated"),
        is_src_only.sum().alias("n_src"),
    ]).row(0)

    existing_col_set = set(existing.columns)
    provenance = (
            {upsert_key, "version"}
            | ( {"version", "first_seen_at", "last_seen_at"}
                & existing_col_set )
            | ({source_col} if source_col else set())
    )
    incoming_col_set = set(incoming.columns) - {upsert_key}
    content_cols = [
        c for c in existing.columns
        if c not in provenance and c in incoming_col_set
    ]

    mutations: list[pl.Expr] = [
        pl.when(takes_incoming)
          .then(pl.col(inc.format(c)))
          .otherwise(pl.col(c))
          .alias(c)
        for c in content_cols
    ]

    if has_version:
        mutations.append(
            pl.when(is_new).then(pl.lit(1, dtype=pl.Int32))
              .when(is_updated).then(
                  pl.col("version").cast(pl.Int32) + pl.lit(1, dtype=pl.Int32)
              )
              .otherwise(pl.col("version"))
              .alias("version")
        )

    if "first_seen_at" in existing_col_set:
        mutations.append(
            pl.when(is_new).then(pl.col("first_seen_at_inc"))
            .otherwise(pl.col("first_seen_at"))
            .alias("first_seen_at")
        )

    if "last_seen_at" in existing_col_set:
        mutations.append(
            pl.when(has_any_incoming).then(pl.col("last_seen_at_inc"))
            .otherwise(pl.col("last_seen_at"))
            .alias("last_seen_at")
        )

    if source_col:
        mutations.append(
            pl.when(has_any_incoming)
              .then(
                  pl.concat_list(
                      pl.when(pl.col(source_col).is_null())
                        .then(pl.lit([], dtype=pl.List(pl.String)))
                        .otherwise(pl.col(source_col)),
                      pl.col(s_inc),
                  ).list.unique()
              )
              .otherwise(pl.col(source_col))
              .alias(source_col)
        )

    merged = (
        joined
        .with_columns(mutations)
        .drop([c for c in joined.columns if c.endswith("_inc")])
    )

    return merged, n_new, n_updated, n_src


# ── HTTP responses ─────────────────────────────────────────────────────────────

def ok_response(body: Any) -> dict:
    return {
        "statusCode": 200,
        "headers":    {"Content-Type": "application/json"},
        "body":       json.dumps(body, ensure_ascii=False, default=str),
    }


def error_response(status_code: int, message: str) -> dict:
    return {
        "statusCode": status_code,
        "headers":    {"Content-Type": "application/json"},
        "body":       json.dumps({"error": message}),
    }
