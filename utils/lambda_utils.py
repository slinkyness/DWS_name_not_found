"""
lambda_utils.py — Shared helpers for all fetch Lambdas
=======================================================
Provides:
  - get_secret(secret_name, region, key)  → str
  - save_to_s3(payload, bucket, s3_folder, s3_key, region, timestamp) → str
  - save_to_s3_parquet(payload, bucket, s3_folder, s3_key, timestamp) → str
  - error_response(status_code, message)  → dict
  - ok_response(body)                     → dict

All Lambdas import from this module instead of duplicating the logic.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import boto3
import s3fs
import polars as pl
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


# ── Secrets Manager ────────────────────────────────────────────────────────────

def get_secret(secret_name: str, region: str, key: str) -> str:
    """
    Retrieve a single string value from an AWS Secrets Manager JSON secret.

    Args:
        secret_name: SecretId, e.g. "prod/App/fetch"
        region:      AWS region name, e.g. "eu-central-1"
        key:         JSON key inside the secret, e.g. "NEWS_API"

    Returns:
        The secret string value.

    Raises:
        ClientError: on AWS access failure.
        KeyError:    if the key is absent from the secret JSON.
        ValueError:  if the resolved value is empty.
    """
    session = boto3.session.Session()
    client  = session.client(service_name="secretsmanager", region_name=region)

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        log.error("Failed to retrieve secret %s: %s", secret_name, exc)
        raise

    raw    = response["SecretString"]
    secret = json.loads(raw)

    if key not in secret:
        raise KeyError(f"Key '{key}' not found in secret '{secret_name}'")

    value = secret[key]
    if not value:
        raise ValueError(f"Key '{key}' is empty in secret '{secret_name}'")

    return value


# ── S3 ─────────────────────────────────────────────────────────────────────────

def find_s3_key(bucket: str, prefix: str, stem: str) -> str:
    """
    Find the first S3 key whose lowercase filename contains *stem*.

    Uses s3fs glob.
    The search pattern is case-insensitive against the final path component (filename only).
    Args:
        bucket: S3 bucket name, e.g. "my-data-bucket"
        stem:   Lowercase substring to match in the filename,
                e.g. "10to11maptomultiplecategories"
        prefix: Optional key prefix to narrow the search, e.g. "raw/icd/"

    Returns:
        The matching S3 key (without the bucket name).
    Raises:
        FileNotFoundError: if no object matches.
    """
    fs = s3fs.S3FileSystem(anon=False)
    pattern = f"{bucket}/{prefix}**"
    all_keys = fs.glob(pattern)

    for full_path in all_keys:
        filename = full_path.split("/")[-1].lower()
        if stem in filename:
            key = full_path[len(bucket) + 1:]
            log.info("Matched  s3://%s/%s  (stem='%s')", bucket, key, stem)
            return key
    raise FileNotFoundError(
        f"No object in s3://{bucket}/{prefix} whose filename contains '{stem}'"
    )

def read_s3_csv(
        bucket: str,
        prefix: str,
        stem: str,
        sep: str = ",",
        **kwargs
) -> pl.DataFrame:
    key = find_s3_key(bucket, prefix, stem)
    return pl.read_csv(f"s3://{bucket}/{key}", separator=sep, **kwargs)


def load_s3_parquet(uri: str, lazy: bool = False) -> pl.DataFrame | pl.LazyFrame | None:
    """
    Read a Parquet file from *uri* (s3://… or local path).

    For lazy evaluation uses ``pl.scan_parquet`` so no data is read until the caller's query
    is collected.
    A ``FileNotFoundError`` (first run, no master yet) is
    caught and returned as ``None``; all other exceptions propagate so
    genuine read errors are not silently swallowed.
    """
    try:
        if lazy:
            lf = pl.scan_parquet(uri)
            lf.collect_schema()
            return lf
        return pl.read_parquet(uri)
    except FileNotFoundError:
        log.info("No existing data at %s — starting fresh.", uri)
        return None

def save_to_s3(
    payload:   dict,
    bucket:    str,
    s3_folder:    str,
    s3_key:    str,
    region:    str,
    timestamp: datetime,
) -> str:
    """
    Serialise payload to JSON and upload to S3.

    Key format: {prefix}/YYYY-MM-DD/{s3_key}_<YYYYMMDDTHHMMSSZ>.json
    Returns the full S3 URI of the saved object.
    """
    date_part = timestamp.strftime("%Y-%m-%d")
    ts_part   = timestamp.strftime("%Y%m%dT%H%M%SZ")
    key       = f"{s3_folder}/{date_part}/{s3_key}_{ts_part}.json"

    body = json.dumps(payload, ensure_ascii=False, default=str, indent=2)

    boto3.client("s3", region_name=region).put_object(
        Bucket      = bucket,
        Key         = key,
        Body        = body.encode("utf-8"),
        ContentType = "application/json",
    )

    uri = f"s3://{bucket}/{key}"
    log.info("Saved to %s  (%d bytes)", uri, len(body))
    return uri


def save_to_s3_parquet(
    payload:   pl.DataFrame,
    bucket:    str,
    s3_folder:     str,
    s3_key: str,
    timestamp: datetime,
) -> str:
    """
    Serialise payload to JSON and upload to S3.

    Key format: {phase}/{s3_key}.parquet
    Returns the full S3 URI of the saved object.
    """
    ts_part = timestamp.strftime("%Y-%m-%d")
    uri       = f"s3://{bucket}/{s3_folder}/{s3_key}_{ts_part}.parquet"
    payload.write_parquet(
        uri,
        compression="zstd",
        use_pyarrow=False,
    )
    log.info("Saved to %s  (%d rows)", uri, len(payload))
    return uri

# ── Generic Polars upsert ──────────────────────────────────────────────────────
def upsert_by_date(
    existing:     pl.DataFrame | None,
    incoming:     pl.DataFrame,
    upsert_key:   str,
    date_col:     str,
    source_col:   str | None = None,
) -> tuple[pl.DataFrame, int, int, int]:
    """
    Merge *incoming* rows into *existing* using a date-comparison upsert.

    Keyed on *upsert_key* (typically ``"url"``), compared on *date_col*
    (a ``Datetime`` column, typically ``"published_at"``).

    When *source_col* is given (e.g. ``"source_apis"``, a ``List[String]``
    column), sources from incoming rows are merged into the stored list
    rather than overwriting it.  This covers the case where the same article
    arrives from two different APIs.

    Rules
    -----
    NEW
        Key not present in existing → append as-is, ``version = 1``.
    UPDATED
        Same key, incoming date strictly newer → replace all content,
        bump ``version`` by 1, preserve ``first_seen_at``, merge sources.
    SOURCE-ONLY / STALE
        Same key, incoming date ≤ stored date → keep existing content,
        merge source lists only (no version bump).
    UNTOUCHED
        Existing key with no incoming counterpart → unchanged.

    Implementation
    --------------
    Single full-outer join of *existing* ← *incoming* (incoming columns
    suffixed ``_inc``).  All four cases are resolved in one
    ``with_columns`` pass using ``pl.when/then/otherwise`` — no
    Python-level row filtering or intermediate URL lists.
    Counts are derived from the same joined frame with a single
    ``select([expr.sum() ...])`` call.

    Args:
        existing:   Current master DataFrame, or ``None`` on first run.
        incoming:   Normalised incoming DataFrame (must contain *upsert_key*,
                    *date_col*, and optionally *source_col* + ``version`` +
                    ``first_seen_at`` / ``last_seen_at``).
        upsert_key: Column name used as the deduplication key.
        date_col:   Datetime column used to decide which row wins.
        source_col: Optional ``List[String]`` column to union across sources.

    Returns:
        ``(merged_df, n_new, n_updated, n_source_only)``
    """
    if existing is None:
        return incoming, len(incoming), 0, 0

    inc = f"{{0}}_inc"
    d_inc = inc.format(date_col)
    s_inc = inc.format(source_col) if source_col else None

    # ── Join to classify each incoming URL ────────────────────────────────────
    joined = existing.join(
        incoming.rename({c: inc.format(c) for c in incoming.columns if c != upsert_key}),
        on=upsert_key, how="full", coalesce=True,
    )
    is_new      = pl.col("version").is_null()
    is_updated  = (
        pl.col("version").is_not_null() &
        pl.col(d_inc).is_not_null() &
        (pl.col(d_inc) > pl.col(date_col))
    )
    is_src_only = (
        pl.col("version").is_not_null() &
        pl.col(d_inc).is_not_null() &
        (pl.col(d_inc) <= pl.col(date_col))
    )
    takes_incoming = is_new | is_updated
    has_any_incoming = is_new | is_updated | is_src_only
    n_new, n_updated, n_src = joined.select([
        is_new.sum().alias("n_new"),
        is_updated.sum().alias("n_updated"),
        is_src_only.sum().alias("n_src"),
    ]).row(0)

    # ── Content columns: existing cols that also appear in incoming ─────────
    # Schema-asymmetric columns (e.g. newsapi "source"/"content" absent from
    # currents) have no _inc counterpart in the join — leave them untouched.
    provenance = {upsert_key, "version", "first_seen_at", "last_seen_at"} | (
        {source_col} if source_col else set()
    )
    incoming_col_set = set(incoming.columns) - {upsert_key}
    content_cols = [
        c for c in existing.columns
        if c not in provenance and c in incoming_col_set
    ]
    mutations: list[pl.Expr] = [
        # Content: take incoming value for new/updated rows
        *[
            pl.when(takes_incoming).then(pl.col(inc.format(c))).otherwise(pl.col(c)).alias(c)
            for c in content_cols
        ],
        # version: 1 for new, +1 for updated, unchanged otherwise
        pl.when(is_new).then(pl.lit(1, dtype=pl.Int32))
          .when(is_updated).then(pl.col("version").cast(pl.Int32) + pl.lit(1, dtype=pl.Int32))
          .otherwise(pl.col("version"))
          .alias("version"),
        # first_seen_at: keep existing for all rows that already existed
        pl.when(is_new).then(pl.col("first_seen_at_inc"))
          .otherwise(pl.col("first_seen_at"))
          .alias("first_seen_at"),
        # last_seen_at: refresh whenever there is any incoming match
        pl.when(has_any_incoming).then(pl.col("last_seen_at_inc"))
          .otherwise(pl.col("last_seen_at"))
          .alias("last_seen_at"),
    ]
    if source_col:
        mutations.append(
            # Union existing + incoming source lists; coerce null existing → []
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
