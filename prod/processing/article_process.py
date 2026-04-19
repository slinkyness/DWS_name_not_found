"""
article_merge.py — Polars-native normalisation + upsert for news articles
==========================================================================
Handles both fetch-file formats:
  - newsapi.org/v2/top-headlines       published_at: "2026-03-26T15:58:54Z"
  - api.currentsapi.services/v1/search published_at: "2026-03-27 13:18:05 +0000"
 
Both arrive with the same envelope:
  { "source_api": "...", "data": { "articles": [...] }, ... }

 Output schema (Parquet)
-----------------------
url            Utf8
topic          Utf8
author         Utf8
title          Utf8
description    Utf8
image_url      Utf8
published_at   Datetime(us, UTC)
language       Utf8
category       List(Utf8)            fetch category tags
source_apis    List(Utf8)            all APIs that have reported this URL
first_seen_at  Utf8                  ISO-8601 UTC, set on first insert
last_seen_at   Utf8                  ISO-8601 UTC, updated on any change
version        Int32                 incremented only on newer published_at
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import polars as pl

from process_lambda_utils import load_s3_parquet, upsert_by_date

log = logging.getLogger(__name__)

UPSERT_KEY = "url"
DATE_COL = "published_at"
SOURCE_COL = "source_apis"
CANONICAL_COL = [
    UPSERT_KEY, "topic", "author", "title",
    "description", "image_url", DATE_COL, "language",
    "category", SOURCE_COL, "first_seen_at", "last_seen_at", "version",
]

# ── Normalise one fetch-file DataFrame ────────────────────────────────────────

def normalise(raw: pl.DataFrame, now_str: str) -> pl.DataFrame:
    """
    Explode and clean a freshly-read fetch-file DataFrame.

    Extracts ``source_api`` from the envelope, explodes ``data.articles``,
    parses ``published_at`` to UTC Datetime, normalises URLs, cleans author
    placeholders, and adds provenance columns ready for ``upsert_by_date``.

    Args:
        raw:     Result of ``pl.read_json(path)`` — one-row envelope DataFrame.
        now_str: Current UTC timestamp as ISO-8601 string (set once per run).

    Returns:
        Normalised DataFrame in the canonical output schema.
    """
    source_api = raw["source_api"][0]
    raw = raw.with_columns(
        pl.lit(source_api).cast(pl.List(pl.String)).alias(SOURCE_COL),
        pl.lit(now_str).alias("first_seen_at"),
        pl.lit(now_str).alias("last_seen_at"),
        pl.lit(1).cast(pl.Int32).alias("version"),
    )

    if "who.int" in source_api:
        return normalise_who(raw)

    date_fmt = "%Y-%m-%dT%H:%M:%SZ" if "newsapi" in source_api else "%Y-%m-%d %H:%M:%S %z"

    return (
        raw
        .with_columns(pl.col("data").struct.field("articles").alias("articles"))
        .explode("articles")
        .unnest("articles")
        .with_columns(
            pl.when("newsapi" in source_api)
            .then(pl.lit("en").alias("language")),
            pl.col(DATE_COL).str.to_datetime(date_fmt, strict=False).dt.replace_time_zone("UTC"),
            pl.col("author").replace("#author.fullName}", None),
            pl.col(UPSERT_KEY).str.replace(r"\?.*", "").str.strip_suffix("/"),
        )
        .select(CANONICAL_COL)
    )


# ── Normalise WHO News ──────────────────────────────────────

def normalise_who(raw: pl.DataFrame) -> pl.DataFrame:
    """
    Only ``disease_outbreak_news`` and ``emergencies`` are ingested; general
    news is intentionally ignored.

    Field mapping
    -------------
    WHO field          → canonical field
    -----------------    ----------------
    url                → url           (upsert key; query-string stripped)
    title              → title
    summary            → description
    published          → published_at
    <who_category>     → category      (["health_emergency"])
    <derived>          → topic         ("disease_outbreak" or "emergency")
    source_api         → source_apis   (list)
    author / image_url / language / source → null (not provided by WHO)

    Args:
        raw:     Result of ``pl.read_json(path)`` — one-row envelope DataFrame.

    Returns:
        Normalised DataFrame in the canonical output schema.
    """
    raw = raw.unnest("data")
    date_fmt = "%Y-%m-%dT%H:%M:%SZ"
    SHARED_LITERALS = [
        pl.lit(None).cast(pl.String).alias("author"),
        pl.lit(None).cast(pl.String).alias("image_url"),
        pl.lit("en").alias("language"),
        pl.lit("WHO").alias("source"),
        pl.lit("health_emergency").cast(pl.List(pl.String)).alias("category"),
        pl.col("published").str.to_datetime(date_fmt, strict=False)
        .dt.replace_time_zone("UTC")
        .alias(DATE_COL),
    ]
    emergencies_description = (
        pl.when(pl.col("summary").is_not_null())
        .then(pl.col("summary"))
        .otherwise(
            pl.concat_str(
                [
                    pl.when(pl.col("start_date").is_not_null()).then(pl.lit("Start Date")),
                    pl.col("start_date"),
                    pl.when(pl.col("end_date").is_not_null()).then(pl.lit("End Date")),
                    pl.col("end_date"),
                    pl.when(pl.col("rating").is_not_null()).then(pl.lit("Rating")),
                    pl.col("rating"),
                ],
                separator=" ",
                ignore_nulls=True,
            )
        )
        .alias("description")
    )
    sections = [
        ("emergencies", "emergencies", [
            pl.selectors.ends_with("_date")
         .cast(pl.String)
         .str.to_datetime(date_fmt, strict=False)
         .dt.date()
         .cast(pl.String),
            pl.col("rating", "summary").replace("", None),
        ], emergencies_description),
        ("disease_outbreak_news", "disease_outbreak", [], pl.col("summary").alias("description")),
    ]
    return (
        pl.concat(
            [
                raw
                .explode(list_col)
                .unnest(list_col)
                .with_columns(*pre_exprs)
                .with_columns(
                    *SHARED_LITERALS,
                    pl.lit(topic).alias("topic"),
                    description_expr,
                )
                .select(CANONICAL_COL)
                for list_col, topic, pre_exprs, description_expr in sections
            ],
            how="vertical",
        )
    )


def load_and_merge(
    fetch_path: str,
    parquet_uri: str,
    now: datetime,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """
    Read one fetch file, normalise it, and upsert into the master Parquet.

    Args:
        fetch_path:  Local path to the JSON fetch file (e.g. ``/tmp/fetch.json``).
                     Streaming to /tmp/ before calling this function is the
                     caller's responsibility (``pl.read_json`` cannot read S3 URIs).
        parquet_uri: s3:// URI (or local path) of the master articles Parquet.
        now:         Current UTC datetime (used for provenance timestamps).

    Returns:
        ``(merged_df, stats)`` where stats keys are
        ``total | new | updated | source_only``.
    """
    now_str  = now.isoformat()
    raw      = pl.read_json(fetch_path, infer_schema_length=1000)
    incoming = normalise(raw, now_str)
    existing = load_s3_parquet(parquet_uri)
    if existing is not None:
        log.info("Loaded %d existing articles from %s", len(existing), parquet_uri)

    merged, n_new, n_updated, n_src = upsert_by_date(
        existing, incoming,
        upsert_key=UPSERT_KEY,
        date_col=DATE_COL,
        source_col=SOURCE_COL,
    )
    stats = {
        "total":       len(merged),
        "new":         n_new,
        "updated":     n_updated,
        "source_only": n_src,
    }
    log.info(
        "Upsert complete — total=%d  new=%d  updated=%d  source_only=%d",
        stats["total"], stats["new"], stats["updated"], stats["source_only"],
    )
    return merged, stats