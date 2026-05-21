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
import os
from datetime import datetime, timezone

import polars as pl

from process_lambda_utils import load_s3_parquet, upsert_by_date

log = logging.getLogger(__name__)

UPSERT_KEY = "work_id"
DATE_COL   = "published_at"
SOURCE_COL = "source_apis"

CANONICAL_COL = [
    UPSERT_KEY, "doi", "pubmed_id", "title", "abstract",
    DATE_COL, "year_published", "authors", "journal",
    "field_of_study", "download_url",
    SOURCE_COL, "icd_11_codes", "icd_11_titles", "icd_chapters",
    "match_method", "first_seen_at", "last_seen_at", "version",
]

_EXCLUDE_CHAPTERS = {
    "x", "complementary_factors", "supplementary_conditions",
    "supplementary_factors", "supplementary_populations", "v", "purposes",
}
_ICD11_RE = (
    r"\b([A-Z]{1,2}\d[A-Z0-9](?:\.[A-Z0-9]{1,2})?"
    r"|\d[A-Z]\d{2}(?:\.[A-Z0-9]{1,2})?)\b"
)
_ICD10_RE = r"\b([A-Z]\d{2}(?:\.\d{1,2})?)\b"

MIN_FUZZY_SCORE: float = float(os.environ.get("CORE_ICD_FUZZY_SCORE", "0.45"))
MIN_TITLE_TOKENS: int = int(os.environ.get("CORE_ICD_MIN_TOKENS", "3"))

OUT_BUCKET = os.environ["S3_BUCKET"]
S3_FOLDER = os.environ["S3_PROCESSED_FOLDER"]
ICD_META_URI   = f"s3://{OUT_BUCKET}/{S3_FOLDER}/icd_11_catalogue.parquet"
PUB_DATA_URI    = f"s3://{OUT_BUCKET}/{S3_FOLDER}/publication_data.parquet"


def _load_metadata() ->  dict | None:
    icd_data = load_s3_parquet(ICD_META_URI)
    substr_name_df = (
        icd_data
        .filter(~pl.col("chapter").is_in(list(_EXCLUDE_CHAPTERS)))
        .select("icd_code", pl.concat_list([
            pl.col("title").cast(pl.List(pl.Utf8)),
            pl.col("index_terms"),
        ]).alias("terms"))
        .explode("terms")
        .filter(
            pl.col("terms").is_not_null()
            & (pl.col("terms").str.len_chars() >= 5)
        )
        .with_columns(
            pl.col("terms").str.to_lowercase().str.strip_chars().alias("name"),
        )
        .with_columns(
            pl.col("name").str.split(" ").list.len().alias("wc"),
            pl.col("name").str.split(" ").list.first().alias("first_word"),
            pl.when(pl.col("name").str.split(" ").list.len() == 1)
            .then(pl.lit(" ") + pl.col("name") + pl.lit(" "))
            .otherwise(pl.col("name"))
            .alias("search_name"),
        )
        .unique("name", keep="first", maintain_order=True)
        .sort("wc", descending=True)
        .select("search_name", "icd_code", "wc", "first_word")
    )
    icd_tok = (
        icd_data
        .filter(~pl.col("chapter").is_in(list(_EXCLUDE_CHAPTERS)))
        .with_columns(
            pl.concat_str(
                [
                    pl.col("title").fill_null(""),
                    pl.col("index_terms").list.join(" ").fill_null(""),
                ],
                separator=" ",
            )
            .str.to_lowercase()
            .str.replace_all(r"[^a-z0-9\s]", " ")
            .str.split(" ")
            .list.eval(pl.element().filter(pl.element().str.len_chars() >= 3))
            .list.unique()
            .alias("cat_tokens")
        )
        .select("icd_code", "cat_tokens")
        .explode("cat_tokens")
        .filter(
            pl.col("cat_tokens").is_not_null()
            & (pl.col("cat_tokens").str.len_chars() > 0)
        )
        .rename({"cat_tokens": "token"})
    )
    icd_sizes = (
        icd_tok
        .group_by("icd_code")
        .agg(pl.len().alias("cat_n"))
    )
    code_lookup = icd_data.select(
        pl.col("icd_code"),
        pl.col("title").alias("icd_title_lookup"),
        pl.col("chapter").alias("icd_chapter_lookup"),
    )
    valid_codes = icd_tok.select("icd_code").unique()
    log.info(
        "ICD index ready — %d codes, %d substr entries, %d tokens",
        len(icd_data), len(substr_name_df), len(icd_tok),
    )
    return {
        "substr_name_df": substr_name_df,
        "cat_tok":        icd_tok,
        "cat_sizes":      icd_sizes,
        "code_lookup":    code_lookup,
        "valid_codes":    valid_codes,
    }


def _regex_match(works: pl.DataFrame, idx: dict) -> pl.DataFrame:
    valid_codes = idx["valid_codes"].rename({"icd_code": "_candidates"})
    return works.join(
        works
        .select(
            pl.col(UPSERT_KEY),
            pl.concat_str(
                [pl.col("title").fill_null(""), pl.col("abstract").fill_null("")],
                separator=" ",
            )
            .str.to_uppercase()
            .pipe(lambda e: pl.concat_list([
                e.str.extract_all(_ICD11_RE),
                e.str.extract_all(_ICD10_RE),
            ]))
            .list.unique()
            .alias("_candidates"),
        )
        .explode("_candidates")
        .filter(pl.col("_candidates").is_not_null())
        .join(valid_codes, on="_candidates", how="inner")
        .group_by(UPSERT_KEY)
        .agg(pl.col("_candidates").unique().alias("_regex_codes")),
        on=UPSERT_KEY,
        how="left",
    )


def _substring_match(works: pl.DataFrame, idx: dict) -> pl.DataFrame:
    name_df = idx["substr_name_df"]
    candidates = (
        works.with_columns(
            (
                    pl.lit(" ")
                    + pl.concat_str(
                [
                    pl.col("title").fill_null(""),
                    pl.col("abstract").fill_null(""),
                ],
                separator=" ",
            )
                    .str.to_lowercase()
                    .str.replace_all(r"[^a-z0-9\s]", " ")
                    .str.strip_chars()
                    + pl.lit(" ")
            ).alias("_text_padded"),
            (
                    pl.lit(" ")
                    + pl.col("title").fill_null("").str.to_lowercase()
                    .str.replace_all(r"[^a-z0-9\s]", " ").str.strip_chars()
                    + pl.lit(" ")
            ).alias("_title_padded")
        )
        .select(
            pl.col(UPSERT_KEY),
            pl.col("_text_padded"),
            pl.col("_title_padded"),
            pl.col("_text_padded")
            .str.split(" ")
            .list.eval(pl.element().filter(pl.element().str.len_chars() >= 3))
            .list.unique()
            .alias("_words"),
        )
        .explode("_words")
        .filter(pl.col("_words").is_not_null() & (pl.col("_words") != ""))
        .rename({"_words": "first_word"})
        .join(name_df, on="first_word", how="inner")
        .unique([UPSERT_KEY, "search_name"])
    )
    if candidates.is_empty():
        return works.with_columns(
            pl.lit(None, dtype=pl.List(pl.Utf8)).alias("_substr_codes")
        )

    best = (
        candidates
        .filter(
            pl.col("_text_padded").str.contains(pl.col("search_name"), literal=True)
        )
        .with_columns(
            pl.col("_title_padded").str.contains(pl.col("search_name"), literal=True)
            .cast(pl.Int8)
            .alias("_in_title"),
        )
        .sort(["wc", "_in_title"], descending=True)
        .group_by(UPSERT_KEY)
        .agg(pl.col("icd_code").first().alias("_substr_code"))
        .with_columns(
            pl.col("_substr_code").cast(pl.List(pl.Utf8))
        )
        .rename({"_substr_code": "_substr_codes"})
    )

    return works.join(best.select(UPSERT_KEY, "_substr_codes"), on=UPSERT_KEY, how="left")


def _jaccard_match(works: pl.DataFrame, idx: dict) -> pl.DataFrame:
    cat_tok   = idx["cat_tok"]
    cat_sizes = idx["cat_sizes"]
    work_tokens = works.select(
        pl.col(UPSERT_KEY),
        pl.col("title")
          .fill_null("")
          .str.to_lowercase()
          .str.replace_all(r"[^a-z0-9\s]", " ")
          .str.split(" ")
          .list.eval(
              pl.element().filter(pl.element().str.len_chars() >= 3)
          )
          .list.unique()
          .alias("tokens"),
    )
    work_sizes = (
        work_tokens
        .with_columns(pl.col("tokens").list.len().alias("work_n"))
        .select(UPSERT_KEY, "work_n")
    )
    work_tok = (
        work_tokens
        .explode("tokens")
        .filter(
            pl.col("tokens").is_not_null()
            & (pl.col("tokens").str.len_chars() > 0)
        )
        .rename({"tokens": "token"})
    )
    shared = (
        work_tok
        .join(cat_tok, on="token", how="inner")
        .group_by(UPSERT_KEY, "icd_code")
        .agg(pl.len().alias("shared"))
    )
    if shared.is_empty():
        return works.with_columns(
            pl.lit(None, dtype=pl.List(pl.Utf8)).alias("_fuzzy_codes")
        )
        print("No matches on shared")
    best = (
        shared
        .join(work_sizes, on=UPSERT_KEY)
        .join(cat_sizes, on="icd_code")
        .with_columns(
            (
                    pl.col("shared")
                    / (pl.col("work_n") + pl.col("cat_n") - pl.col("shared"))
            ).alias("jaccard")
        )
        .filter(
            (pl.col("jaccard") >= 0.45)
            & (pl.col("work_n") >= 3)
        )
        # Retain only the maximum-Jaccard code(s) per work
        .with_columns(
            pl.col("jaccard").max().over(UPSERT_KEY).alias("best_jaccard")
        )
        .filter(pl.col("jaccard") == pl.col("best_jaccard"))
        .group_by(UPSERT_KEY)
        .agg(pl.col("icd_code").unique().alias("_fuzzy_codes"))
    )
    return works.join(best.select(UPSERT_KEY, "_fuzzy_codes"), on=UPSERT_KEY, how="left")



def _enrich_and_merge(works: pl.DataFrame, idx: dict) -> pl.DataFrame:
    code_lookup = idx["code_lookup"]
    return (
        works
        .with_columns(
            pl.when(pl.col("_regex_codes").is_not_null())
            .then(pl.col("_regex_codes"))
            .when(pl.col("_substr_codes").is_not_null())
            .then(pl.col("_substr_codes"))
            .when(pl.col("_fuzzy_codes").is_not_null())
            .then(pl.col("_fuzzy_codes"))
            .otherwise(pl.lit([], dtype=pl.List(pl.Utf8)))
            .alias("icd_11_codes"),
            pl.when(pl.col("_regex_codes").is_not_null())
            .then(pl.lit("regex"))
            .when(pl.col("_substr_codes").is_not_null())
            .then(pl.lit("substring"))
            .when(pl.col("_fuzzy_codes").is_not_null())
            .then(pl.lit("fuzzy"))
            .otherwise(pl.lit("none"))
            .alias("match_method"),
        )
        .drop("_regex_codes", "_substr_codes", "_fuzzy_codes")
        .with_row_index("_row")
        .explode("icd_11_codes")
        .join(code_lookup, left_on="icd_11_codes", right_on="icd_code", how="left")
        .group_by("_row")
        .agg(
            pl.all().exclude(["_row", "icd_title_lookup", "icd_chapter_lookup", "icd_11_codes"]).first(),
            pl.col("icd_11_codes"),
            pl.col("icd_title_lookup").drop_nulls().alias("icd_11_titles"),
            pl.col("icd_chapter_lookup").drop_nulls().unique().alias("icd_chapters"),
        )
        .drop("_row")
    )


def _add_icd_columns(works: pl.DataFrame, idx: dict) -> pl.DataFrame:
    if idx is None:
        return works.with_columns(
            pl.lit([], dtype=pl.List(pl.Utf8)).alias("icd_11_codes"),
            pl.lit([], dtype=pl.List(pl.Utf8)).alias("icd_11_titles"),
            pl.lit([], dtype=pl.List(pl.Utf8)).alias("icd_chapters"),
            pl.lit("none").alias("match_method"),
        )

    return (
        works
        .pipe(_regex_match, idx)
        .pipe(_substring_match, idx)
        .pipe(_jaccard_match, idx)
        .pipe(_enrich_and_merge, idx)
    )

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
    catalogue = _load_metadata()
    df = (
        raw
        .with_columns(
            pl.col("data").struct.field("works").alias("_works")
        )
        .explode("_works")
        .unnest("_works")
        .with_columns(
            pl.col("identifiers")
            .list.eval(
                pl.when(pl.element().struct.field("type") == "CORE_ID")
                .then(pl.element().struct.field("identifier"))
            )
            .list.drop_nulls()
            .list.first()
            .alias(UPSERT_KEY),
            pl.col("identifiers")
            .list.eval(
                pl.when(pl.element().struct.field("type") == "DOI")
                .then(pl.element().struct.field("identifier"))
            )
            .list.drop_nulls()
            .list.first()
            .alias("doi"),
            pl.col("identifiers")
            .list.eval(
                pl.when(pl.element().struct.field("type") == "PUBMED_ID")
                .then(pl.element().struct.field("identifier"))
            )
            .list.drop_nulls()
            .list.first()
            .alias("pubmed_id"),
            pl.col("authors")
            .list.eval(pl.element().struct.field("name"))
            .alias("authors"),
            pl.col("journals")
            .list.eval(pl.element().struct.field("title"))
            .list.drop_nulls()
            .list.first()
            .alias("journal"),
            pl.col("publishedDate")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S", strict=False)
            .dt.replace_time_zone("UTC")
            .alias(DATE_COL),
            pl.col("yearPublished")
            .cast(pl.Int32, strict=False)
            .alias("year_published"),
            pl.lit([source_api]).alias(SOURCE_COL),
            pl.lit(now_str).alias("first_seen_at"),
            pl.lit(now_str).alias("last_seen_at"),
            pl.lit(1).cast(pl.Int32).alias("version"),
        )
        .rename({
            "fieldOfStudy": "field_of_study",
            "downloadUrl":  "download_url",
        })
        .filter(pl.col(UPSERT_KEY).is_not_null())
    )


    return _add_icd_columns(df, catalogue).select(CANONICAL_COL)


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
    now_str = now.isoformat()
    raw = pl.read_json(fetch_path, infer_schema_length=1000)
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
    n_matched = (incoming ["match_method"] != "none").sum()
    n_regex = (incoming ["match_method"] == "regex").sum()
    n_sub_s = (incoming ["match_method"] == "fuzzy").sum()
    n_fuzzy = (incoming ["match_method"] == "fuzzy").sum()
    n_unmatched = (incoming ["match_method"] == "none").sum()
    stats = {
        "total": len(merged),
        "new": n_new,
        "updated": n_updated,
        "source_only":  n_src,
        "icd_matched": n_matched,
        "icd_regex": n_regex,
        "icd_substr": n_sub_s,
        "icd_fuzzy": n_fuzzy,
        "icd_no_match": n_unmatched,
    }
    log.info(
        "Upsert complete — total=%d  new=%d  updated=%d  source_only=%d  "
        "icd_matches=%d (regex=%d fuzzy=%d substring=%d) unmatched=%d",
        stats["total"], stats["new"], stats["updated"], stats["source_only"],
        stats["icd_matched"], stats["icd_regex"], stats["icd_fuzzy"], stats["icd_substr"],
        stats["icd_no_match"],
    )
    return merged, stats