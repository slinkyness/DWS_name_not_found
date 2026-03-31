"""
NewsAPI Lambda Handler — with Secrets Manager + S3 persistence
==============================================================
Wraps the existing newsapi_fetch.py logic with:
  - AWS Secrets Manager for NEWSAPI_KEY retrieval
  - S3 output: results saved to s3://REDACTED_S3_BUCKET/newsapi/<date>/

Secret format expected at prod/App/fetch:
  {"NEWSAPI_KEY": "your_key_here"}

S3 key pattern:
  newsapi/YYYY-MM-DD/newsapi_<ISO timestamp>.json
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

# Re-use the fetch logic from newsapi_fetch.py (must be in the same Lambda package)
from newsapi_fetch import fetch_health_news

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
SECRET_NAME = "prod/App/fetch"
REGION      = "us-east-1"
S3_BUCKET   = "REDACTED_S3_BUCKET"
S3_PREFIX   = "newsapi"
S3_KEY      = "who"
KEY_NAME    = "NEWS_API"


# ── Lambda entry point ─────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: Any) -> dict:
    """
    AWS Lambda entry point.

    Reads NEWSAPI_KEY from Secrets Manager (prod/App/fetch).
    Fetches health news via newsapi_fetch.fetch_health_news().
    Saves JSON result to s3://REDACTED_S3_BUCKET/newsapi/<date>/.

    Event keys (all optional):
        days_back    (int)        Articles from last N days.     Default: 7
        language     (str)        ISO-639-1 code.               Default: "en"
        page_size    (int)        Per-page results (max 100).   Default: 100
        max_pages    (int)        Pages per topic group.        Default: 1
        topic_filter (list[str])  Subset of topic labels.       Default: all

    Valid topic labels:
        core_health, mental_health, disability, cancer_research,
        rare_chronic, neurology, healthcare_policy, medical_research

    Returns:
        Standard API Gateway-compatible response with:
          - total_fetched, by_topic, s3_uri, fetched_at
    """
    now = datetime.now(timezone.utc)

    # -- 1. Retrieve API key ---------------------------------------------------
    try:
        secrets = get_secret()
        api_key = secrets[KEY_NAME]
    except (ClientError, KeyError) as exc:
        log.error("Secret retrieval failed: %s", exc)
        return _error(500, f"Secret retrieval failed: {exc}")

    if not api_key:
        return _error(500, f"{KEY_NAME} empty in secret store")

    # -- 2. Parse event parameters --------------------------------------------
    days_back    = int(event.get("days_back",  os.environ.get("NEWSAPI_DAYS_BACK",  7)))
    language     =     event.get("language",   os.environ.get("NEWSAPI_LANGUAGE",   "en"))
    page_size    = int(event.get("page_size",  os.environ.get("NEWSAPI_PAGE_SIZE",  100)))
    max_pages    = int(event.get("max_pages",  os.environ.get("NEWSAPI_MAX_PAGES",  1)))
    topic_filter =     event.get("topic_filter")  # None → all groups

    log.info(
        "Starting fetch — days_back=%d  language=%s  page_size=%d  "
        "max_pages=%d  topic_filter=%s",
        days_back, language, page_size, max_pages, topic_filter,
    )

    # -- 3. Fetch news ---------------------------------------------------------
    try:
        payload = fetch_health_news(
            api_key=api_key,
            days_back=days_back,
            language=language,
            page_size=page_size,
            max_pages=max_pages,
            topic_filter=topic_filter,
        )
    except Exception as exc:
        log.error("fetch_health_news failed: %s", exc)
        return _error(500, f"Fetch failed: {exc}")

    # -- 4. Enrich payload metadata -------------------------------------------
    payload["fetched_at"] = now.isoformat()
    payload["source_api"] = "newsapi.org/v2/everything"
    payload["fetch_params"] = {
        "days_back":    days_back,
        "language":     language,
        "page_size":    page_size,
        "max_pages":    max_pages,
        "topic_filter": topic_filter,
    }

    # -- 5. Save to S3 ---------------------------------------------------------
    try:
        s3_uri = save_to_s3(payload, now)
        payload["s3_uri"] = s3_uri
    except Exception as exc:
        log.error("S3 upload failed: %s", exc)
        return _error(500, f"S3 upload failed: {exc}")

    # -- 6. Return summary (omit full articles list from response body) --------
    summary = {
        "total_fetched":      payload["total_fetched"],
        "by_topic":           payload["by_topic"],
        "duplicates_removed": payload["duplicates_removed"],
        "date_range":         payload["date_range"],
        "fetched_at":         payload["fetched_at"],
        "s3_uri":             s3_uri,
        "source_api":         payload["source_api"],
    }

    log.info(
        "Done — %d articles across %d topics saved to %s",
        payload["total_fetched"], len(payload["by_topic"]), s3_uri,
    )

    return {
        "statusCode": 200,
        "headers":    {"Content-Type": "application/json"},
        "body":       json.dumps(summary, ensure_ascii=False, default=str),
    }
