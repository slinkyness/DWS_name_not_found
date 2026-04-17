"""
current_function.py
Fetches health/science news from the Currents API.
Reads CURRENT_API key from Secrets Manager.
Saves results to S3 and returns a summary.
Event params (all optional):
    Param           Type        Default
    days_back       int         7
    language        str         "en"
    page_size       int         50
    topic_filter    list[str]   all — health, science, medical
Env vars (required): AWS_REGION_NAME, S3_BUCKET, SECRET_NAME, S3_FETCH_FOLDER
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from current_fetch import fetch_health_news
from lambda_utils import get_secret, save_to_s3, ok_response, error_response

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
REGION      = os.environ["AWS_REGION_NAME"]
S3_BUCKET   = os.environ["S3_BUCKET"]
SECRET_NAME = os.environ["SECRET_NAME"]
S3_PREFIX   = os.environ["S3_FETCH_FOLDER"]
SECRET_KEY  = "CURRENT_API"
S3_KEY      = "currentapi"
SOURCE_API  = "api.currentsapi.services/v1/search"


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Secret -------------------------------------------------------------
    try:
        api_key = get_secret(SECRET_NAME, REGION, SECRET_KEY)
    except Exception as exc:
        log.error("Secret retrieval failed: %s", exc)
        return error_response(500, f"Secret retrieval failed: {exc}")

    # -- 2. Parameters ---------------------------------------------------------
    days_back    = int(event.get("days_back",  os.environ.get("CURRENTAPI_DAYS_BACK",  7)))
    language     =     event.get("language",   os.environ.get("CURRENTAPI_LANGUAGE",   "en"))
    page_size    = int(event.get("page_size",  os.environ.get("CURRENTAPI_PAGE_SIZE",  50)))
    topic_filter =     event.get("topic_filter")   # None → all categories

    log.info(
        "Starting fetch — days_back=%d  language=%s  page_size=%d  topic_filter=%s",
        days_back, language, page_size, topic_filter,
    )

    # -- 3. Fetch --------------------------------------------------------------
    try:
        result = fetch_health_news(
            api_key      = api_key,
            days_back    = days_back,
            language     = language,
            page_size    = page_size,
            topic_filter = topic_filter,
        )
    except Exception as exc:
        log.error("fetch_health_news failed: %s", exc)
        return error_response(500, f"Fetch failed: {exc}")

    # -- 4. Build unified payload ----------------------------------------------
    payload = {
        "fetched_at":  now.isoformat(),
        "source_api":  SOURCE_API,
        "fetch_params": {
            "days_back":    days_back,
            "language":     language,
            "page_size":    page_size,
            "topic_filter": topic_filter,
        },
        "data": result,
    }

    # -- 5. Save to S3 ---------------------------------------------------------
    try:
        s3_uri = save_to_s3(payload, S3_BUCKET, S3_PREFIX, S3_KEY, REGION, now)
    except Exception as exc:
        log.error("S3 upload failed: %s", exc)
        return error_response(500, f"S3 upload failed: {exc}")

    # -- 6. Summary response ---------------------------------------------------
    data = payload["data"]
    summary = {
        "fetched_at":         payload["fetched_at"],
        "source_api":         payload["source_api"],
        "s3_uri":             s3_uri,
        "total_fetched":      data["total_fetched"],
        "by_topic":           data["by_topic"],
        "duplicates_removed": data["duplicates_removed"],
        "requests_used":      data["requests_used"],
        "date_range":         data["date_range"],
    }

    log.info(
        "Done — %d articles across %d topics, %d API requests used, saved to %s",
        data["total_fetched"], len(data["by_topic"]), data["requests_used"], s3_uri,
    )
    return ok_response(summary)
