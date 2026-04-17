"""
core_function.py
Fetches research works and outputs from the CORE API (api.core.ac.uk/v3).
Reads CORE_KEY from Secrets Manager.
Saves results to S3 and returns a summary.
Event params (all optional):
    Param           Type        Default
    limit           int         100
    max_pages       int         10
    entity_filter   list[str]   both (["works", "outputs"])
Env vars (required): AWS_REGION_NAME, S3_BUCKET, SECRET_NAME, S3_FETCH_FOLDER
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from core_fetch import fetch_core_data
from lambda_utils import get_secret, save_to_s3, ok_response, error_response

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
REGION      = os.environ["AWS_REGION_NAME"]
S3_BUCKET   = os.environ["S3_BUCKET"]
SECRET_NAME = os.environ["SECRET_NAME"]
S3_PREFIX   = os.environ["S3_FETCH_FOLDER"]
SECRET_KEY  = "CORE_KEY"
S3_KEY      = "core"
SOURCE_API  = "api.core.ac.uk/v3"


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Secret -------------------------------------------------------------
    try:
        api_key = get_secret(SECRET_NAME, REGION, SECRET_KEY)
    except Exception as exc:
        log.error("Secret retrieval failed: %s", exc)
        return error_response(500, f"Secret retrieval failed: {exc}")

    # -- 2. Parameters ---------------------------------------------------------
    limit         = int(event.get("limit",      os.environ.get("CORE_LIMIT",     100)))
    max_pages     = int(event.get("max_pages",  os.environ.get("CORE_MAX_PAGES", 10)))
    entity_filter =     event.get("entity_filter")  # None → both

    log.info(
        "Starting fetch — limit=%d  max_pages=%d  entity_filter=%s",
        limit, max_pages, entity_filter,
    )

    # -- 3. Fetch --------------------------------------------------------------
    try:
        result = fetch_core_data(
            api_key       = api_key,
            limit         = limit,
            max_pages     = max_pages,
            entity_filter = entity_filter,
        )
    except Exception as exc:
        log.error("fetch_core_data failed: %s", exc)
        return error_response(500, f"Fetch failed: {exc}")

    # -- 4. Build unified payload ----------------------------------------------
    payload = {
        "fetched_at":  now.isoformat(),
        "source_api":  SOURCE_API,
        "fetch_params": {
            "limit":         limit,
            "max_pages":     max_pages,
            "entity_filter": entity_filter,
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
        "fetched_at":    payload["fetched_at"],
        "source_api":    payload["source_api"],
        "s3_uri":        s3_uri,
        "total_works":   data["total_works"],
        "total_outputs": data["total_outputs"],
    }

    log.info(
        "Done — %d works and %d outputs saved to %s",
        data["total_works"], data["total_outputs"], s3_uri,
    )
    return ok_response(summary)