"""
who_function.py — AWS Lambda: WHO news fetcher
===============================================
No API key required (fully public WHO API).
Calls who_fetch.fetch_all_news() for the heavy logic.
Stores results in S3 and returns a summary.

Payload structure stored in S3:
  fetched_at   str          ISO-8601 UTC timestamp
  source_api   str          API base URL
  fetch_params dict         Echo of all resolved input parameters
  data         dict         Fetch results:
    general_news           list[dict]
    disease_outbreak_news  list[dict]
    emergencies            list[dict]
    total_fetched          int
    by_category            dict[str, int]

Environment variables (required):
  AWS_REGION_NAME, S3_BUCKET, S3_FETCH_FOLDER

Event keys (all optional):
  general_top    int   Max general news items.          Default: 20
  don_top        int   Max disease outbreak news items. Default: 10
  emergency_top  int   Max emergency items.             Default: 10
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from who_fetch import fetch_all_news
from lambda_utils import save_to_s3, ok_response, error_response

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
REGION    = os.environ["AWS_REGION_NAME"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ["S3_FETCH_FOLDER"]
S3_KEY    = "who"
SOURCE_API = "who.int/api/news"


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Parameters ---------------------------------------------------------
    general_top   = int(event.get("general_top",   20))
    don_top       = int(event.get("don_top",       10))
    emergency_top = int(event.get("emergency_top", 10))

    log.info(
        "Starting fetch — general_top=%d  don_top=%d  emergency_top=%d",
        general_top, don_top, emergency_top,
    )

    # -- 2. Fetch --------------------------------------------------------------
    try:
        result = fetch_all_news(
            general_top   = general_top,
            don_top       = don_top,
            emergency_top = emergency_top,
        )
    except Exception as exc:
        log.error("fetch_all_news failed: %s", exc)
        return error_response(500, f"Fetch failed: {exc}")

    # -- 3. Build unified payload ----------------------------------------------
    payload = {
        "fetched_at":  now.isoformat(),
        "source_api":  SOURCE_API,
        "fetch_params": {
            "general_top":   general_top,
            "don_top":       don_top,
            "emergency_top": emergency_top,
        },
        "data": result,
    }

    # -- 4. Save to S3 ---------------------------------------------------------
    try:
        s3_uri = save_to_s3(payload, S3_BUCKET, S3_PREFIX, S3_KEY, REGION, now)
    except Exception as exc:
        log.error("S3 upload failed: %s", exc)
        return error_response(500, f"S3 upload failed: {exc}")

    # -- 5. Summary response ---------------------------------------------------
    data = payload["data"]
    summary = {
        "fetched_at":   payload["fetched_at"],
        "source_api":   payload["source_api"],
        "s3_uri":       s3_uri,
        "total_fetched": data["total_fetched"],
        "by_category":  data["by_category"],
    }

    log.info(
        "Done — %d items across %d categories saved to %s",
        data["total_fetched"], len(data["by_category"]), s3_uri,
    )
    return ok_response(summary)
