"""
nih_function.py — AWS Lambda: NIH RePORTER research awards fetcher
===================================================================
No API key required (fully public NIH API).
Calls nih_fetch.fetch_health_awards() for the heavy logic.
Stores results in S3 and returns a summary.

Payload structure stored in S3:
  fetched_at   str          ISO-8601 UTC timestamp
  source_api   str          API base URL
  fetch_params dict         Echo of all resolved input parameters
  data         dict         Fetch results:
    projects               list[dict]
    total_fetched          int
    by_search              dict[str, int]
    duplicates_removed     int
    fiscal_years_queried   list[int]

Environment variables (required):
  AWS_REGION_NAME, S3_BUCKET, S3_FETCH_FOLDER

Event keys (all optional):
  fiscal_years    list[int]   FY to query.             Default: last 2 FY
  page_size       int         Records per request.     Default: 500
  max_records     int         Cap per search.          Default: 2000
  mode            str         "all" | "categories" |
                               "daly_text" | "disability_text"
                                                        Default: "all"
  include_active  bool        Include active projects. Default: True
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from nih_fetch import fetch_health_awards
from lambda_utils import save_to_s3, ok_response, error_response

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
REGION    = os.environ["AWS_REGION_NAME"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ["S3_FETCH_FOLDER"]
S3_KEY    = "nih_awards"
SOURCE_API = "api.reporter.nih.gov/v2/projects/search"


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Parameters ---------------------------------------------------------
    fiscal_years   =      event.get("fiscal_years")              # None → auto last 2 FY
    page_size      = int( event.get("page_size",      100))
    max_records    = int( event.get("max_records",   200))
    mode           =      event.get("mode",          "all")
    include_active = bool(event.get("include_active", True))

    log.info(
        "Starting fetch — fiscal_years=%s  page_size=%d  max_records=%d  "
        "mode=%s  include_active=%s",
        fiscal_years, page_size, max_records, mode, include_active,
    )

    # -- 2. Fetch --------------------------------------------------------------
    try:
        result = fetch_health_awards(
            fiscal_years   = fiscal_years,
            page_size      = page_size,
            max_records    = max_records,
            mode           = mode,
            include_active = include_active,
        )
    except Exception as exc:
        log.error("fetch_health_awards failed: %s", exc)
        return error_response(500, f"Fetch failed: {exc}")

    # -- 3. Build unified payload ----------------------------------------------
    payload = {
        "fetched_at":  now.isoformat(),
        "source_api":  SOURCE_API,
        "fetch_params": {
            "fiscal_years":   fiscal_years,
            "page_size":      page_size,
            "max_records":    max_records,
            "mode":           mode,
            "include_active": include_active,
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
        "fetched_at":           payload["fetched_at"],
        "source_api":           payload["source_api"],
        "s3_uri":               s3_uri,
        "total_fetched":        data["total_fetched"],
        "by_search":            data["by_search"],
        "duplicates_removed":   data["duplicates_removed"],
        "fiscal_years_queried": data["fiscal_years_queried"],
    }

    log.info(
        "Done — %d projects across %d searches saved to %s",
        data["total_fetched"], len(data["by_search"]), s3_uri,
    )
    return ok_response(summary)
