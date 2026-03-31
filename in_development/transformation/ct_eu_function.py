"""
ct_eu_function.py — AWS Lambda: EU CTIS clinical trials fetcher
================================================================
No API key required (fully public CTIS API).
Calls ct_eu_fetch.fetch_health_trials() for the heavy logic.
Stores results in S3 and returns a summary.

Payload structure stored in S3:
  fetched_at   str          ISO-8601 UTC timestamp
  source_api   str          API base URL
  fetch_params dict         Echo of all resolved input parameters
  data         dict         Fetch results:
    trials               list[dict]
    total_fetched        int
    by_group             dict[str, int]
    duplicates_removed   int
    active_only          bool

Environment variables (required):
  AWS_REGION_NAME, S3_BUCKET, S3_FETCH_FOLDER

Event keys (all optional):
  mode         str    "all" or single group label.    Default: "all"
  page_size    int    Results per page, max 100.       Default: 100
  max_records  int    Cap per query group.             Default: 500
  active_only  bool   Filter to status {1,2,8}.        Default: True
  detail_fetch bool   Fetch full trial detail.         Default: False

Valid group labels:
  mental_health, disability, cancer, chronic_disease, rare_neurological

Status codes when active_only=True:
  1=Authorised  2=Ongoing  8=Authorised (recruitment not started)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from ct_eu_fetch import fetch_health_trials
from lambda_utils import save_to_s3, ok_response, error_response

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
REGION    = os.environ["AWS_REGION_NAME"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ["S3_FETCH_FOLDER"]
S3_KEY    = "ctis_europe"
SOURCE_API = "euclinicaltrials.eu/ctis-public-api"


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Parameters ---------------------------------------------------------
    mode         =      event.get("mode",         "all")
    page_size    = int( event.get("page_size",     100))
    max_records  = int( event.get("max_records",   500))
    active_only  = bool(event.get("active_only",   True))
    detail_fetch = bool(event.get("detail_fetch",  False))

    log.info(
        "Starting fetch — mode=%s  page_size=%d  max_records=%d  "
        "active_only=%s  detail_fetch=%s",
        mode, page_size, max_records, active_only, detail_fetch,
    )

    # -- 2. Fetch --------------------------------------------------------------
    try:
        result = fetch_health_trials(
            active_only  = active_only,
            page_size    = page_size,
            max_records  = max_records,
            mode         = mode,
            detail_fetch = detail_fetch,
        )
    except Exception as exc:
        log.error("fetch_health_trials failed: %s", exc)
        return error_response(500, f"Fetch failed: {exc}")

    # -- 3. Build unified payload ----------------------------------------------
    payload = {
        "fetched_at":  now.isoformat(),
        "source_api":  SOURCE_API,
        "fetch_params": {
            "mode":         mode,
            "page_size":    page_size,
            "max_records":  max_records,
            "active_only":  active_only,
            "detail_fetch": detail_fetch,
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
        "fetched_at":         payload["fetched_at"],
        "source_api":         payload["source_api"],
        "s3_uri":             s3_uri,
        "total_fetched":      data["total_fetched"],
        "by_group":           data["by_group"],
        "duplicates_removed": data["duplicates_removed"],
        "active_only":        data["active_only"],
    }

    log.info(
        "Done — %d trials across %d groups saved to %s",
        data["total_fetched"], len(data["by_group"]), s3_uri,
    )
    return ok_response(summary)
