"""
ct_us_function.py — AWS Lambda: ClinicalTrials.gov (US) trials fetcher
=======================================================================
No API key required (fully public API).
Calls ct_us_fetch.fetch_health_trials() for the heavy logic.
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
    status_filter        list[str]
    phase_filter         list[str] | None
    updated_since        str | None

Environment variables (required):
  AWS_REGION_NAME, S3_BUCKET, S3_FETCH_FOLDER

Event keys (all optional):
  status        list[str]   Trial statuses to include.      Default: RECRUITING,
                             NOT_YET_RECRUITING, ACTIVE_NOT_RECRUITING
  phase         list[str]   Phase filter.                   Default: all phases
  page_size     int         Records per page, max 1000.     Default: 200
  max_records   int         Cap per query group.            Default: 1000
  mode          str         "all" or single group label.    Default: "all"
  updated_since str         ISO date — only recently updated trials.

Valid group labels:
  mental_health, disability, cancer, chronic_disease, rare_neurological
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from ct_us_fetch import fetch_health_trials
from lambda_utils import save_to_s3, ok_response, error_response

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
REGION    = os.environ["AWS_REGION_NAME"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ["S3_FETCH_FOLDER"]
S3_KEY    = "clinical_trials_us"
SOURCE_API = "clinicaltrials.gov/api/v2/studies"


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Parameters ---------------------------------------------------------
    statuses      =     event.get("status")           # None → default statuses
    phase_filter  =     event.get("phase")            # None → all phases
    page_size     = int(event.get("page_size",   200))
    max_records   = int(event.get("max_records", 500))
    mode          =     event.get("mode",        "all")
    updated_since =     event.get("updated_since")    # None → no date filter

    log.info(
        "Starting fetch — statuses=%s  phase=%s  page_size=%d  "
        "max_records=%d  mode=%s  updated_since=%s",
        statuses, phase_filter, page_size, max_records, mode, updated_since,
    )

    # -- 2. Fetch --------------------------------------------------------------
    try:
        result = fetch_health_trials(
            statuses      = statuses,
            phase_filter  = phase_filter,
            page_size     = page_size,
            max_records   = max_records,
            mode          = mode,
            updated_since = updated_since,
        )
    except Exception as exc:
        log.error("fetch_health_trials failed: %s", exc)
        return error_response(500, f"Fetch failed: {exc}")

    # -- 3. Build unified payload ----------------------------------------------
    payload = {
        "fetched_at":  now.isoformat(),
        "source_api":  SOURCE_API,
        "fetch_params": {
            "statuses":     statuses,
            "phase_filter": phase_filter,
            "page_size":    page_size,
            "max_records":  max_records,
            "mode":         mode,
            "updated_since": updated_since,
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
        "status_filter":      data["status_filter"],
        "phase_filter":       data["phase_filter"],
        "updated_since":      data["updated_since"],
    }

    log.info(
        "Done — %d trials across %d groups saved to %s",
        data["total_fetched"], len(data["by_group"]), s3_uri,
    )
    return ok_response(summary)
