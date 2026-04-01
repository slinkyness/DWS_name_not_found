"""
cordis_function.py — AWS Lambda: CORDIS EU research grants fetcher
===================================================================
Reads CORDIS_API key from Secrets Manager (optional — falls back to CSV
bulk download mode if the key is absent or use_csv=True).
Calls cordis_fetch.fetch_health_grants() for the heavy logic.
Stores results in S3 and returns a summary.

Payload structure stored in S3:
  fetched_at   str          ISO-8601 UTC timestamp
  source_api   str          API base URL
  fetch_params dict         Echo of all resolved input parameters
  data         dict         Fetch results:
    projects             list[dict]
    total_fetched        int
    by_topic             dict[str, int]
    duplicates_removed   int
    programmes_queried   list[str]
    min_year_filter      int
    fetch_mode           str   "DET_API" | "CSV_BULK"

Environment variables (required):
  AWS_REGION_NAME, S3_BUCKET, SECRET_NAME, S3_FETCH_FOLDER

Event keys (all optional):
  mode         str         "all" or single topic label.     Default: "all"
  programmes   list[str]   ["HORIZON","H2020"] subset.      Default: both
  page_size    int         DET API results/page, max 50.    Default: 50
  max_records  int         Cap per (topic × programme).     Default: 300
  min_year     int         Min project start year.          Default: 2018
  use_csv      bool        Force CSV bulk download mode.     Default: False

Valid topic labels:
  mental_health, disability, cancer, rare_chronic, neurology_brain

Valid programme values:
  HORIZON  (Horizon Europe, 2021-2027)
  H2020    (Horizon 2020, 2014-2020)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from cordis_fetch import fetch_health_grants
from lambda_utils import get_secret, save_to_s3, ok_response, error_response

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
REGION      = os.environ["AWS_REGION_NAME"]
S3_BUCKET   = os.environ["S3_BUCKET"]
SECRET_NAME = os.environ["SECRET_NAME"]
S3_PREFIX   = os.environ["S3_FETCH_FOLDER"]
SECRET_KEY  = "CORDIS_API"
S3_KEY      = "cordis"
SOURCE_API  = "cordis.europa.eu/dataextractions/api"


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Secret (optional for CORDIS) ---------------------------------------
    api_key: str | None = None
    try:
        api_key = get_secret(SECRET_NAME, REGION, SECRET_KEY)
    except Exception as exc:
        log.warning("CORDIS API key unavailable (%s) — will use CSV fallback", exc)

    # -- 2. Parameters ---------------------------------------------------------
    mode        =      event.get("mode",        "all")
    programmes  =      event.get("programmes")               # None → both
    page_size   = int( event.get("page_size",   50))
    max_records = int( event.get("max_records", 300))
    min_year    = int( event.get("min_year",    2018))
    use_csv     = bool(event.get("use_csv",     False))

    if not api_key and not use_csv:
        log.warning(
            "CORDIS_API key not set — falling back to CSV bulk download. "
            "Register free at https://cordis.europa.eu/dataextractions/register"
        )

    log.info(
        "Starting fetch — mode=%s  programmes=%s  page_size=%d  "
        "max_records=%d  min_year=%d  use_csv=%s",
        mode, programmes, page_size, max_records, min_year, use_csv,
    )

    # -- 3. Fetch --------------------------------------------------------------
    try:
        result = fetch_health_grants(
            api_key     = api_key,
            programmes  = programmes,
            page_size   = page_size,
            max_records = max_records,
            mode        = mode,
            min_year    = min_year,
            use_csv     = use_csv,
        )
    except Exception as exc:
        log.error("fetch_health_grants failed: %s", exc)
        return error_response(500, f"Fetch failed: {exc}")

    # -- 4. Build unified payload ----------------------------------------------
    payload = {
        "fetched_at":  now.isoformat(),
        "source_api":  SOURCE_API,
        "fetch_params": {
            "mode":        mode,
            "programmes":  programmes,
            "page_size":   page_size,
            "max_records": max_records,
            "min_year":    min_year,
            "use_csv":     use_csv,
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
        "programmes_queried": data["programmes_queried"],
        "fetch_mode":         data["fetch_mode"],
        "min_year_filter":    data["min_year_filter"],
    }

    log.info(
        "Done — %d projects across %d topics (%s mode) saved to %s",
        data["total_fetched"], len(data["by_topic"]), data["fetch_mode"], s3_uri,
    )
    return ok_response(summary)
