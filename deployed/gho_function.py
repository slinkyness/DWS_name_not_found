"""
gho_function.py — AWS Lambda: WHO Global Health Observatory (GHO) fetcher
=========================================================================
No API key required (fully public OData API).
Calls gho_fetch.fetch_gho() for the heavy logic.
Stores results in S3 and returns a summary.

Payload structure stored in S3:
  fetched_at   str          ISO-8601 UTC timestamp
  source_api   str          API base URL
  fetch_params dict         Echo of all resolved input parameters
  data         dict         Fetch results:
    indicator_catalogue  list[dict]   Full GHO indicator list
    geo_catalogue        dict         All countries + regions
    indicators           dict         {code: fetch_indicator() result, ...}
    total_fetched        int          Total records across all indicators (after filter)
    total_indicators     int          Number of indicator codes fetched

Environment variables (required):
  AWS_REGION_NAME, S3_BUCKET, S3_FETCH_FOLDER

Env-var fallbacks (also accepted as event keys):
  GHO_INDICATORS        comma-separated indicator codes
  GHO_SEARCH            keyword search term
  GHO_YEAR              integer year
  GHO_REGION            region code
  GHO_SPATIAL_DIM_TYPE  spatial dim type filter

Event keys (all optional):
  input_indicators  list[str]        Explicit indicator codes to fetch
  search            str              Keyword for catalogue search
  year              int | list[int]  Client-side year filter
  region            str | list[str]  Client-side SpatialDim filter
  spatial_dim_type  str              Client-side SpatialDimType filter
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from gho_fetch import fetch_gho
from lambda_utils import save_to_s3, ok_response, error_response

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
REGION    = os.environ["AWS_REGION_NAME"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ["S3_FETCH_FOLDER"]
S3_KEY    = "gho"
SOURCE_API = "ghoapi.azureedge.net/api"


def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Parameters ---------------------------------------------------------
    input_indicators: list[str] = event.get("input_indicators") or []
    if not input_indicators:
        env_codes = os.environ.get("GHO_INDICATORS", "")
        input_indicators = [c.strip() for c in env_codes.split(",") if c.strip()]

    search           = event.get("search")           or os.environ.get("GHO_SEARCH")
    spatial_dim_type = event.get("spatial_dim_type") or os.environ.get("GHO_SPATIAL_DIM_TYPE")

    year_raw = event.get("year") or os.environ.get("GHO_YEAR")
    year: int | list[int] | None = (
        [int(y) for y in year_raw] if isinstance(year_raw, list)
        else int(year_raw)          if year_raw
        else None
    )
    region: str | list[str] | None = event.get("region") or os.environ.get("GHO_REGION")

    log.info(
        "Starting fetch — indicators=%s  search=%s  year=%s  region=%s  spatial_dim_type=%s",
        input_indicators, search, year, region, spatial_dim_type,
    )

    # -- 2. Fetch --------------------------------------------------------------
    try:
        result = fetch_gho(
            input_indicators = input_indicators,
            search           = search,
            year             = year,
            region           = region,
            spatial_dim_type = spatial_dim_type,
        )
    except requests.HTTPError as exc:
        log.error("HTTP error: %s", exc)
        return error_response(exc.response.status_code if exc.response else 500, str(exc))
    except Exception as exc:
        log.error("fetch_gho failed: %s", exc)
        return error_response(500, f"Fetch failed: {exc}")

    # -- 3. Build unified payload ----------------------------------------------
    payload = {
        "fetched_at":  now.isoformat(),
        "source_api":  SOURCE_API,
        "fetch_params": {
            "input_indicators": input_indicators,
            "search":           search,
            "year":             year,
            "region":           region,
            "spatial_dim_type": spatial_dim_type,
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
        "fetched_at":      payload["fetched_at"],
        "source_api":      payload["source_api"],
        "s3_uri":          s3_uri,
        "total_indicators": data["total_indicators"],
        "total_fetched":   data["total_fetched"],
        "indicators":      {c: v["total_after_filter"] for c, v in data["indicators"].items()},
    }

    log.info(
        "Done — %d indicator(s), %d total records saved to %s",
        data["total_indicators"], data["total_fetched"], s3_uri,
    )
    return ok_response(summary)


