from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import icd_fetch
from lambda_utils import get_secret, save_to_s3, ok_response, error_response

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
REGION          = os.environ["AWS_REGION_NAME"]
S3_BUCKET       = os.environ["S3_BUCKET"]
SECRET_NAME     = os.environ["SECRET_NAME"]
S3_PREFIX       = os.environ["S3_FETCH_FOLDER"]
SECRET_ID       = "ICD_API_CLIENT_ID"
SECRET_SECRET   = "ICD_API_CLIENT_SECRET"
S3_KEY          = "icd_api"
SOURCE_API      = "id.who.int/icd/"

def lambda_handler(event: dict, context: Any) -> dict:
    now = datetime.now(timezone.utc)

    # -- 1. Secret -------------------------------------------------------------
    try:
        icd_fetch.ICD_API_CLIENT_ID = get_secret(SECRET_NAME, REGION, SECRET_ID)
        icd_fetch.ICD_API_CLIENT_SECRET = get_secret(SECRET_NAME, REGION, SECRET_SECRET)
    except Exception as exc:
        log.error("Secret retrieval failed: %s", exc)
        return error_response(500, f"Secret retrieval failed: {exc}")

    # -- 2. Parameters ---------------------------------------------------------
    action       = event.get("action",  os.environ.get("ICD_ACTION",  "query_details"))
    query        = event.get("query", os.environ.get("ICD_ACTION",  "Depression"))
    entity_uri   = event.get("uri", os.environ.get("ICD_URIS", []))

    log.info(
        "Starting fetch — mode=%s  query=%s  entity=%s",
        action, query, entity_uri
    )

    # -- 3. Fetch --------------------------------------------------------------
    try:
        if action == "query_details":
            result = icd_fetch.get_query_details(query)
        elif action == "entity_detail":
            result = icd_fetch.get_entity_details(entity_uri)
        else:
            raise ValueError(f"Unknown action: '{action}'. "
                             "Choose from: get_query_details, search_entity")
    except Exception as exc:
        log.error("fetch_health_news failed: %s", exc)
        return error_response(500, f"Fetch failed: {exc}")

    # -- 4. Build unified payload ----------------------------------------------
    payload = {
        "fetched_at":  now.isoformat(),
        "source_api":  SOURCE_API,
        "fetch_params": {
            "action": action,
            "query": query,
            "entity_uri": entity_uri,
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
    summary = {
        "fetched_at":         payload["fetched_at"],
        "source_api":         payload["source_api"],
        "s3_uri":             s3_uri,
        "total_fetched":      len(result),
    }

    log.info(
        "Done — %d fetched items, saved to %s",
        len(result), s3_uri,
    )
    return ok_response(summary)
