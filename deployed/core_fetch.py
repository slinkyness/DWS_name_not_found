"""
core_fetch.py — CORE API fetch logic
=====================================
Fetches works and outputs from the CORE API v3.
Handles pagination and retries automatically.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

log = logging.getLogger(__name__)

BASE_URL   = "https://api.core.ac.uk/v3"
ENTITIES   = ["works", "outputs"]
MAX_RETRY  = 3
RETRY_WAIT = 5  # seconds between retries on 500


def fetch_core_data(
    api_key:       str,
    limit:         int       = 100,
    max_pages:     int       = 10,
    entity_filter: list[str] | None = None,
) -> dict[str, Any]:
    """Fetch works and/or outputs from CORE API and return combined result."""

    entities = entity_filter or ENTITIES
    all_works   = []
    all_outputs = []

    for entity in entities:
        if entity not in ENTITIES:
            log.warning("Unknown entity '%s', skipping.", entity)
            continue

        log.info("Fetching entity: %s", entity)
        records = _fetch_entity(api_key, entity, limit, max_pages)

        if entity == "works":
            all_works = records
        elif entity == "outputs":
            all_outputs = records

    return {
        "works":         all_works,
        "outputs":       all_outputs,
        "total_works":   len(all_works),
        "total_outputs": len(all_outputs),
    }


def _fetch_entity(
    api_key:   str,
    entity:    str,
    limit:     int,
    max_pages: int,
) -> list[dict]:
    """Paginate through a single CORE entity endpoint."""

    records = []
    offset  = 0

    for page in range(max_pages):
        log.info("  %s — page %d (offset=%d)", entity, page + 1, offset)

        response = _get_with_retry(
            url     = f"{BASE_URL}/{entity}",
            api_key = api_key,
            params  = {"limit": limit, "offset": offset},
        )

        if response is None:
            log.error("  Failed to fetch %s at offset %d, stopping.", entity, offset)
            break

        results = response.get("results", [])
        if not results:
            log.info("  No more results for %s.", entity)
            break

        records.extend(results)
        offset += limit

        # Stop if we received fewer results than requested (last page)
        if len(results) < limit:
            log.info("  Last page reached for %s.", entity)
            break

    log.info("  Total %s fetched: %d", entity, len(records))
    return records


def _get_with_retry(
    url:     str,
    api_key: str,
    params:  dict,
) -> dict | None:
    """GET request with up to MAX_RETRY retries on server errors."""

    headers = {"Authorization": f"Bearer {api_key}"}

    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 500:
                log.warning("  HTTP 500 (attempt %d/%d), retrying in %ds...",
                            attempt, MAX_RETRY, RETRY_WAIT)
                time.sleep(RETRY_WAIT)
            else:
                log.error("  HTTP %d: %s", resp.status_code, resp.text[:200])
                return None

        except requests.exceptions.RequestException as exc:
            log.warning("  Request error (attempt %d/%d): %s", attempt, MAX_RETRY, exc)
            time.sleep(RETRY_WAIT)

    return None