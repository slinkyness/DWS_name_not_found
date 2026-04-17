from __future__ import annotations

import logging
from typing import Any

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
NEWS_BASE = "https://www.who.int/api/news"
WHO_BASE  = "https://www.who.int"
TIMEOUT   = 15

HEADERS: dict[str, str] = {
    "Accept":     "application/json",
    "User-Agent": "WHO-News-Fetcher/1.0",
}

# OData params applied to all pageable endpoints
ODATA_PARAMS: dict[str, Any] = {
    "sf_culture": "en",
    "$orderby":   "PublicationDateAndTime desc",
    "$top":       20,
}

# Endpoints that must be called without query parameters
_NO_PARAMS: frozenset[str] = frozenset({"emergencies"})


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _get(endpoint: str, params: dict | None = None) -> list | dict:
    """GET {NEWS_BASE}/{endpoint}, unwrap OData 'value' arrays."""
    url    = f"{NEWS_BASE}/{endpoint}"
    merged = {} if endpoint in _NO_PARAMS else {**ODATA_PARAMS, **(params or {})}
    log.info("GET %s  params=%s", url, merged or "(none)")
    resp = requests.get(url, headers=HEADERS, params=merged, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    return data["value"] if isinstance(data, dict) and "value" in data else data


# ── URL helper ─────────────────────────────────────────────────────────────────

def _full_url(path: str) -> str:
    if not path:
        return ""
    return path if path.startswith("http") else f"{WHO_BASE}{path}"


# ── Formatters ─────────────────────────────────────────────────────────────────

def _fmt_newsitem(item: dict) -> dict:
    return {
        "title":      item.get("Title", "").strip(),
        "news_type":  item.get("NewsType", ""),
        "published":  item.get("FormatedDate") or item.get("PublicationDateAndTime", ""),
        "url":        _full_url(item.get("ItemDefaultUrl", "")),
        "source_key": item.get("SystemSourceKey", ""),
        "provider":   item.get("Provider", ""),
    }


def _fmt_don(item: dict) -> dict:
    return {
        "title":     item.get("Title", "").strip(),
        "published": item.get("PublicationDateAndTime") or item.get("FormatedDate", ""),
        "url":       _full_url(item.get("ItemDefaultUrl", "")),
        "summary":   item.get("Summary", ""),
        "provider":  item.get("Provider", ""),
    }


def _fmt_emergency(item: dict) -> dict:
    return {
        "title":      item.get("Title", "").strip(),
        "published":  item.get("PublicationDate", ""),
        "start_date": item.get("EmergencyStartDate", ""),
        "end_date":   item.get("EmergencyEndDate", ""),
        "rating":     item.get("EmergencyRatingTextual", ""),
        "summary":    item.get("Summary", "") or item.get("Overview", ""),
        "url":        _full_url(item.get("ItemDefaultUrl", "")),
        "provider":   item.get("Provider", ""),
    }


# ── Fetchers ───────────────────────────────────────────────────────────────────

def _fetch_general(top: int = 20) -> list[dict]:
    return [_fmt_newsitem(r) for r in _get("newsitems", {"$top": top})]


def _fetch_don(top: int = 20) -> list[dict]:
    return [_fmt_don(r) for r in _get("diseaseoutbreaknews", {"$top": top})]


def _fetch_emergencies(top: int = 10) -> list[dict]:
    raw = sorted(
        _get("emergencies"),
        key=lambda x: x.get("PublicationDate") or "",
        reverse=True,
    )
    return [_fmt_emergency(r) for r in raw[:top]]


# ── Public entry point ─────────────────────────────────────────────────────────

def fetch_all_news(
    general_top:   int = 20,
    don_top:       int = 10,
    emergency_top: int = 10,
) -> dict[str, Any]:
    """
    Aggregate news from all relevant WHO endpoints.

    Returns dict with:
      general_news           list[dict]
      disease_outbreak_news  list[dict]
      emergencies            list[dict]
      total_fetched          int
      by_category            dict[str, int]
    """
    general: list[dict] = []
    don:     list[dict] = []
    emerg:   list[dict] = []

    try:
        general = _fetch_general(top=general_top)
        log.info("  → %d general news items", len(general))
    except Exception as exc:
        log.warning("general_news fetch failed: %s", exc)

    try:
        don = _fetch_don(top=don_top)
        log.info("  → %d disease outbreak news items", len(don))
    except Exception as exc:
        log.warning("disease_outbreak_news fetch failed: %s", exc)

    try:
        emerg = _fetch_emergencies(top=emergency_top)
        log.info("  → %d emergency items", len(emerg))
    except Exception as exc:
        log.warning("emergencies fetch failed: %s", exc)

    by_category = {
        "general_news":          len(general),
        "disease_outbreak_news": len(don),
        "emergencies":           len(emerg),
    }
    total = len(general) + len(don) + len(emerg)

    log.info("Done — %d total WHO items", total)

    return {
        "general_news":          general,
        "disease_outbreak_news": don,
        "emergencies":           emerg,
        "total_fetched":         total,
        "by_category":           by_category,
    }
