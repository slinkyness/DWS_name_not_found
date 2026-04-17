from __future__ import annotations


import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from dotenv import load_dotenv
load_dotenv()
import requests

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
NEWSAPI_BASE = "https://newsapi.org/v2/top-headlines"
TIMEOUT      = 15

HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "NewsAPI-HealthFetcher/1.0",
}
_AUTH_HEADER = "X-Api-Key"

# ── Categories ───────────────────────────────────────────────────────────────
CATEGORY_REQUESTS: list[tuple[str, str]] = [
    ("health",     "health_general"),
    ("science",    "science_general"),
    ("technology", "technology_general"),
]

TOPIC_KEYWORDS: dict[str, list[str]] = {
    "core_health": [
        "public health", "global health", "health crisis", "disease outbreak",
        "pandemic", "epidemic", "vaccination", "vaccine", "who ",
        "infectious disease", "preventive care", "immunization",
    ],
    "mental_health": [
        "mental health", "mental illness", "psychiatry", "depression",
        "anxiety disorder", "bipolar disorder", "schizophrenia",
        "suicide prevention", "eating disorder", "ptsd",
        "psychological wellbeing", "psychotherapy",
    ],
    "disability": [
        "disability", "disabled", "accessibility", "assistive technology",
        "neurodiversity", "autism spectrum", "adhd", "learning disability",
        "chronic pain", "invisible illness",
    ],
    "cancer_research": [
        "cancer research", "oncology", "tumour", "tumor", "chemotherapy",
        "immunotherapy", "cancer clinical trial", "breast cancer",
        "lung cancer", "cancer screening", "leukaemia", "melanoma",
    ],
    "rare_chronic": [
        "rare disease", "orphan drug", "chronic illness",
        "multiple sclerosis", "parkinson", "alzheimer",
        "cystic fibrosis", "sickle cell", "lupus",
        "rheumatoid arthritis", "fibromyalgia",
    ],
    "neurology": [
        "neurology", "neuroscience", "brain health", "stroke",
        "dementia", "epilepsy", "traumatic brain injury",
        "spinal cord", "neurodegeneration", "cognitive decline",
    ],
    "healthcare_policy": [
        "healthcare policy", "nhs", "universal health coverage",
        "health insurance", "medical reform", "patient rights",
        "health equity", "social determinants of health",
        "drug pricing", "pharmaceutical regulation",
    ],
    "medical_research": [
        "medical research", "clinical trial", "drug discovery",
        "genomics", "gene therapy", "biomarker",
        "precision medicine", "mrna", "crispr",
        "ai in healthcare", "digital health",
    ],
}

# ── HTTP helper ────────────────────────────────────────────────────────────────

def _get(api_key: str, params: dict) -> dict:
    """GET NEWSAPI_BASE with params, raise on HTTP error, return parsed JSON."""
    log.info("GET %s  params=%s", NEWSAPI_BASE, params)
    headers = {**HEADERS, _AUTH_HEADER: api_key}
    resp = requests.get(NEWSAPI_BASE, headers=headers, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ── Formatters ─────────────────────────────────────────────────────────────────

def format_article(raw: dict, topic: str, category: str) -> dict:
    """Clean dict from a raw NewsAPI article record."""
    return {
        "topic":        topic,
        "category":     [category],
        "source":       raw.get("source", {}).get("name", ""),
        "author":       raw.get("author", ""),
        "title":        (raw.get("title") or "").strip(),
        "description":  raw.get("description", ""),
        "url":          raw.get("url", ""),
        "image_url":    raw.get("urlToImage", ""),
        "published_at": raw.get("publishedAt", ""),
        "content":      raw.get("content", ""),   # truncated to 200 chars by API
    }


def _tag_article(title: str, description: str, fallback_label: str) -> str:
    """
    Assign a fine-grained topic label by scanning title + description
    for keyword phrases from TOPIC_KEYWORDS.

    Checks entries in definition order; returns the label for the first
    phrase that matches.  Falls back to fallback_label when nothing matches
    (e.g. "health_general", "science_general", "technology_general").
    """
    haystack = (title + " " + (description or "")).lower()
    for label, phrases in TOPIC_KEYWORDS.items():
        if any(phrase in haystack for phrase in phrases):
            return label
    return fallback_label

# ── Fetchers ───────────────────────────────────────────────────────────────────

def fetch_category(
    api_key:        str,
    category:       str,
    fallback_label: str,
    page_size:      int,
    country:        str,
    url_map:        dict[str, dict],
) -> tuple[int, int, int]:
    """
    Fetch all available pages for one top-headlines category.

    Iterates pages until page * page_size >= totalResults or the API
    returns an empty article list (handles free-plan page cap gracefully).

    Args:
        api_key:        NewsAPI key.
        category:       Top-headlines category param (e.g. "health").
        fallback_label: Topic label for articles that match no keyword.
        page_size:      Results per page (max 100).
        country:        Optional ISO 3166-1 alpha-2 country code.
        url_map:        Shared {url: article_dict}; updated in place.

    Returns:
        (new_count, cross_category_count, pages_fetched)
    """
    new_count      = 0
    cross_category = 0
    pages_fetched = 0
    page = 1

    while True:
        params: dict[str, Any] = {
            "category": category,
            "pageSize": page_size,
            "page": page,
        }
        if country:
            params["country"] = country

        try:
            data = _get(api_key, params)
        except requests.HTTPError as exc:
            log.warning(
                "HTTP %s on category=%s page=%d: %s",
                exc.response.status_code, category, page, exc,
            )
            break
        except Exception as exc:
            log.warning("Request failed category=%s page=%d: %s", category, page, exc)
            break

        if data.get("status") != "ok":
            log.warning("API error category=%s: %s", category, data.get("message"))
            break

        raw_articles = data.get("articles", [])
        total_results = data.get("totalResults", 0)
        pages_fetched += 1

        if not raw_articles:
            log.info(
                "  category=%s page=%d — no articles returned, stopping",
                category, page,
            )
            break

        for raw in raw_articles:
            url = raw.get("url", "")
            if not url:
                continue
            if url in url_map:
                # Article already fetched from another category — just extend the list
                if category not in url_map[url]["category"]:
                    url_map[url]["category"].append(category)
                cross_category += 1
                continue
            topic = _tag_article(
                raw.get("title", ""),
                raw.get("description", ""),
                fallback_label,
            )
            url_map[url] = format_article(raw, topic, category)
            new_count += 1

        log.info(
            "  category=%s page=%d — %d new, %d cross-category "
            "(total_results=%d, url_map_size=%d)",
            category, page, new_count, cross_category,
            total_results, len(url_map),
        )

        # Stop when we have consumed all available results
        if page * page_size >= total_results:
            break

        page += 1

    return new_count, cross_category, pages_fetched


def fetch_health_news(
    api_key:         str,
    page_size:       int = 100,
    country:         str = "",
    category_filter: list[str] | None = None,
) -> dict[str, Any]:
    """
    Fetch health-related headlines across all configured categories.

    Makes one paginated request sequence per category (health, science,
    technology).  Each category is exhausted before moving to the next.
    Articles are tagged client-side and deduplicated on URL.

    Args:
        api_key:         NewsAPI key.
        page_size:       Results per page (max 100).
        country:         Optional ISO 3166-1 alpha-2 country code.
                         Leave empty for global results.
        category_filter: If set, only fetch these category labels
                         (e.g. ["health", "science"]).  Default: all.

    Returns dict with keys:
        articles           list[dict]
        total_fetched      int
        by_topic           dict[str, int]   fine-grained topic counts
        by_category        dict[str, int]   raw category counts
        duplicates_removed int
        requests_used      int
    """
    url_map:       dict[str, dict] = {}   # url -> article dict; shared across categories
    by_topic:      dict[str, int]  = {}
    by_category:   dict[str, int]  = {}
    total_cross    = 0
    total_requests = 0

    categories = [
        (cat, fallback) for cat, fallback in CATEGORY_REQUESTS
        if category_filter is None or cat in category_filter
    ]

    for category, fallback_label in categories:
        log.info("Fetching category: %s", category)
        new_count, cross, pages = fetch_category(
            api_key, category, fallback_label,
            page_size, country, url_map,
        )
        by_category[category] = new_count
        total_cross    += cross
        total_requests += pages
        log.info(
            "  → %d new articles from category=%s (%d cross-category, %d pages)",
            new_count, category, cross, pages,
        )

    all_articles = list(url_map.values())
    for article in all_articles:
        topic = article["topic"]
        by_topic[topic] = by_topic.get(topic, 0) + 1

    log.info(
        "Done — %d total articles (%d appear in multiple categories), "
        "%d API requests used",
        len(all_articles), total_cross, total_requests,
    )

    return {
        "articles":           all_articles,
        "total_fetched":      len(all_articles),
        "by_topic":           by_topic,
        "by_category":        by_category,
        "duplicates_removed": total_cross,
        "requests_used":      total_requests,
    }