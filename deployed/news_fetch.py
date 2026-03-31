"""
NewsAPI — Health & Medical News Fetcher
========================================
Endpoint : GET https://newsapi.org/v2/everything
Auth     : NEWSAPI_KEY environment variable (passed as apiKey query param)

────────────────────────────────────────────────────────────────────────
QUERY STRATEGY
────────────────────────────────────────────────────────────────────────

  /v2/everything supports boolean q syntax:
    AND / OR / NOT  +  "quoted phrases"  +  (grouping)
    Max 500 chars per q. searchIn= restricts to title,description.

  We define TOPIC GROUPS — one request per group — then merge and
  deduplicate on URL.  This keeps each q short and lets you enable
  or disable topics via topic_filter in the Lambda event.

  Topic groups:
    core_health         → public health, outbreaks, WHO, vaccines
    mental_health       → depression, anxiety, PTSD, psychiatry
    disability          → accessibility, autism, ADHD, chronic pain
    cancer_research     → oncology, chemotherapy, immunotherapy
    rare_chronic        → MS, Parkinson's, Alzheimer's, rare disease
    neurology           → stroke, dementia, TBI, neuroscience
    healthcare_policy   → NHS, UHC, drug pricing, health equity
    medical_research    → clinical trials, CRISPR, AI in healthcare

────────────────────────────────────────────────────────────────────────
KEY API FACTS  (confirmed from docs)
────────────────────────────────────────────────────────────────────────

  pageSize max : 100
  sortBy       : publishedAt | relevancy | popularity
  from / to    : ISO 8601  (e.g. 2024-01-01 or 2024-01-01T00:00:00)
  language     : 2-letter ISO-639-1  (e.g. "en")
  searchIn     : title | description | content  (comma-separated)
  Response     : {status, totalResults, articles:[{source,author,title,
                  description,url,urlToImage,publishedAt,content}]}

────────────────────────────────────────────────────────────────────────
AWS LAMBDA
────────────────────────────────────────────────────────────────────────

  Handler : newsapi_lambda.lambda_handler
  Runtime : Python 3.12  |  Memory: 256 MB  |  Timeout: 60 s

  Environment variables:
    NEWS_API           required — your NewsAPI key
    NEWSAPI_DAYS_BACK  optional — lookback window in days  (default 7)
    NEWSAPI_LANGUAGE   optional — ISO-639-1 code           (default "en")
    NEWSAPI_PAGE_SIZE  optional — results per page, max 100 (default 100)
    NEWSAPI_MAX_PAGES  optional — pages per topic group    (default 1)

  Example event payloads:
    {}
    {"days_back": 3}
    {"topic_filter": ["mental_health", "cancer_research"], "days_back": 14}
    {"max_pages": 2, "language": "en"}

Usage (local):
    export NEWSAPI_KEY=your_key_here
    pip install requests
    python newsapi_health_fetcher.py
"""

from __future__ import annotations


import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
NEWSAPI_BASE = "https://newsapi.org/v2/everything"
TIMEOUT      = 15

HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "NewsAPI-HealthFetcher/1.0",
}

# ── Topic groups ───────────────────────────────────────────────────────────────
# Each entry: (label, q_string)
# q supports AND / OR / NOT / "phrases" / (grouping). Max 500 chars URL-encoded.

TOPIC_GROUPS: list[tuple[str, str]] = [
    (
        "core_health",
        (
            '("public health" OR "global health" OR "health crisis" OR '
            '"disease outbreak" OR "pandemic" OR "epidemic" OR '
            '"vaccination" OR "WHO" OR "infectious disease" OR "preventive care")'
        ),
    ),
    (
        "mental_health",
        (
            '("mental health" OR "mental illness" OR "psychiatry" OR '
            '"depression" OR "anxiety disorder" OR "bipolar disorder" OR '
            '"schizophrenia" OR "suicide prevention" OR "eating disorder" OR '
            '"PTSD" OR "psychological wellbeing")'
        ),
    ),
    (
        "disability",
        (
            '("disability" OR "disabled" OR "accessibility" OR '
            '"assistive technology" OR "neurodiversity" OR '
            '"autism spectrum" OR "ADHD" OR "learning disability" OR '
            '"chronic pain" OR "invisible illness")'
        ),
    ),
    (
        "cancer_research",
        (
            '("cancer research" OR "oncology" OR "tumour" OR "tumor" OR '
            '"chemotherapy" OR "immunotherapy" OR "cancer clinical trial" OR '
            '"breast cancer" OR "lung cancer" OR "cancer screening" OR '
            '"leukaemia" OR "melanoma")'
        ),
    ),
    (
        "rare_chronic",
        (
            '("rare disease" OR "orphan drug" OR "chronic illness" OR '
            '"multiple sclerosis" OR "Parkinson" OR "Alzheimer" OR '
            '"cystic fibrosis" OR "sickle cell" OR "lupus" OR '
            '"rheumatoid arthritis" OR "fibromyalgia")'
        ),
    ),
    (
        "neurology",
        (
            '("neurology" OR "neuroscience" OR "brain health" OR "stroke" OR '
            '"dementia" OR "epilepsy" OR "traumatic brain injury" OR '
            '"spinal cord" OR "neurodegeneration" OR "cognitive decline")'
        ),
    ),
    (
        "healthcare_policy",
        (
            '("healthcare policy" OR "NHS" OR "universal health coverage" OR '
            '"health insurance" OR "medical reform" OR "patient rights" OR '
            '"health equity" OR "social determinants of health" OR '
            '"drug pricing" OR "pharmaceutical regulation")'
        ),
    ),
    (
        "medical_research",
        (
            '("medical research" OR "clinical trial" OR "drug discovery" OR '
            '"genomics" OR "gene therapy" OR "biomarker" OR '
            '"precision medicine" OR "mRNA" OR "CRISPR" OR '
            '"AI in healthcare" OR "digital health")'
        ),
    ),
]


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _get(params: dict) -> dict:
    """GET NEWSAPI_BASE with params, raise on HTTP error, return parsed JSON."""
    log.info("GET %s  params=%s", NEWSAPI_BASE, {k: v for k, v in params.items() if k != "apiKey"})
    resp = requests.get(NEWSAPI_BASE, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ── Formatters ─────────────────────────────────────────────────────────────────

def format_article(raw: dict, topic: str) -> dict:
    """Clean dict from a raw NewsAPI article record."""
    return {
        "topic":        topic,
        "source":       raw.get("source", {}).get("name", ""),
        "author":       raw.get("author", ""),
        "title":        (raw.get("title") or "").strip(),
        "description":  raw.get("description", ""),
        "url":          raw.get("url", ""),
        "image_url":    raw.get("urlToImage", ""),
        "published_at": raw.get("publishedAt", ""),
        "content":      raw.get("content", ""),   # truncated to 200 chars by API
    }


# ── Fetchers ───────────────────────────────────────────────────────────────────

def fetch_topic(
    api_key: str,
    label: str,
    q: str,
    from_date: str,
    to_date: str,
    language: str,
    page_size: int,
    max_pages: int,
    seen_urls: set[str],
) -> tuple[list[dict], int]:
    """
    Fetch one topic group across up to max_pages pages.
    Returns (articles, duplicate_count).
    Skips URLs already present in seen_urls and updates the set in place.
    """
    articles: list[dict] = []
    duplicates = 0

    for page in range(1, max_pages + 1):
        params: dict[str, Any] = {
            "apiKey":   api_key,
            "q":        q,
            "from":     from_date,
            "to":       to_date,
            "language": language,
            "sortBy":   "publishedAt",
            "pageSize": page_size,
            "page":     page,
            "searchIn": "title,description",
        }
        try:
            data = _get(params)
        except requests.HTTPError as exc:
            log.warning("HTTP %s on topic=%s page=%d: %s",
                        exc.response.status_code, label, page, exc)
            break
        except Exception as exc:
            log.warning("Request failed topic=%s page=%d: %s", label, page, exc)
            break

        if data.get("status") != "ok":
            log.warning("API error topic=%s: %s", label, data.get("message"))
            break

        raw_articles = data.get("articles", [])
        if not raw_articles:
            break

        for raw in raw_articles:
            url = raw.get("url", "")
            if url in seen_urls:
                duplicates += 1
                continue
            seen_urls.add(url)
            articles.append(format_article(raw, label))

        if page * page_size >= data.get("totalResults", 0):
            break   # all available pages consumed

    return articles, duplicates


def fetch_health_news(
    api_key: str,
    days_back: int = 7,
    language: str = "en",
    page_size: int = 100,
    max_pages: int = 1,
    topic_filter: list[str] | None = None,
) -> dict[str, Any]:
    """
    Fetch health-related articles across all topic groups.

    Args:
        api_key:       NewsAPI key.
        days_back:     Articles published within the last N days.
        language:      ISO-639-1 language code.
        page_size:     Results per page (max 100).
        max_pages:     Pages to fetch per topic group (free plan: 1).
        topic_filter:  If set, only fetch these topic labels.

    Returns dict with keys: articles, total_fetched, by_topic,
                            duplicates_removed, date_range.
    """
    now       = datetime.now(timezone.utc)
    from_date = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%S")
    to_date   = now.strftime("%Y-%m-%dT%H:%M:%S")

    seen_urls:    set[str]   = set()
    all_articles: list[dict] = []
    by_topic:     dict[str, int] = {}
    total_dupes   = 0

    groups = [
        (label, q) for label, q in TOPIC_GROUPS
        if topic_filter is None or label in topic_filter
    ]

    for label, q in groups:
        log.info("Fetching topic: %s", label)
        articles, dupes = fetch_topic(
            api_key, label, q, from_date, to_date,
            language, page_size, max_pages, seen_urls,
        )
        all_articles.extend(articles)
        by_topic[label] = len(articles)
        total_dupes    += dupes
        log.info("  → %d unique articles (topic: %s)", len(articles), label)

    return {
        "articles":           all_articles,
        "total_fetched":      len(all_articles),
        "by_topic":           by_topic,
        "duplicates_removed": total_dupes,
        "date_range":         {"from": from_date, "to": to_date},
    }