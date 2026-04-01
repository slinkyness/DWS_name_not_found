from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

# -- Logging -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# -- Constants -----------------------------------------------------------------
CURRENTS_BASE  = "https://api.currentsapi.services/v1/search"
TIMEOUT        = 15

HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "Currents-HealthFetcher/1.0",
}

# -- Topic keyword map ---------------------------------------------------------
# Used for CLIENT-SIDE tagging after fetching the broad category pages.
# Each entry: label -> list of phrases (checked against lowercased
# title + description).  First matching label wins; articles that match
# none fall through to the category-level fallback label.

TOPIC_KEYWORDS: dict[str, list[str]] = {
    # Mental health
    "mental_health":      ["mental health", "mental illness", "psychiatric",
                           "depression", "anxiety disorder", "bipolar",
                           "schizophrenia", "suicide prevention", "eating disorder",
                           "ptsd", "psychological wellbeing"],
    # Disability & accessibility
    "disability":         ["disability", "disabled", "accessibility",
                           "assistive technology", "neurodiversity",
                           "autism spectrum", "adhd", "learning disability",
                           "chronic pain", "invisible illness", "wheelchair"],
    # Cancer & oncology
    "cancer_research":    ["cancer", "oncology", "tumour", "tumor",
                           "chemotherapy", "immunotherapy", "leukaemia",
                           "melanoma", "cancer screening", "oncologist"],
    # Rare & chronic disease
    "rare_chronic":       ["rare disease", "orphan drug", "chronic illness",
                           "multiple sclerosis", "parkinson", "alzheimer",
                           "cystic fibrosis", "sickle cell", "lupus",
                           "rheumatoid arthritis", "fibromyalgia", "dementia"],
    # Neurology & brain
    "neurology":          ["neurology", "neuroscience", "brain health", "stroke",
                           "epilepsy", "traumatic brain injury", "spinal cord",
                           "neurodegeneration", "cognitive decline", "brain tumour"],
    # Healthcare policy
    "healthcare_policy":  ["healthcare policy", "nhs", "universal health coverage",
                           "health insurance", "medical reform", "patient rights",
                           "health equity", "social determinants", "drug pricing",
                           "pharmaceutical regulation", "health funding"],
    # Medical research (science category articles mostly land here)
    "medical_research":   ["clinical trial", "drug discovery", "genomics",
                           "gene therapy", "biomarker", "precision medicine",
                           "mrna", "crispr", "ai in healthcare", "digital health",
                           "medical research", "randomised controlled"],
    # Core / general public health (broad sweep more specific labels above take priority)
    "core_health":        ["public health", "global health", "health crisis",
                           "disease outbreak", "pandemic", "epidemic",
                           "vaccination", "immunisation", "infectious disease",
                           "who ", "preventive care"],
}

# The two category requests we fire.
# (label used in logs, category param sent to API)
CATEGORY_REQUESTS: list[tuple[str, str]] = [
    ("health",  "health"),
    ("science", "science"),
    ("technology", "technology"),
]


# -- HTTP helper ---------------------------------------------------------------

def get(params: dict) -> tuple[dict, dict]:
    """
    GET CURRENTS_BASE with params.
    Returns (parsed_json, rate_limit_headers).  Raises on HTTP error.
    """
    log.info("GET %s  params=%s", CURRENTS_BASE,
             {k: v for k, v in params.items() if k != "apiKey"})
    resp = requests.get(CURRENTS_BASE, headers=HEADERS, params=params, timeout=TIMEOUT)
    rate = {
        "remaining": resp.headers.get("X-RateLimit-Remaining", "?"),
        "limit":     resp.headers.get("X-RateLimit-Limit",     "?"),
    }
    resp.raise_for_status()
    return resp.json(), rate


# -- Formatters ----------------------------------------------------------------

def format_article(raw: dict, topic: str) -> dict:
    """Clean dict from a raw Currents API article record."""
    return {
        "id":           raw.get("id", ""),
        "topic":        topic,
        "title":        (raw.get("title") or "").strip(),
        "description":  raw.get("description", ""),
        "url":          raw.get("url", ""),
        "image_url":    raw.get("image", ""),
        "published_at": raw.get("published", ""),
        "author":       raw.get("author", ""),
        "category":     raw.get("category", []),
        "language":     raw.get("language", ""),
    }


# -- Client-side topic tagger --------------------------------------------------

def _tag_article(title: str, description: str, fallback_label: str) -> str:
    """
    Assign a topic label by scanning title + description for keyword phrases.

    Checks TOPIC_KEYWORDS in definition order; returns the label for the
    first phrase that matches.  Falls back to fallback_label (e.g.
    "health_general" or "science_general") when nothing matches.
    """
    haystack = (title + " " + (description or "")).lower()
    for label, phrases in TOPIC_KEYWORDS.items():
        if any(phrase in haystack for phrase in phrases):
            return label
    return fallback_label

def fetch_health_news(
    api_key: str,
    days_back: int = 7,
    language: str = "en",
    page_size: int = 200,
    topic_filter: list[str] | None = None,
) -> dict[str, Any]:
    """
    Fetch health-related articles using exactly 2 API requests.

    Request 1: category=health,  page_size up to 200
    Request 2: category=science, page_size up to 200

    Articles are tagged client-side via TOPIC_KEYWORDS and deduplicated
    on article id (primary) then url (fallback).

    Args:
        api_key:   Currents API key.
        days_back: Articles published within the last N days.
        language:  Language code (default "en").
        page_size: Articles per request (max 200).
        topic_filer: categories to look, if none look for all 3

    Returns dict with keys: articles, total_fetched, by_topic,
                            duplicates_removed, requests_used,
                            rate_limit, date_range.
    """
    now        = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%S.00+00:00")
    end_date   = now.strftime("%Y-%m-%dT%H:%M:%S.00+00:00")

    seen_ids:     set[str]   = set()
    seen_urls:    set[str]   = set()
    all_articles: list[dict] = []
    by_topic:     dict[str, int] = {}
    total_dupes   = 0
    last_rate:    dict = {}
    requests_used = 0

    active_categories = [
        (log_label, category)
        for log_label, category in CATEGORY_REQUESTS
        if topic_filter is None or log_label in topic_filter
    ]
    log.info(
        "topic_filter=%s  ->  running %d / %d categories: %s",
        topic_filter,
        len(active_categories),
        len(CATEGORY_REQUESTS),
        [c for _, c in active_categories],
    )

    for log_label, category in active_categories:
        params: dict[str, Any] = {
            "apiKey":     api_key,
            "category":   category,
            "language":   language,
            "type":       1,
            "start_date": start_date,
            "end_date":   end_date,
            "page_size":  page_size,
        }

        try:
            data, rate = get(params)
            last_rate  = rate
            requests_used += 1
            log.info(
                "  category=%-8s  rate limit: %s / %s remaining",
                category, rate["remaining"], rate["limit"],
            )
        except requests.HTTPError as exc:
            log.warning("HTTP %s on category=%s: %s",
                        exc.response.status_code, category, exc)
            continue
        except Exception as exc:
            log.warning("Request failed category=%s: %s", category, exc)
            continue

        if data.get("status") != "ok":
            log.warning("API error category=%s: %s", category, data.get("message", data))
            continue

        fallback = f"{log_label}_general"
        accepted = 0

        for raw in data.get("news", []):
            article_id  = raw.get("id", "")
            article_url = raw.get("url", "")

            # Deduplicate: id first, then url
            if (article_id and article_id in seen_ids) or \
               (article_url and article_url in seen_urls):
                total_dupes += 1
                continue

            if article_id:  seen_ids.add(article_id)
            if article_url: seen_urls.add(article_url)

            # Tag client-side
            title       = (raw.get("title") or "").strip()
            description = raw.get("description", "")
            topic       = _tag_article(title, description, fallback)

            all_articles.append(format_article(raw, topic))
            by_topic[topic] = by_topic.get(topic, 0) + 1
            accepted += 1

        log.info("  -> %d unique articles accepted (category: %s)", accepted, category)

    log.info(
        "Done -- %d articles, %d dupes removed, %d API requests used",
        len(all_articles), total_dupes, requests_used,
    )

    return {
        "articles":           all_articles,
        "total_fetched":      len(all_articles),
        "by_topic":           by_topic,
        "duplicates_removed": total_dupes,
        "requests_used":      requests_used,
        "rate_limit":         last_rate,
        "date_range":         {"from": start_date, "to": end_date},
    }