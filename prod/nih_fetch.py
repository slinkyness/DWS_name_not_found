from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
NIH_BASE  = "https://api.reporter.nih.gov/v2/projects/search"
TIMEOUT   = 30
REQ_DELAY = 1.1   # NIH asks ≤ 1 req/s

HEADERS = {
    "Content-Type": "application/json",
    "Accept":       "application/json",
    "User-Agent":   "NIH-RePORTER-HealthFetcher/1.0",
}

INCLUDE_FIELDS: list[str] = [
    "ApplId", "ProjectNum", "ProjectTitle", "AbstractText", "PhrText",
    "FiscalYear", "ProjectStartDate", "ProjectEndDate",
    "AwardAmount", "DirectCostAmt", "IndirectCostAmt",
    "OrgName", "OrgCity", "OrgState", "OrgCountry",
    "PrincipalInvestigators", "ProgramOfficers",
    "AgencyCode", "ActivityCode", "FundingMechanism",
    "SpendingCategories", "SpendingCategoriesDesc",
    "RcDcTerms", "ProjectTerms", "ProjectDetailUrl",
    "AwardNoticeDate", "BudgetStart", "BudgetEnd",
]

# ── Spending categories ────────────────────────────────────────────────────────
SPENDING_CATEGORY_NAMES: dict[int, str] = {
    6: "Acquired Cognitive Impairment (incl. Alzheimer's)", 7: "Aging",
    14: "Brain Disorders", 16: "Behavioral and Social Science",
    26: "Cancer", 28: "Cardiovascular", 29: "Chronic Pain",
    31: "Clinical Research", 54: "Depression", 57: "Diabetes",
    72: "Health Disparities", 76: "HIV/AIDS", 85: "Infectious Diseases",
    87: "Injury (Trauma -- Accidents and Adverse Effects)", 89: "Kidney Disease",
    90: "Lung Disease", 92: "Mental Health", 96: "Neurological",
    98: "Nutrition", 100: "Obesity", 101: "Orphan Drug", 105: "Pediatric",
    108: "Physical Activity", 116: "Rare Diseases", 120: "Rehabilitation",
    124: "Sexually Transmitted Infections", 126: "Spinal Cord Injury",
    128: "Stroke", 138: "Tobacco", 141: "Traumatic Brain Injury (TBI)",
    146: "Vision Research", 167: "Global Health", 172: "Health Economics",
    199: "Substance Abuse",
}
HEALTH_SPENDING_CATEGORY_IDS = list(SPENDING_CATEGORY_NAMES.keys())

# ── Text search definitions ────────────────────────────────────────────────────
TEXT_SEARCHES: dict[str, dict[str, str]] = {
    "daly_text": {
        "operator":     "advanced",
        "search_field": "all",
        "search_text": (
            '"disability adjusted life years" OR "DALY" OR '
            '"burden of disease" OR "years lived with disability" OR '
            '"disability-adjusted life" OR "disease burden" OR '
            '"global burden of disease" OR "DALYs"'
        ),
    },
    "disability_text": {
        "operator":     "advanced",
        "search_field": "all",
        "search_text": (
            '"mental disability" OR "intellectual disability" OR '
            '"functional disability" OR "disability rights" OR '
            '"health-related quality of life" OR "HRQOL" OR '
            '"years of life lost" OR "premature mortality" OR '
            '"disability burden" OR "years lived with disability"'
        ),
    },
}


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _post(body: dict) -> dict:
    log.info("POST %s", NIH_BASE)
    resp = requests.post(NIH_BASE, headers=HEADERS,
                         data=json.dumps(body), timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ── Formatter ─────────────────────────────────────────────────────────────────

def _format_project(raw: dict) -> dict:
    pi_names = [p.get("full_name", "") for p in (raw.get("principal_investigators") or [])
                if p.get("full_name")]
    po_names = [p.get("full_name", "") for p in (raw.get("program_officers") or [])
                if p.get("full_name")]

    cat_descs = raw.get("spending_categories_desc") or ""
    if cat_descs and isinstance(cat_descs, str):
        cat_names = [c.strip() for c in cat_descs.split(";") if c.strip()]
    elif isinstance(cat_descs, list):
        cat_names = [str(d).strip() for d in cat_descs if d]
    else:
        cat_names = [
            SPENDING_CATEGORY_NAMES.get(c, f"Category {c}")
            for c in (raw.get("spending_categories") or [])
            if isinstance(c, int)
        ]

    return {
        "appl_id":            raw.get("appl_id"),
        "project_num":        raw.get("project_num", ""),
        "title":              (raw.get("project_title") or "").strip(),
        "abstract":           (raw.get("abstract_text") or "").strip(),
        "public_health_relevance": (raw.get("phr_text") or "").strip(),
        "fiscal_year":        raw.get("fiscal_year"),
        "project_start":      raw.get("project_start_date", ""),
        "project_end":        raw.get("project_end_date", ""),
        "award_amount":       raw.get("award_amount"),
        "direct_cost":        raw.get("direct_cost_amt"),
        "indirect_cost":      raw.get("indirect_cost_amt"),
        "org_name":           raw.get("org_name", ""),
        "org_city":           raw.get("org_city", ""),
        "org_state":          raw.get("org_state", ""),
        "org_country":        raw.get("org_country", ""),
        "principal_investigators": pi_names,
        "program_officers":   po_names,
        "agency_code":        raw.get("agency_code", ""),
        "activity_code":      raw.get("activity_code", ""),
        "funding_mechanism":  raw.get("funding_mechanism", ""),
        "spending_categories": cat_names,
        "rcdc_terms":         raw.get("rcdc_terms") or [],
        "project_terms":      raw.get("project_terms") or [],
        "award_notice_date":  raw.get("award_notice_date", ""),
        "budget_start":       raw.get("budget_start", ""),
        "budget_end":         raw.get("budget_end", ""),
        "detail_url":         raw.get("project_detail_url", ""),
        "source":             "NIH RePORTER",
    }


# ── Pagination helper ──────────────────────────────────────────────────────────

def _fetch_all_pages(
    criteria:    dict,
    page_size:   int,
    max_records: int,
) -> list[dict]:
    all_results: list[dict] = []
    offset = 0

    while True:
        remaining  = max_records - len(all_results)
        this_limit = min(page_size, remaining)
        if this_limit <= 0:
            break

        time.sleep(REQ_DELAY)
        body: dict[str, Any] = {
            "criteria":       criteria,
            "include_fields": INCLUDE_FIELDS,
            "limit":          this_limit,
            "offset":         offset,
            "sort_field":     "award_amount",
            "sort_order":     "desc",
        }
        data = _post(body)
        raw  = data.get("results", [])
        if not raw:
            break

        total = (data.get("meta") or {}).get("total", 0)
        all_results.extend(raw)
        log.info("  offset=%d  got=%d  total_available=%d  collected=%d",
                 offset, len(raw), total, len(all_results))

        offset += len(raw)
        if offset >= total or offset >= 14999:
            break

    return all_results


# ── Named search runners ───────────────────────────────────────────────────────

def _by_categories(fiscal_years: list[int], include_active: bool,
                   page_size: int, max_records: int) -> list[dict]:
    criteria: dict[str, Any] = {
        "fiscal_years": fiscal_years,
        "spending_categories": {"values": HEALTH_SPENDING_CATEGORY_IDS, "match_all": False},
        "include_active_projects": include_active,
    }
    log.info("Search: categories (%d IDs), FY=%s", len(HEALTH_SPENDING_CATEGORY_IDS), fiscal_years)
    return _fetch_all_pages(criteria, page_size, max_records)


def _by_text(search_key: str, fiscal_years: list[int], include_active: bool,
             page_size: int, max_records: int) -> list[dict]:
    text_def = TEXT_SEARCHES[search_key]
    criteria: dict[str, Any] = {
        "fiscal_years": fiscal_years,
        "advanced_text_search": text_def,
        "include_active_projects": include_active,
    }
    log.info("Search: %s, FY=%s", search_key, fiscal_years)
    return _fetch_all_pages(criteria, page_size, max_records)


# ── Public entry point ─────────────────────────────────────────────────────────

def fetch_health_awards(
    fiscal_years:   list[int] | None = None,
    page_size:      int = 500,
    max_records:    int = 2000,
    mode:           str = "all",
    include_active: bool = True,
) -> dict[str, Any]:
    """
    Fetch NIH health/medical/DALY research awards.

    Args:
        fiscal_years:   FY to query. None → current FY + previous FY.
        page_size:      Records per page (max 500).
        max_records:    Cap per individual search before merging.
        mode:           "all" | "categories" | "daly_text" | "disability_text"
        include_active: Include active/ongoing grants.

    Returns dict with: projects, total_fetched, by_search,
                       duplicates_removed, fiscal_years_queried.
    """
    if fiscal_years is None:
        fy = datetime.now(timezone.utc).year
        fiscal_years = [fy - 1, fy]

    seen_appl_ids: set[int] = set()
    all_projects:  list[dict] = []
    by_search:     dict[str, int] = {}
    dupes = 0

    searches: dict[str, Any] = {}
    if mode in ("all", "categories"):
        searches["categories"] = lambda: _by_categories(
            fiscal_years, include_active, page_size, max_records)
    if mode in ("all", "daly_text"):
        searches["daly_text"] = lambda: _by_text(
            "daly_text", fiscal_years, include_active, page_size, max_records)
    if mode in ("all", "disability_text"):
        searches["disability_text"] = lambda: _by_text(
            "disability_text", fiscal_years, include_active, page_size, max_records)

    for name, run in searches.items():
        raw_results = run()
        accepted = 0
        for raw in raw_results:
            appl_id = raw.get("appl_id")
            if appl_id and appl_id in seen_appl_ids:
                dupes += 1
                continue
            if appl_id:
                seen_appl_ids.add(appl_id)
            all_projects.append(_format_project(raw))
            accepted += 1
        by_search[name] = accepted
        log.info("  -> %d unique projects (search: %s)", accepted, name)

    log.info("Done — %d total projects, %d duplicates removed",
             len(all_projects), dupes)

    return {
        "projects":            all_projects,
        "total_fetched":       len(all_projects),
        "by_search":           by_search,
        "duplicates_removed":  dupes,
        "fiscal_years_queried": fiscal_years,
    }
