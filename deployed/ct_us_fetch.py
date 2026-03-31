"""
ct_us_fetch.py — ClinicalTrials.gov (v2) Health Trials Fetcher
==============================================================
Base URL : https://clinicaltrials.gov/api/v2/studies
Auth     : None — fully public API

Fetches health / disability / DALY-related clinical trials across
five condition query groups and deduplicates on NCT ID.

Returns fetch_health_trials() → dict with:
  trials               list[dict]
  total_fetched        int
  by_group             dict[str, int]
  duplicates_removed   int
  status_filter        list[str]
  phase_filter         list[str] | None
  updated_since        str | None
"""

from __future__ import annotations

import logging
from typing import Any

import requests

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
CT_BASE = "https://clinicaltrials.gov/api/v2/studies"
TIMEOUT  = 20

HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "ClinicalTrials-HealthFetcher/1.0",
}

# Slim field list — keeps each response payload small
FIELDS = ",".join([
    "NCTId", "BriefTitle", "OfficialTitle", "OverallStatus",
    "StartDate", "PrimaryCompletionDate", "StudyFirstPostDate",
    "LastUpdatePostDate", "HasResults", "BriefSummary",
    "StudyType", "Phase", "EnrollmentCount",
    "Condition", "Keyword", "InterventionType", "InterventionName",
    "MinimumAge", "MaximumAge", "Sex", "LocationCountry",
    "LeadSponsorName", "LeadSponsorClass", "PrimaryOutcomeMeasure",
])

DEFAULT_STATUSES = ["RECRUITING", "NOT_YET_RECRUITING", "ACTIVE_NOT_RECRUITING"]

# ── Query groups ───────────────────────────────────────────────────────────────
QUERY_GROUPS: list[tuple[str, str]] = [
    (
        "mental_health",
        "mental health OR depression OR anxiety OR bipolar OR "
        "schizophrenia OR PTSD OR suicide OR eating disorder OR "
        "psychological OR psychiatric",
    ),
    (
        "disability",
        "disability OR intellectual disability OR neurodevelopmental OR "
        "autism spectrum OR cerebral palsy OR spinal cord injury OR "
        "acquired brain injury OR learning disability",
    ),
    (
        "cancer",
        "cancer OR oncology OR carcinoma OR lymphoma OR leukaemia OR "
        "melanoma OR tumor OR tumour OR glioma OR sarcoma",
    ),
    (
        "chronic_disease",
        "chronic disease OR diabetes OR cardiovascular disease OR "
        "stroke OR COPD OR kidney disease OR obesity OR hypertension OR "
        "heart failure OR liver disease",
    ),
    (
        "rare_neurological",
        "rare disease OR neurological disorder OR multiple sclerosis OR "
        "Parkinson disease OR Alzheimer OR epilepsy OR Huntington OR "
        "amyotrophic lateral sclerosis OR ALS OR dementia",
    ),
]


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _get(params: dict) -> dict:
    log.info("GET %s  params=%s", CT_BASE, params)
    resp = requests.get(CT_BASE, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ── Formatter ─────────────────────────────────────────────────────────────────

def _first(obj: Any, *keys: str, default: Any = "") -> Any:
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return default
        if obj is None:
            return default
    return obj if obj is not None else default


def _format_trial(raw: dict, group: str) -> dict:
    ps      = raw.get("protocolSection", {})
    ident   = ps.get("identificationModule", {})
    status  = ps.get("statusModule", {})
    desc    = ps.get("descriptionModule", {})
    design  = ps.get("designModule", {})
    cond    = ps.get("conditionsModule", {})
    arms    = ps.get("armsInterventionsModule", {})
    elig    = ps.get("eligibilityModule", {})
    locs    = ps.get("contactsLocationsModule", {})
    spons   = ps.get("sponsorCollaboratorsModule", {})
    outc    = ps.get("outcomesModule", {})

    interventions = arms.get("interventions") or []
    intr_names    = [i.get("name", "") for i in interventions if i.get("name")]
    intr_types    = list({i.get("type", "") for i in interventions if i.get("type")})

    locations  = locs.get("locations") or []
    countries  = sorted({loc.get("country", "") for loc in locations if loc.get("country")})

    outcomes    = outc.get("primaryOutcomes") or []
    out_measures = [o.get("measure", "") for o in outcomes if o.get("measure")]

    return {
        "nct_id":             ident.get("nctId", ""),
        "query_group":        group,
        "brief_title":        ident.get("briefTitle", "").strip(),
        "official_title":     ident.get("officialTitle", "").strip(),
        "overall_status":     status.get("overallStatus", ""),
        "has_results":        status.get("hasResults", False),
        "start_date":         _first(status, "startDateStruct", "date"),
        "primary_completion": _first(status, "primaryCompletionDateStruct", "date"),
        "first_posted":       _first(status, "studyFirstPostDateStruct", "date"),
        "last_updated":       _first(status, "lastUpdatePostDateStruct", "date"),
        "brief_summary":      desc.get("briefSummary", "").strip(),
        "study_type":         design.get("studyType", ""),
        "phases":             design.get("phases") or [],
        "enrollment":         _first(design, "enrollmentInfo", "count"),
        "conditions":         cond.get("conditions") or [],
        "keywords":           cond.get("keywords") or [],
        "intervention_types": intr_types,
        "intervention_names": intr_names,
        "min_age":            elig.get("minimumAge", ""),
        "max_age":            elig.get("maximumAge", ""),
        "sex":                elig.get("sex", ""),
        "countries":          countries,
        "lead_sponsor":       _first(spons, "leadSponsor", "name"),
        "sponsor_class":      _first(spons, "leadSponsor", "class"),
        "primary_outcomes":   out_measures,
        "detail_url":         f"https://clinicaltrials.gov/study/{ident.get('nctId', '')}",
        "source":             "ClinicalTrials.gov",
    }


# ── Group fetcher ──────────────────────────────────────────────────────────────

def _fetch_group(
    label:          str,
    condition_query: str,
    statuses:       list[str],
    phase_filter:   list[str] | None,
    page_size:      int,
    max_records:    int,
    updated_since:  str | None,
    seen_nct_ids:   set[str],
) -> list[dict]:
    trials:     list[dict] = []
    page_token: str | None = None

    while True:
        remaining = max_records - len(trials)
        this_size = min(page_size, remaining)
        if this_size <= 0:
            break

        params: dict[str, Any] = {
            "query.cond":           condition_query,
            "filter.overallStatus": "|".join(statuses),
            "pageSize":             this_size,
            "countTotal":           "true",
            "format":               "json",
            "fields":               FIELDS,
            "sort":                 "LastUpdatePostDate:desc",
        }
        if phase_filter:
            params["filter.phase"] = ",".join(phase_filter)
        if updated_since:
            params["filter.advanced"] = f"AREA[LastUpdatePostDate]RANGE[{updated_since},MAX]"
        if page_token:
            params["pageToken"] = page_token

        try:
            data = _get(params)
        except requests.HTTPError as exc:
            log.warning("HTTP %s on group=%s: %s", exc.response.status_code, label, exc)
            break
        except Exception as exc:
            log.warning("Request failed group=%s: %s", label, exc)
            break

        raw_studies = data.get("studies", [])
        if not raw_studies:
            break

        log.info("  group=%-20s  page_got=%d  total_available=%s  accepted=%d",
                 label, len(raw_studies), data.get("totalCount", "?"), len(trials))

        for raw in raw_studies:
            nct_id = _first(raw, "protocolSection", "identificationModule", "nctId")
            if not nct_id or nct_id in seen_nct_ids:
                continue
            seen_nct_ids.add(nct_id)
            trials.append(_format_trial(raw, label))

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return trials


# ── Public entry point ─────────────────────────────────────────────────────────

def fetch_health_trials(
    statuses:      list[str] | None = None,
    phase_filter:  list[str] | None = None,
    page_size:     int = 200,
    max_records:   int = 1000,
    mode:          str = "all",
    updated_since: str | None = None,
) -> dict[str, Any]:
    """
    Fetch health / disability / DALY-related clinical trials from ClinicalTrials.gov.

    Args:
        statuses:      Trial status codes.  None → DEFAULT_STATUSES.
        phase_filter:  Phase codes.         None → all phases.
        page_size:     Records per page (max 1000).
        max_records:   Cap per query group.
        mode:          "all" or a single group label.
        updated_since: ISO date string — filter to recently updated trials.

    Returns dict with: trials, total_fetched, by_group,
                       duplicates_removed, status_filter, phase_filter, updated_since.
    """
    if statuses is None:
        statuses = DEFAULT_STATUSES

    seen_nct_ids: set[str]   = set()
    all_trials:   list[dict] = []
    by_group:     dict[str, int] = {}
    dupes = 0

    groups = [
        (label, q) for label, q in QUERY_GROUPS
        if mode == "all" or mode == label
    ]

    for label, condition_query in groups:
        log.info("Fetching group: %s", label)
        before = len(seen_nct_ids)
        trials = _fetch_group(
            label           = label,
            condition_query = condition_query,
            statuses        = statuses,
            phase_filter    = phase_filter,
            page_size       = page_size,
            max_records     = max_records,
            updated_since   = updated_since,
            seen_nct_ids    = seen_nct_ids,
        )
        after        = len(seen_nct_ids)
        dupes       += max((after - before) - len(trials), 0)
        all_trials.extend(trials)
        by_group[label] = len(trials)
        log.info("  -> %d unique trials (group: %s)", len(trials), label)

    log.info("Done — %d total trials, %d cross-group duplicates removed",
             len(all_trials), dupes)

    return {
        "trials":             all_trials,
        "total_fetched":      len(all_trials),
        "by_group":           by_group,
        "duplicates_removed": dupes,
        "status_filter":      statuses,
        "phase_filter":       phase_filter,
        "updated_since":      updated_since,
    }
