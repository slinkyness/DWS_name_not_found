"""
ct_us_function.py
Fetches clinical trial data from ClinicalTrials.gov (clinicaltrials.gov/api/v2/studies).
No API key required.
Saves results to S3 and returns a summary.
Event params (all optional):
    Param           Type        Default
    status          list[str]   RECRUITING, NOT_YET_RECRUITING, ACTIVE_NOT_RECRUITING
    phase           list[str]   all phases
    page_size       int         200 (max 1000)
    max_records     int         500
    mode            str         "all" — or single group label
    updated_since   str         none — ISO date filter
Group labels:
    mental_health, disability, cancer,
    chronic_disease, rare_neurological
Env vars (required): AWS_REGION_NAME, S3_BUCKET, S3_FETCH_FOLDER
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
    # Identifiers & titles
    "NCTId", "OrgStudyId", "BriefTitle", "OfficialTitle",
    # Status & dates
    "OverallStatus", "HasResults",
    "StartDate", "PrimaryCompletionDate", "CompletionDate",
    "StudyFirstPostDate", "LastUpdatePostDate",
    # Design
    "StudyType", "Phase", "EnrollmentCount",
    # Conditions / therapeutic area
    "Condition", "Keyword",
    # MeshID to determine therapeutic area
    "ConditionMeshId",
    # Interventions
    "InterventionType", "InterventionName",
    # Eligibility (age group, age range, sex)
    "MinimumAge", "MaximumAge", "Sex",
    # Locations for recruitment status)
    "LocationCountry", "LocationStatus",
    # Sponsor
    "LeadSponsorName", "LeadSponsorClass",
    # Outcomes
    "PrimaryOutcomeMeasure", "SecondaryOutcomeMeasure",
])

DEFAULT_STATUSES = ["RECRUITING",
                    "NOT_YET_RECRUITING",
                    "ACTIVE_NOT_RECRUITING",
                    "COMPLETED",
                    "ENROLLING_BY_INVITATION",
                    "TERMINATED",
                    "SUSPENDED",
                    "WITHDRAWN",
                    "AVAILABLE",
                    "NO_LONGER_AVAILABLE",
                    "TEMPORARILY_NOT_AVAILABLE",
                    "APPROVED_FOR_MARKETING",
                    "WITHHELD"]

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


def _format_trial(raw: dict, group: str) -> dict:
    ps = raw.get("protocolSection", {})
    ds = raw.get("derivedSection", {})

    ident = ps.get("identificationModule", {})
    status = ps.get("statusModule", {})
    design = ps.get("designModule", {})
    cond = ps.get("conditionsModule", {})
    arms = ps.get("armsInterventionsModule", {})
    elig = ps.get("eligibilityModule", {})
    locs = ps.get("contactsLocationsModule", {}).get("locations", [])
    spons = ps.get("sponsorCollaboratorsModule", {})
    outc = ps.get("outcomesModule", {})

    interventions = arms.get("interventions", [])
    intr_names = [i.get("name", None) for i in interventions]
    intr_types = [i.get("type", None) for i in interventions]

    locations = [
        {
            "facility":           loc.get("facility", ""),
            "city":               loc.get("city", ""),
            "country":            loc.get("country", ""),
            "recruitment_status": loc.get("status", ""),
        }
        for loc in locs
    ]

    primary_measures = [o.get("measure", None) for o in outc.get("primaryOutcomes", [])]
    secondary_measures = [o.get("measure", None) for o in outc.get("secondaryOutcomes", [])]

    org_study_id = ident.get("orgStudyIdInfo", {}).get("id", None)

    meshes = ds.get("conditionBrowseModule", {}).get("meshes", [])
    mesh_ids = list({m.get("id", None) for m in meshes})

    return {
        "nct_id":             ident.get("nctId", None),
        "sponsor_code":       org_study_id,
        "query_group":        group,
        "overall_status":     status.get("overallStatus", None),
        "title":              ident.get("officialTitle", "").strip(),

        "results":            status.get("hasResults", False),
        "start_date":         status.get("startDateStruct", {}).get("date", None),
        "end_date":           status.get("primaryCompletionDateStruct", {}).get("date", None),
        "global_end_date":    status.get("completionDateStruct", {}).get("date", None),
        "decision_date":      status.get("studyFirstPostDateStruct", {}).get("date", None),
        "last_updated":       status.get("lastUpdatePostDateStruct", {}).get("date", None),

        "phases":             design.get("phases", []),
        "enrollment":         design.get("enrollmentInfo", {}).get("count", None),

        "conditions":         cond.get("conditions", []),
        "keywords":           cond.get("keywords", []),
        "mesh_ids":           mesh_ids,

        "intervention_types": intr_types,
        "intervention_names": intr_names,

        "min_age":            elig.get("minimumAge", None),
        "max_age":            elig.get("maximumAge", None),
        "sex":                elig.get("sex", None),

        "locations":          locations,

        "sponsor":            spons.get("leadSponsor", {}).get("name", None),
        "sponsor_type":       spons.get("leadSponsor", {}).get("class", None),

        "primary_outcomes":   primary_measures,
        "secondary_outcomes": secondary_measures,

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
            nct_id = raw.get("protocolSection", {}).get("identificationModule", {}).get("nctId")
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

if __name__ == "__main__":
    from datetime import datetime, timezone
    import json

    SOURCE_API = "clinicaltrials.gov/api/v2/studies"
    now = datetime.now(timezone.utc)
    statuses      = None
    phase_filter  = None
    page_size     = 200
    max_records   = 500
    mode          = "all"
    updated_since = None

    result = fetch_health_trials(
                statuses      = statuses,
                phase_filter  = phase_filter,
                page_size     = page_size,
                max_records   = max_records,
                mode          = mode,
                updated_since = updated_since,
            )
    payload = {
        "fetched_at":  now.isoformat(),
        "source_api":  SOURCE_API,
        "fetch_params": {
            "statuses":     statuses,
            "phase_filter": phase_filter,
            "page_size":    page_size,
            "max_records":  max_records,
            "mode":         mode,
            "updated_since": updated_since,
        },
        "data": result,
    }

    with open("../../data/ct_us_fetch.json", mode="w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str, indent=4)
