"""
CORDIS — EU Health Research Funding Fetcher (Horizon Europe & H2020)
=====================================================================
Base URL  : https://cordis.europa.eu/api/dataextractions
Auth      : API key as query param  ?key=<your-key>
            Register free at: https://cordis.europa.eu/dataextractions/register
Swagger   : https://cordis.europa.eu/dataextractions/api-docs-ui

------------------------------------------------------------------------
DET API  (async job model)
------------------------------------------------------------------------

  1.  GET /api/dataextractions/getExtraction
          ?key=<api-key> &query=<query> &outputFormat=json &archived=false
      → { status: true, payload: { taskID: 12345 } }

  2.  Poll GET /api/dataextractions/getExtractionStatus
          ?key=<api-key> &taskId=<taskID>
      → { ..., payload: { progress: "DONE", destinationFileUri: "..." } }

  3.  GET <destinationFileUri>  →  JSON array of project records

------------------------------------------------------------------------
QUERY SYNTAX
------------------------------------------------------------------------

  Confirmed working form (mirrored from the CORDIS search UI):

    frameworkProgramme='HORIZON','H2020' AND startDate=YYYY-01-01-YYYY-12-31

  The API rejects queries returning more than 25 000 records.
  We issue ONE EXTRACTION PER YEAR, all submitted concurrently.
  The final year uses today's date as the upper bound.

------------------------------------------------------------------------
CONCURRENCY MODEL
------------------------------------------------------------------------

  All year extractions run concurrently via asyncio + aiohttp:

    1.  All submissions fired at once (asyncio.gather).
    2.  Each year's poll loop runs as its own coroutine, sleeping with
        asyncio.sleep so no coroutine blocks another.
    3.  A single global_deadline (loop.time() timestamp) is shared
        across all coroutines.  As earlier years consume wall time the
        remaining budget automatically shrinks for later ones.
    4.  The public fetch_health_grants() is synchronous (asyncio.run)
        so callers and cordis_function.py need no changes.

  HTTP timeout per request: CONNECT_TIMEOUT / READ_TIMEOUT (separate).
  Read timeout is generous (60 s) because the submission endpoint on
  a slow server caused 30 s read-timeouts under the old requests code.

------------------------------------------------------------------------
CLIENT-SIDE TOPIC CLASSIFICATION
------------------------------------------------------------------------

  No server-side keyword filter — all classification is client-side.
  Each project (title + objective) is matched in priority order;
  first match wins, "unclassified" if nothing matches.

  mental_health   → depression, anxiety, bipolar, schizophrenia, ptsd,
                    mental health, psychiatric, eating disorder,
                    psychological wellbeing
  disability      → disability, intellectual disability,
                    neurodevelopmental, autism, cerebral palsy, daly,
                    burden of disease, years lived with disability
  cancer          → cancer, oncology, carcinoma, lymphoma, leukaemia,
                    melanoma, glioma, tumour, myeloma, chemotherapy,
                    immunotherapy
  rare_chronic    → rare disease, multiple sclerosis, parkinson,
                    alzheimer, cystic fibrosis, sickle cell, lupus,
                    rheumatoid arthritis, chronic illness
  neurology_brain → neurology, stroke, dementia, epilepsy,
                    traumatic brain injury, neurodegeneration,
                    cognitive decline, spinal cord
  other_health    → health, patient, clinical, hospital, therapy,
                    treatment, disease, disorder, syndrome, vaccine,
                    pharmaceutical, drug, medical, diagnosis, symptom
  unclassified    → catch-all (no keywords matched)

------------------------------------------------------------------------
RAW FIELD INSPECTION
------------------------------------------------------------------------

  result["raw_sample"] contains the first 5 raw records from the
  earliest year that returned data, unmodified.

------------------------------------------------------------------------
LOCAL TESTING
------------------------------------------------------------------------

  export CORDIS_API_KEY=your_key_here
  pip install aiohttp
  python cordis_fetch.py

  Env var overrides:
    CORDIS_MODE           "all" or single topic label  (default: all)
    CORDIS_MIN_YEAR       Integer                      (default: 2018)
    CORDIS_MAX_RECORDS    Cap on total accepted        (default: 5000)
    CORDIS_POLL_INTERVAL  Seconds between polls        (default: 5)
    CORDIS_POLL_TIMEOUT   Total wall budget in seconds (default: 800)

------------------------------------------------------------------------
AWS LAMBDA
------------------------------------------------------------------------

  Handler : cordis_function.lambda_handler
  Runtime : Python 3.12  |  Memory: 512 MB  |  Timeout: 900 s
  Env var : CORDIS_API_KEY  (required — stored in Secrets Manager)

  Event keys (all optional):
    mode          str    "all" or single topic label.  Default: "all"
    max_records   int    Cap on total accepted.         Default: 5000
    min_year      int    First year to fetch.           Default: 2018
    poll_interval float  Seconds between polls.         Default: 5.0
    poll_timeout  float  Total wall budget (all years). Default: 800

  Valid topic labels:
    mental_health, disability, cancer, rare_chronic, neurology_brain,
    other_health, unclassified
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import date
from typing import Any

import aiohttp


from dotenv import load_dotenv

load_dotenv()

# -- Logging -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# -- Constants -----------------------------------------------------------------
DET_BASE = "https://cordis.europa.eu/api/dataextractions"
EP_GET_EXTRACTION = f"{DET_BASE}/getExtraction"
EP_GET_STATUS = f"{DET_BASE}/getExtractionStatus"
EP_CANCEL_EXTRACTION = f"{DET_BASE}/cancelExtraction"

CONNECT_TIMEOUT = 15
READ_TIMEOUT    = 60
POLL_INTERVAL   = 5.0
POLL_TIMEOUT    = 800.0

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "CORDIS-HealthFetcher/1.0",
}

RAW_SAMPLE_SIZE = 5

def _build_query(year: int) -> str:
    """
    Build the CORDIS query string for both programmes in one extraction.

    Mirrors the confirmed working search URL:
      frameworkProgramme='HORIZON','H2020'
      AND startDate=2021-01-01-2026-03-30
    """
    today      = date.today()
    start_from = f"{year}-01-01"
    start_to   = (
        today.isoformat() if year >= today.year else f"{year}-12-31"
    )
    return (
        f"frameworkProgramme='HORIZON','H2020'"
        f" AND startDate={start_from}-{start_to}"
    )

# ── Client-side topic classification ──────────────────────────────────────────

TOPIC_GROUPS: list[tuple[str, list[str]]] = [
    ("mental_health", [
        "depression", "anxiety", "bipolar", "schizophrenia",
        "ptsd", "mental health", "psychiatric", "eating disorder",
        "psychological wellbeing",
    ]),
    ("disability", [
        "disability", "intellectual disability", "neurodevelopmental",
        "autism", "cerebral palsy", "daly", "burden of disease",
        "years lived with disability",
    ]),
    ("cancer", [
        "cancer", "oncology", "carcinoma", "lymphoma", "leukaemia",
        "melanoma", "glioma", "tumour", "myeloma", "chemotherapy",
        "immunotherapy",
    ]),
    ("rare_chronic", [
        "rare disease", "multiple sclerosis", "parkinson", "alzheimer",
        "cystic fibrosis", "sickle cell", "lupus",
        "rheumatoid arthritis", "chronic illness",
    ]),
    ("neurology_brain", [
        "neurology", "stroke", "dementia", "epilepsy",
        "traumatic brain injury", "neurodegeneration",
        "cognitive decline", "spinal cord",
    ]),
]

VALID_TOPIC_LABELS: list[str] = (
    [label for label, _ in TOPIC_GROUPS] + ["unclassified"]
)

def _classify_topic(raw: dict) -> str:
    """Scan title + objective; return first matching label or 'unclassified'."""
    haystack = (
        (raw.get("title") or "") + " " + (raw.get("objective") or "")
    ).lower()
    for label, keywords in TOPIC_GROUPS:
        if any(kw in haystack for kw in keywords):
            return label
    return "unclassified"

# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_remaining_seconds(remaining_time: str) -> float | None:
    """Parse "HH:MM:SS" → total seconds. Returns None on failure."""
    try:
        parts = remaining_time.strip().split(":")
        if len(parts) == 3:
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
            return h * 3600 + m * 60 + s
    except (ValueError, AttributeError):
        pass
    return None


def _aiohttp_timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(
        connect=CONNECT_TIMEOUT,
        sock_read=READ_TIMEOUT,
    )

MAX_EXTRACTIONS = 5

# ── Async API layer ────────────────────────────────────────────────────────────

async def _list_extractions(
    session: aiohttp.ClientSession,
    api_key: str,
) -> list[dict]:
    """
    Return the current list of extractions for this API key.
    Each item is a GetExtractionStatusDTO with at least taskID and progress.
    """
    async with session.get(
        f"{DET_BASE}/listExtractions",
        params={"key": api_key},
    ) as resp:
        resp.raise_for_status()
        body = await resp.json(content_type=None)
    if not body.get("status"):
        raise ValueError(f"listExtractions returned status=false: {body}")
    return body["payload"].get("result", [])


async def _submit_extraction(
        session: aiohttp.ClientSession,
        api_key: str,
        query: str,
) -> int:
    """Submit one extraction job. Returns taskID."""
    params = {
        "key": api_key,
        "query": query,
        "outputFormat": "json",
        "archived": "false",
    }
    log.info("Submitting  query=%s", query)
    async with session.get(EP_GET_EXTRACTION, params=params) as resp:
        resp.raise_for_status()
        body = await resp.json(content_type=None)
    if not body.get("status"):
        raise ValueError(f"getExtraction returned status=false: {body}")
    task_id = body["payload"]["taskID"]
    log.info("  → taskID=%s", task_id)
    return task_id


async def _poll_until_done(
        session: aiohttp.ClientSession,
        api_key: str,
        task_id: int,
        poll_interval: float,
        global_deadline: float,
) -> str:
    """
    Poll getExtractionStatus until DONE. Returns destinationFileUri.

    global_deadline is loop.time()-based and shared across all year
    coroutines, so the remaining budget automatically reflects how much
    time prior years have consumed.

    Sleep strategy:
      - First poll after poll_interval (let the job start).
      - If remainingTime parses: sleep that long (+2 s), capped at
        remaining wall time.  Abort immediately if eta > wall time left.
      - Fallback: sleep poll_interval.
    """
    loop = asyncio.get_event_loop()
    first_poll = True

    while True:
        wall_left = global_deadline - loop.time()
        if wall_left <= 0:
            raise TimeoutError(
                f"taskID={task_id} aborted — global time budget exhausted."
            )

        await asyncio.sleep(poll_interval if first_poll else 0)
        first_poll = False

        async with session.get(
                EP_GET_STATUS,
                params={"key": api_key, "taskId": task_id},
        ) as resp:
            resp.raise_for_status()
            body = await resp.json(content_type=None)

        if not body.get("status"):
            raise ValueError(f"getExtractionStatus returned status=false: {body}")

        payload = body["payload"]
        progress = payload.get("progress", "")
        remaining_str = payload.get("remainingTime", "")
        wall_left = global_deadline - loop.time()  # recalculate after I/O

        log.info(
            "  taskID=%s  progress=%s  processed=%s/%s  eta=%s  wall_left=%.0fs",
            task_id,
            progress,
            payload.get("numberOfProcessedRecords", "?"),
            payload.get("numberOfRecords", "?"),
            remaining_str or "?",
            wall_left,
        )

        if progress == "DONE":
            uri = payload.get("destinationFileUri", "")
            if not uri:
                raise ValueError(f"taskID={task_id} DONE but destinationFileUri is empty")
            return uri

        # --- smart sleep ---
        remaining_secs = _parse_remaining_seconds(remaining_str)

        if remaining_secs is not None:
            if remaining_secs > wall_left:
                raise TimeoutError(
                    f"taskID={task_id} eta {remaining_str} ({remaining_secs:.0f}s) "
                    f"exceeds remaining wall time ({wall_left:.0f}s) — aborting."
                )
            sleep_for = min(remaining_secs + 2, wall_left - 1)
            log.info("  Smart sleep %.0f s (eta %s, wall_left %.0f s)",
                     sleep_for, remaining_str, wall_left)
            await asyncio.sleep(max(sleep_for, 0))
        else:
            await asyncio.sleep(min(poll_interval, max(wall_left - 1, 0)))


async def _delete_extraction(
    session: aiohttp.ClientSession,
    api_key: str,
    task_id: int,
) -> None:
    """Delete a completed extraction to free a slot."""
    async with session.delete(
        f"{DET_BASE}/deleteExtraction",
        params={"key": api_key, "taskId": task_id},
    ) as resp:
        resp.raise_for_status()
        body = await resp.json(content_type=None)
    if not body.get("status"):
        log.warning("deleteExtraction status=false for taskID=%s: %s", task_id, body)
    else:
        log.info("Deleted extraction taskID=%s", task_id)


async def _cancel_extraction(
    session: aiohttp.ClientSession,
    api_key: str,
    task_id: int,
) -> None:
    """Best-effort cancel."""
    try:
        async with session.get(
            EP_CANCEL_EXTRACTION,
            params={"key": api_key, "taskId": task_id},
        ) as resp:
            resp.raise_for_status()
        log.info("Cancelled taskID=%s", task_id)
    except Exception as exc:
        log.warning("Failed to cancel taskID=%s: %s", task_id, exc)


async def _download_results(
    session: aiohttp.ClientSession,
    download_uri: str,
) -> list[dict]:
    """Download and parse the completed extraction JSON."""
    log.info("Downloading %s ...", download_uri[:80])
    async with session.get(download_uri) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    if isinstance(data, list):
        return data
    for key in ("results", "data", "records", "payload"):
        if key in data and isinstance(data[key], list):
            return data[key]
    log.warning("Unexpected download JSON shape: %s", str(data)[:200])
    return []


class _SlotManager:
    """
    Ensures we never exceed MAX_EXTRACTIONS concurrent slots on the CORDIS
    account.  All year coroutines share one instance.

    Usage (inside _fetch_year):

        async with slot_manager:
            task_id = await _submit_extraction(...)
            ...download...
            await _delete_extraction(..., task_id)   # free slot before releasing

    How it works:
      - A semaphore(MAX_EXTRACTIONS) serialises entry so at most 5 coroutines
        are inside the block at once.
      - On entry we also call listExtractions and delete the oldest DONE
        extraction if the live count is already at the limit — handles the
        case where previous runs left extractions behind.
      - The semaphore is released in __aexit__ after the caller has already
        deleted its own extraction, so the slot is truly free for the next
        waiter.
    """

    def __init__(self, session: aiohttp.ClientSession, api_key: str) -> None:
        self._session = session
        self._api_key = api_key
        self._semaphore = asyncio.Semaphore(MAX_EXTRACTIONS)

    async def __aenter__(self) -> None:
        await self._semaphore.acquire()
        await self._ensure_slot()

    async def __aexit__(self, *_: Any) -> None:
        self._semaphore.release()

    async def _ensure_slot(self) -> None:
        """
        If the account is already at MAX_EXTRACTIONS, delete the oldest
        completed (DONE) extraction to make room.  If none are DONE yet,
        log a warning and proceed anyway — the submission will get the
        capacity error and _fetch_year will catch it.
        """
        try:
            extractions = await _list_extractions(self._session, self._api_key)
        except Exception as exc:
            log.warning("listExtractions failed, proceeding anyway: %s", exc)
            return

        if len(extractions) < MAX_EXTRACTIONS:
            return

        # Pick the oldest DONE extraction to delete
        done = [e for e in extractions if e.get("progress") == "DONE"]
        if not done:
            log.warning(
                "At slot limit (%d) but no DONE extractions to delete — "
                "submission may fail.", MAX_EXTRACTIONS,
            )
            return

        oldest = done[0]  # listExtractions returns oldest first
        log.info(
            "At slot limit — deleting oldest DONE extraction taskID=%s "
            "(query: %.60s)", oldest["taskID"], oldest.get("query", ""),
        )
        try:
            await _delete_extraction(self._session, self._api_key, oldest["taskID"])
        except Exception as exc:
            log.warning("Failed to delete extraction taskID=%s: %s", oldest["taskID"], exc)



# ── Per-year coroutine ─────────────────────────────────────────────────────────

async def _fetch_year(
    session: aiohttp.ClientSession,
    api_key: str,
    year: int,
    poll_interval: float,
    global_deadline: float,
    slot_manager: _SlotManager,
) -> tuple[int, list[dict]]:
    """
    Submit, poll and download one year's extraction.
    Returns (year, raw_list).  On failure returns (year, []) after logging.
    """
    task_id: int | None = None
    try:
        async with slot_manager:
            task_id      = await _submit_extraction(session, api_key, _build_query(year))
            download_uri = await _poll_until_done(
                session, api_key, task_id, poll_interval, global_deadline,
            )
            raw_list = await _download_results(session, download_uri)
            log.info("  year=%d  downloaded %d records", year, len(raw_list))
            # Delete before releasing the slot so the next waiter gets a clean slot
            await _delete_extraction(session, api_key, task_id)
        return year, raw_list
    except Exception as exc:
        log.error("  year=%d failed: %s", year, exc)
        if task_id is not None:
            await _cancel_extraction(session, api_key, task_id)
        return year, []


# ── Async orchestrator ─────────────────────────────────────────────────────────

async def _run(
        api_key: str,
        years: list[int],
        max_records: int,
        mode: str,
        poll_interval: float,
        poll_timeout: float,
) -> dict[str, Any]:
    """
    Submit all year extractions concurrently, collect and classify results.
    """
    loop = asyncio.get_event_loop()
    global_deadline = loop.time() + poll_timeout

    timeout = _aiohttp_timeout()
    connector = aiohttp.TCPConnector(limit=10)

    async with aiohttp.ClientSession(
        headers=HEADERS,
        timeout=timeout,
        connector=connector,
    ) as session:
        slot_manager = _SlotManager(session, api_key)
        # Fire all year submissions concurrently; slot_manager caps in-flight to 5
        results: list[tuple[int, list[dict]]] = await asyncio.gather(
            *[
                _fetch_year(session, api_key, year, poll_interval, global_deadline, slot_manager)
                for year in years
            ],
            return_exceptions=False,   # exceptions already caught inside _fetch_year
        )

    # Sort by year so raw_sample comes from the earliest year
    results.sort(key=lambda t: t[0])

    seen_ids: set = set()
    all_projects: list[dict] = []
    raw_sample: list[dict] = []
    by_year: dict[int, int] = {}
    dupes = 0

    for year, raw_list in results:
        by_year[year] = len(raw_list)

        if not raw_sample and raw_list:
            raw_sample = raw_list[:RAW_SAMPLE_SIZE]

        for raw in raw_list:
            if len(all_projects) >= max_records:
                break

            proj_id = raw.get("id")
            if proj_id and proj_id in seen_ids:
                dupes += 1
                continue
            if proj_id:
                seen_ids.add(proj_id)

            topic = _classify_topic(raw)

            if mode != "all" and topic != mode:
                continue

            all_projects.append(_format_project(raw, topic))

    by_topic: dict[str, int] = {}
    for proj in all_projects:
        t = proj["topic"]
        by_topic[t] = by_topic.get(t, 0) + 1

    log.info(
        "Done — %d accepted, %d duplicates removed  by_topic=%s",
        len(all_projects), dupes, by_topic,
    )

    return {
        "projects": all_projects,
        "total_fetched": len(all_projects),
        "by_topic": by_topic,
        "by_year": by_year,
        "duplicates_removed": dupes,
        "years_fetched": years,
        "min_year_filter": years[0] if years else None,
        "fetch_mode": "DET_API",
        "raw_sample": raw_sample,
    }


# ── Formatter ─────────────────────────────────────────────────────────────────

def _format_project(raw: dict, topic: str) -> dict:
    """Flatten a raw CORDIS record into a normalised dict."""
    coord = raw.get("coordinator") or {}
    parts_raw = raw.get("participants") or []

    if isinstance(parts_raw, str):
        parts_raw = [{"name": p.strip()} for p in parts_raw.split(";") if p.strip()]

    participant_names = [p.get("name", "") for p in parts_raw if p.get("name")]
    participant_countries = sorted(
        {p.get("country", "") for p in parts_raw if p.get("country")}
    )

    proj_id = raw.get("id")
    return {
        "id": proj_id,
        "rcn": raw.get("rcn", ""),
        "topic": topic,
        "programme": raw.get("frameworkProgramme", ""),
        "acronym": (raw.get("acronym") or "").strip(),
        "title": (raw.get("title") or "").strip(),
        "status": raw.get("status", ""),
        "start_date": raw.get("startDate", ""),
        "end_date": raw.get("endDate", ""),
        "total_cost": raw.get("totalCost"),
        "ec_contribution": raw.get("ecMaxContribution"),
        "objective": (raw.get("objective") or "").strip()[:1000],
        "topics": raw.get("topics", ""),
        "funding_scheme": raw.get("fundingScheme", ""),
        "call": raw.get("call", ""),
        "coordinator": coord.get("name", "") if isinstance(coord, dict) else str(coord),
        "coordinator_country": coord.get("country", "") if isinstance(coord, dict) else "",
        "participants": participant_names[:10],
        "participant_countries": participant_countries,
        "content_updated": raw.get("contentUpdateDate", ""),
        "detail_url": (
                raw.get("url")
                or f"https://cordis.europa.eu/project/id/{proj_id or ''}"
        ),
        "source": "CORDIS",
        "fetch_method": "DET_API",
    }


# ── Public sync entry point ────────────────────────────────────────────────────

def fetch_health_grants(
        api_key: str,
        max_records: int = 5000,
        mode: str = "all",
        min_year: int = 2018,
        poll_interval: float = POLL_INTERVAL,
        poll_timeout: float = POLL_TIMEOUT,
) -> dict[str, Any]:
    """
    Fetch EU Horizon + H2020 projects from CORDIS, one extraction per year,
    all years running concurrently.

    The CORDIS DET API rejects queries returning more than 25 000 records.
    Splitting by year keeps each extraction safely below that limit.
    Running all years concurrently via asyncio means the total wall time
    is roughly the slowest single year, not the sum of all years.

    Args:
        api_key:       CORDIS DET API key (required).
        max_records:   Cap on total accepted projects after classification.
        mode:          "all" → keep everything; or a single topic label.
        min_year:      First calendar year to fetch.
        poll_interval: Seconds between status polls per coroutine.
        poll_timeout:  Total wall-clock budget for the entire run (all years).
                       Smart-sleep cap per job reflects how much time remains.

    Returns dict with:
        projects           list[dict]
        total_fetched      int
        by_topic           dict[str, int]
        by_year            dict[int, int]   raw record count per year
        duplicates_removed int
        years_fetched      list[int]
        min_year_filter    int
        fetch_mode         str              always "DET_API"
        raw_sample         list[dict]       first 5 raw records (field inspection)

    Raises:
        ValueError: if api_key is empty or mode is unrecognised.
    """
    if not api_key:
        raise ValueError(
            "CORDIS_API_KEY is required. "
            "Register free at https://cordis.europa.eu/dataextractions/register"
        )

    if mode != "all" and mode not in VALID_TOPIC_LABELS:
        raise ValueError(
            f"Unknown mode {mode!r}. Valid values: 'all' or one of {VALID_TOPIC_LABELS}"
        )

    years = list(range(min_year, date.today().year + 1))

    return asyncio.run(
        _run(
            api_key       = api_key,
            years         = years,
            max_records   = max_records,
            mode          = mode,
            poll_interval = poll_interval,
            poll_timeout  = poll_timeout,
        )
    )


if __name__ == "__main__":
    api_key = os.environ.get("CORDIS_KEY", "").strip()
    mode        = os.environ.get("CORDIS_MODE",        "all")
    min_year    = int(os.environ.get("CORDIS_MIN_YEAR",    "2018"))
    max_records = int(os.environ.get("CORDIS_MAX_RECORDS", "50"))
    page_size   = int(os.environ.get("CORDIS_PAGE_SIZE",   "10"))
    poll_interval = float(os.environ.get("CORDIS_POLL_INTERVAL", "10"))
    poll_timeout  = float(os.environ.get("CORDIS_POLL_TIMEOUT",  "800"))

    log.info(
        "Local test — mode=%s  min_year=%d  "
        "max_records=%d",
        mode, min_year, max_records,
    )

    result = fetch_health_grants(
        api_key       = api_key,
        max_records   = max_records,
        mode          = mode,
        min_year      = min_year,
        poll_interval = poll_interval,
        poll_timeout  = poll_timeout,
    )

    # Print raw sample so you can see the actual field names CORDIS returns
    print("\n── Raw sample (first 5 records — all fields as returned by CORDIS) ──")
    print(json.dumps(result["raw_sample"], indent=4, default=str))

    # Summary (exclude projects list and raw_sample for readability)
    summary = {k: v for k, v in result.items() if k not in ("projects", "raw_sample")}
    print("\n── Summary ──")
    print(json.dumps(summary, indent=4))

    out_path = "cordis_local_results.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2, default=str)
    print(f"\n── Full results written to {out_path}  ({result['total_fetched']} projects) ──")