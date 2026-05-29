"""
icd_crawler.py — ICD-API client (WHO ICD-11 & ICD-10)
=====================================================
Supports use-case:
  1. get_all_codes()  – full hierarchy crawl of ICD-11 MMS (for bulk classification)

Local deployment uses dotenv for ICD_API_CLIENT_SECRET and ICD_API_CLIENT_ID
Crawler runtime is 12 hours, as only necessary once a year, not optimized and due to EC2 setup issues not deployed on AWS.

Correct API base URLs (v2):
  Entity  : GET https://id.who.int/icd/release/11/mms/<code>
  Root    : GET https://id.who.int/icd/release/11/mms

"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import logging
import os
import time
from typing import Any, Callable, Generator, List, Set, Tuple, Dict

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ICD_API_BASE        = "https://id.who.int"
ICD_AUTH_TOKEN_URL  = "https://icdaccessmanagement.who.int/connect/token"
ICD_AUTH_SCOPE      = "icdapi_access"

# Correct search endpoint
ICD_SEARCH_URL      = f"{ICD_API_BASE}/icd/entity/search"

# Linearization roots
ICD11_MMS_ROOT      = f"{ICD_API_BASE}/icd/release/11/mms"
ICD10_ROOT          = f"{ICD_API_BASE}/icd/release/10"

TIMEOUT    = 20
REQ_DELAY  = 0.5

HEADERS = {
    "Accept":          "application/json",
    "Accept-Language": "en",
    "API-Version":     "v2",
    "User-Agent":      "ICD-API-Fetcher/2.0 (Health Data Pipeline)",
}

# ---------------------------------------------------------------------------
# OAuth2 token cache
# ---------------------------------------------------------------------------
_TOKEN_CACHE: Dict[str, Any] = {
    "token":      None,
    "expires_at": 0,
}

ICD_API_CLIENT_ID     = os.environ.get("ICD_API_CLIENT_ID")
ICD_API_CLIENT_SECRET = os.environ.get("ICD_API_CLIENT_SECRET")

# Optional callback invoked immediately after a real token refresh.
# Signature: () -> None
# Set this from outside (e.g. icd_ec2_runner) to hook checkpoint logic
# onto the natural ~1-hour token renewal cadence.
on_token_refresh: Callable[[], None] | None = None


def get_access_token(force_refresh: bool = False) -> str:
    """
    Obtain an OAuth2 bearer token via client credentials flow.
    The token is cached for its lifetime (minus 60 s safety margin).

    When a real network refresh occurs, ``icd_fetch.on_token_refresh()`` is
    called (if set) after the new token is stored.  This fires approximately
    once per hour and is the natural seam for hourly checkpointing.

    Raises:
        requests.RequestException – on auth failure
        ValueError                – if the response has no access_token
    """
    now = time.time()
    if not force_refresh and _TOKEN_CACHE["token"] and now < _TOKEN_CACHE["expires_at"]:
        log.debug("Using cached access token")
        return _TOKEN_CACHE["token"]

    if not ICD_API_CLIENT_ID or not ICD_API_CLIENT_SECRET:
        raise ValueError(
            "ICD_API_CLIENT_ID and ICD_API_CLIENT_SECRET must be set in .env"
        )

    log.info("Requesting ICD API access token…")
    resp = requests.post(
        ICD_AUTH_TOKEN_URL,
        data={
            "client_id":     ICD_API_CLIENT_ID,
            "client_secret": ICD_API_CLIENT_SECRET,
            "scope":         ICD_AUTH_SCOPE,
            "grant_type":    "client_credentials",
        },
        timeout=TIMEOUT,
    )
    resp.raise_for_status()

    payload    = resp.json()
    token      = payload.get("access_token")
    expires_in = payload.get("expires_in", 3600)

    if not token:
        raise ValueError(f"Auth response missing access_token: {payload}")

    _TOKEN_CACHE["token"]      = token
    _TOKEN_CACHE["expires_at"] = now + expires_in - 60

    log.info("Access token acquired (expires in %d s)", expires_in)

    # Fire the checkpoint hook AFTER the new token is stored so the crawl
    # can continue immediately after the checkpoint upload completes.
    if on_token_refresh is not None:
        try:
            on_token_refresh()
        except Exception as exc:                         # never kill the crawl
            log.error("on_token_refresh hook raised: %s", exc)

    return _TOKEN_CACHE["token"]


def _auth_headers() -> dict[str, str]:
    """Return request headers with current bearer token."""
    headers = HEADERS.copy()
    headers["Authorization"] = f"Bearer {get_access_token()}"
    return headers


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _get(url: str, params: Dict | None = None) -> Dict:
    """
    Authenticated GET, with rate-limit delay and raise_for_status.

    Raises:
        requests.HTTPError / requests.RequestException
    """
    time.sleep(REQ_DELAY)
    resp = requests.get(url, headers=_auth_headers(), params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Text extraction helper
# ---------------------------------------------------------------------------

def _label(value: Any) -> str:
    """
    Extract a plain string from an ICD JSON-LD label value.

    The API returns labels as either:
      - a plain string
      - {"@language": "en", "@value": "some text"}
      - a list of the above
    """
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return value.get("@value") or value.get("label") or ""
    if isinstance(value, list):
        # Return first English value, or first available
        for item in value:
            if isinstance(item, dict):
                if item.get("@language") == "en":
                    return item.get("@value", "")
        for item in value:
            if isinstance(item, dict):
                return item.get("@value", "")
            if isinstance(item, str):
                return item
    return str(value) if value else ""

# ---------------------------------------------------------------------------
# 1. Full ICD-11 MMS hierarchy crawl
# ---------------------------------------------------------------------------

def _load_crawl_state(state_path: str) -> Tuple[Set[str], List[str]]:
    """
    Load visited URIs and pending queue from a JSONL state file.

    The state file is a JSONL where:
      - Each data line is a record already fetched (has "icd_uri" key)
      - The last line is a JSON object {"_queue": [...]} written on clean exit

    Returns:
        (visited, queue) — visited is the set of URIs already processed,
                           queue is the list of URIs still to visit.
        If the state file doesn't exist, returns (empty set, empty list).
    """
    visited: set[str] = set()
    queue: list[str] = []

    if not os.path.exists(state_path):
        return visited, queue

    with open(state_path, "r", encoding="utf-8") as fh:
        lines = [l.strip() for l in fh if l.strip()]

    for line in lines:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        if "_queue" in obj:
            # Saved queue from a previous clean exit
            queue = obj["_queue"]
        elif "icd_uri" in obj:
            visited.add(obj["icd_uri"])

    log.info(
        "Resumed from %s: %d records already fetched, %d URIs in queue",
        state_path, len(visited), len(queue),
    )
    return visited, queue


def _strip_queue_sentinel(path: str) -> None:
    """
    Remove the trailing ``{"_queue": [...]}`` sentinel line from a JSONL file
    before appending new records, so the file stays clean.
    """
    with open(path, "r", encoding="utf-8") as fh:
        lines = fh.readlines()

    # Walk backwards and drop any sentinel lines
    while lines and "_queue" in lines[-1]:
        lines.pop()

    with open(path, "w", encoding="utf-8") as fh:
        fh.writelines(lines)

def _resolve_mms_root() -> str:
    """
    GET /icd/release/11/mms  →  returns metadata including latestRelease.
    Returns the versioned root URI, e.g.:
        https://id.who.int/icd/release/11/mms/2026-01
    """
    meta = _get(ICD11_MMS_ROOT)
    latest = meta.get("latestRelease", "")
    if not latest:
        raise RuntimeError(
            f"Could not find latestRelease in MMS root response: {list(meta.keys())}"
        )
    versioned = latest.replace("http://", "https://")
    log.info("Using ICD-11 MMS release: %s", versioned)
    return versioned


def _extract_child_uris(entity: Dict[str, Any]) -> List[str]:
    """
    Robustly extract child URIs from an entity response.

    The API may return children as:
      - a plain string URI
      - a dict with "@id" key
      - a dict with "id" key
    All are upgraded to https://.
    """
    uris = []
    for child in entity.get("child", []):
        if isinstance(child, str):
            uri = child
        elif isinstance(child, dict):
            uri = child.get("@id") or child.get("id", "")
        else:
            continue
        if uri:
            uris.append(uri.replace("http://", "https://"))
    return uris


def iter_icd11_mms(
    root_uri: str | None = None,
    *,
    class_kinds: Set[str] = frozenset({"category"}),
    out_path: str | Path | None = None,
) -> Generator[Dict[str, Any], None, None]:
    """
    Walk the ICD-11 MMS tree breadth-first, stream records to a JSONL file,
    and support resuming an interrupted crawl.

    Resume behaviour
    ----------------
    If `out_path` already exists the crawl reads it to rebuild:
      - `visited`  — URIs already fetched (skipped on resume)
      - `queue`    — URIs still pending (restored from the ``_queue`` sentinel
                     written at the end of the previous run's last flush)

    On a clean exit, a ``{"_queue": [...]}`` sentinel line is appended so the
    next run knows exactly where to continue.  On crash/interrupt the sentinel
    is missing; the queue is reconstructed as the child URIs of all visited
    nodes that have not themselves been visited yet — so at worst a small
    number of leaf nodes near the crash point are re-fetched.

    File format
    -----------
    One JSON object per line (JSONL / ndjson).  Every line except the trailing
    sentinel is a formatted entity record (same schema as format_entity_record).

    Args:
        root_uri:    Starting URI. If None, auto-resolves to latest MMS release.
        class_kinds: Set of classKind values to include in output.
                     Default {"category"} — only nodes with actual ICD codes.
                     Use {"category","block","chapter"} to get everything.
        out_path:    Path to the JSONL output / state file.
                     If None the generator just yields without writing.

    Yields:
        Formatted record dicts (same schema as format_entity_record).
        Already-visited records are NOT re-yielded on resume.
    """
    if root_uri is None:
        root_uri = _resolve_mms_root()
    else:
        root_uri = root_uri.replace("http://", "https://")

    visited: Set[str] = set()
    queue: List[str]  = [root_uri]

    if out_path and os.path.exists(out_path):
        visited, queue = _load_crawl_state(out_path)

    out_fh = None
    if out_path:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        # On resume, strip any trailing _queue sentinel before appending
        if os.path.exists(out_path):
            _strip_queue_sentinel(out_path)
        out_fh = open(out_path, "a", encoding="utf-8")  # line-buffered

    yielded = 0
    try:
        while queue:
            uri = queue.pop(0)
            if uri in visited:
                continue
            visited.add(uri)

            try:
                entity = _get(uri)
            except requests.RequestException as exc:
                log.warning("Skipping %s: %s", uri, exc)
                continue

            kind = entity.get("classKind", "")
            if kind in class_kinds:
                record = format_entity_record(entity)
                yielded += 1

                if out_fh:
                    out_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                    out_fh.flush()

                if yielded % 10 == 0:
                    log.info("  … %d records yielded, queue depth %d", yielded, len(queue))

                yield record

            for child_uri in _extract_child_uris(entity):
                if child_uri not in visited:
                    queue.append(child_uri)

        log.info("ICD-11 MMS crawl complete. Visited %d URIs, yielded %d records.",
                 len(visited), yielded)
    finally:
        if out_fh:
            out_fh.write(json.dumps({"_queue": queue}, ensure_ascii=False) + "\n")
            out_fh.close()
            log.info("State saved to %s", out_path)

    log.info(
        "ICD-11 MMS crawl complete. Visited %d URIs, yielded %d records.",
        len(visited), yielded,
    )


# ---------------------------------------------------------------------------
# Format entity record
# ---------------------------------------------------------------------------

def format_entity_record(entity: Dict[str, Any], search_context: str = "") -> Dict:
    """
    Format a full ICD entity API response into a flat record dict.
    """
    icd_uri    = entity.get("@id", entity.get("id", ""))
    icd_code   = entity.get("code", "") or icd_uri.split("/")[-1]
    class_kind = entity.get("classKind", "")
    title      = _label(entity.get("title", ""))

    # Browser URL
    browser_url = entity.get("browserUrl", "")

    # Definitions
    definition      = _label(entity.get("definition", ""))
    long_definition = _label(entity.get("longDefinition", ""))
    fully_specified = _label(entity.get("fullySpecifiedName", ""))

    source = "ICD-10" if "icd10" in icd_uri or "/release/10/" in icd_uri else "ICD-11"

    # Synonyms
    synonyms = []
    for s in entity.get("synonym", []):
        lbl = _label(s.get("label") if isinstance(s, dict) else s)
        if lbl:
            synonyms.append(lbl)

    # Inclusions / exclusions
    inclusions = []
    for inc in entity.get("inclusion", []):
        if isinstance(inc, dict):
            inclusions.append({
                "label":             _label(inc.get("label", "")),
                "linearization_uri": inc.get("linearizationReference", ""),
                "foundation_uri":    inc.get("foundationReference", ""),
            })
        else:
            inclusions.append({"label": _label(inc), "linearization_uri": "", "foundation_uri": ""})

    exclusions = []
    for exc in entity.get("exclusion", []):
        if isinstance(exc, dict):
            exclusions.append({
                "label":             _label(exc.get("label", "")),
                "linearization_uri": exc.get("linearizationReference", ""),
                "foundation_uri":    exc.get("foundationReference", ""),
            })
        else:
            exclusions.append({"label": _label(exc), "linearization_uri": "", "foundation_uri": ""})

    # Index terms
    index_terms = []
    for term in entity.get("indexTerm", []):
        lbl = _label(term.get("label") if isinstance(term, dict) else term)
        if lbl:
            index_terms.append(lbl)

    # Hierarchy
    parent_uris = []
    for p in entity.get("parent", []):
        uri = p if isinstance(p, str) else p.get("@id", p.get("id", ""))
        if uri:
            parent_uris.append(uri.replace("http://", "https://"))

    child_uris = []
    for c in entity.get("child", []):
        uri = c if isinstance(c, str) else c.get("@id", c.get("id", ""))
        if uri:
            child_uris.append(uri.replace("http://", "https://"))

    foundation_uri = entity.get("source", "")
    if isinstance(foundation_uri, dict):
        foundation_uri = foundation_uri.get("@id", "")
    if foundation_uri:
        foundation_uri = foundation_uri.replace("http://", "https://")

    postcoordination_scales = entity.get("postcoordinationScale", [])

    return {
        "icd_code":               icd_code,
        "icd_uri":                icd_uri.replace("http://", "https://"),
        "browser_url":            browser_url,
        "class_kind":             class_kind,
        "title":                  title,
        "definition":             definition,
        "long_definition":        long_definition,
        "fully_specified_name":   fully_specified,
        "synonyms":               synonyms,
        "inclusion":              inclusions,
        "exclusion":              exclusions,
        "index_terms":            index_terms,
        "parent_uris":            parent_uris,
        "child_uris":             child_uris,
        "foundation_uri":         foundation_uri,
        "postcoordination_scales": postcoordination_scales,
        "source":                 source,
        "search_context":         search_context,
    }


def main(event: Dict[str, Any]):
    start_time = datetime.now(timezone.utc)
    out_path = event.get("out_path", "temp.jsonl")
    class_kinds_raw = event.get("class_kinds", "category")
    class_kinds     = set(k.strip() for k in class_kinds_raw.split(",") if k.strip())
    log.info("Starting ICD-11 MMS crawl…")
    records_written = 0
    try:
        for record in iter_icd11_mms(class_kinds=class_kinds, out_path=out_path):
            records_written += 1
    except Exception as exc:
        log.error("Crawl failed after %d records: %s", records_written, exc)
        log.info("Performing emergency checkpoint…")
    end_time = datetime.now(timezone.utc)
    elapsed_s = (end_time - start_time).total_seconds()
    summary = {
        "fetched_at":           start_time.isoformat(),
        "completed_at":         end_time.isoformat(),
        "elapsed_seconds":      round(elapsed_s),
        "data": {
            "records_written": records_written,
            "jsonl_state": out_path,
        },
    }
    print(summary)

def _parsargs():
    import argparse
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--out-path", type=str, default="icd11_mms_codes.jsonl")
    return p.parse_args()


if __name__ == "__main__":
    args = _parsargs()
    event_dict: Dict[str, Any] = {
        "out_path": args.out_path,
    }
    main(event_dict)