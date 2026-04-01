
"""
icd_fetch.py — ICD-API client
=====================================================
Correct API base URLs (v2):
  Search  : GET https://id.who.int/icd/entity/search
  Entity  : GET https://id.who.int/icd/release/11/mms/<code>
  ICD-10  : GET https://id.who.int/icd/release/10/<code>
  Root    : GET https://id.who.int/icd/release/11/mms
"""
from __future__ import annotations
import logging
import os
import time
from typing import Any, List, Dict

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ICD_API_BASE        = "https://id.who.int"
ICD_AUTH_TOKEN_URL  = "https://icdaccessmanagement.who.int/connect/token"
ICD_AUTH_SCOPE      = "icdapi_access"

# Correct search endpoint
ICD_SEARCH_URL      = f"{ICD_API_BASE}/icd/entity/search"
ICD_FOUNDATION_BASE = f"{ICD_API_BASE}/icd/entity"

# Linearization roots
ICD10_MMS_ROOT      = f"{ICD_API_BASE}/icd/release/10/mms"

TIMEOUT    = 20
REQ_DELAY  = 0.5

HEADERS = {
    "Accept":          "application/json",
    "Accept-Language": "en",
    "API-Version":     "v2",
    "User-Agent":      "ICD-API-Fetcher/2.0 (Health Data Pipeline)",
}
ICD_API_CLIENT_ID     = os.environ.get("ICD_API_CLIENT_ID")
ICD_API_CLIENT_SECRET = os.environ.get("ICD_API_CLIENT_SECRET")
# ---------------------------------------------------------------------------
# OAuth2 token cache
# ---------------------------------------------------------------------------
_TOKEN_CACHE: dict[str, Any] = {
    "token":      None,
    "expires_at": 0,
}

def get_access_token(force_refresh: bool = False) -> str:
    """
    Obtain an OAuth2 bearer token via client credentials flow.
    The token is cached for its lifetime (minus 60 s safety margin).

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
    return token


def _auth_headers() -> Dict[str, str]:
    """Return request headers with current bearer token."""
    headers = HEADERS.copy()
    headers["Authorization"] = f"Bearer {get_access_token()}"
    return headers


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _get(url: str, params: dict | None = None) -> dict:
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
# Text extraction helper (ICD JSON-LD uses {"@language": "en", "@value": "…"})
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
# 1. Keyword search
# ---------------------------------------------------------------------------

def get_query_details(
    query: str,
    *,
    use_flexisearch: bool = False,
    chapter_filter: str | None = None,
    subtrees_filter: str | None = None,
    properties_to_search: list[str] | None = None,
    release_id: str | None = None,
    flat_results: bool = True,
    highlighting_enabled: bool = False,
) -> List[Dict[str, Any]]:
    """
    Search ICD-11 (Foundation) by keyword via /icd/entity/search.

    Args:
        query:                 Search string, e.g. "depression". Append % for wildcard.
        use_flexisearch:       Flexible mode — results don't need to contain all words.
                               Use only when exact search yields nothing.
        chapter_filter:        Comma/semicolon-separated chapter codes, e.g. "01;06;21".
        subtrees_filter:       Comma-separated URIs to restrict search to those subtrees
                               and their descendants.
        properties_to_search:  Which properties to match against. Defaults to Title,
                               Synonym, FullySpecifiedName. Valid values: "Title",
                               "Synonym", "NarrowerTerm", "FullySpecifiedName",
                               "Definition", "Exclusion".
        release_id:            Foundation release, e.g. "2024-01". Defaults to latest.
        flat_results:          True (default) = flat list. False = nested by ICD hierarchy.
        highlighting_enabled:  Whether results include <em> highlight tags. Default False
                               since we process text programmatically.

    Returns:
        Raw API dict with keys: destinationEntities, error, errorMessage, resultChopped, words
    """
    log.info("Searching ICD-11 for: %s", query)

    params: dict[str, Any] = {
        "q":            query,
        "useFlexisearch": use_flexisearch,
        "flatResults":    flat_results,
        "highlightingEnabled": highlighting_enabled,
    }
    if chapter_filter:
        params["chapterFilter"] = chapter_filter
    if subtrees_filter:
        params["subtreesFilter"] = subtrees_filter
    if properties_to_search:
        params["propertiesToBeSearched"] = ",".join(properties_to_search)
    if release_id:
        params["releaseId"] = release_id

    result = _get(ICD_SEARCH_URL, params=params)
    hits = result.get("destinationEntities", [])
    records = [format_search_hit(h, search_context=query) for h in hits]
    num = len(hits)
    log.info("  → found %d entities", num)
    return records

# ---------------------------------------------------------------------------
# 2. Entity lookup
# ---------------------------------------------------------------------------

def _resolve_entity_uri(id_or_uri: str) -> str:
    """
    Normalise a single entity reference to a full https:// URI.

    Accepts:
      - Full Foundation URI:    "https://id.who.int/icd/entity/1839638766"
      - Full MMS URI:           "https://id.who.int/icd/release/11/mms/1839638766"
      - Full URI with http:     "http://id.who.int/icd/entity/1839638766"
      - Bare numeric ID:        "1839638766"

    Bare IDs are resolved to Foundation URIs (/icd/entity/{id}).
    If you need the MMS/linearization view, pass the full URI.
    """
    id_or_uri = id_or_uri.strip().replace("http://", "https://")
    if id_or_uri.startswith("https://"):
        return id_or_uri
    # bare numeric ID → Foundation
    return f"{ICD_FOUNDATION_BASE}/{id_or_uri}"


def get_entity_details(
        ids_or_uris: str | list[str],
        *,
        verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch and format one or more ICD entities.

    Accepts a flexible input — any of:
      - A single full URI:           "https://id.who.int/icd/entity/1839638766"
      - A single bare numeric ID:    "1839638766"
      - Comma-separated IDs/URIs:    "1839638766,1839638768"
      - A list of any of the above:  ["1839638766", "https://..."]

    Bare numeric IDs resolve to Foundation entity URIs (/icd/entity/{id}).
    Pass full URIs to target a specific linearization release instead.

    Returns:
        List of formatted entity records. Failures are skipped with a warning.
        Always returns a list, even for a single input.
    """
    if isinstance(ids_or_uris, str):
        items = [part.strip() for part in ids_or_uris.split(",") if part.strip()]
    else:
        items = []
        for entry in ids_or_uris:
            items.extend(part.strip() for part in entry.split(",") if part.strip())
    results = []
    total   = len(items)
    for i, item in enumerate(items, 1):
        uri = _resolve_entity_uri(item)
        try:
            raw       = _get(uri)
            formatted = format_entity_record(raw)
            results.append(formatted)
            if verbose:
                log.info("  [%d/%d] %s → %s", i, total, uri, formatted.get("title", "?"))
        except Exception as exc:
            log.warning("  [%d/%d] Failed %s: %s", i, total, uri, exc)

    return results

# ---------------------------------------------------------------------------
# Record formatter
# ---------------------------------------------------------------------------

def format_entity_record(entity: Dict[str, Any], search_context: str = "") -> Dict:
    """
    Normalise a raw ICD API entity dict into a flat record.

    Handles JSON-LD label objects ({"@language":..., "@value":...}) correctly.

    Fields returned:
        icd_code        – alphanumeric code (e.g. "6A70", "F32.1", "")
        icd_uri         – full API URI
        browser_url     – link to ICD browser
        class_kind      – "chapter" | "block" | "window" | "category"
        title           – preferred label (English)
        definition      – short definition
        long_definition – additional information / long definition
        fully_specified_name
        synonyms        – list of altLabel strings
        inclusion       – list of inclusion term strings
        exclusion       – list of {"label": …, "linearization_uri": …} dicts
        index_terms     – flattened list of index term strings
        parent_uris     – list of parent URIs
        child_uris      – list of child URIs
        foundation_uri  – source URI in ICD-11 Foundation (for ICD-10→11 mapping)
        postcoordination_scales – raw postcoordination axes (for thorough coding)
        source          – "ICD-11" or "ICD-10"
        search_context  – original query (audit trail)
    """
    # --- identifiers ---------------------------------------------------------
    icd_uri     = entity.get("@id") or entity.get("id", "")
    icd_code    = _label(entity.get("code", "")) or icd_uri.split("/")[-1]
    browser_url = entity.get("browserUrl", "")
    class_kind  = entity.get("classKind", "")

    # Detect ICD-10 vs ICD-11 from the URI
    source = "ICD-10" if "/release/10/" in icd_uri else "ICD-11"

    # --- labels --------------------------------------------------------------
    title               = _label(entity.get("title", ""))
    definition          = _label(entity.get("definition", ""))
    long_definition     = _label(entity.get("longDefinition", ""))
    fully_specified     = _label(entity.get("fullySpecifiedName", ""))

    # --- synonyms (SKOS altLabel) --------------------------------------------
    synonyms = []
    for s in entity.get("synonym", []):
        lbl = _label(s.get("label") if isinstance(s, dict) else s)
        if lbl:
            synonyms.append(lbl)

    # --- inclusion terms -----------------------------------------------------
    inclusions = []
    for inc in entity.get("inclusion", []):
        lbl = _label(inc.get("label") if isinstance(inc, dict) else inc)
        if lbl:
            inclusions.append(lbl)

    # --- exclusion terms (with cross-reference URI) -------------------------
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

    # --- index terms (synonyms + foundation children not in linearization) ---
    index_terms = []
    for term in entity.get("indexTerm", []):
        lbl = _label(term.get("label") if isinstance(term, dict) else term)
        if lbl:
            index_terms.append(lbl)

    # --- hierarchy -----------------------------------------------------------
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

    # ICD-11 Foundation source URI (used for ICD-10 → ICD-11 cross-walking)
    foundation_uri = entity.get("source", "")
    if isinstance(foundation_uri, dict):
        foundation_uri = foundation_uri.get("@id", "")
    if foundation_uri:
        foundation_uri = foundation_uri.replace("http://", "https://")

    # --- postcoordination (thorough ICD-11 coding) --------------------------
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


# ---------------------------------------------------------------------------
# Format search result entities
# ---------------------------------------------------------------------------

def format_search_hit(hit: Dict[str, Any], search_context: str = "") -> Dict:
    """
    Format a single entry from the search API's destinationEntities list.

    The search result schema is lighter than the full entity schema.
    Fields: id, title, synonyms, theCode, foundationUri, linearizationUri, ...
    """
    uri        = hit.get("id", "")
    icd_code   = hit.get("theCode", "") or uri.split("/")[-1]
    title      = _label(hit.get("title", ""))
    definition = _label(hit.get("definition", ""))

    synonyms = []
    for s in hit.get("synonyms", []):
        lbl = _label(s.get("label", s) if isinstance(s, dict) else s)
        if lbl:
            synonyms.append(lbl)

    matching_terms = [
        t.get("label") or t.get("Label", "")
        for t in hit.get("matchingPVs", []) if t
    ]
    source = "ICD-10" if "icd10" in uri or "/release/10/" in uri else "ICD-11"

    return {
        "icd_code":        icd_code,
        "icd_uri":         uri.replace("http://", "https://"),
        "title":           title,
        "definition":      definition,
        "synonyms":        synonyms,
        "matching_terms":  matching_terms,   # terms that caused this hit
        "foundation_uri":  hit.get("foundationUri", "").replace("http://", "https://"),
        "score":           hit.get("score", 0),
        "source":          source,
        "search_context":  search_context,
    }