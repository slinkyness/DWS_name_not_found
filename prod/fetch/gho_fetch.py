from __future__ import annotations

import logging
from typing import Any

import requests

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
GHO_BASE = "https://ghoapi.azureedge.net/api"
TIMEOUT = 30

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "GHO-Indicators-Fetcher/1.0",
}


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _get(path: str, params: dict | None = None) -> list | dict:
    """GET {GHO_BASE}/{path}, unwrap OData 'value' wrapper."""
    url  = f"{GHO_BASE}/{path}"
    log.info("GET %s  params=%s", url, params or "(none)")
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    return data.get("value", data) if isinstance(data, dict) else data


# ── Catalogues ─────────────────────────────────────────────────────────────────

def fetch_all_indicators(search: str | None = None) -> list[dict]:
    """
    Fetch the GHO indicator catalogue.
    If search is given, filters server-side with OData contains().
    """
    params: dict[str, Any] = {}
    if search:
        params["$filter"]  = f"contains(IndicatorName,'{search}')"
        params["$orderby"] = "IndicatorCode asc"
    raw = _get("Indicator", params or None)
    return [{"code": r["IndicatorCode"], "name": r["IndicatorName"]} for r in raw]


def fetch_geo_catalogue() -> dict[str, dict]:
    """
    Fetch WHO region and country dimension values.
    Returns a lookup dict keyed by spatial code with name, type, region info.
    """
    lookup: dict[str, dict] = {}

    for r in _get("DIMENSION/REGION/DimensionValues"):
        code = r.get("Code", "")
        if not code:
            continue
        name = r.get("Title", code)
        lookup[code] = {
            "code":        code,
            "name":        name,
            "type":        r.get("Dimension", "REGION"),
            "region_code": code,
            "region_name": name,
        }

    for r in _get("DIMENSION/COUNTRY/DimensionValues"):
        code = r.get("Code", "")
        if not code:
            continue
        parent_code  = r.get("ParentCode", "")
        parent_entry = lookup.get(parent_code, {})
        lookup[code] = {
            "code":        code,
            "name":        r.get("Title", code),
            "type":        r.get("Dimension", "COUNTRY"),
            "region_code": parent_code,
            "region_name": parent_entry.get("name", parent_code),
        }

    log.info(
        "fetch_geo_catalogue — %d entries (%d regions, %d countries)",
        len(lookup),
        sum(1 for v in lookup.values() if v["type"] == "REGION"),
        sum(1 for v in lookup.values() if v["type"] == "COUNTRY"),
    )
    return lookup


# ── Indicator data ─────────────────────────────────────────────────────────────

def _clean_record(r: dict) -> dict:
    sdt         = r.get("SpatialDimType", "")
    spatial_dim = r.get("SpatialDim", "") or ""

    if sdt == "REGION":
        country_code = ""
        region_code  = spatial_dim
    elif sdt == "COUNTRY":
        country_code = spatial_dim
        region_code  = r.get("ParenLocationCode", "")
    else:
        country_code = country_name = region_code = region_name = ""

    return {
        "indicator_code":    r.get("IndicatorCode", ""),
        "spatial_dim_type":  sdt,
        "spatial_dim":       spatial_dim,
        "country_code":      country_code,
        "region_code":       region_code,
        "year":              r.get("TimeDim"),
        "dim1_type":         r.get("Dim1Type"),
        "dim1":              r.get("Dim1"),
        "value_display":     r.get("Value"),
        "numeric_value":     r.get("NumericValue"),
        "low":               r.get("Low"),
        "high":              r.get("High"),
        "comments":          r.get("Comments"),
        "last_updated":      r.get("Date"),
        "period_begin":      r.get("TimeDimensionBegin"),
        "period_end":        r.get("TimeDimensionEnd"),
    }


def fetch_indicator(
    indicator_code:   str,
    year:             int | list[int] | None = None,
    region:           str | list[str] | None = None,
    spatial_dim_type: str | None = None,
) -> dict[str, Any]:
    """
    Fetch and filter records for one GHO indicator.

    Returns a dict with: indicator_code, records, total_fetched,
    total_after_filter, available_years, available_regions,
    available_spatial_types, fetch_params.
    """
    year_set   = ({year}           if isinstance(year,   int) else
                  set(year)        if year   is not None      else None)
    region_set = ({region.upper()} if isinstance(region, str) else
                  {r.upper() for r in region} if region is not None else None)
    sdt_filter = spatial_dim_type.upper() if spatial_dim_type else None

    raw           = _get(indicator_code)
    total_fetched = len(raw)

    records:           list[dict] = []
    available_years:   set[int]   = set()
    available_regions: set[str]   = set()
    available_sdts:    set[str]   = set()

    for r in raw:
        cleaned = _clean_record(r)
        if cleaned["year"] is not None:
            available_years.add(cleaned["year"])
        if cleaned["spatial_dim"]:
            available_regions.add(cleaned["spatial_dim"])
        if cleaned["spatial_dim_type"]:
            available_sdts.add(cleaned["spatial_dim_type"])

        if year_set and cleaned["year"] not in year_set:
            continue
        if region_set and cleaned["spatial_dim"].upper() not in region_set:
            continue
        if sdt_filter and cleaned["spatial_dim_type"].upper() != sdt_filter:
            continue
        records.append(cleaned)

    log.info(
        "fetch_indicator %s — fetched=%d  after_filter=%d",
        indicator_code, total_fetched, len(records),
    )

    return {
        "records":                 records,
        "total_fetched":           total_fetched,
        "total_after_filter":      len(records),
        "available_years":         sorted(available_years),
        "available_regions":       sorted(available_regions),
        "available_spatial_types": sorted(available_sdts),
    }


# ── Public entry point ─────────────────────────────────────────────────────────

def fetch_gho(
    input_indicators: list[str]                 = (),
    search:           str | None                = None,
    year:             int | list[int] | None    = None,
    region:           str | list[str] | None    = None,
    spatial_dim_type: str | None                = None,
    get_catalogue:    bool                      = False,
) -> dict[str, Any]:
    """
    Fetch GHO catalogues and any requested indicator data.

    Indicator resolution order:
      1. input_indicators — used as-is
      2. search           — keyword searched against catalogue; matching codes appended

    If neither is given, only the catalogues are stored (no indicator data).

    Returns dict with:
      indicator_catalogue  list[dict]
      geo_catalogue        dict
      indicators           dict  {code: fetch_indicator() result}
      total_fetched        int   sum of total_after_filter across indicators
      total_indicators     int   len(indicators)
    """
    log.info("Fetching indicator catalogue and geo catalogue...")
    indicator_catalogue = geo_catalogue = []
    if get_catalogue:
        indicator_catalogue = fetch_all_indicators()
        geo_catalogue       = fetch_geo_catalogue()

    codes: list[str] = list(input_indicators)

    if search:
        log.info("Searching indicator catalogue for: %s", search)
        matches  = fetch_all_indicators(search=search)
        existing = set(codes)
        for entry in matches:
            if entry["code"] not in existing:
                codes.append(entry["code"])
                existing.add(entry["code"])
        log.info("Search '%s' matched %d indicator(s), total codes: %d",
                 search, len(matches), len(codes))

    flat_records: list[dict] = []
    total_fetched = 0
    total_indicators = 0

    for code in codes:
        result = fetch_indicator(
            indicator_code=code,
            year=year,
            region=region,
            spatial_dim_type=spatial_dim_type,
        )
        total_indicators += 1
        total_fetched += result["total_after_filter"]
        if result and "records" in result:
            flat_records.extend(result["records"])

    log.info("Done — %d indicator(s), %d total records", total_indicators, total_fetched)

    return {
        "indicator_catalogue": indicator_catalogue,
        "geo_catalogue":       geo_catalogue,
        "records":             flat_records,
        "total_fetched":       total_fetched,
        "total_indicators":    total_indicators,
    }