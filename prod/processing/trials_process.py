"""
ctis_lambda.py — AWS Lambda: CTIS Clinical Trials CSV transformer
=================================================================
Triggered by an S3 event when a new CTIS CSV or CTUS JSON lands in IN_BUCKET.
Reads the precomputed lookups from OUT_BUCKET,
then upserts into a single processed parquet on OUT_BUCKET.

Upsert key:  trial_id
Update rule: existing row is overwritten when the incoming row has a
             strictly newer last_updated date; all replacements are counted and
             returned in the response.

Env vars (required):
    S3_BUCKET            – destination bucket
    S3_PROCESSED_FOLDER  – key prefix for output, e.g. "processed/ctis"
"""

from __future__ import annotations

import csv
import io
import os
import json
import re
import logging
from pathlib import Path

import polars as pl
import pycountry

log = logging.getLogger(__name__)


root_dir = Path(__file__).parent.parent.parent
DATA_DIR = root_dir / "data"
UPSERT_KEY = "trial_id"
DATE_COL = "last_updated"
_SKIP_CAST = (pl.List, pl.Struct)
_D, _W, _M, _Y = 1, 7, 30, 365
HARMONIZED_SCHEMA: dict[str, pl.DataType] = {
    # Identifiers
    "trial_id": pl.Utf8,
    # Title & summary
    "title": pl.Utf8,
    # Status
    "overall_status": pl.Utf8,
    # Phase — list of ints so multi-phase trials ("Phase I and Phase II") are preserved
    "phases": pl.List(pl.Int8),
    # Population
    "groupings": pl.List(pl.Utf8),  # categorical buckets
    "range_in_days": pl.List(pl.Struct({"min": pl.Int64, "max": pl.Int64})),
    "gender": pl.List(pl.Utf8),
    # Enrollment
    "enrollment": pl.Int64,
    # Geography — struct preserves country + approval + status together
    "countries_status": pl.List(pl.Struct({"country": pl.Utf8, "approval": pl.Utf8, "status": pl.Utf8})),
    # Conditions
    "conditions": pl.List(pl.Utf8),
    "keywords": pl.List(pl.Utf8),
    # Dates
    "start_date": pl.Datetime,
    "end_date": pl.Datetime,
    "last_updated": pl.Datetime,
    "global_end_date": pl.Datetime,
    "decision_date": pl.Datetime,
    # Sponsor
    "sponsor": pl.Utf8,
    "sponsor_type": pl.Utf8,
    "sponsor_code": pl.Utf8,
    # Product / intervention
    "methods": pl.Struct({"products": pl.List(pl.Utf8), "interventions": pl.List(pl.Struct({"name": pl.List(pl.Utf8), "type": pl.List(pl.Utf8)}))}),
    # Endpoints
    "outcomes": pl.List(pl.Struct({"text": pl.Utf8, "type": pl.Utf8, "label": pl.List(pl.Utf8)})),
    "results": pl.Boolean,
    "codes": pl.Struct({"icd_10": pl.List(pl.Utf8), "icd_11": pl.List(pl.Struct({"code": pl.Utf8, "chapter": pl.Utf8}))}),
}

UNIT_DAYS = {
    "minutes": 1/1440,
    "hours":   1/24,
    "days":    1.0,
    "weeks":   7.0,
    "months":  30.0,
    "years":   365.0,
}

LEGACY_NAMING_CTUS = {
    "lead_sponsor": "sponsor",
    "sponsor_class": "sponsor_type"
}
RENAMING_CTIS = {
    "Title of the trial": "title",
    "Trial number": "trial_id",
    "Trial results": "results",
    "Number of participants enrolled": "enrollment",
    "Overall trial status": "overall_status",
    "Protocol code": "sponsor_code",
    "Last updated": "last_updated",
    "Start date": "start_date",
    "End date": "end_date",
    "Decision date": "decision_date",
    "Global end of the trial": "global_end_date",
    "Sponsor/Co-Sponsors": "sponsor",
    "Sponsor type": "sponsor_type",
}

def _extract_num(col_name: str) -> pl.Expr:
    return pl.col(col_name).str.extract(r"(\d+(?:\.\d+)?)\s+\w+", 1)


def _extract_unit(col_name: str) -> pl.Expr:
    return pl.col(col_name).str.extract(r"\d+(?:\.\d+)?\s+(\w+)", 1).str.to_lowercase()


def _pluralize(unit_expr: pl.Expr) -> pl.Expr:
    return pl.when(unit_expr.str.ends_with("s")).then(unit_expr).otherwise(unit_expr + "s")


def unit_days_chain(unit_expr: pl.Expr) -> pl.Expr:
    keys = pl.Series(list(UNIT_DAYS.keys()))
    values = pl.Series(list(UNIT_DAYS.values()))
    return unit_expr.str.to_lowercase().replace(keys, values).cast(pl.Float64)


def parse_age_str_expr(col_name: str) -> pl.Expr:
    digits = _extract_num(col_name).cast(pl.Float64)
    unit = _pluralize(_extract_unit(col_name))
    return (digits * unit_days_chain(unit)).round(0).cast(pl.Int64).alias(col_name)


def age_range_label_expr(min_col: str, max_col: str) -> pl.Expr:
    min_num = _extract_num(min_col)
    max_num = _extract_num(max_col)
    min_unit = _pluralize(_extract_unit(min_col))
    max_unit = _pluralize(_extract_unit(max_col))

    return (
        pl.when(
            (pl.col(min_col).is_not_null()) & (pl.col(max_col).is_not_null())
        )
        .then(min_num + "-" + max_num + " " + min_unit)
        .when(pl.col(min_col).is_not_null())
        .then(min_num + "+ " + min_unit)
        .when(pl.col(max_col).is_not_null())
        .then(pl.lit("<") + max_num + " " + max_unit)
        .otherwise(None)
    )


_CTIS_RANGES: list[tuple[str, int | None, int | None]] = [
    ("preterm newborn infants (up to gestational age<37 weeks)", None, 0),
    ("in utero", None, 0),
    ("0-27 days", 0, 28),
    ("28 days-23 months", 28, 24 * _M),
    ("2-5 years", 2 * _Y, 6 * _Y),
    ("6-11 years", 6 * _Y, 12 * _Y),
    ("12-17 years", 12 * _Y, 18 * _Y),
    ("0-17 years", 0, 18 * _Y),
    ("18-64 years", 18 * _Y, 65 * _Y),
    ("65-84 years", 65 * _Y, 85 * _Y),
    ("85+ years", 85 * _Y, None),
    ("65+ years", 65 * _Y, None),
]

STATUS_LOOKUP = {
    "NOT_YET_RECRUITING": "Authorised, recruitment pending",
    "RECRUITING": "Ongoing, recruiting",
    "ACTIVE_NOT_RECRUITING": "Ongoing, recruitment ended",
    "ENROLLING_BY_INVITATION": "Ongoing, recruiting",
    "COMPLETED": "Ended, ended",
    "TERMINATED": "Ended, ended",
    "WITHDRAWN": "Ended, ended",
    "SUSPENDED": "Ended, ended",
}

_icd_11_lookup_flat = pl.read_parquet(DATA_DIR / "icd_lookup_fresh.parquet").with_columns(
    pl.col("icd11_codes").list.join("&"),
    pl.col("icd11_chapter").list.join("&")
)
_icd_10_lookup_flat = pl.read_parquet(DATA_DIR / "11_10_lookup_new.parquet").with_columns(
    pl.col("icd10_codes").list.join("&"),
    pl.col("icd11_chapter").list.join("&")
)
_icd_chapter_lookup = pl.read_parquet(DATA_DIR / "icd_11.parquet").select("chapter", "icd_code")


def build_lookup_chain(text_expr: pl.Expr, side: str) -> pl.Expr:
    expr = pl.lit(None, dtype=pl.Int64)
    for pattern, mn, mx in reversed(_CTIS_RANGES):
        value = mn if side == "min" else mx
        expr = pl.when(text_expr == pattern).then(pl.lit(value, dtype=pl.Int64)).otherwise(expr)
    return expr


def _build_lookup_map():
    lookup = {"Turkey (Türkiye)": "TUR"}

    for country in pycountry.countries:
        lookup[country.name] = country.alpha_3
        for alias in getattr(country, 'common_name', []):
            lookup[alias] = country.alpha_3

    return lookup


COUNTRY_MAP = _build_lookup_map()

# Get range_in_days and grouping ctis

def _get_range_and_grouping_ctis(df: pl.DatFrame) -> pl.DataFrame:
    return(
        df
        .select("Age group", "Age range secondary identifier")
        .with_columns(
            pl.col("Age group")
              .str.replace_all(r'"', "")
              .str.strip_chars()
              .str.strip_chars('"')
              .str.replace_all(r"(?i)\bN/?A\b", "")
              .str.replace_all(r",+", ",")
              .str.strip_chars(",")
              .str.strip_chars()
              .str.split(",")
              .list.eval(
                pl.when(pl.element().str.strip_chars() != "")
                .then(pl.element().str.strip_chars().str.to_lowercase())
            )
            .list.drop_nulls()
              .alias("grp_list"),
            pl.col("Age range secondary identifier")
              .str.replace_all(r'"', "")
              .str.strip_chars()
              .str.strip_chars('"')
              .str.replace_all(r"(?i)\bN/?A\b", "")
              .str.replace_all(r",+", ",")
              .str.strip_chars(",")
              .str.strip_chars()
              .str.split(",")
              .list.eval(
                pl.when(pl.element().str.strip_chars() != "")
                .then(pl.element().str.strip_chars().str.to_lowercase())
            )
            .list.drop_nulls()
            .alias("sec_list"),
        )
        .with_row_index("_idx")
        .with_columns([
            pl.col("grp_list")
              .list.eval(pl.struct(
                  pl.element().alias("label"),
                  build_lookup_chain(pl.element(), "min").alias("min"),
                  build_lookup_chain(pl.element(), "max").alias("max"),
              ))
              .list.drop_nulls()
              .alias("grp_structs"),
            pl.col("sec_list")
              .list.eval(pl.struct(
                  pl.element().alias("label"),
                  build_lookup_chain(pl.element(), "min").alias("min"),
                  build_lookup_chain(pl.element(), "max").alias("max"),
              ))
              .list.drop_nulls()
              .alias("sec_structs"),
        ])
        .explode("grp_structs")
        .unnest("grp_structs")
        .with_columns([
            pl.col("sec_structs")
              .list.eval(pl.element().struct.field("min"))
              .list.contains(pl.col("min"))
              .alias("_sec_min_aligns"),
            pl.col("sec_structs")
              .list.eval(pl.element().struct.field("max"))
              .list.contains(pl.col("max"))
              .alias("_sec_max_aligns"),
        ])
        .with_columns(
           pl.when(pl.col("_sec_min_aligns") | pl.col("_sec_max_aligns"))
              .then(pl.col("sec_structs"))
              .otherwise(pl.lit(None))
              .alias("sec_structs"),
            pl.when(pl.col("_sec_min_aligns") & pl.col("_sec_max_aligns"))
              .then(None)
              .otherwise(pl.col("label"))
              .alias("label"),
            pl.when(pl.col("_sec_min_aligns") & pl.col("_sec_max_aligns"))
              .then(None)
              .otherwise(pl.col("min"))
              .alias("min"),
            pl.when(pl.col("_sec_min_aligns") & pl.col("_sec_max_aligns"))
              .then(None)
              .otherwise(pl.col("max"))
              .alias("max"),
        )
        .pipe(lambda df: pl.concat([
            df.filter(pl.col("label").is_not_null())
              .select("_idx", "label", "min", "max"),
            df.filter(pl.col("_sec_min_aligns") | pl.col("_sec_max_aligns"))
              .explode("sec_structs")
              .select(
                  "_idx",
                  pl.col("sec_structs").struct.field("label").alias("label"),
                  pl.col("sec_structs").struct.field("min").alias("min"),
                  pl.col("sec_structs").struct.field("max").alias("max"),
              ),
        ]))
        .with_columns(
            pl.struct("min", "max").alias("range_in_days")
        )
        .sort("_idx", "min", nulls_last=False)
        .group_by("_idx")
        .agg([
            pl.col("range_in_days"),
            pl.col("label").alias("groupings"),
        ])
    )

def _get_range_and_grouping_ctus(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_row_index("_idx")
        .with_columns([
            parse_age_str_expr("min_age").alias("min"),
            parse_age_str_expr("max_age").alias("max"),
            pl.when(age_range_label_expr("min_age", "max_age").is_not_null())
            .then(age_range_label_expr("min_age", "max_age").cast(pl.List(pl.Utf8)))
            .otherwise(pl.lit([], dtype=pl.List(pl.Utf8)))
            .alias("groupings"),
        ])
        .with_columns(
            pl.when(
                pl.col("min").is_not_null() | pl.col("max").is_not_null()
            )
            .then(pl.concat_list(pl.struct("min", "max")))
            .otherwise(pl.lit([], dtype=pl.List(pl.Struct({"min": pl.Int64, "max": pl.Int64}))))
            .alias("range_in_days")
        )
        .select("_idx", "range_in_days", "groupings")

    )

def _get_ctus_step_1(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df
        .with_columns(
            # ── Identifiers ──────────────────────────────────────────────────
            pl.col("nct_id").alias("trial_id"),
            pl.col("overall_status").str.to_uppercase()
            .replace_strict(STATUS_LOOKUP, default=pl.col("overall_status"))
            .alias("overall_status"),
            # ── Phase ─────────────────────────────────────────────────────────
            pl.col("phases")
                .list.eval(
                pl.element()
                .replace({"": None, "NA": None, "N/A": None})
                .str.strip_prefix("EARLY_")
                .str.strip_prefix("PHASE")
                .cast(pl.Int8, strict=False)
            )
            .list.eval(
                pl.element().filter(pl.element().is_not_null())
            )
            .cast(pl.List(pl.Int8)),
            # ── Sponsor ─────────────────────────────────────────────────────────
            pl.col("sponsor_type").str.to_titlecase(),
            # ── Outcome ─────────────────────────────────────────────────────────
            pl.concat_list(
                pl.struct(
                    text=pl.col("primary_outcomes"),
                    type=pl.lit("primary"),
                    label=pl.lit([], dtype=pl.List(pl.Utf8)),
                ),
                pl.struct(
                    text=pl.col("secondary_outcomes"),
                    type=pl.lit("secondary"),
                    label=pl.lit([], dtype=pl.List(pl.Utf8)),
                )
            ).alias("outcomes"),
            # ── Gender ─────────────────────────────────────────────────────────
            pl.when(pl.col("sex").str.to_lowercase() == "all")
            .then(["Female", "Male"])
            .otherwise(
                pl.col("sex")
                .str.to_titlecase()
                .cast(pl.List(pl.String))
            )
            .alias("gender"),
            # ── Date Formatting ─────────────────────────────────────────────────────────
            (pl.selectors.matches(r"(?i).*(date|updated)$") & pl.selectors.string())
            .str.replace(r"^(\d{4}-\d{2})$", r"${1}-01").str.to_date(format="%Y-%m-%d", strict=False),
            # ── Location and Status  ─────────────────────────────────────────────────────────
            pl.col("locations")
            .list.eval(
                pl.struct(
                    country=pl.element().struct.field("country")
                    .replace_strict(COUNTRY_MAP, default=pl.element().struct.field("country")),
                    approval=pl.element().struct.field("recruitment_status")
                    .str.to_uppercase()
                    .replace_strict(STATUS_LOOKUP, default="Ended, ended")
                    .str.extract(r"^([^,]+)")
                    .str.strip_chars(),
                    status=pl.element().struct.field("recruitment_status")
                    .str.to_uppercase()
                    .replace_strict(STATUS_LOOKUP, default="Ended, ended")
                    .str.extract(r",(.+)$")
                    .str.strip_chars(),
                )
            )
            .list.unique()
            .alias("countries_status")
            )
    )
    return df

def _get_ctus_step_2(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_row_index("_idx")
        .with_columns(
            pl.col("intervention_types").list.len().alias("_type_len"),
            pl.col("intervention_names").list.len().alias("_names_len"),
        )
        .with_columns(
            pl.when(pl.col("_type_len") < pl.col("_names_len"))
            .then(pl.col("intervention_types").list.concat(
                pl.lit("OTHER")
                .repeat_by(
                    pl.col("intervention_names").list.len() - pl.col("intervention_types").list.len()
                )))
            .when(pl.col("_type_len") == pl.col("_names_len"))
            .then(pl.col("intervention_types"))
            .otherwise(pl.lit(None))
            .alias("types")
        )
        .with_columns(
            pl.struct(
                name=pl.col("intervention_names"),
                type=pl.col("types"),
            )
            .alias("intervention"),
            pl.lit([], dtype=pl.List(pl.Utf8)).alias("products")
        )
        .group_by("_idx")
        .agg(
            pl.col("intervention").alias("interventions"),
            pl.col("products").first().alias("products"),
        )
        .with_columns(
            pl.struct(
                products=pl.col("products"),
                interventions=pl.col("interventions"),
            ).alias("methods")
        )
        .drop("products", "interventions")
    )

def _get_ctus_step_3(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_row_index("_idx")
        .with_columns(
            pl.col("mesh_ids")
            .list.eval(
                pl.element().str.slice(0, 3).replace(
                    pl.Series(_icd_11_lookup_flat["icd10Code"]),
                    pl.Series(_icd_11_lookup_flat["icd11_codes"]),
                )
                .str.split("&")
                .explode()
            )
            .list.unique().alias("icd_11_codes"),
            pl.col("mesh_ids")
            .list.eval(
                pl.element().str.slice(0, 3)
            )
            .list.unique().alias("icd_10_codes")
        )
        .with_columns(
            pl.col("icd_11_codes")
            .list.eval(
                pl.element().replace(
                    pl.Series(_icd_chapter_lookup["icd_code"]),
                    pl.Series(_icd_chapter_lookup["chapter"]),
                )
            )
            .list.unique()
            .alias("icd_11_chapter")
        )
        .explode("icd_11_codes")
        .explode("icd_11_chapter")
        .with_columns(
            pl.struct(
                code=pl.col("icd_11_codes"),
                chapter=pl.col("icd_11_chapter"),
            ).alias("icd_11")
        )
        .group_by("_idx")
        .agg(
            pl.col("icd_11").alias("icd_11_codes"),
            pl.col("icd_10_codes").first(),
        )
        .with_columns(
            pl.struct(
                icd_10=pl.col("icd_10_codes"),
                icd_11=pl.col("icd_11_codes"),
            ).alias("codes")
        )
    )

def _get_ctis_step_1(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_columns(
            pl.col("Trial results").replace_strict({"Yes": True, "No": False}, default=None,
                                                   return_dtype=pl.Boolean),
            pl.struct(
                products=pl.when(pl.col("Product").is_not_null())
                .then(pl.concat_list(pl.col("Product")))
                .otherwise(pl.lit([], dtype=pl.List(pl.Utf8))),
                interventions=pl.lit([],
                                     dtype=pl.List(
                                         pl.Struct(
                                             {"name": pl.List(pl.Utf8), "type": pl.List(pl.Utf8)}
                                         )
                                     )
                                     ),
            )
            .alias("methods"),
            pl.col("Gender").str.split(",")
            .list.eval(
                pl.element().str.strip_chars()
            ).alias("gender"),
            pl.lit([]).alias("keywords"),
            pl.col("Medical conditions").str.split(".").alias("conditions"),
            pl.col("Trial phase").str.split("and")
                .list.eval(
                pl.element()
                .str.strip_chars()
                .str.extract(r"\b(IV|III|II|I)\b")
                .replace({"IV": "4", "III": "3", "II": "2", "I": "1"})
                .cast(pl.Int8, strict=False)
            )
            .alias("phases"),
            pl.col("Location(s) and recruitment status")
            .str.replace_all(r", ([A-Z])", r"||$1")
            .str.replace_all("Ended", "ended, ended")
            .str.split("||")
            .list.eval(
                pl.struct(
                    country=pl.element().str.extract(r"^([^:]+)").str.strip_chars()
                    .replace_strict(COUNTRY_MAP, default=pl.element()),
                    approval=pl.element().str.extract(r":([^,]+)").str.strip_chars(),
                    status=pl.element().str.extract(r",(.+)$").str.strip_chars(),
                )
            ).alias("countries_status"),
            (pl.selectors.matches(r"(?i).*(date|updated)$") & pl.selectors.string()).str.to_date("%d/%m/%Y"),
            pl.concat_list(
                pl.struct(
                    text=pl.when(pl.col("Primary endpoint").is_not_null())
                .then(pl.concat_list(pl.col("Primary endpoint")))
                .otherwise(pl.lit([], dtype=pl.List(pl.Utf8))),
                    type=pl.lit("primary"),
                    label=pl.lit([], dtype=pl.List(pl.Utf8)),
                ),
                pl.struct(
                    text=pl.when(pl.col("Secondary endpoints").is_not_null())
                .then(pl.concat_list(pl.col("Secondary endpoints")))
                .otherwise(pl.lit([], dtype=pl.List(pl.Utf8))),
                    type=pl.lit("secondary"),
                    label=pl.lit([], dtype=pl.List(pl.Utf8)),
                )
            ).list.eval(
                pl.element().filter(pl.element().struct.field("text").is_not_null())
            ).alias("outcomes")
        )
        .rename(RENAMING_CTIS)
    )


def _get_ctis_step_2(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_row_index("_idx")
        .select("Therapeutic area", "_idx")
        .with_columns(
            pl.col("Therapeutic area")
            .str.extract_all(r"\[([A-Z]\d{2}(?:\.\d{1,2})?)\]")
            .list.eval(pl.element().str.strip_chars("[]")).alias("icd_11_codes"),
            pl.col("Therapeutic area")
            .str.extract_all(r"\[([A-Z0-9]{2}\d{2}(?:\.\d{1,2})?)\]")
            .list.eval(pl.element().str.strip_chars("[]")).alias("icd_10_codes"),
        )
        .with_columns(
            pl.when(pl.col("icd_10_codes").list.len() > 0)
            .then(
                pl.col("icd_10_codes")
                .list.eval(
                    pl.element().replace(
                        pl.Series(_icd_11_lookup_flat["icd10Code"]),
                        pl.Series(_icd_11_lookup_flat["icd11_codes"]),
                    )
                    .str.split("&")
                    .explode()
                )
                .list.concat(pl.col("icd_11_codes"))
                .list.unique()
            )
            .otherwise(pl.col("icd_11_codes"))
            .alias("icd_11_codes"),
            pl.when(pl.col("icd_11_codes").list.len() > 0)
            .then(
                pl.col("icd_11_codes")
                .list.eval(
                    pl.element().replace(
                        pl.Series(_icd_10_lookup_flat["icd11Code"]),
                        pl.Series(_icd_10_lookup_flat["icd10_codes"]),
                    )
                    .str.split("&")
                    .explode()
                )
                .list.concat(pl.col("icd_10_codes"))
                .list.unique()
            )
            .otherwise(pl.col("icd_10_codes"))
            .alias("icd_10_codes"),
        )
        .with_columns(
            pl.col("icd_11_codes")
            .list.eval(
                pl.element().replace(
                    pl.Series(_icd_chapter_lookup["icd_code"]),
                    pl.Series(_icd_chapter_lookup["chapter"]),
                )
            )
            .list.unique()
            .alias("icd_11_chapter")
        )
        .explode("icd_11_codes")
        .explode("icd_11_chapter")
        .with_columns(
            pl.struct(
                code=pl.col("icd_11_codes"),
                chapter=pl.col("icd_11_chapter"),
            ).alias("icd_11")
        )
        .group_by("_idx")
        .agg(
            pl.col("icd_11").alias("icd_11_codes"),
            pl.col("icd_10_codes").first(),
        )
        .with_columns(
            pl.struct(
                icd_10=pl.col("icd_10_codes"),
                icd_11=pl.col("icd_11_codes"),
            ).alias("codes")
        )
    )


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_ctis(path: str) -> pl.DataFrame:
    import csv
    with open(path, "r", encoding="utf-8-sig") as csvfile:
        reader = csv.reader(csvfile, quotechar='"', doublequote=True)
        headers = [h.strip() for h in next(reader)]
        rows = list(reader)
    return (
        pl.DataFrame(rows, schema=headers, orient="row")
        .with_columns(
            pl.selectors.string().replace({"": None, "NA": None, "N/A": None})
        )
    )


def load_ctus(path: str) -> pl.DataFrame:
    return (
        pl.read_json(path)
        .unnest("data")
        .select("trials")
        .explode("trials")
        .unnest("trials")
        .with_columns(
            pl.selectors.string().replace({"": None, "NA": None, "N/A": None})
        )
        .rename(LEGACY_NAMING_CTUS)
    )


# ── Unified entry point ────────────────────────────────────────────────────────

def process(path: str, source: str) -> pl.DataFrame:
    """
    Transform a raw CTIS (.csv) or CTUS (.json) file into the harmonized schema.

    Parameters
    ----------
    path   : file path to the raw input
    source : "ctis" or "ctus"
    """
    if source == "ctis":
        raw = load_ctis(path)
        step1 = _get_ctis_step_1(raw)
        codes = _get_ctis_step_2(raw)
        range_grp = _get_range_and_grouping_ctis(raw)
        result = (
            step1
            .with_row_index("_idx")
            .join(codes.select("_idx", "codes"), on="_idx", how="left")
            .join(range_grp.select("_idx", "range_in_days", "groupings"), on="_idx", how="left")
            .drop("_idx")
        )
    elif source == "ctus":
        raw = load_ctus(path)
        step1 = _get_ctus_step_1(raw)
        step2 = _get_ctus_step_2(raw)
        codes = _get_ctus_step_3(raw)
        range_grp = _get_range_and_grouping_ctus(raw)
        result = (
            step1
            .with_row_index("_idx")
            .join(step2.select("_idx", "methods"), on="_idx", how="left")
            .join(codes.select("_idx", "codes"), on="_idx", how="left")
            .join(range_grp.select("_idx", "range_in_days", "groupings"), on="_idx", how="left")
            .drop("_idx")
        )
    else:
        raise ValueError(f"Unknown source '{source}'. Expected 'ctis' or 'ctus'.")

    # Cast to harmonized schema — only columns present in the schema
    return result.select(
        [
            pl.col(col) if isinstance(dtype, _SKIP_CAST)
            else pl.col(col).cast(dtype, strict=False)
            for col, dtype in HARMONIZED_SCHEMA.items()
        ]
    )


if __name__ == "__main__":
    ctus_path = "../../data/ct_us_fetch.json"
    ctis_path = "../../data/CTIS_trials_20260327.csv"
    ctus_result = process(ctus_path, "ctus")
    ctis_result = process(ctis_path, "ctis")
    merged = pl.concat([ctus_result, ctis_result])
    merged.write_parquet("trials.parquet")