import csv
import re
import polars as pl

def extract_icd_codes(value: str) -> dict:
    if value is None:
        return {"icd10": [], "icd11": []}
    return {
        "icd10_codes": re.findall(r'\[([A-Z]\d{2}(?:\.\d{1,2})?)\]', value),
        "icd11_codes": re.findall(r'\[([A-Z0-9]{2}\d{2}(?:\.\d{1,2})?)\]', value),
    }

PHASE_MAP = {
    "I": 1, "II": 2, "III": 3, "IV": 4, "V": 5
}
def parse_phase(value: str) -> int | None:
    if value is None:
        return None
    match = re.search(r'\b(V?I{1,3}|IV)\b', value)
    return PHASE_MAP.get(match.group(1)) if match else None

def parse_country_status(value: str) -> list[dict] | None:
    if value is None:
        return None
    pairs = re.split(r',\s*(?=[A-Z][a-z]+:)', value)
    return [
        {"country": p.split(":")[0].strip(), "status": p.split(":")[1].strip()}
        for p in pairs
        if ":" in p
    ]

def normalize_medical_condition(value: str) -> str | None:
    if value is None:
        return None
    return " ".join(
        word.capitalize() if word.isupper() and len(word) > 1 else word
        for word in value.split(" ")
    )

with open("data/CTIS_trials_20260327.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f, quotechar='"', doublequote=True)
    headers = next(reader)
    rows = list(reader)

df = (
    pl.DataFrame(rows, schema=headers, orient="row")
    .with_columns(
        pl.col(pl.String).replace("N/A", None)
    )
    .with_columns(
        pl.col("Therapeutic area")
        .map_elements(
            extract_icd_codes,
            return_dtype=pl.Struct({"icd10_codes": pl.List(pl.String),
                                    "icd11_codes": pl.List(pl.String)})
        ).alias("icd_codes"),
        pl.col("Medical conditions").map_elements(
            normalize_medical_condition,
            return_dtype=pl.String
        ),
        pl.col("Trial results").map_elements(
            lambda v: v == "Yes" if v is not None else None,
            return_dtype=pl.Boolean
        ),
        pl.col("Trial phase").map_elements(
            parse_phase, return_dtype=pl.Int8
        )
        .alias("phase"),
        pl.col("Location(s) and recruitment status").map_elements(
            parse_country_status, return_dtype=pl.List(
                pl.Struct(
                    {"country": pl.String, "status": pl.String}
                )
            )
        )
        .alias("country_status"),
        pl.col("Number of participants enrolled").str.to_integer(),
        *[
            pl.col(col).str.to_date("%d/%m/%Y")
            for col in headers
            if "date" in col.lower()
        ],
        *[
            pl.col(col).str.split(", ")
            for col in headers
            if any(x in col.lower() for x in ["gender", "age group", "age range secondary identifier"])
        ],
    )
    .rename({col: col.lower().replace(" ", "_") for col in headers})
)
icd10_codes = df["icd_codes"].struct.field("icd10_codes").explode().drop_nulls().unique().to_list()