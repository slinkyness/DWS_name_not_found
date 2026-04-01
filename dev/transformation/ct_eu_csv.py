from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd
import numpy as np


CTEU_PATH = "data/CTIS_trials_20260327.csv"
IHME_PATH = "data/IHME-GBD_2023_DATA-f8f3eec3-1.csv"
IHME_METADATA = "data/ihme_metadata.json"

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

def _extract_pairs(df) -> Dict[str, Tuple[str, str]]:
    pairs = {}
    cols = set(df.columns)

    for col in df.columns:
        if col.endswith('_id'):
            base = col[:-3]
            name_col = f"{base}_name"
            if name_col in cols:
                pairs[base] = (col, name_col)

    return pairs

def _create_metadata(df: pd.DataFrame, output_path: str | Path) -> Dict[str, Any]:
    output_path = Path(output_path)
    metadata = {}
    pairs = _extract_pairs(df)
    for dimension, (id_col, name_col) in sorted(pairs.items()):
        log.info("Extracting %s...", dimension)
        unique_pairs = df[[id_col, name_col]].drop_duplicates(id_col, keep='first')
        sorted_pairs = unique_pairs.sort_values(by=id_col, ascending=True)
        id_to_name = dict(
            zip(
                sorted_pairs[id_col].astype(str),
                sorted_pairs[name_col].astype(str)
            )
        )
        metadata[dimension] = {
            "id_column": id_col,
            "name_column": name_col,
            "count": len(id_to_name),
            "mappings": id_to_name,
        }
        log.info("  -> %d unique %s values", len(id_to_name), dimension)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    log.info("Metadata saved to: %s", output_path)

    return metadata

def main():
    cteu_df = pd.read_csv(CTEU_PATH, sep=",", header=0)
    ihme_df = pd.read_csv(IHME_PATH, sep=",", header=0)
    ihme_mapping = _create_metadata(ihme_df, IHME_METADATA)
    cols_to_crop = [col for col in ihme_df.columns if col.endswith("_name")]
    ihme_df.drop(columns=cols_to_crop, inplace=True)

    ...


if __name__ == "__main__":
    main()