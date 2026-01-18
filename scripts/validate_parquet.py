from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate transformed Parquet output (schema + basic DQ).")
    parser.add_argument("--input", required=True, help="Parquet file path to validate.")
    args = parser.parse_args()

    p = Path(args.input)
    if not p.exists():
        raise FileNotFoundError(f"Validation FAIL: Parquet file not found: {p}")

    df = pd.read_parquet(p)

    # Contract: required columns must exist
    required_cols = ["ReadingDate", "Date", "Time", "Price", "Price_with_VAT"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Validation FAIL: Missing required columns: {missing}. Found: {list(df.columns)}")

    # DQ: must have rows
    if df.empty:
        raise ValueError("Validation FAIL: Parquet contains 0 rows.")

    # DQ: Price columns must be numeric and not null
    for col in ("Price", "Price_with_VAT"):
        bad_nulls = df[col].isna().sum()
        if bad_nulls:
            raise ValueError(f"Validation FAIL: Column '{col}' has {bad_nulls} null values.")

        # numeric check (tolerant): convert and ensure not all become NaN
        conv = pd.to_numeric(df[col], errors="coerce")
        if conv.isna().any():
            examples = df.loc[conv.isna(), col].head(5).tolist()
            raise ValueError(f"Validation FAIL: Column '{col}' has non-numeric values. Examples: {examples}")

    logger.info("Validation OK: %s", p)
    logger.info("Rows=%s Cols=%s", len(df), len(df.columns))
    logger.info("ReadingDate min=%s max=%s", df["ReadingDate"].min(), df["ReadingDate"].max())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
