from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import logging

from pendulum import now

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def project_root() -> Path:
    # Works on host and inside Docker (/opt/airflow/scripts -> /opt/airflow)
    return Path(__file__).resolve().parents[1]


def latest_json_file(raw_dir: Path) -> Path:
    files = sorted(raw_dir.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError(f"No JSON files found in: {raw_dir}")
    return files[-1]


def extract_records(payload: Any) -> list[dict]:
    """
    EnergyZero responses can be a list OR a dict with a list under some key.
    This function tries common keys safely.
    """
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("Prices", "prices", "data", "results", "items"):
            val = payload.get(key)
            if isinstance(val, list):
                return val

    raise ValueError(
        "Could not find a list of records in the JSON payload. "
        "Expected a list or a dict containing a list under keys like Prices/prices/data/results/items."
    )


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Rename common variations to match the project requirement
    rename_map = {}
    if "readingDate" in df.columns and "ReadingDate" not in df.columns:
        rename_map["readingDate"] = "ReadingDate"
    if "price" in df.columns and "Price" not in df.columns:
        rename_map["price"] = "Price"
    if "value" in df.columns and "Price" not in df.columns:
        # sometimes APIs use 'value' instead of 'price'
        rename_map["value"] = "Price"

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def main() -> int:
    root = project_root()
    raw_dir = root / "data" / "raw"
    processed_dir = root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    src = latest_json_file(raw_dir)
    payload = json.loads(src.read_text(encoding="utf-8"))
    records = extract_records(payload)

    df = pd.DataFrame.from_records(records)
    df = standardize_columns(df)

    # Validate required columns
    if "ReadingDate" not in df.columns:
        raise KeyError(f"Missing required column 'ReadingDate'. Found columns: {list(df.columns)}")
    if "Price" not in df.columns:
        raise KeyError(f"Missing required column 'Price'. Found columns: {list(df.columns)}")

    # Parse datetime (robust parsing)
    dt = pd.to_datetime(df["ReadingDate"], utc=True, errors="coerce")
    if dt.isna().any():
        bad = df.loc[dt.isna(), "ReadingDate"].head(5).tolist()
        raise ValueError(f"Some ReadingDate values could not be parsed. Examples: {bad}")

    df["Date"] = dt.dt.date
    df["Time"] = dt.dt.strftime("%H:%M:%S")

    # Ensure Price numeric and compute VAT
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    if df["Price"].isna().any():
        bad = df.loc[df["Price"].isna(), "Price"].head(5).tolist()
        raise ValueError(f"Some Price values are not numeric. Examples: {bad}")

    df["Price_with_VAT"] = df["Price"] * 1.21

    # Optional: tidy column order (keep all original columns too)
    preferred = ["ReadingDate", "Date", "Time", "Price", "Price_with_VAT"]
    cols = preferred + [c for c in df.columns if c not in preferred]
    df = df[cols]

    
    out_file = processed_dir / f"energy_transformed_{now('UTC'):%Y%m%d_%H%M%S}.parquet"
    df.to_parquet(out_file, index=False)

    print(f"Source JSON: {src}")
    print(f"Saved Parquet: {out_file}")
    print(f"Rows: {len(df)} | Columns: {len(df.columns)}")
    print(df.head(3).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
