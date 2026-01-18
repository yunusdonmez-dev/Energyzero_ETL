from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def latest_json_file(raw_dir: Path) -> Path:
    files = sorted(raw_dir.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError(f"No JSON files found in: {raw_dir}")
    return files[-1]


def extract_records(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("Prices", "prices", "data", "results", "items"):
            val = payload.get(key)
            if isinstance(val, list):
                return val
    raise ValueError(
        "Could not find list of records in JSON payload. "
        "Expected list or dict containing list under Prices/prices/data/results/items."
    )


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    if "readingDate" in df.columns and "ReadingDate" not in df.columns:
        rename_map["readingDate"] = "ReadingDate"
    if "price" in df.columns and "Price" not in df.columns:
        rename_map["price"] = "Price"
    if "value" in df.columns and "Price" not in df.columns:
        rename_map["value"] = "Price"
    return df.rename(columns=rename_map) if rename_map else df


def main() -> int:
    parser = argparse.ArgumentParser(description="Transform EnergyZero JSON to Parquet.")
    parser.add_argument("--input", type=str, default="", help="Input JSON path. If empty, use latest in data/raw.")
    parser.add_argument("--output", type=str, default="", help="Output Parquet path. If empty, write timestamped file.")
    parser.add_argument("--vat-rate", type=float, default=0.21, help="VAT rate (e.g., 0.21 for 21%).")
    args = parser.parse_args()

    root = project_root()
    raw_dir = root / "data" / "raw"
    processed_dir = root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    src = Path(args.input) if args.input else latest_json_file(raw_dir)
    payload = json.loads(src.read_text(encoding="utf-8"))
    records = extract_records(payload)

    df = pd.DataFrame.from_records(records)
    df = standardize_columns(df)

    if "ReadingDate" not in df.columns:
        raise KeyError(f"Missing required column 'ReadingDate'. Found: {list(df.columns)}")
    if "Price" not in df.columns:
        raise KeyError(f"Missing required column 'Price'. Found: {list(df.columns)}")

    dt = pd.to_datetime(df["ReadingDate"], utc=True, errors="coerce")
    if dt.isna().any():
        bad = df.loc[dt.isna(), "ReadingDate"].head(5).tolist()
        raise ValueError(f"Some ReadingDate values could not be parsed. Examples: {bad}")

    df["Date"] = dt.dt.date
    df["Time"] = dt.dt.strftime("%H:%M:%S")

    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    if df["Price"].isna().any():
        raise ValueError("Some Price values are not numeric after conversion.")

    df["Price_with_VAT"] = df["Price"] * (1.0 + args.vat_rate)

    preferred = ["ReadingDate", "Date", "Time", "Price", "Price_with_VAT"]
    cols = preferred + [c for c in df.columns if c not in preferred]
    df = df[cols]

    if args.output:
        out_file = Path(args.output)
    else:
        out_file = processed_dir / f"energy_transformed_{datetime.utcnow():%Y%m%d_%H%M%S}.parquet"
    out_file.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(out_file, index=False)

    if not out_file.exists():
        raise FileNotFoundError(f"Transform FAIL: expected output not created: {out_file}")

    logger.info("Source JSON: %s", src)
    logger.info("Saved Parquet: %s", out_file)
    logger.info("Rows=%s Cols=%s", len(df), len(df.columns))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
