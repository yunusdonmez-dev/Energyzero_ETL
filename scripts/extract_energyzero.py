from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
import logging
import requests

API_URL = "https://api.energyzero.nl/v1/energyprices"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def project_root() -> Path:
    # Works both locally (energyzero_etl/scripts/..) and inside Airflow (/opt/airflow/scripts/..)
    return Path(__file__).resolve().parents[1]


def build_params(
    start_date, end_date, interval: int, usage_type: int, incl_btw: bool
) -> dict[str, str]:
    from_dt = f"{start_date:%Y-%m-%d}T00:00:00.000Z"
    till_dt = f"{end_date:%Y-%m-%d}T23:59:59.999Z"
    return {
        "fromDate": from_dt,
        "tillDate": till_dt,
        "interval": str(interval),
        "usageType": str(usage_type),
        "inclBtw": "true" if incl_btw else "false",
    }


def fetch_energy_data(params: dict[str, str], timeout_s: int) -> Any:
    headers = {"User-Agent": "energyzero-etl/1.0"}
    with requests.Session() as session:
        resp = session.get(API_URL, params=params, headers=headers, timeout=timeout_s)
        resp.raise_for_status()
        return resp.json()


def save_json(data: Any, raw_dir: Path, ts_utc: datetime) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_file = raw_dir / f"energy_{ts_utc:%Y%m%d_%H%M%S}.json"
    out_file.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return out_file


def main() -> int:
    try:
        parser = argparse.ArgumentParser(description="Extract EnergyZero prices to raw JSON.")
        parser.add_argument("--days", type=int, default=7, help="How many days back to fetch.")
        parser.add_argument("--interval", type=int, default=4, help="Interval parameter for API.")
        parser.add_argument("--usage-type", type=int, default=1, help="Usage type for API (e.g., 1).")
        parser.add_argument(
            "--incl-btw",
            action="store_true",
            help="If set, request prices including VAT (inclBtw=true). Default is false.",
        )
        parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds.")
        parser.add_argument(
            "--output",
            type=str,
            default="",
            help="Optional output JSON path. If not provided, a timestamped file is created in data/raw/.",
        )
        args = parser.parse_args()

        now_utc = datetime.now(timezone.utc)
        end_date = now_utc.date()
        start_date = (now_utc - timedelta(days=args.days)).date()

        params = build_params(
            start_date=start_date,
            end_date=end_date,
            interval=args.interval,
            usage_type=args.usage_type,
            incl_btw=args.incl_btw,
        )

        data = fetch_energy_data(params=params, timeout_s=args.timeout)

        raw_dir = project_root() / "data" / "raw"
        if args.output:
            out_file = Path(args.output)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            out_file = save_json(data=data, raw_dir=raw_dir, ts_utc=now_utc)

        logger.info("Saved: %s", out_file)
        

        # Optional: try to print record count if the payload has a common list field
        record_count = None
        if isinstance(data, list):
            record_count = len(data)
        elif isinstance(data, dict):
            for key in ("Prices", "prices", "data", "results"):
                if key in data and isinstance(data[key], list):
                    record_count = len(data[key])
                    break

        logger.info("Saved: %s", out_file)
        if record_count is not None:
            logger.info("Records: %s", record_count)

        return 0

    except Exception as e:
        logger.exception("Extract failed: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
