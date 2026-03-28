#!/usr/bin/env python3
"""Validate and optionally backfill daily rainfall aggregation.

Compares rain totals derived from raw `measurements.fields.rain` (mm/sec) with
stored `daily_measurements.fields.rain.sum` for recent UTC days.
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

sys.modules.setdefault("weatherhat", MagicMock())

from pymongo import MongoClient

from weatherhat_app.data_processing import downsample_daily, get_daily_rain_stats


def load_env_vars(root_path):
    env_path = os.path.join(root_path, ".env")
    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare raw-vs-daily rain totals and optionally backfill daily data"
    )
    parser.add_argument("--days", type=int, default=30, help="Number of full UTC days to validate")
    parser.add_argument(
        "--recompute-days",
        type=int,
        default=0,
        help="If > 0, overwrite daily_measurements for this many most recent full UTC days",
    )
    parser.add_argument(
        "--tolerance-mm",
        type=float,
        default=0.001,
        help="Allowed absolute difference in mm before a day is flagged",
    )
    return parser.parse_args()


def fmt(value):
    if value is None:
        return "None"
    return f"{value:.6f}"


def main():
    args = parse_args()
    root = os.path.dirname(os.path.abspath(__file__))
    load_env_vars(root)

    mongo_uri = os.environ.get("MONGO_URI", "mongodb://akuma:27017")
    mongo_db = os.environ.get("MONGO_DB", "weather_data")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[mongo_db]

    today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    if args.recompute_days > 0:
        print(f"Recomputing last {args.recompute_days} full UTC days in daily_measurements...")
        for offset in range(1, args.recompute_days + 1):
            target_day = today_utc - timedelta(days=offset)
            downsample_daily(db, target_day=target_day, overwrite=True)

    print(f"\nValidating last {args.days} full UTC days (excluding today)...")
    mismatches = []
    rainy_zero_daily = []

    for offset in range(1, args.days + 1):
        day_start = today_utc - timedelta(days=offset)
        day_end = day_start + timedelta(days=1)
        day_key = day_start.strftime("%Y-%m-%d")

        raw_stats = get_daily_rain_stats(db, day_start, day_end)
        daily_docs = list(db.daily_measurements.find({"day_timestamp": int(day_start.timestamp() * 1e9)}))

        daily_map = {}
        for doc in daily_docs:
            tags = doc.get("tags", {})
            group_key = (tags.get("location", "unknown"), tags.get("sensor_type", "weatherhat"))
            rain = doc.get("fields", {}).get("rain", {})
            daily_map[group_key] = {
                "sum": float(rain.get("sum", 0.0) or 0.0),
                "max": float(rain.get("max", 0.0) or 0.0),
            }

        group_keys = set(raw_stats.keys()) | set(daily_map.keys())
        if not group_keys:
            continue

        for group_key in sorted(group_keys):
            raw_sum = float(raw_stats.get(group_key, {}).get("sum", 0.0) or 0.0)
            raw_max = float(raw_stats.get(group_key, {}).get("max", 0.0) or 0.0)
            raw_positive = int(raw_stats.get(group_key, {}).get("positive_samples", 0) or 0)

            daily_sum = float(daily_map.get(group_key, {}).get("sum", 0.0) or 0.0)
            daily_max = float(daily_map.get(group_key, {}).get("max", 0.0) or 0.0)

            diff = abs(raw_sum - daily_sum)
            if diff > args.tolerance_mm:
                mismatches.append((day_key, group_key, raw_sum, daily_sum, diff))

            if raw_positive > 0 and daily_sum <= 0.0:
                rainy_zero_daily.append((day_key, group_key, raw_positive, raw_max, daily_max))

            print(
                f"{day_key} | loc={group_key[0]} sensor={group_key[1]} | "
                f"raw_sum_mm={fmt(raw_sum)} daily_sum_mm={fmt(daily_sum)} diff_mm={fmt(diff)} | "
                f"raw_max_rate={fmt(raw_max)} daily_max_rate={fmt(daily_max)}"
            )

    print("\nSummary:")
    print(f"  Mismatches over tolerance: {len(mismatches)}")
    print(f"  Rainy raw days with zero daily total: {len(rainy_zero_daily)}")

    if rainy_zero_daily:
        print("\nRainy raw data but zero daily totals:")
        for day_key, group_key, raw_positive, raw_max, daily_max in rainy_zero_daily:
            print(
                f"  {day_key} | loc={group_key[0]} sensor={group_key[1]} | "
                f"raw_positive_samples={raw_positive} raw_max_rate={fmt(raw_max)} daily_max_rate={fmt(daily_max)}"
            )

    if mismatches:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
