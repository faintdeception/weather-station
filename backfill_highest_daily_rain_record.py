#!/usr/bin/env python3
"""
One-off backfill script: populate records.rain_daily_total highest values from historical daily_measurements.

Safety:
- Defaults to dry-run mode (no writes).
- Requires --apply to perform writes.
- Idempotent: only updates when candidate value is greater than existing record value.

Usage examples:
  python backfill_highest_daily_rain_record.py --dry-run
  python backfill_highest_daily_rain_record.py --apply
  python backfill_highest_daily_rain_record.py --apply --location backyard
"""

import argparse
import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient


DEFAULT_MONGO_URI = os.environ.get("MONGO_URI", "mongodb://akuma:27017")
DEFAULT_DB_NAME = os.environ.get("MONGO_DB", "weather_data")


def load_env_vars():
    """Load .env file from repository root if present."""
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        if not os.path.exists(env_path):
            return False

        with open(env_path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()
        return True
    except Exception as exc:
        print(f"Warning: failed to load .env file: {exc}", file=sys.stderr)
        return False


def build_top_daily_rain_pipeline(location=None, sensor_type=None):
    """Return aggregation pipeline selecting highest historical daily rain per location/sensor."""
    match_stage = {
        "fields.rain.sum": {"$exists": True, "$ne": None},
        "tags.location": {"$exists": True},
    }
    if location:
        match_stage["tags.location"] = location
    if sensor_type:
        match_stage["tags.sensor_type"] = sensor_type

    return [
        {"$match": match_stage},
        {
            "$project": {
                "location": "$tags.location",
                "sensor_type": {"$ifNull": ["$tags.sensor_type", "weatherhat"]},
                "rain_sum": "$fields.rain.sum",
                "day_timestamp": "$day_timestamp",
                "date": "$date",
                "timestamp_ms": "$timestamp_ms",
            }
        },
        {
            "$sort": {
                "location": 1,
                "sensor_type": 1,
                "rain_sum": -1,
                "day_timestamp": 1,
            }
        },
        {
            "$group": {
                "_id": {
                    "location": "$location",
                    "sensor_type": "$sensor_type",
                },
                "value": {"$first": "$rain_sum"},
                "day_timestamp": {"$first": "$day_timestamp"},
                "date": {"$first": "$date"},
                "timestamp_ms": {"$first": "$timestamp_ms"},
            }
        },
        {"$sort": {"_id.location": 1, "_id.sensor_type": 1}},
    ]


def upsert_highest_daily_rain(records_collection, candidate, apply_changes):
    """Upsert rain_daily_total highest record for a single location/sensor candidate."""
    location = candidate["_id"]["location"]
    sensor_type = candidate["_id"]["sensor_type"]
    value = float(candidate.get("value", 0.0) or 0.0)
    day_timestamp = candidate.get("day_timestamp")
    date = candidate.get("date")

    query = {
        "field": "rain_daily_total",
        "record_type": "highest",
        "location": location,
        "sensor_type": sensor_type,
    }

    existing = records_collection.find_one(query, {"value": 1, "date": 1})
    existing_value = None if existing is None else float(existing.get("value", float("-inf")))

    if existing_value is not None and value <= existing_value:
        print(
            (
                "SKIP "
                f"location={location} sensor_type={sensor_type} "
                f"candidate={value:.4f} existing={existing_value:.4f}"
            )
        )
        return {"updated": False, "skipped": True}

    if not apply_changes:
        action = "INSERT" if existing is None else "UPDATE"
        print(
            (
                f"DRY-RUN {action} location={location} sensor_type={sensor_type} "
                f"value={value:.4f} date={date}"
            )
        )
        return {"updated": False, "skipped": False}

    now_utc = datetime.now(timezone.utc)
    update_doc = {
        "$set": {
            "value": value,
            "timestamp": day_timestamp,
            "date": date,
            "updated_at": now_utc,
            "source": "one_off_backfill_highest_daily_rain",
        },
        "$setOnInsert": {
            "field": "rain_daily_total",
            "record_type": "highest",
            "location": location,
            "sensor_type": sensor_type,
            "created_at": now_utc,
        },
    }
    records_collection.update_one(query, update_doc, upsert=True)

    action = "Inserted" if existing is None else "Updated"
    print(f"{action} location={location} sensor_type={sensor_type} value={value:.4f} date={date}")
    return {"updated": True, "skipped": False}


def main():
    load_env_vars()

    parser = argparse.ArgumentParser(
        description=(
            "One-off backfill for highest daily rain records from daily_measurements. "
            "Defaults to dry-run mode."
        )
    )
    parser.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", DEFAULT_MONGO_URI))
    parser.add_argument("--db-name", default=os.environ.get("MONGO_DB", DEFAULT_DB_NAME))
    parser.add_argument("--location", help="Optional location filter (e.g., backyard)")
    parser.add_argument("--sensor-type", help="Optional sensor_type filter (e.g., weatherhat)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing (default behavior unless --apply is set).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates to the records collection.",
    )

    args = parser.parse_args()
    apply_changes = bool(args.apply)

    mode_label = "APPLY" if apply_changes else "DRY-RUN"
    print(f"Mode: {mode_label}")
    print(f"Mongo URI: {args.mongo_uri}")
    print(f"Database: {args.db_name}")

    client = None
    try:
        client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=10000)
        client.admin.command("ping")
        db = client[args.db_name]

        pipeline = build_top_daily_rain_pipeline(location=args.location, sensor_type=args.sensor_type)
        candidates = list(db.daily_measurements.aggregate(pipeline))

        if not candidates:
            print("No daily rain records found in daily_measurements for the selected filters.")
            return 0

        touched = 0
        skipped = 0

        for candidate in candidates:
            result = upsert_highest_daily_rain(db.records, candidate, apply_changes=apply_changes)
            if result["updated"]:
                touched += 1
            if result["skipped"]:
                skipped += 1

        print(
            (
                f"Summary: candidates={len(candidates)} touched={touched} skipped={skipped} "
                f"mode={mode_label}"
            )
        )
        return 0
    except Exception as exc:
        print(f"Backfill failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
