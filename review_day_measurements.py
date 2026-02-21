#!/usr/bin/env python3
"""
Review and optionally delete WeatherHAT measurements for a specific day.

Examples:
  # Review all records for Feb 21, 2026 in UTC
  python review_day_measurements.py --date 2026-02-21

  # Review in your local timezone and show up to 200 rows
  python review_day_measurements.py --date 2026-02-21 --tz America/New_York --show 200

  # Dry-run delete from 00:00 to 09:30 on that date
  python review_day_measurements.py --date 2026-02-21 --delete-range 00:00 09:30

  # Actually delete after reviewing dry-run count
  python review_day_measurements.py --date 2026-02-21 --delete-range 00:00 09:30 --apply
"""
import os
import sys
import json
import argparse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from bson import ObjectId

from weatherhat_app.data_processing import connect_to_mongodb


def load_env_vars():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if not os.path.exists(env_path):
        return

    with open(env_path, encoding='utf-8') as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, value = line.split('=', 1)
            os.environ[key.strip()] = value.strip()


class MongoJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)


def parse_hhmm(value: str):
    try:
        return datetime.strptime(value, '%H:%M').time()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid time '{value}', expected HH:MM") from exc


def build_day_window(target_date: str, tz_name: str):
    tz = ZoneInfo(tz_name)
    day_local = datetime.strptime(target_date, '%Y-%m-%d').replace(tzinfo=tz)
    day_end_local = day_local + timedelta(days=1)
    return day_local, day_end_local, day_local.astimezone(timezone.utc), day_end_local.astimezone(timezone.utc)


def format_row(doc):
    fields = doc.get('fields', {})
    tags = doc.get('tags', {})
    ts = doc.get('timestamp_ms')
    if isinstance(ts, datetime):
        ts_value = ts.isoformat()
    else:
        ts_value = str(ts)

    return (
        f"{doc.get('_id')} | {ts_value} | "
        f"temp={fields.get('temperature')}C hum={fields.get('humidity')}% "
        f"wind={fields.get('wind_speed')}m/s rain={fields.get('rain')} "
        f"loc={tags.get('location', 'unknown')}"
    )


def main():
    parser = argparse.ArgumentParser(description='Review/delete WeatherHAT measurements for one day')
    default_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    parser.add_argument('--date', default=default_date, help=f'Date in YYYY-MM-DD (default: {default_date})')
    parser.add_argument('--tz', default='UTC', help='IANA timezone, e.g. UTC or America/New_York (default: UTC)')
    parser.add_argument('--show', type=int, default=100, help='Max rows to print for review (default: 100)')
    parser.add_argument('--export', help='Optional path to export matching docs as JSON')
    parser.add_argument(
        '--delete-range',
        nargs=2,
        metavar=('START_HH:MM', 'END_HH:MM'),
        help='Optional local-time range inside the day to delete, e.g. 00:00 09:30',
    )
    parser.add_argument('--delete-all-day', action='store_true', help='Delete the entire selected day window')
    parser.add_argument('--apply', action='store_true', help='Actually apply deletion (without this, delete is dry-run)')

    args = parser.parse_args()

    load_env_vars()
    mongo_uri = os.environ.get('MONGO_URI')
    db_name = os.environ.get('MONGO_DB', 'weather_data')

    if not mongo_uri:
        print('MONGO_URI is not set in environment or .env', file=sys.stderr)
        return 1

    try:
        day_local_start, day_local_end, day_utc_start, day_utc_end = build_day_window(args.date, args.tz)
    except Exception as exc:
        print(f'Invalid --date or --tz: {exc}', file=sys.stderr)
        return 1

    print(
        f"Review window local [{day_local_start.isoformat()} -> {day_local_end.isoformat()}) "
        f"UTC [{day_utc_start.isoformat()} -> {day_utc_end.isoformat()})"
    )

    client = connect_to_mongodb(mongo_uri)
    db = client[db_name]
    collection = db['measurements']

    base_filter = {
        'timestamp_ms': {
            '$gte': day_utc_start,
            '$lt': day_utc_end,
        }
    }

    total = collection.count_documents(base_filter)
    print(f'Matching docs for {args.date}: {total}')

    if args.delete_all_day and args.delete_range:
        print('Use either --delete-all-day or --delete-range, not both', file=sys.stderr)
        client.close()
        return 1

    if total == 0:
        client.close()
        return 0

    first_doc = collection.find(base_filter).sort('timestamp_ms', 1).limit(1).next()
    last_doc = collection.find(base_filter).sort('timestamp_ms', -1).limit(1).next()

    print(f"First doc: {first_doc.get('_id')} @ {first_doc.get('timestamp_ms')}")
    print(f"Last doc:  {last_doc.get('_id')} @ {last_doc.get('timestamp_ms')}")

    print(f'\nShowing up to {args.show} docs:')
    cursor = collection.find(base_filter).sort('timestamp_ms', 1).limit(args.show)
    for doc in cursor:
        print(format_row(doc))

    if args.export:
        docs = list(collection.find(base_filter).sort('timestamp_ms', 1))
        with open(args.export, 'w', encoding='utf-8') as output_file:
            json.dump(docs, output_file, cls=MongoJsonEncoder, indent=2)
        print(f'Exported {len(docs)} docs to {args.export}')

    if args.delete_range:
        start_time = parse_hhmm(args.delete_range[0])
        end_time = parse_hhmm(args.delete_range[1])

        delete_start_local = day_local_start.replace(hour=start_time.hour, minute=start_time.minute)
        delete_end_local = day_local_start.replace(hour=end_time.hour, minute=end_time.minute)

        if delete_end_local <= delete_start_local:
            print('Delete end time must be after start time within the same day', file=sys.stderr)
            client.close()
            return 1

        delete_filter = {
            'timestamp_ms': {
                '$gte': delete_start_local.astimezone(timezone.utc),
                '$lt': delete_end_local.astimezone(timezone.utc),
            }
        }

        delete_count = collection.count_documents(delete_filter)
        print(
            f"\nDelete candidate window local [{delete_start_local.isoformat()} -> {delete_end_local.isoformat()}) "
            f"matches {delete_count} docs"
        )

        if not args.apply:
            print('Dry-run only. Re-run with --apply to perform deletion.')
        else:
            result = collection.delete_many(delete_filter)
            print(f'Deleted {result.deleted_count} docs.')

    if args.delete_all_day:
        delete_count = collection.count_documents(base_filter)
        print(
            f"\nDelete candidate full-day window local [{day_local_start.isoformat()} -> {day_local_end.isoformat()}) "
            f"matches {delete_count} docs"
        )
        if not args.apply:
            print('Dry-run only. Re-run with --apply to perform full-day deletion.')
        else:
            result = collection.delete_many(base_filter)
            print(f'Deleted {result.deleted_count} docs from full-day window.')

    client.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
