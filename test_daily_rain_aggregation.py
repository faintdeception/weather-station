#!/usr/bin/env python3
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
import sys

sys.modules['weatherhat'] = MagicMock()

from weatherhat_app.data_processing import compute_daily_rain_stats, downsample_daily


class FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, field, direction):
        reverse = direction < 0
        self._docs = sorted(self._docs, key=lambda d: d.get(field), reverse=reverse)
        return self

    def __iter__(self):
        return iter(self._docs)


class TestDailyRainAggregation(unittest.TestCase):
    def test_compute_daily_rain_stats_non_zero_sum(self):
        day_start = datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)

        docs = [
            {
                "timestamp_ms": day_start + timedelta(minutes=0),
                "fields": {"rain": 0.0},
                "tags": {"location": "backyard", "sensor_type": "weatherhat"},
            },
            {
                "timestamp_ms": day_start + timedelta(minutes=5),
                "fields": {"rain": 0.01},
                "tags": {"location": "backyard", "sensor_type": "weatherhat"},
            },
            {
                "timestamp_ms": day_start + timedelta(minutes=10),
                "fields": {"rain": 0.02},
                "tags": {"location": "backyard", "sensor_type": "weatherhat"},
            },
        ]

        stats = compute_daily_rain_stats(docs, day_start, day_end, max_gap_seconds=600)
        rain = stats[("backyard", "weatherhat")]

        self.assertGreater(rain["sum"], 0.0)
        self.assertAlmostEqual(rain["max"], 0.02)
        self.assertEqual(rain["positive_samples"], 2)

    @patch("weatherhat_app.data_processing.update_daily_date_records")
    def test_downsample_daily_writes_rain_shape(self, mock_update_daily_records):
        day_start = datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc)

        # Synthetic raw rain-rate samples (mm/sec)
        raw_docs = [
            {
                "timestamp_ms": day_start + timedelta(minutes=0),
                "fields": {"rain": 0.0},
                "tags": {"location": "backyard", "sensor_type": "weatherhat"},
            },
            {
                "timestamp_ms": day_start + timedelta(minutes=5),
                "fields": {"rain": 0.01},
                "tags": {"location": "backyard", "sensor_type": "weatherhat"},
            },
            {
                "timestamp_ms": day_start + timedelta(minutes=10),
                "fields": {"rain": 0.02},
                "tags": {"location": "backyard", "sensor_type": "weatherhat"},
            },
        ]

        # One hourly aggregate row to allow daily creation
        hourly_aggregate = [
            {
                "_id": {"location": "backyard", "sensor_type": "weatherhat", "day": day_start},
                "avg_temperature": 20.0,
                "min_temperature": 15.0,
                "max_temperature": 25.0,
                "avg_humidity": 50.0,
                "avg_pressure": 1005.0,
                "avg_wind_speed": 1.5,
                "max_wind_speed": 3.0,
                "avg_lux": 400.0,
                "hour_count": 24,
            }
        ]

        db = MagicMock()
        db.daily_measurements.find_one.return_value = None
        db.measurements.find.return_value = FakeCursor(raw_docs)
        db.hourly_measurements.aggregate.return_value = hourly_aggregate

        result = downsample_daily(db, target_day=day_start, overwrite=True)

        self.assertEqual(result, 1)
        db.daily_measurements.update_one.assert_called_once()

        update_filter, update_doc = db.daily_measurements.update_one.call_args[0][0], db.daily_measurements.update_one.call_args[0][1]
        self.assertIn("day_timestamp", update_filter)

        written = update_doc["$set"]
        self.assertIn("rain", written["fields"])
        self.assertGreater(written["fields"]["rain"]["sum"], 0.0)
        self.assertGreaterEqual(written["fields"]["rain"]["max"], 0.02)
        self.assertIn("max_rate", written["fields"]["rain"])


if __name__ == "__main__":
    unittest.main()
