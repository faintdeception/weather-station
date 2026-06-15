#!/usr/bin/env python3
import os
import pickle
import sys
import tempfile
import unittest
from unittest.mock import MagicMock

from pymongo.errors import BulkWriteError

sys.modules['weatherhat'] = MagicMock()

import weatherhat_app.data_processing as data_processing
from weatherhat_app.data_processing import MeasurementBuffer, calculate_trends, update_records


class TestMeasurementBuffer(unittest.TestCase):
    def setUp(self):
        data_processing._measurement_buffer = None
        data_processing._deferred_database_task_queue = None

    def tearDown(self):
        data_processing._measurement_buffer = None
        data_processing._deferred_database_task_queue = None

    def test_flush_drops_duplicate_key_entries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = os.path.join(tmpdir, 'measurement_buffer_test.pickle')
            mock_db = MagicMock()
            collection = MagicMock()
            mock_db.__getitem__.return_value = collection

            collection.bulk_write.side_effect = BulkWriteError({
                'writeErrors': [
                    {'index': 0, 'code': 11000, 'errmsg': "Duplicate key violation on '_id_'"}
                ],
                'writeConcernErrors': [],
                'nInserted': 0,
                'nUpserted': 0,
                'nMatched': 0,
                'nModified': 0,
                'nRemoved': 0,
                'upserted': []
            })

            buffer = MeasurementBuffer(db=mock_db, cache_file=cache_file)
            buffer.buffer = [
                {'_id': 'duplicate-doc', 'timestamp': 1, 'timestamp_ms': None, 'fields': {}, 'tags': {}},
                {'timestamp': 2, 'timestamp_ms': None, 'fields': {}, 'tags': {}}
            ]

            result = buffer.flush_to_db()

            self.assertTrue(result)
            self.assertEqual(len(buffer.buffer), 1)
            self.assertEqual(buffer.buffer[0].get('timestamp'), 2)

    def test_save_cache_strips_mongo_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = os.path.join(tmpdir, 'measurement_buffer_test.pickle')
            mock_db = MagicMock()

            buffer = MeasurementBuffer(db=mock_db, cache_file=cache_file)
            buffer.buffer = [
                {'_id': 'abc123', 'timestamp': 42, 'timestamp_ms': None, 'fields': {}, 'tags': {}}
            ]

            buffer._save_to_cache()

            with open(cache_file, 'rb') as f:
                cached = pickle.load(f)

            self.assertEqual(len(cached), 1)
            self.assertNotIn('_id', cached[0])
            self.assertEqual(cached[0]['timestamp'], 42)

    def test_flush_replays_deferred_record_updates_after_measurement_commit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            measurement_cache = os.path.join(tmpdir, 'measurement_buffer_test.pickle')
            deferred_cache = os.path.join(tmpdir, 'deferred_db_tasks_test.pickle')

            deferred_queue = data_processing.get_deferred_database_task_queue(cache_file=deferred_cache)

            failing_records = MagicMock()
            failing_records.find_one.side_effect = RuntimeError('db offline')
            failing_db = MagicMock()
            failing_db.__getitem__.side_effect = {'records': failing_records}.__getitem__

            measurement = {
                'timestamp': 42,
                'timestamp_ms': None,
                'fields': {'temperature': 21.5},
                'tags': {'location': 'backyard', 'sensor_type': 'weatherhat'}
            }

            result = update_records(failing_db, measurement)

            self.assertFalse(result)
            self.assertEqual(len(deferred_queue.tasks), 1)

            working_measurements = MagicMock()
            working_records = MagicMock()
            working_records.find_one.return_value = None
            working_db = MagicMock()
            working_db.__getitem__.side_effect = {
                'measurements': working_measurements,
                'records': working_records,
            }.__getitem__

            buffer = MeasurementBuffer(db=working_db, cache_file=measurement_cache)
            buffer.buffer = [measurement]

            flush_result = buffer.flush_to_db()

            self.assertTrue(flush_result)
            working_measurements.bulk_write.assert_called_once()
            self.assertGreaterEqual(working_records.update_one.call_count, 2)
            self.assertEqual(deferred_queue.tasks, [])
            self.assertFalse(os.path.exists(deferred_cache))

    def test_flush_replays_deferred_trend_updates_after_measurement_commit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            measurement_cache = os.path.join(tmpdir, 'measurement_buffer_test.pickle')
            deferred_cache = os.path.join(tmpdir, 'deferred_db_tasks_test.pickle')

            deferred_queue = data_processing.get_deferred_database_task_queue(cache_file=deferred_cache)

            failing_measurements = MagicMock()
            failing_measurements.find.side_effect = RuntimeError('db offline')
            failing_trends = MagicMock()
            failing_db = MagicMock()
            failing_db.__getitem__.side_effect = {
                'measurements': failing_measurements,
                'trends': failing_trends,
            }.__getitem__

            measurement = {
                'timestamp': 1775069355369154048,
                'timestamp_ms': None,
                'fields': {'temperature': 21.5},
                'tags': {'location': 'backyard', 'sensor_type': 'weatherhat'}
            }

            result = calculate_trends(failing_db, measurement)

            self.assertIsNone(result)
            self.assertEqual(len(deferred_queue.tasks), 1)

            historical_cursor = MagicMock()
            historical_cursor.sort.return_value = []
            working_measurements = MagicMock()
            working_measurements.find.return_value = historical_cursor
            working_trends = MagicMock()
            working_db = MagicMock()
            working_db.__getitem__.side_effect = {
                'measurements': working_measurements,
                'trends': working_trends,
            }.__getitem__

            buffer = MeasurementBuffer(db=working_db, cache_file=measurement_cache)
            buffer.buffer = [measurement]

            flush_result = buffer.flush_to_db()

            self.assertTrue(flush_result)
            working_measurements.bulk_write.assert_called_once()
            working_trends.insert_one.assert_called_once()
            self.assertEqual(deferred_queue.tasks, [])
            self.assertFalse(os.path.exists(deferred_cache))


if __name__ == '__main__':
    unittest.main()
