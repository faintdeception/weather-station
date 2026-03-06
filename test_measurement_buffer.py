#!/usr/bin/env python3
import os
import pickle
import sys
import tempfile
import unittest
from unittest.mock import MagicMock

from pymongo.errors import BulkWriteError

sys.modules['weatherhat'] = MagicMock()

from weatherhat_app.data_processing import MeasurementBuffer


class TestMeasurementBuffer(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
