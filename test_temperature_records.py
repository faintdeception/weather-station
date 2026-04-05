#!/usr/bin/env python3
import sys
import unittest
from unittest.mock import MagicMock

sys.modules['weatherhat'] = MagicMock()

from weatherhat_app.data_processing import backfill_temperature_record_context, update_records


class TestTemperatureRecords(unittest.TestCase):
    def test_update_records_uses_calibrated_temperature_not_device_temperature(self):
        db = MagicMock()
        records_collection = MagicMock()
        records_collection.find_one.return_value = None
        db.__getitem__.return_value = records_collection

        measurement = {
            'timestamp': 1775069355369154048,
            'fields': {
                'temperature': 25.0,
                'device_temperature': 41.0,
                'humidity': 37.5,
                'wind_speed': 1.25,
                'lux': 850.0,
            },
            'tags': {
                'location': 'backyard',
            },
        }

        update_records(db, measurement)

        temperature_updates = [
            call_args for call_args in records_collection.update_one.call_args_list
            if call_args.args[0]['field'] == 'temperature'
        ]

        self.assertEqual(len(temperature_updates), 2)
        self.assertEqual(temperature_updates[0].args[1]['$set']['value'], 25.0)
        self.assertEqual(temperature_updates[1].args[1]['$set']['value'], 25.0)
        self.assertEqual(
            temperature_updates[0].args[1]['$set']['context']['conditions'],
            {'humidity': 37.5, 'wind_speed': 1.25, 'lux': 850.0}
        )

    def test_non_temperature_records_do_not_get_context(self):
        db = MagicMock()
        records_collection = MagicMock()
        records_collection.find_one.return_value = None
        db.__getitem__.return_value = records_collection

        measurement = {
            'timestamp': 1775069355369154048,
            'fields': {
                'humidity': 55.0,
            },
            'tags': {
                'location': 'backyard',
            },
        }

        update_records(db, measurement)

        update_doc = records_collection.update_one.call_args.args[1]['$set']
        self.assertNotIn('context', update_doc)

    def test_backfill_temperature_record_context_adds_day_and_condition_context(self):
        records_collection = MagicMock()
        daily_collection = MagicMock()
        measurements_collection = MagicMock()

        record_doc = {
            '_id': 'rec-1',
            'field': 'temperature',
            'record_type': 'highest',
            'location': 'backyard',
            'timestamp': 1775069355369154048,
        }
        records_collection.find.return_value = [record_doc]

        daily_collection.find_one.return_value = {
            'date': '2026-04-01',
            'fields': {
                'temperature': {
                    'avg': 27.3199,
                    'min': 18.7794,
                    'max': 38.2686,
                }
            }
        }
        measurements_collection.find_one.return_value = {
            'fields': {
                'humidity': 18.96,
                'wind_speed': 0.143,
                'lux': 2079.1,
            }
        }

        collections = {
            'records': records_collection,
            'daily_measurements': daily_collection,
            'measurements': measurements_collection,
        }

        db = MagicMock()
        db.__getitem__.side_effect = collections.__getitem__

        backfill_temperature_record_context(db)

        records_collection.update_one.assert_called_once_with(
            {'_id': 'rec-1'},
            {
                '$set': {
                    'context': {
                        'day': {
                            'date': '2026-04-01',
                            'avg': 27.3199,
                            'min': 18.7794,
                            'max': 38.2686,
                        },
                        'conditions': {
                            'humidity': 18.96,
                            'wind_speed': 0.143,
                            'lux': 2079.1,
                        },
                    }
                }
            },
        )


if __name__ == '__main__':
    unittest.main()
