#!/usr/bin/env python3
"""
Test the maintenance tracker functionality
"""
import unittest
import time
from unittest.mock import MagicMock, patch
import sys

# Mock the hardware modules before importing our code
sys.modules['weatherhat'] = MagicMock()
sys.modules['pymongo'] = MagicMock()
sys.modules['pymongo.operations'] = MagicMock()
sys.modules['bson'] = MagicMock()
sys.modules['bson.objectid'] = MagicMock()

from weatherhat_app.maintenance_tracker import MaintenanceTracker


class TestMaintenanceTracker(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_db = MagicMock()
        self.mock_collection = MagicMock()
        self.mock_db.__getitem__.return_value = self.mock_collection
        self.tracker = MaintenanceTracker(self.mock_db)
    
    def test_should_run_hourly_when_no_record(self):
        """Test that hourly maintenance should run when no record exists"""
        self.mock_collection.find_one.return_value = None
        self.assertTrue(self.tracker.should_run_hourly_maintenance())
    
    def test_should_run_hourly_when_time_passed(self):
        """Test that hourly maintenance should run when enough time has passed"""
        old_time = time.time() - 7200  # 2 hours ago
        self.mock_collection.find_one.return_value = {'last_run': old_time}
        self.assertTrue(self.tracker.should_run_hourly_maintenance())
    
    def test_should_not_run_hourly_when_recent(self):
        """Test that hourly maintenance should not run when recently executed"""
        recent_time = time.time() - 1800  # 30 minutes ago
        self.mock_collection.find_one.return_value = {'last_run': recent_time}
        self.assertFalse(self.tracker.should_run_hourly_maintenance())
    
    def test_should_run_daily_when_no_record(self):
        """Test that daily maintenance should run when no record exists"""
        self.mock_collection.find_one.return_value = None
        self.assertTrue(self.tracker.should_run_daily_maintenance())
    
    def test_should_run_daily_when_time_passed(self):
        """Test that daily maintenance should run when enough time has passed"""
        old_time = time.time() - 172800  # 2 days ago
        self.mock_collection.find_one.return_value = {'last_run': old_time}
        self.assertTrue(self.tracker.should_run_daily_maintenance())
    
    def test_should_not_run_daily_when_recent(self):
        """Test that daily maintenance should not run when recently executed"""
        recent_time = time.time() - 43200  # 12 hours ago
        self.mock_collection.find_one.return_value = {'last_run': recent_time}
        self.assertFalse(self.tracker.should_run_daily_maintenance())
    
    @patch('weatherhat_app.maintenance_tracker.downsample_hourly')
    def test_run_hourly_maintenance_success(self, mock_downsample):
        """Test successful hourly maintenance execution"""
        mock_downsample.return_value = 42
        
        result = self.tracker.run_hourly_maintenance()
        
        self.assertEqual(result, 42)
        mock_downsample.assert_called_once_with(self.mock_db)
        self.mock_collection.update_one.assert_called_once()
    
    @patch('weatherhat_app.maintenance_tracker.perform_database_maintenance')
    def test_run_daily_maintenance_success(self, mock_maintenance):
        """Test successful daily maintenance execution"""
        mock_maintenance.return_value = 'success'
        
        result = self.tracker.run_daily_maintenance()
        
        self.assertEqual(result, 'success')
        mock_maintenance.assert_called_once_with(self.mock_db)
        self.mock_collection.update_one.assert_called_once()


if __name__ == '__main__':
    unittest.main()