#!/usr/bin/env python3
"""
Test the rain measurement fix
"""
import unittest
from unittest.mock import MagicMock, patch
import sys

# Mock the hardware modules before importing our code
sys.modules['weatherhat'] = MagicMock()
sys.modules['pymongo'] = MagicMock()
sys.modules['pymongo.operations'] = MagicMock()
sys.modules['bson'] = MagicMock()
sys.modules['bson.objectid'] = MagicMock()

from weatherhat_app.sensor_utils import accumulate_rainfall


class TestRainMeasurement(unittest.TestCase):
    
    def test_rain_accumulate_returns_current_count(self):
        """Test that accumulate_rainfall returns the current rain count, not sum"""
        # Simulate readings where rain gauge shows consistent count (no new rain)
        readings = [
            {"rain": 154},  # Same count in all readings
            {"rain": 154},
            {"rain": 154}
        ]
        
        current_count, reset_time = accumulate_rainfall(readings, 0, None)
        
        # Should return the current count (154), not sum (462)
        self.assertEqual(current_count, 154)
    
    def test_rain_accumulate_uses_latest_reading(self):
        """Test that accumulate_rainfall uses the latest reading for current count"""
        # Simulate readings where rain gauge count increases during the session
        readings = [
            {"rain": 154},  
            {"rain": 155},  # Rain detected mid-session
            {"rain": 156}   # More rain detected
        ]
        
        current_count, reset_time = accumulate_rainfall(readings, 0, None)
        
        # Should return the latest/highest count (156)
        self.assertEqual(current_count, 156)
    
    def test_rain_24_hour_reset(self):
        """Test that 24-hour reset still works"""
        import time
        
        readings = [{"rain": 100}]
        old_reset_time = time.time() - (25 * 60 * 60)  # 25 hours ago
        
        current_count, new_reset_time = accumulate_rainfall(readings, 42.5, old_reset_time)
        
        # Should reset after 24 hours
        self.assertEqual(current_count, 100)  # Returns current count
        self.assertGreater(new_reset_time, old_reset_time)  # Reset time updated


if __name__ == '__main__':
    unittest.main()
