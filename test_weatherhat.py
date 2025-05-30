#!/usr/bin/env python3
import unittest
from unittest.mock import patch, MagicMock
import sys
import math

# Mock the hardware modules before import
sys.modules['weatherhat'] = MagicMock()
sys.modules['pymongo'] = MagicMock()
sys.modules['pymongo.operations'] = MagicMock()
sys.modules['bson'] = MagicMock()
sys.modules['bson.objectid'] = MagicMock()

# Import after mocking
sys.path.append('.')
from weatherhat_app.sensor_utils import calculate_average_readings

class TestWeatherHAT(unittest.TestCase):
    """Test cases for core WeatherHAT functionality"""
    
    def test_calculate_average_readings(self):
        """Test the calculation of average readings"""
        # Create test readings with normal values
        readings = [
            {'temperature': 20.0, 'humidity': 50.0, 'wind_direction': 90.0},
            {'temperature': 22.0, 'humidity': 52.0, 'wind_direction': 100.0},
            {'temperature': 21.0, 'humidity': 51.0, 'wind_direction': 110.0}
        ]
        
        # Calculate averages
        avg_fields = calculate_average_readings(readings)
        
        # Check results
        self.assertAlmostEqual(avg_fields['temperature'], 21.0)
        self.assertAlmostEqual(avg_fields['humidity'], 51.0)
        self.assertAlmostEqual(avg_fields['wind_direction'], 100.0)
    
    def test_wind_direction_averaging_across_north(self):
        """Test wind direction averaging when values cross the North direction"""
        # Create test readings with wind directions that wrap around North (0/360)
        readings = [
            {'wind_direction': 350.0},
            {'wind_direction': 10.0},
            {'wind_direction': 370.0}  # Same as 10 degrees
        ]
        
        # Calculate averages
        avg_fields = calculate_average_readings(readings)
        
        # The average should be near North (0 or close to it)
        # We allow some margin since the circular mean might not be exactly 0
        self.assertTrue(
            avg_fields['wind_direction'] < 10.0 or 
            avg_fields['wind_direction'] > 350.0,
            f"Wind direction average {avg_fields['wind_direction']} should be close to North"
        )

if __name__ == '__main__':
    unittest.main()