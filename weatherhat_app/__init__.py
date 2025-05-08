#!/usr/bin/env python3
"""
Weather HAT application package

This package provides functionality for collecting data from the
Pimoroni Weather HAT, processing the data, and generating reports.
"""

__version__ = '1.0.0'

# Import key functions to make them available at the package level
from .sensor_utils import initialize_sensor, take_readings, calculate_average_readings, cleanup_sensor
from .data_processing import connect_to_mongodb, prepare_measurement, store_measurement, update_records, calculate_trends
from .reporting import generate_daily_report, generate_weather_prediction