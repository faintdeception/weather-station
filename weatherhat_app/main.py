#!/usr/bin/env python3
"""
Main module for the WeatherHAT application

This module ties together the sensor, data processing, and reporting functionality.
"""
import os
import sys
import json
import time
import traceback

from weatherhat_app.sensor_utils import initialize_sensor, take_readings, calculate_average_readings, cleanup_sensor
from weatherhat_app.data_processing import connect_to_mongodb, prepare_measurement, store_measurement, update_records, calculate_trends
from weatherhat_app.reporting import generate_daily_report

# MongoDB connection settings
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://akuma:27017')
DB_NAME = os.environ.get('MONGO_DB', 'weather_data')

# Add a delay at startup to allow MongoDB to initialize if starting together
STARTUP_DELAY = int(os.environ.get('STARTUP_DELAY', '0'))

def run():
    """Main function to run the WeatherHAT application"""
    sensor = None
    mongo_client = None
    
    try:
        # Apply startup delay if configured
        if STARTUP_DELAY > 0:
            print(f"Waiting {STARTUP_DELAY} seconds for MongoDB to start...", file=sys.stderr)
            time.sleep(STARTUP_DELAY)
        
        # Connect to MongoDB
        mongo_client = connect_to_mongodb(MONGO_URI)
        db = mongo_client[DB_NAME]
        
        # Generate daily report if needed
        generate_daily_report(db)
        
        # Initialize sensor
        sensor = initialize_sensor()
        
        # Take readings (discards first reading, takes 3 valid readings)
        readings = take_readings(sensor, num_readings=3, discard_first=True)
        
        # Calculate average values
        avg_fields = calculate_average_readings(readings)
        
        # Add cardinal wind direction
        if "wind_direction" in avg_fields:
            avg_fields["wind_direction_cardinal"] = sensor.degrees_to_cardinal(avg_fields["wind_direction"])
        
        # Prepare measurement
        measurement = prepare_measurement(avg_fields, sensor)
        
        # Update record-breaking values
        update_records(db, measurement)
        
        # Calculate and store trend data
        calculate_trends(db, measurement)
        
        # Store current measurement in measurements collection
        store_measurement(db, measurement)
        
        # Output the measurement as JSON
        print(json.dumps(measurement))
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1

    finally:
        # Clean up resources
        if mongo_client:
            mongo_client.close()
        
        if sensor:
            cleanup_sensor(sensor)

if __name__ == "__main__":
    sys.exit(run())