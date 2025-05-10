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

from weatherhat_app.sensor_utils import initialize_sensor, take_readings, calculate_average_readings, accumulate_rainfall, cleanup_sensor
from weatherhat_app.data_processing import (connect_to_mongodb, prepare_measurement, store_measurement, 
                                           update_records, calculate_trends, setup_retention_policies, setup_indexes,
                                           DateTimeEncoder)
from weatherhat_app.reporting import generate_daily_report
from weatherhat_app.scheduler import MaintenanceScheduler

# MongoDB connection settings
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://akuma:27017')
DB_NAME = os.environ.get('MONGO_DB', 'weather_data')

# Add a delay at startup to allow MongoDB to initialize if starting together
STARTUP_DELAY = int(os.environ.get('STARTUP_DELAY', '0'))

# Global variables for rain accumulation
ACCUMULATED_RAIN = 0
LAST_RAIN_RESET = None

def run():
    """Main function to run the WeatherHAT application"""
    global ACCUMULATED_RAIN, LAST_RAIN_RESET
    
    sensor = None
    mongo_client = None
    scheduler = None
    
    try:
        # Apply startup delay if configured
        if STARTUP_DELAY > 0:
            print(f"Waiting {STARTUP_DELAY} seconds for MongoDB to start...", file=sys.stderr)
            time.sleep(STARTUP_DELAY)
        
        # Connect to MongoDB
        mongo_client = connect_to_mongodb(MONGO_URI)
        db = mongo_client[DB_NAME]
        
        # Set up data retention policies and performance indexes
        setup_retention_policies(db)
        setup_indexes(db)
        
        # Start the maintenance scheduler
        scheduler = MaintenanceScheduler(db)
        scheduler.start()
        
        # Try to load the last accumulated rain value and reset time from the database
        try:
            rain_state = db['rain_state'].find_one({'_id': 'rain_accumulation'})
            if rain_state:
                ACCUMULATED_RAIN = rain_state.get('accumulated_rain', 0)
                LAST_RAIN_RESET = rain_state.get('last_reset_time')
                print(f"Loaded rain state: accumulated={ACCUMULATED_RAIN}, last reset={LAST_RAIN_RESET}", file=sys.stderr)
        except Exception as e:
            print(f"Error loading rain state (using defaults): {e}", file=sys.stderr)
        
        # Generate daily report if needed
        generate_daily_report(db)
        
        # Initialize sensor
        sensor = initialize_sensor()
        
        # Take readings (discards first reading, takes 3 valid readings)
        readings = take_readings(sensor, num_readings=3, discard_first=True)
        
        # Calculate average values for most measurements
        avg_fields = calculate_average_readings(readings)
        
        # Handle rain accumulation separately (don't average it)
        ACCUMULATED_RAIN, LAST_RAIN_RESET = accumulate_rainfall(readings, ACCUMULATED_RAIN, LAST_RAIN_RESET)
        
        # Replace the averaged rain value with the accumulated value
        avg_fields['rain'] = ACCUMULATED_RAIN
        
        # Store the updated rain state
        db['rain_state'].update_one(
            {'_id': 'rain_accumulation'}, 
            {'$set': {
                'accumulated_rain': ACCUMULATED_RAIN,
                'last_reset_time': LAST_RAIN_RESET
            }}, 
            upsert=True
        )
        
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
        print(json.dumps(measurement, cls=DateTimeEncoder))
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1

    finally:
        # Clean up resources
        if scheduler:
            scheduler.stop()
            
        if mongo_client:
            mongo_client.close()
        
        if sensor:
            cleanup_sensor(sensor)

if __name__ == "__main__":
    sys.exit(run())