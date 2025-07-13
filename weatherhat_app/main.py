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
from datetime import datetime

# Load environment variables from .env file manually
def load_env_vars():
    try:
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
        if os.path.exists(env_path):
            print(f"Loading environment from {env_path}", file=sys.stderr)
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
            return True
        else:
            print(f"No .env file found at {env_path}", file=sys.stderr)
            return False
    except Exception as e:
        print(f"Error loading .env file: {e}", file=sys.stderr)
        return False

# Try to load from .env file
load_env_vars()

from weatherhat_app.sensor_utils import initialize_sensor, take_readings, calculate_average_readings, accumulate_rainfall, cleanup_sensor
from weatherhat_app.data_processing import (connect_to_mongodb, prepare_measurement, store_measurement, 
                                           update_records, calculate_trends, setup_retention_policies, setup_indexes,
                                           DateTimeEncoder, get_sampling_config, get_measurement_buffer)
from weatherhat_app.reporting import generate_daily_report
from weatherhat_app.maintenance_tracker import MaintenanceTracker

# MongoDB connection settings
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://akuma:27017')
DB_NAME = os.environ.get('MONGO_DB', 'weather_data')

# Add a delay at startup to allow MongoDB to initialize if starting together
STARTUP_DELAY = int(os.environ.get('STARTUP_DELAY', '0'))

# Global variables for rain accumulation
ACCUMULATED_RAIN = 0
LAST_RAIN_RESET = None

def process_rain_measurement(db, current_rain_count, accumulated_rain, last_reset_time):
    """
    Process rain measurements using difference calculation.
    
    The WeatherHAT rain gauge provides cumulative tip counts, so we track
    the difference between readings to calculate actual rainfall.
    
    Args:
        db: MongoDB database connection
        current_rain_count: Current rain gauge tip count
        accumulated_rain: Current accumulated rainfall in mm
        last_reset_time: Time of last daily reset
        
    Returns:
        tuple: (new_accumulated_rain, new_last_reset_time)
    """
    current_time = time.time()
    RAIN_CALIBRATION_FACTOR = 0.2794  # mm per rain gauge tip
    
    # Initialize last_reset_time if it's None
    if last_reset_time is None:
        last_reset_time = current_time
    
    # Check if 24 hours have passed since the last reset
    time_diff = current_time - last_reset_time
    hours_24 = 24 * 60 * 60  # 24 hours in seconds
    
    if time_diff >= hours_24:
        # Reset the accumulation after 24 hours
        accumulated_rain = 0
        last_reset_time = current_time
        print(f"Resetting rain accumulation after 24-hour period", file=sys.stderr)
    
    # Load the previous rain count from database to calculate the difference
    try:
        rain_state = db['rain_state'].find_one({'_id': 'rain_accumulation'})
        if rain_state:
            previous_rain_count = rain_state.get('last_rain_count', 0)
            
            # Calculate the difference in tip counts
            rain_count_diff = current_rain_count - previous_rain_count
            
            if rain_count_diff > 0:
                # New rain detected - convert tips to mm
                new_rain_mm = rain_count_diff * RAIN_CALIBRATION_FACTOR
                accumulated_rain += new_rain_mm
                print(f"New rain detected: {rain_count_diff} tips = {new_rain_mm:.2f}mm, total: {accumulated_rain:.2f}mm", file=sys.stderr)
            elif rain_count_diff < 0:
                # Rain gauge was reset (count went backwards) - start fresh
                accumulated_rain = 0
                print("Rain gauge appears to have been reset", file=sys.stderr)
            else:
                # No new rain
                print(f"No new rain, total remains: {accumulated_rain:.2f}mm", file=sys.stderr)
        else:
            # First time setup
            accumulated_rain = 0
            print(f"Initializing rain tracking with count: {current_rain_count}", file=sys.stderr)
            
        # Store the updated rain state
        db['rain_state'].update_one(
            {'_id': 'rain_accumulation'}, 
            {'$set': {
                'accumulated_rain': accumulated_rain,
                'last_reset_time': last_reset_time,
                'last_rain_count': current_rain_count
            }}, 
            upsert=True
        )
        
    except Exception as e:
        print(f"Error calculating rain difference: {e}", file=sys.stderr)
        # Don't change accumulated_rain on error
    
    return accumulated_rain, last_reset_time

def run():
    """Main function to run the WeatherHAT application"""
    global ACCUMULATED_RAIN, LAST_RAIN_RESET
    
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
          # Set up data retention policies and performance indexes
        setup_retention_policies(db)
        setup_indexes(db)
        
        # Create maintenance tracker and check for needed maintenance
        maintenance_tracker = MaintenanceTracker(db)
        maintenance_tasks = maintenance_tracker.check_and_run_maintenance()
        if maintenance_tasks:
            print(f"Completed maintenance tasks: {maintenance_tasks}", file=sys.stderr)
        
        # Try to load the last accumulated rain value and reset time from the database
        try:
            rain_state = db['rain_state'].find_one({'_id': 'rain_accumulation'})
            if rain_state:
                ACCUMULATED_RAIN = rain_state.get('accumulated_rain', 0)
                LAST_RAIN_RESET = rain_state.get('last_reset_time')
                last_rain_count = rain_state.get('last_rain_count', 0)
                print(f"Loaded rain state: accumulated={ACCUMULATED_RAIN}mm, last reset={LAST_RAIN_RESET}, last count={last_rain_count}", file=sys.stderr)
        except Exception as e:
            print(f"Error loading rain state (using defaults): {e}", file=sys.stderr)
        
        # Generate daily report if needed
        generate_daily_report(db)
        
        # Get adaptive sampling configuration based on weather variability
        sampling_config = get_sampling_config(db)
        print(f"Using sampling config: {sampling_config}", file=sys.stderr)
        
        # Initialize measurement buffer with parameters from sampling config
        buffer = get_measurement_buffer(
            db=db, 
            max_size=sampling_config.get('buffer_size', 10),
            max_age_seconds=sampling_config.get('buffer_max_age_seconds', 300)
        )
        
        # Initialize sensor
        sensor = initialize_sensor()
        
        # Take readings with adaptive number of samples
        readings = take_readings(
            sensor, 
            num_readings=sampling_config.get('num_readings', 3), 
            discard_first=sampling_config.get('discard_first', True)
        )
        
        # Calculate average values for most measurements
        avg_fields = calculate_average_readings(readings)
        
        # Handle rain accumulation separately using difference calculation
        current_rain_count, _ = accumulate_rainfall(readings, ACCUMULATED_RAIN, LAST_RAIN_RESET)
        ACCUMULATED_RAIN, LAST_RAIN_RESET = process_rain_measurement(db, current_rain_count, ACCUMULATED_RAIN, LAST_RAIN_RESET)
        
        # Replace the averaged rain value with the accumulated value
        avg_fields['rain'] = ACCUMULATED_RAIN
        
        # Add cardinal wind direction
        if "wind_direction" in avg_fields:
            avg_fields["wind_direction_cardinal"] = sensor.degrees_to_cardinal(avg_fields["wind_direction"])
        
        # Prepare measurement
        measurement = prepare_measurement(avg_fields, sensor)
        
        # Update record-breaking values
        update_records(db, measurement)
        
        # Calculate and store trend data - only do this every hour to reduce DB load
        current_minute = datetime.now().minute
        if current_minute < 5:  # Only calculate trends at the start of each hour
            calculate_trends(db, measurement)
        
        # Store current measurement in measurements collection
        store_measurement(db, measurement)
        
        # Make sure any remaining buffered measurements are flushed to the database
        buffer.flush_to_db()
          # Output the measurement as JSON
        print(json.dumps(measurement, cls=DateTimeEncoder))
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1

    finally:
        # Clean up resources
        if mongo_client:
            # Make sure any remaining buffered measurements are flushed
            buffer = get_measurement_buffer(db)
            buffer.flush_to_db()
            
            # Close the MongoDB connection
            mongo_client.close()
        
        if sensor:
            cleanup_sensor(sensor)

if __name__ == "__main__":
    sys.exit(run())