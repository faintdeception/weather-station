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

from weatherhat_app.sensor_utils import initialize_sensor, take_readings, calculate_average_readings, cleanup_sensor
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

# Rain processing is now handled entirely in the database
# No global variables needed

def process_rain_measurement(db, current_rain_count):
    """
    Process rain measurements and return incremental rainfall for this measurement.
    
    The WeatherHAT rain gauge provides cumulative tip counts, so we track
    the difference between readings to calculate incremental rainfall.
    This returns the amount of rain that fell since the last measurement,
    not the cumulative daily total.
    
    Args:
        db: MongoDB database connection
        current_rain_count: Current rain gauge tip count
        
    Returns:
        float: Incremental rainfall in mm for this measurement period
    """
    current_time = time.time()
    RAIN_CALIBRATION_FACTOR = 0.2794  # mm per rain gauge tip
    
    # Load the previous rain state from database
    try:
        rain_state = db['rain_state'].find_one({'_id': 'rain_accumulation'})
        if not rain_state:
            # First time setup - initialize state
            print(f"Initializing rain tracking with count: {current_rain_count}", file=sys.stderr)
            db['rain_state'].update_one(
                {'_id': 'rain_accumulation'},
                {'$set': {
                    'accumulated_rain': 0,
                    'last_reset_time': current_time,
                    'last_rain_count': current_rain_count
                }},
                upsert=True
            )
            return 0.0  # No incremental rain for first measurement
        
        accumulated_rain = rain_state.get('accumulated_rain', 0)
        last_reset_time = rain_state.get('last_reset_time', current_time)
        previous_rain_count = rain_state.get('last_rain_count', 0)
        
        # Check for daily reset (at midnight)
        from datetime import datetime
        current_date = datetime.fromtimestamp(current_time).date()
        last_reset_date = datetime.fromtimestamp(last_reset_time).date()
        
        if current_date > last_reset_date:
            print(f"Daily reset - clearing accumulated rain", file=sys.stderr)
            accumulated_rain = 0
            last_reset_time = current_time
            previous_rain_count = current_rain_count  # Reset baseline count
        
        print(f"Rain comparison: current={current_rain_count}, previous={previous_rain_count}", file=sys.stderr)
        
        # Calculate incremental rain for THIS measurement
        rain_count_diff = current_rain_count - previous_rain_count
        incremental_rain_mm = 0.0
        
        # Handle gauge reset (count went backwards or dropped significantly)
        if rain_count_diff < 0 or current_rain_count < (previous_rain_count * 0.5):
            print(f"Rain gauge reset detected (count went from {previous_rain_count} to {current_rain_count})", file=sys.stderr)
            rain_count_diff = 0
            # Don't add any incremental rain, just update the base count
        
        # Calculate incremental rain for this measurement
        if rain_count_diff > 0:
            incremental_rain_mm = rain_count_diff * RAIN_CALIBRATION_FACTOR
            accumulated_rain += incremental_rain_mm
            print(f"New rain detected: {rain_count_diff} tips = {incremental_rain_mm:.2f}mm, daily total: {accumulated_rain:.2f}mm", file=sys.stderr)
        else:
            print(f"No new rain, daily total remains: {accumulated_rain:.2f}mm", file=sys.stderr)
        
        # Update rain state in database
        db['rain_state'].update_one(
            {'_id': 'rain_accumulation'},
            {'$set': {
                'accumulated_rain': accumulated_rain,
                'last_reset_time': last_reset_time,
                'last_rain_count': current_rain_count
            }},
            upsert=True
        )
        
        # Return INCREMENTAL rain for this measurement (not cumulative)
        return incremental_rain_mm
        
    except Exception as e:
        print(f"Error calculating rain difference: {e}", file=sys.stderr)
        return 0.0

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
          # Set up data retention policies and performance indexes
        setup_retention_policies(db)
        setup_indexes(db)
        
        # Create maintenance tracker and check for needed maintenance
        maintenance_tracker = MaintenanceTracker(db)
        maintenance_tasks = maintenance_tracker.check_and_run_maintenance()
        if maintenance_tasks:
            print(f"Completed maintenance tasks: {maintenance_tasks}", file=sys.stderr)
        
        # Rain state is now loaded automatically in process_rain_measurement function
        
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
        # Use a longer interval to ensure wind and rain measurements have time to accumulate
        # Based on working averaging.py example, we use fewer readings with longer intervals
        readings = take_readings(
            sensor, 
            num_readings=min(sampling_config.get('num_readings', 3), 2),  # Max 2 readings due to longer intervals
            discard_first=sampling_config.get('discard_first', True)
        )
        
        # Calculate average values for most measurements
        avg_fields = calculate_average_readings(readings)
        
        # Rain handling: sensor.rain already provides mm/sec rate (like working example)
        # No need for complex tip count processing - use the sensor value directly
        print(f"Rain rate from sensor: {avg_fields.get('rain', 0):.3f} mm/sec", file=sys.stderr)
        
        # The avg_fields['rain'] already contains the correct mm/sec value from sensor.rain
        # This matches the working averaging.py example output format
        
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