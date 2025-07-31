#!/usr/bin/env python3
"""
Long-running weather monitoring service for Raspberry Pi with WeatherHAT

This service runs continuously and takes measurements at regular intervals,
properly maintaining state for rain accumulation.
"""
import os
import sys
import time
import signal
import json
import traceback
from datetime import datetime

# Load environment variables from .env file manually
def load_env_vars():
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
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

# Load environment variables
load_env_vars()

from weatherhat_app.sensor_utils import initialize_sensor, take_readings, calculate_average_readings, cleanup_sensor
from weatherhat_app.data_processing import (connect_to_mongodb, prepare_measurement, store_measurement, 
                                           update_records, calculate_trends, setup_retention_policies, setup_indexes,
                                           DateTimeEncoder)
from weatherhat_app.reporting import generate_daily_report
from weatherhat_app.maintenance_tracker import MaintenanceTracker

# MongoDB connection settings
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://akuma:27017')
DB_NAME = os.environ.get('MONGO_DB', 'weather_data')


class WeatherService:
    def __init__(self, interval_seconds=60):
        self.interval_seconds = interval_seconds
        self.running = True
        self.sensor = None
        self.mongo_client = None
        self.db = None
        self.maintenance_tracker = None
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print(f"\nReceived signal {signum}, shutting down gracefully...", file=sys.stderr)
        self.running = False
        
    def initialize(self):
        """Initialize sensor and database connections"""
        try:
            # Initialize sensor first
            print("Initializing WeatherHAT sensor...", file=sys.stderr)
            self.sensor = initialize_sensor()
            
            # Connect to MongoDB
            print("Connecting to MongoDB...", file=sys.stderr)
            self.mongo_client = connect_to_mongodb(MONGO_URI)
            self.db = self.mongo_client[DB_NAME]
            
            # Set up data retention policies and performance indexes
            setup_retention_policies(self.db)
            setup_indexes(self.db)
            
            # Initialize maintenance tracker
            self.maintenance_tracker = MaintenanceTracker(self.db)
            
            print("Weather service initialized successfully", file=sys.stderr)
            return True
            
        except Exception as e:
            print(f"Failed to initialize weather service: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            return False
            
    def take_measurement(self):
        """Take a single measurement and store it"""
        try:
            # Take sensor readings (with proper rain accumulation)
            # Use single reading since we're running continuously
            readings = take_readings(self.sensor, num_readings=1, discard_first=False)
            
            if not readings:
                print("No readings obtained from sensor", file=sys.stderr)
                return
                
            # Calculate averages (will be single values since num_readings=1)
            avg_fields = calculate_average_readings(readings)
            
            # Add cardinal wind direction
            if "wind_direction" in avg_fields:
                avg_fields["wind_direction_cardinal"] = self.sensor.degrees_to_cardinal(avg_fields["wind_direction"])
            
            # Prepare measurement using existing data processing logic
            measurement = prepare_measurement(avg_fields, self.sensor)
            
            # Update record-breaking values
            update_records(self.db, measurement)
            
            # Calculate and store trend data - only do this every hour to reduce DB load
            current_minute = datetime.now().minute
            if current_minute < 5:  # Only calculate trends at the start of each hour
                calculate_trends(self.db, measurement)
            
            # Store current measurement in measurements collection
            store_measurement(self.db, measurement)
            
            # Output the measurement as JSON for compatibility
            print(json.dumps(measurement, cls=DateTimeEncoder))
            sys.stdout.flush()
            
            print(f"Measurement stored successfully", file=sys.stderr)
            
        except Exception as e:
            print(f"Error taking measurement: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            
    def run_maintenance(self):
        """Run periodic maintenance tasks"""
        try:
            maintenance_tasks = self.maintenance_tracker.check_and_run_maintenance()
            if maintenance_tasks:
                print(f"Completed maintenance tasks: {maintenance_tasks}", file=sys.stderr)
        except Exception as e:
            print(f"Error running maintenance: {e}", file=sys.stderr)
            
    def run_daily_report(self):
        """Generate daily report if needed"""
        try:
            generate_daily_report(self.db)
        except Exception as e:
            print(f"Error generating daily report: {e}", file=sys.stderr)
            
    def cleanup(self):
        """Clean up resources"""
        print("Cleaning up resources...", file=sys.stderr)
        
        if self.sensor:
            cleanup_sensor(self.sensor)
            
        if self.mongo_client:
            self.mongo_client.close()
            
    def run(self):
        """Main service loop"""
        if not self.initialize():
            return 1
            
        print(f"Starting weather monitoring service (interval: {self.interval_seconds}s)", file=sys.stderr)
        
        # Take initial measurement immediately
        self.take_measurement()
        
        # Main loop
        last_measurement_time = time.time()
        last_maintenance_time = time.time()
        last_daily_report_time = time.time()
        
        while self.running:
            try:
                current_time = time.time()
                
                # Take measurement at specified interval
                if current_time - last_measurement_time >= self.interval_seconds:
                    self.take_measurement()
                    last_measurement_time = current_time
                    
                # Run maintenance every 5 minutes
                if current_time - last_maintenance_time >= 300:
                    self.run_maintenance()
                    last_maintenance_time = current_time
                    
                # Run daily report check every hour
                if current_time - last_daily_report_time >= 3600:
                    self.run_daily_report()
                    last_daily_report_time = current_time
                    
                # Sleep for a short time to avoid busy waiting
                time.sleep(1)
                
            except KeyboardInterrupt:
                print("Received keyboard interrupt", file=sys.stderr)
                break
            except Exception as e:
                print(f"Error in main loop: {e}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                time.sleep(5)  # Wait a bit before retrying
                
        # Cleanup
        self.cleanup()
        return 0


def main():
    """Main entry point"""
    # Get interval from environment or default to 60 seconds
    interval = int(os.environ.get('WEATHER_INTERVAL', '60'))
    
    print(f"WeatherHAT Service starting with {interval}s interval", file=sys.stderr)
    
    service = WeatherService(interval_seconds=interval)
    return service.run()


if __name__ == "__main__":
    sys.exit(main())
