#!/usr/bin/env python3
"""
Data processing functions for the WeatherHAT application
"""
import time
import sys
import traceback
import json
import math
from datetime import datetime
from pymongo import MongoClient

def connect_to_mongodb(mongo_uri, max_retries=5, retry_interval=5):
    """Connect to MongoDB with retry logic"""
    retry_count = 0
    while retry_count < max_retries:
        try:
            print(f"Connecting to MongoDB at {mongo_uri}", file=sys.stderr)
            mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            # Force a connection to verify it works
            mongo_client.server_info()
            print("Successfully connected to MongoDB", file=sys.stderr)
            return mongo_client
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                raise Exception(f"Failed to connect to MongoDB after {max_retries} attempts: {e}")
            print(f"MongoDB connection attempt {retry_count} failed: {e}. Retrying in {retry_interval} seconds...", file=sys.stderr)
            time.sleep(retry_interval)
    
    # Should never reach here due to exception in loop
    return None

def prepare_measurement(avg_fields, sensor, location="backyard", sensor_type="weatherhat"):
    """Prepare a measurement object from sensor readings"""
    # Add cardinal direction if not present
    if "wind_direction_cardinal" not in avg_fields and "wind_direction" in avg_fields:
        avg_fields["wind_direction_cardinal"] = sensor.degrees_to_cardinal(avg_fields["wind_direction"])
    
    # Prepare measurement data
    measurement = {
        "timestamp": int(time.time() * 1e9),  # Nanoseconds timestamp for InfluxDB compatibility
        "fields": avg_fields,
        "tags": {
            "location": location,
            "sensor_type": sensor_type
        }
    }
    
    return measurement

def store_measurement(db, measurement):
    """Store a measurement in MongoDB"""
    try:
        # Store current measurement in measurements collection
        result = db['measurements'].insert_one(measurement)
        
        # Remove the _id field from the measurement before returning (for JSON output)
        if '_id' in measurement:
            del measurement['_id']
        
        return measurement
    except Exception as e:
        print(f"Error storing measurement: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None

def update_records(db, measurement):
    """Update record-breaking values in MongoDB"""
    try:
        records_collection = db['records']
        
        # Extract fields from measurement
        fields = measurement.get('fields', {})
        timestamp = measurement.get('timestamp')
        location = measurement.get('tags', {}).get('location', 'unknown')
        
        # Fields to track records for
        record_fields = ['temperature', 'humidity', 'wind_speed', 'pressure', 'lux']
        
        for field in record_fields:
            if field in fields:
                current_value = fields[field]
                
                # Check for highest record
                highest_record = records_collection.find_one(
                    {'field': field, 'location': location, 'record_type': 'highest'}
                )
                
                # If no record exists or the current value is higher, update the record
                if highest_record is None or current_value > highest_record['value']:
                    records_collection.update_one(
                        {'field': field, 'location': location, 'record_type': 'highest'},
                        {'$set': {
                            'value': current_value,
                            'timestamp': timestamp,
                            'date': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1e9))
                        }},
                        upsert=True
                    )
                    print(f"New highest record for {field}: {current_value}", file=sys.stderr)
                
                # Check for lowest record
                lowest_record = records_collection.find_one(
                    {'field': field, 'location': location, 'record_type': 'lowest'}
                )
                
                # If no record exists or the current value is lower, update the record
                if lowest_record is None or current_value < lowest_record['value']:
                    records_collection.update_one(
                        {'field': field, 'location': location, 'record_type': 'lowest'},
                        {'$set': {
                            'value': current_value,
                            'timestamp': timestamp,
                            'date': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1e9))
                        }},
                        upsert=True
                    )
                    print(f"New lowest record for {field}: {current_value}", file=sys.stderr)
    except Exception as e:
        print(f"Error in update_records: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

def calculate_trends(db, measurement):
    """Calculate and store trend data based on recent measurements"""
    try:
        trends_collection = db['trends']
        measurements_collection = db['measurements']
        
        # Extract current values and metadata
        fields = measurement.get('fields', {})
        timestamp = measurement.get('timestamp')
        location = measurement.get('tags', {}).get('location', 'unknown')
        current_time = datetime.fromtimestamp(timestamp/1e9)
        
        # Time ranges for trend calculations
        time_ranges = {
            'hour_1': current_time - datetime.timedelta(hours=1),
            'hour_3': current_time - datetime.timedelta(hours=3),
            'hour_6': current_time - datetime.timedelta(hours=6),
            'hour_12': current_time - datetime.timedelta(hours=12),
            'hour_24': current_time - datetime.timedelta(hours=24),
        }
        
        # Parameters to analyze
        trend_parameters = ['temperature', 'pressure', 'humidity', 'wind_speed']
        
        trends_data = {
            "timestamp": timestamp,
            "date": current_time.strftime('%Y-%m-%d %H:%M:%S'),
            "location": location,
            "trends": {}
        }
        
        # Process each parameter
        for param in trend_parameters:
            if param in fields:
                current_value = fields[param]
                param_trends = {}
                
                # Calculate trends for each time range
                for range_name, start_time in time_ranges.items():
                    # Convert start_time to timestamp in nanoseconds
                    start_timestamp = int(start_time.timestamp() * 1e9)
                    
                    # Query for measurements in the time range
                    historical_data = list(measurements_collection.find(
                        {
                            'timestamp': {'$gte': start_timestamp, '$lt': timestamp},
                            'tags.location': location,
                            f'fields.{param}': {'$exists': True}
                        },
                        {f'fields.{param}': 1, 'timestamp': 1}
                    ).sort('timestamp', 1))
                    
                    # Only calculate if we have data
                    if historical_data:
                        # Extract values
                        values = [doc['fields'][param] for doc in historical_data]
                        
                        # Get first value in the range for calculating change
                        first_value = values[0] if values else current_value
                        
                        # Calculate metrics
                        import statistics  # Import here to avoid potential circular imports
                        param_trends[range_name] = {
                            "count": len(values),
                            "min": min(values) if values else current_value,
                            "max": max(values) if values else current_value,
                            "avg": statistics.mean(values) if values else current_value,
                            "change": current_value - first_value,
                            "change_pct": ((current_value - first_value) / first_value * 100) if first_value != 0 else 0,
                            "rate_per_hour": (current_value - first_value) / (len(time_ranges) if len(time_ranges) > 0 else 1)
                        }
                
                # Store trends for this parameter
                trends_data["trends"][param] = param_trends
        
        # Store the trend data
        trends_collection.insert_one(trends_data)
        print(f"Stored trend data for {current_time.strftime('%Y-%m-%d %H:%M:%S')}", file=sys.stderr)
        
        return trends_data
    except Exception as e:
        print(f"Error in calculate_trends: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None