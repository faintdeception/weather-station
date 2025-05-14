#!/usr/bin/env python3
"""
Data processing functions for the WeatherHAT application
"""
import time
import sys
import traceback
import json
import math
import os
import pickle
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne, InsertOne
from bson.objectid import ObjectId

# Custom JSON encoder to handle datetime objects and MongoDB ObjectIds
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            # Convert datetime to string
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            # Convert ObjectId to string
            return str(obj)
        return super().default(obj)

class MeasurementBuffer:
    """Buffer for collecting measurements before writing to database"""
    def __init__(self, db=None, max_size=10, max_age_seconds=300, cache_file=None):
        self.buffer = []
        self.max_size = max_size
        self.max_age_seconds = max_age_seconds
        self.last_flush_time = time.time()
        self.db = db
        self.cache_file = cache_file or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            'measurement_buffer.pickle'
        )
        self._load_from_cache()
    
    def _load_from_cache(self):
        """Load any cached measurements from disk in case of previous failure"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'rb') as f:
                    cached_data = pickle.load(f)
                    if isinstance(cached_data, list):
                        self.buffer.extend(cached_data)
                        print(f"Loaded {len(cached_data)} cached measurements", file=sys.stderr)
                # Remove the cache file after successful load
                os.remove(self.cache_file)
        except Exception as e:
            print(f"Error loading measurement cache: {e}", file=sys.stderr)
    
    def _save_to_cache(self):
        """Save buffer to disk in case of failure"""
        try:
            if self.buffer:
                with open(self.cache_file, 'wb') as f:
                    pickle.dump(self.buffer, f)
                print(f"Saved {len(self.buffer)} measurements to cache", file=sys.stderr)
        except Exception as e:
            print(f"Error saving measurement cache: {e}", file=sys.stderr)
    
    def add(self, measurement):
        """Add a measurement to the buffer"""
        self.buffer.append(measurement)
        
        # Check if it's time to flush the buffer
        current_time = time.time()
        if (len(self.buffer) >= self.max_size or 
            current_time - self.last_flush_time >= self.max_age_seconds):
            return self.flush_to_db()
        return True
    
    def flush_to_db(self):
        """Flush all buffered measurements to the database"""
        if not self.buffer:
            return True
        
        try:
            if self.db is None:
                raise ValueError("Database connection not provided")
            
            # Use bulk write for efficiency
            bulk_ops = [InsertOne(item) for item in self.buffer]
            result = self.db['measurements'].bulk_write(bulk_ops)
            
            print(f"Flushed {len(self.buffer)} measurements to database", file=sys.stderr)
            
            # Reset buffer and update flush time
            self.buffer = []
            self.last_flush_time = time.time()
            
            return True
        except Exception as e:
            print(f"Error flushing measurement buffer: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            # Save to cache in case of failure
            self._save_to_cache()
            return False

# Global measurement buffer
_measurement_buffer = None

def get_measurement_buffer(db=None, max_size=10, max_age_seconds=300):
    """Get the singleton measurement buffer instance"""
    global _measurement_buffer
    if _measurement_buffer is None:
        _measurement_buffer = MeasurementBuffer(db, max_size, max_age_seconds)
    elif db is not None and _measurement_buffer.db is None:
        _measurement_buffer.db = db
    return _measurement_buffer

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
    
    # Get current timestamp in nanoseconds
    timestamp_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    
    # Prepare measurement data
    measurement = {
        "timestamp": timestamp_ns,  # Nanoseconds UTC timestamp for InfluxDB compatibility
        "timestamp_ms": datetime.fromtimestamp(timestamp_ns/1e9, timezone.utc),  # MongoDB date for TTL
        "fields": avg_fields,
        "tags": {
            "location": location,
            "sensor_type": sensor_type
        }
    }
    
    return measurement

def store_measurement(db, measurement):
    """Store a measurement in MongoDB using the measurement buffer"""
    try:
        # Get the measurement buffer and add the measurement
        buffer = get_measurement_buffer(db)
        buffer.add(measurement)
        
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
            'hour_1': current_time - timedelta(hours=1),
            'hour_3': current_time - timedelta(hours=3),
            'hour_6': current_time - timedelta(hours=6),
            'hour_12': current_time - timedelta(hours=12),
            'hour_24': current_time - timedelta(hours=24),
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

def setup_retention_policies(db):
    """Set up TTL indexes for automatic data expiration and ensure collections exist"""
    try:
        # Create collections if they don't exist
        collections = db.list_collection_names()
        
        if "hourly_measurements" not in collections:
            db.create_collection("hourly_measurements")
            print("Created hourly_measurements collection", file=sys.stderr)
            
        if "daily_measurements" not in collections:
            db.create_collection("daily_measurements")
            print("Created daily_measurements collection", file=sys.stderr)
        
        # Set up TTL indexes for each collection with appropriate retention periods
        
        # Keep raw measurements for 30 days (much lower than before to save space)
        db.measurements.create_index(
            [("timestamp_ms", 1)], 
            expireAfterSeconds=7776000,  # 90 days
            background=True
        )
        
        # Keep hourly data for 90 days
        db.hourly_measurements.create_index(
            [("timestamp_ms", 1)],
            expireAfterSeconds=7776000,  # 90 days
            background=True
        )
        
        # Keep trend data for 90 days (reduced from 180)
        db.trends.create_index(
            [("timestamp_ms", 1)],
            expireAfterSeconds=7776000,  # 90 days
            background=True
        )
        
        # Daily data can stay longer - 365 days
        db.daily_measurements.create_index(
            [("timestamp_ms", 1)],
            expireAfterSeconds=31536000,  # 365 days
            background=True
        )
        
        print("Set up data retention policies with tiered storage", file=sys.stderr)
    except Exception as e:
        print(f"Error setting up retention policies: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

def setup_indexes(db):
    """Set up indexes for improved query performance"""
    try:
        # Index for faster location-based queries
        db.measurements.create_index([("tags.location", 1), ("timestamp", -1)])
        db.hourly_measurements.create_index([("tags.location", 1), ("timestamp", -1)])
        db.daily_measurements.create_index([("tags.location", 1), ("timestamp", -1)])
        
        # Index for trend calculations
        db.measurements.create_index([("timestamp", 1), ("tags.location", 1)])
        
        print("Set up performance indexes", file=sys.stderr)
    except Exception as e:
        print(f"Error setting up indexes: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

def downsample_hourly(db):
    """Aggregate measurement data to hourly records"""
    try:
        # Get the current time and one hour ago
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        
        # Convert to nanosecond timestamps
        now_ns = int(now.timestamp() * 1e9)
        one_hour_ago_ns = int(one_hour_ago.timestamp() * 1e9)
        
        # Get the hour timestamp (rounded to the hour)
        hour_start = datetime(one_hour_ago.year, one_hour_ago.month, one_hour_ago.day, 
                             one_hour_ago.hour, 0, 0, tzinfo=timezone.utc)
        hour_timestamp = int(hour_start.timestamp() * 1e9)
        
        # Check if we already have an hourly record for this hour
        existing = db.hourly_measurements.find_one({
            "hour_timestamp": hour_timestamp,
            "tags.location": {"$exists": True}
        })
        
        if existing:
            print(f"Hourly record for {hour_start} already exists", file=sys.stderr)
            return None
            
        # Find measurements in the hour that need to be aggregated
        pipeline = [
            {
                "$match": {
                    "timestamp": {"$gte": one_hour_ago_ns, "$lt": now_ns},
                    "tags.location": {"$exists": True}
                }
            },
            {
                "$group": {
                    "_id": {
                        "hour": {"$dateTrunc": {"date": "$timestamp_ms", "unit": "hour"}},
                        "location": "$tags.location",
                        "sensor_type": "$tags.sensor_type"
                    },
                    "avg_temperature": {"$avg": "$fields.temperature"},
                    "min_temperature": {"$min": "$fields.temperature"},
                    "max_temperature": {"$max": "$fields.temperature"},
                    "avg_humidity": {"$avg": "$fields.humidity"},
                    "avg_pressure": {"$avg": "$fields.pressure"},
                    "avg_wind_speed": {"$avg": "$fields.wind_speed"},
                    "max_wind_speed": {"$max": "$fields.wind_speed"},
                    "avg_lux": {"$avg": "$fields.lux"},
                    "count": {"$sum": 1}
                }
            }
        ]
        
        results = list(db.measurements.aggregate(pipeline))
        
        # Store hourly records
        for result in results:
            if result["count"] < 5:  # Require minimum number of readings
                continue
                
            hourly_data = {
                "timestamp": hour_timestamp,
                "timestamp_ms": hour_start,
                "hour_timestamp": hour_timestamp,  # For easy querying
                "fields": {
                    "temperature": {
                        "avg": result["avg_temperature"],
                        "min": result["min_temperature"],
                        "max": result["max_temperature"]
                    },
                    "humidity": {"avg": result["avg_humidity"]},
                    "pressure": {"avg": result["avg_pressure"]},
                    "wind_speed": {
                        "avg": result["avg_wind_speed"],
                        "max": result["max_wind_speed"]
                    },
                    "lux": {"avg": result["avg_lux"]},
                    "sample_count": result["count"]
                },
                "tags": {
                    "location": result["_id"]["location"],
                    "sensor_type": result["_id"]["sensor_type"]
                }
            }
            
            db.hourly_measurements.insert_one(hourly_data)
            print(f"Created hourly record for {hour_start}", file=sys.stderr)
            
        return len(results)
    except Exception as e:
        print(f"Error in downsample_hourly: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None

def downsample_daily(db):
    """Aggregate hourly data to daily records"""
    try:
        # Get the current time and yesterday
        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)
        
        # Get the day timestamp (rounded to the day)
        day_start = datetime(yesterday.year, yesterday.month, yesterday.day, 
                            0, 0, 0, tzinfo=timezone.utc)
        day_timestamp = int(day_start.timestamp() * 1e9)
        
        # Check if we already have a daily record for this day
        existing = db.daily_measurements.find_one({
            "day_timestamp": day_timestamp,
            "tags.location": {"$exists": True}
        })
        
        if existing:
            print(f"Daily record for {day_start.date()} already exists", file=sys.stderr)
            return None
            
        # Find hourly records for the day
        pipeline = [
            {
                "$match": {
                    "timestamp_ms": {
                        "$gte": day_start,
                        "$lt": day_start + timedelta(days=1)
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "day": {"$dateTrunc": {"date": "$timestamp_ms", "unit": "day"}},
                        "location": "$tags.location",
                        "sensor_type": "$tags.sensor_type"
                    },
                    "avg_temperature": {"$avg": "$fields.temperature.avg"},
                    "min_temperature": {"$min": "$fields.temperature.min"},
                    "max_temperature": {"$max": "$fields.temperature.max"},
                    "avg_humidity": {"$avg": "$fields.humidity.avg"},
                    "avg_pressure": {"$avg": "$fields.pressure.avg"},
                    "avg_wind_speed": {"$avg": "$fields.wind_speed.avg"},
                    "max_wind_speed": {"$max": "$fields.wind_speed.max"},
                    "avg_lux": {"$avg": "$fields.lux.avg"},
                    "hour_count": {"$sum": 1}
                }
            }
        ]
        
        results = list(db.hourly_measurements.aggregate(pipeline))
        
        # Store daily records
        for result in results:
            if result["hour_count"] < 12:  # Require at least 12 hours of data
                continue
                
            daily_data = {
                "timestamp": day_timestamp,
                "timestamp_ms": day_start,
                "day_timestamp": day_timestamp,  # For easy querying
                "date": day_start.strftime("%Y-%m-%d"),
                "fields": {
                    "temperature": {
                        "avg": result["avg_temperature"],
                        "min": result["min_temperature"],
                        "max": result["max_temperature"]
                    },
                    "humidity": {"avg": result["avg_humidity"]},
                    "pressure": {"avg": result["avg_pressure"]},
                    "wind_speed": {
                        "avg": result["avg_wind_speed"],
                        "max": result["max_wind_speed"]
                    },
                    "lux": {"avg": result["avg_lux"]},
                    "hour_count": result["hour_count"]
                },
                "tags": {
                    "location": result["_id"]["location"],
                    "sensor_type": result["_id"]["sensor_type"]
                }
            }
            
            db.daily_measurements.insert_one(daily_data)
            print(f"Created daily record for {day_start.date()}", file=sys.stderr)
            
        return len(results)
    except Exception as e:
        print(f"Error in downsample_daily: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None

def get_collection_sizes(db):
    """Get the size of each collection in the database"""
    try:
        stats = {}
        for collection_name in db.list_collection_names():
            stats[collection_name] = db.command("collStats", collection_name)["size"] / (1024 * 1024)  # MB
        return stats
    except Exception as e:
        print(f"Error getting collection sizes: {e}", file=sys.stderr)
        return {}

def perform_database_maintenance(db):
    """Run all database maintenance tasks"""
    try:
        print("Starting database maintenance...", file=sys.stderr)
        
        # Downsample hourly data
        hourly_result = downsample_hourly(db)
        print(f"Hourly downsampling complete: {hourly_result} records created", file=sys.stderr)
        
        # Downsample daily data
        daily_result = downsample_daily(db)
        print(f"Daily downsampling complete: {daily_result} records created", file=sys.stderr)
        
        # Verify TTL indexes are working
        measurements_index = db.measurements.index_information()
        if "timestamp_ms_1" not in measurements_index:
            print("WARNING: TTL index on measurements collection is missing!", file=sys.stderr)
            setup_retention_policies(db)
        
        # Report collection sizes
        sizes = get_collection_sizes(db)
        print("Current database collection sizes (MB):", file=sys.stderr)
        for collection, size in sizes.items():
            print(f"  {collection}: {size:.2f} MB", file=sys.stderr)
        
        print("Database maintenance complete", file=sys.stderr)
    except Exception as e:
        print(f"Error in perform_database_maintenance: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

def get_sampling_config(db):
    """
    Determine optimal sampling configuration based on weather variability
    
    Returns a dictionary with sampling parameters that adjust based on
    current weather condition variability.
    """
    try:
        # Default configuration (moderate sampling)
        default_config = {
            "frequency_minutes": 10,
            "num_readings": 3,
            "discard_first": True,
            "buffer_size": 10,
            "buffer_max_age_seconds": 300  # 5 minutes
        }
        
        if db is None:
            return default_config
            
        # Check recent weather variability from the last hour
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        one_hour_ago_ns = int(one_hour_ago.timestamp() * 1e9)
        now_ns = int(now.timestamp() * 1e9)
        
        # Get recent measurements
        recent_measurements = list(db.measurements.find(
            {
                "timestamp": {"$gte": one_hour_ago_ns, "$lt": now_ns},
                "fields.temperature": {"$exists": True},
                "fields.pressure": {"$exists": True}
            },
            {
                "fields.temperature": 1,
                "fields.pressure": 1,
                "fields.wind_speed": 1,
                "timestamp": 1
            }
        ).sort("timestamp", 1))
        
        # If we don't have enough data, use default config
        if len(recent_measurements) < 3:
            return default_config
            
        # Calculate variability in key parameters
        temps = [m['fields']['temperature'] for m in recent_measurements if 'temperature' in m['fields']]
        pressures = [m['fields']['pressure'] for m in recent_measurements if 'pressure' in m['fields']]
        wind_speeds = [m['fields']['wind_speed'] for m in recent_measurements if 'wind_speed' in m['fields']]
        
        # Calculate variability metrics
        temp_range = max(temps) - min(temps) if temps else 0
        pressure_change = abs(pressures[-1] - pressures[0]) if len(pressures) > 1 else 0
        max_wind = max(wind_speeds) if wind_speeds else 0
        
        # Determine config based on variability
        high_variability = (
            temp_range > 3.0 or  # More than 3°C change in an hour
            pressure_change > 2.0 or  # Pressure changing rapidly (potential storm)
            max_wind > 15.0  # High winds
        )
        
        low_variability = (
            temp_range < 0.5 and  # Very stable temperature
            pressure_change < 0.5 and  # Stable pressure
            max_wind < 5.0  # Light wind
        )
        
        if high_variability:
            # Higher sampling rate for rapidly changing conditions
            return {
                "frequency_minutes": 5,
                "num_readings": 5,
                "discard_first": True,
                "buffer_size": 5,  # Flush more frequently
                "buffer_max_age_seconds": 180  # 3 minutes
            }
        elif low_variability:
            # Lower sampling rate for stable conditions (save power & bandwidth)
            return {
                "frequency_minutes": 15,
                "num_readings": 2,
                "discard_first": True,
                "buffer_size": 15,
                "buffer_max_age_seconds": 600  # 10 minutes
            }
        else:
            # Default/moderate variability
            return default_config
            
    except Exception as e:
        print(f"Error determining sampling config: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        # Return default config if anything goes wrong
        return {
            "frequency_minutes": 10,
            "num_readings": 3,
            "discard_first": True,
            "buffer_size": 10,
            "buffer_max_age_seconds": 300
        }