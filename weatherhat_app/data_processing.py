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
from pymongo.errors import BulkWriteError
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
                        sanitized = [self._sanitize_for_write(item) for item in cached_data]
                        self.buffer.extend(sanitized)
                        print(f"Loaded {len(cached_data)} cached measurements", file=sys.stderr)
                # Remove the cache file after successful load
                os.remove(self.cache_file)
        except Exception as e:
            print(f"Error loading measurement cache: {e}", file=sys.stderr)
    
    def _save_to_cache(self):
        """Save buffer to disk in case of failure"""
        try:
            if self.buffer:
                sanitized = [self._sanitize_for_write(item) for item in self.buffer]
                with open(self.cache_file, 'wb') as f:
                    pickle.dump(sanitized, f)
                print(f"Saved {len(sanitized)} measurements to cache", file=sys.stderr)
        except Exception as e:
            print(f"Error saving measurement cache: {e}", file=sys.stderr)

    def _sanitize_for_write(self, measurement):
        """Return a copy of measurement safe for DB writes and cache persistence."""
        if not isinstance(measurement, dict):
            return measurement

        sanitized = dict(measurement)
        sanitized.pop('_id', None)
        return sanitized
    
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

            buffer_size_before_flush = len(self.buffer)
            sanitized_buffer = [self._sanitize_for_write(item) for item in self.buffer]
            
            # Use bulk write for efficiency
            bulk_ops = [InsertOne(item) for item in sanitized_buffer]

            try:
                self.db['measurements'].bulk_write(bulk_ops)
                self.buffer = []
                print(f"Flushed {buffer_size_before_flush} measurements to database", file=sys.stderr)
            except BulkWriteError as bulk_error:
                details = bulk_error.details or {}
                write_errors = details.get('writeErrors', [])
                duplicate_indexes = {
                    err.get('index') for err in write_errors
                    if err.get('code') == 11000 and isinstance(err.get('index'), int)
                }

                if duplicate_indexes:
                    # Drop duplicate entries from the live buffer so they don't poison all future flush attempts.
                    self.buffer = [
                        item for idx, item in enumerate(self.buffer)
                        if idx not in duplicate_indexes
                    ]

                    print(
                        f"Skipped {len(duplicate_indexes)} duplicate buffered measurements; "
                        f"{len(self.buffer)} remaining in buffer",
                        file=sys.stderr
                    )

                    # If only duplicates failed, treat as successful flush for this cycle.
                    non_duplicate_errors = [
                        err for err in write_errors if err.get('code') != 11000
                    ]
                    if not non_duplicate_errors:
                        self.last_flush_time = time.time()
                        return True

                raise
            
            # Reset buffer and update flush time
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


def _get_record_field_value(fields, field):
    """Return the value used for record tracking for a measurement field.

    Temperature records intentionally use the calibrated ambient
    `fields.temperature` value. `fields.device_temperature` is stored for
    diagnostics but should not drive record books.
    """
    if field == 'temperature':
        return fields.get('temperature')

    return fields.get(field)


def _timestamp_ns_to_utc_datetime(timestamp_ns):
    """Convert a nanosecond timestamp to a UTC datetime."""
    if timestamp_ns is None:
        return None

    try:
        return datetime.fromtimestamp(timestamp_ns / 1e9, timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _build_temperature_record_context(day_data=None, measurement_fields=None):
    """Build contextual metadata for a temperature record."""
    context = {}

    if day_data:
        day_temperature = day_data.get('fields', {}).get('temperature', {})
        day_date = day_data.get('date')

        day_context = {
            'date': day_date,
            'avg': day_temperature.get('avg'),
            'min': day_temperature.get('min'),
            'max': day_temperature.get('max'),
        }
        day_context = {key: value for key, value in day_context.items() if value is not None}
        if day_context:
            context['day'] = day_context

    if measurement_fields:
        conditions = {
            'humidity': measurement_fields.get('humidity'),
            'wind_speed': measurement_fields.get('wind_speed'),
            'lux': measurement_fields.get('lux'),
        }
        conditions = {key: value for key, value in conditions.items() if value is not None}
        if conditions:
            context['conditions'] = conditions

    return context


def _lookup_daily_temperature_summary(db, day_dt, location, sensor_type=None):
    """Return the daily aggregate document for the given date/location."""
    if day_dt is None:
        return None

    day_start = datetime(day_dt.year, day_dt.month, day_dt.day, tzinfo=timezone.utc)
    day_timestamp = int(day_start.timestamp() * 1e9)

    query = {
        'day_timestamp': day_timestamp,
        'tags.location': location,
    }
    if sensor_type:
        query['tags.sensor_type'] = sensor_type

    return db['daily_measurements'].find_one(
        query,
        {
            'date': 1,
            'fields.temperature.avg': 1,
            'fields.temperature.min': 1,
            'fields.temperature.max': 1,
        },
    )


def _lookup_measurement_fields(db, timestamp_ns, location, sensor_type=None):
    """Return the raw measurement fields for a record timestamp when available."""
    if timestamp_ns is None:
        return None

    query = {
        'timestamp': timestamp_ns,
        'tags.location': location,
    }
    if sensor_type:
        query['tags.sensor_type'] = sensor_type

    measurement = db['measurements'].find_one(
        query,
        {
            'fields.humidity': 1,
            'fields.wind_speed': 1,
            'fields.lux': 1,
        },
    )
    if measurement is None:
        return None

    return measurement.get('fields', {})


def _enrich_temperature_record(record_doc, db, day_data=None, measurement_fields=None):
    """Populate context for a stored temperature record document."""
    if not record_doc or record_doc.get('field') != 'temperature':
        return False

    location = record_doc.get('location', 'unknown')
    sensor_type = record_doc.get('sensor_type')
    timestamp_ns = record_doc.get('timestamp')

    if day_data is None:
        record_dt = _timestamp_ns_to_utc_datetime(timestamp_ns)
        day_data = _lookup_daily_temperature_summary(db, record_dt, location, sensor_type=sensor_type)

    if measurement_fields is None:
        measurement_fields = _lookup_measurement_fields(db, timestamp_ns, location, sensor_type=sensor_type)

    context = _build_temperature_record_context(day_data=day_data, measurement_fields=measurement_fields)
    if not context:
        return False

    db['records'].update_one(
        {'_id': record_doc['_id']},
        {'$set': {'context': context}},
    )
    return True


def backfill_temperature_record_context(db):
    """Backfill contextual metadata on temperature records.

    Context is intentionally temperature-only so consumers can better interpret
    unusually hot or cold records without changing the record value semantics.
    """
    try:
        records = db['records']
        cursor = records.find(
            {'field': 'temperature', 'record_type': {'$in': ['highest', 'lowest']}},
            {
                'field': 1,
                'record_type': 1,
                'location': 1,
                'sensor_type': 1,
                'timestamp': 1,
            },
        )

        updated_count = 0
        for record_doc in cursor:
            if _enrich_temperature_record(record_doc, db):
                updated_count += 1

        print(f"Backfilled context for {updated_count} temperature records", file=sys.stderr)
    except Exception as e:
        print(f"Error backfilling temperature record context: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

def update_records(db, measurement):
    """Update record-breaking values in MongoDB.

    Temperature records are based on the calibrated `temperature` field rather
    than raw `device_temperature` readings.
    """
    try:
        records_collection = db['records']
        
        # Extract fields from measurement
        fields = measurement.get('fields', {})
        timestamp = measurement.get('timestamp')
        location = measurement.get('tags', {}).get('location', 'unknown')
        sensor_type = measurement.get('tags', {}).get('sensor_type')
        temperature_context = _build_temperature_record_context(measurement_fields=fields)
        
        # Fields to track records for
        record_fields = ['temperature', 'humidity', 'wind_speed', 'pressure', 'lux']
        
        for field in record_fields:
            current_value = _get_record_field_value(fields, field)
            if current_value is not None:
                
                # Check for highest record
                highest_record = records_collection.find_one(
                    {'field': field, 'location': location, 'record_type': 'highest'}
                )
                
                # If no record exists or the current value is higher, update the record
                if highest_record is None or current_value > highest_record['value']:
                    set_fields = {
                        'value': current_value,
                        'timestamp': timestamp,
                        'date': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1e9))
                    }
                    if field == 'temperature' and temperature_context:
                        set_fields['context'] = temperature_context

                    records_collection.update_one(
                        {'field': field, 'location': location, 'record_type': 'highest'},
                        {'$set': set_fields},
                        upsert=True
                    )
                    print(f"New highest record for {field}: {current_value}", file=sys.stderr)
                
                # Check for lowest record
                lowest_record = records_collection.find_one(
                    {'field': field, 'location': location, 'record_type': 'lowest'}
                )
                
                # If no record exists or the current value is lower, update the record
                if lowest_record is None or current_value < lowest_record['value']:
                    set_fields = {
                        'value': current_value,
                        'timestamp': timestamp,
                        'date': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1e9))
                    }
                    if field == 'temperature' and temperature_context:
                        set_fields['context'] = temperature_context

                    records_collection.update_one(
                        {'field': field, 'location': location, 'record_type': 'lowest'},
                        {'$set': set_fields},
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

        def ensure_ttl_index(collection, field_name, seconds):
            idx_name = f"{field_name}_1"
            desired_key = [(field_name, 1)]
            info = collection.index_information()
            existing = info.get(idx_name)

            if existing:
                existing_key = existing.get('key')
                existing_ttl = existing.get('expireAfterSeconds')

                # Index already matches what we need; skip create_index to avoid
                # provider-specific option conflicts (e.g., Cosmos extra metadata).
                if existing_key == desired_key and existing_ttl == seconds:
                    return

                # Existing index differs, so replace it.
                try:
                    collection.drop_index(idx_name)
                    print(f"Dropped conflicting TTL index {idx_name} on {collection.name}", file=sys.stderr)
                except Exception as drop_err:
                    print(f"Warning: could not drop index {idx_name} on {collection.name}: {drop_err}", file=sys.stderr)
                    # If we can't drop it, bail to avoid repeated failures.
                    return

            collection.create_index(
                desired_key,
                name=idx_name,
                expireAfterSeconds=seconds,
                background=True
            )

        # Keep raw measurements for ~90 days
        ensure_ttl_index(db.measurements, "timestamp_ms", 7776000)

        # Keep hourly data for ~90 days
        ensure_ttl_index(db.hourly_measurements, "timestamp_ms", 7776000)

        # Keep trend data for ~180 days
        ensure_ttl_index(db.trends, "timestamp_ms", 15552000)

        # Daily data kept longer for date-record features - 5 years
        ensure_ttl_index(db.daily_measurements, "timestamp_ms", 157680000)
        
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

        # Index for fast lookup of calendar-date records
        db.daily_date_records.create_index([("month_day", 1), ("location", 1)])

        # Index for records collection lookups (instantaneous + aggregate record tracking)
        db.records.create_index([
            ("field", 1),
            ("record_type", 1),
            ("location", 1),
            ("sensor_type", 1),
        ])
        
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


def compute_daily_rain_stats(docs, day_start, day_end, max_gap_seconds=600):
    """Compute daily rain totals (mm) and max rate (mm/sec) per location/sensor.

    Raw `fields.rain` is treated as a rain rate in mm/sec. Daily rainfall depth is
    computed by integrating rate over time between consecutive samples.
    """
    grouped = {}

    for doc in docs:
        tags = doc.get("tags", {})
        location = tags.get("location", "unknown")
        sensor_type = tags.get("sensor_type", "weatherhat")
        group_key = (location, sensor_type)

        ts = doc.get("timestamp_ms")
        if ts is None:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        rain_rate = float(doc.get("fields", {}).get("rain", 0.0) or 0.0)

        bucket = grouped.setdefault(
            group_key,
            {
                "sum": 0.0,
                "max": 0.0,
                "sample_count": 0,
                "positive_samples": 0,
                "_prev_ts": None,
            },
        )

        prev_ts = bucket["_prev_ts"]
        bucket["_prev_ts"] = ts

        # Prime state with the first sample for the location/sensor stream.
        if prev_ts is None:
            if day_start <= ts < day_end and rain_rate > 0:
                bucket["positive_samples"] += 1
                if rain_rate > bucket["max"]:
                    bucket["max"] = rain_rate
            continue

        if ts < day_start or ts >= day_end:
            continue

        bucket["sample_count"] += 1
        if rain_rate > 0:
            bucket["positive_samples"] += 1
            if rain_rate > bucket["max"]:
                bucket["max"] = rain_rate

        # Clip start to day boundary so pre-day samples only contribute in-day time.
        interval_start = prev_ts if prev_ts >= day_start else day_start
        delta_seconds = (ts - interval_start).total_seconds()
        if delta_seconds <= 0:
            continue

        # Guardrail for sparse outages or delayed inserts that can inflate totals.
        effective_delta = min(delta_seconds, max_gap_seconds)
        bucket["sum"] += max(rain_rate, 0.0) * effective_delta

    # Remove internal state before returning.
    cleaned = {}
    for group_key, values in grouped.items():
        cleaned[group_key] = {
            "sum": float(values["sum"]),
            "max": float(values["max"]),
            "sample_count": int(values["sample_count"]),
            "positive_samples": int(values["positive_samples"]),
        }
    return cleaned


def get_daily_rain_stats(db, day_start, day_end, max_gap_seconds=600):
    """Load raw rain-rate samples and compute per-day rain totals per location/sensor."""
    lookback_start = day_start - timedelta(hours=1)
    cursor = db.measurements.find(
        {
            "timestamp_ms": {"$gte": lookback_start, "$lt": day_end},
            "fields.rain": {"$exists": True},
            "tags.location": {"$exists": True},
        },
        {
            "timestamp_ms": 1,
            "fields.rain": 1,
            "tags.location": 1,
            "tags.sensor_type": 1,
        },
    ).sort("timestamp_ms", 1)

    docs = list(cursor)
    return compute_daily_rain_stats(docs, day_start, day_end, max_gap_seconds=max_gap_seconds)


def downsample_daily(db, target_day=None, overwrite=False):
    """Aggregate hourly data to daily records"""
    try:
        # Default target is yesterday in UTC.
        if target_day is None:
            now = datetime.now(timezone.utc)
            base_day = now - timedelta(days=1)
        elif isinstance(target_day, datetime):
            base_day = target_day.astimezone(timezone.utc)
        else:
            # Assume date-like object (e.g., datetime.date)
            base_day = datetime(target_day.year, target_day.month, target_day.day, tzinfo=timezone.utc)
        
        # Get the day timestamp (rounded to the day)
        day_start = datetime(base_day.year, base_day.month, base_day.day, 0, 0, 0, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        day_timestamp = int(day_start.timestamp() * 1e9)
        
        # Check if we already have a daily record for this day
        existing = db.daily_measurements.find_one({
            "day_timestamp": day_timestamp,
            "tags.location": {"$exists": True}
        })
        
        if existing and not overwrite:
            print(f"Daily record for {day_start.date()} already exists", file=sys.stderr)
            return None

        rain_stats = get_daily_rain_stats(db, day_start, day_end)
            
        # Find hourly records for the day
        pipeline = [
            {
                "$match": {
                    "timestamp_ms": {
                        "$gte": day_start,
                        "$lt": day_end
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
            group_key = (result["_id"].get("location", "unknown"), result["_id"].get("sensor_type", "weatherhat"))
            rain_for_group = rain_stats.get(
                group_key,
                {"sum": 0.0, "max": 0.0, "sample_count": 0, "positive_samples": 0},
            )

            # Keep the historical data-quality gate, but do not discard rainy days.
            if result["hour_count"] < 12 and rain_for_group["positive_samples"] <= 0:
                continue

            if rain_for_group["positive_samples"] > 0 and rain_for_group["sum"] <= 0.0:
                print(
                    (
                        "WARNING: Rainy raw data detected but computed daily rain total is zero "
                        f"for {day_start.date()} location={group_key[0]} sensor={group_key[1]}"
                    ),
                    file=sys.stderr,
                )
                
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
                    "rain": {
                        # Keep legacy shape and add max_rate for explicit rate semantics.
                        "sum": rain_for_group["sum"],
                        "max": rain_for_group["max"],
                        "max_rate": rain_for_group["max"],
                        "sample_count": rain_for_group["sample_count"]
                    },
                    "lux": {"avg": result["avg_lux"]},
                    "hour_count": result["hour_count"]
                },
                "tags": {
                    "location": result["_id"]["location"],
                    "sensor_type": result["_id"]["sensor_type"]
                }
            }

            db.daily_measurements.update_one(
                {
                    "day_timestamp": day_timestamp,
                    "tags.location": result["_id"]["location"],
                    "tags.sensor_type": result["_id"]["sensor_type"],
                },
                {"$set": daily_data},
                upsert=True,
            )
            print(f"Created daily record for {day_start.date()}", file=sys.stderr)

            # Update long-lived calendar-date records for temperature extremes
            try:
                update_daily_date_records(db, daily_data)
            except Exception as e:
                print(f"Error updating daily date records: {e}", file=sys.stderr)

            # Track highest daily rain total ever recorded.
            try:
                update_highest_daily_rain_record(db, daily_data)
            except Exception as e:
                print(f"Error updating highest daily rain record: {e}", file=sys.stderr)

            # Add full-day context to temperature records once the daily summary exists.
            try:
                backfill_temperature_record_context(db)
            except Exception as e:
                print(f"Error backfilling temperature record context: {e}", file=sys.stderr)
            
        return len(results)
    except Exception as e:
        print(f"Error in downsample_daily: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None


def update_daily_date_records(db, daily_data):
    """Maintain highest/lowest temperature ever seen for a given calendar date (month-day).

    Stores a compact, non-TTL collection keyed by month_day (e.g., "01-24").
    Only temperature is tracked per requirements; values are in UTC.
    `daily_data.fields.temperature` is expected to already be the calibrated
    ambient temperature series, not `device_temperature`.
    """
    records = db['daily_date_records']

    # Extract needed fields
    day_dt = daily_data.get('timestamp_ms')  # datetime with tz
    if not isinstance(day_dt, datetime):
        return

    month_day = day_dt.strftime('%m-%d')
    location = daily_data.get('tags', {}).get('location', 'unknown')
    temp_fields = daily_data.get('fields', {}).get('temperature', {})
    day_min = temp_fields.get('min')
    day_max = temp_fields.get('max')

    if day_min is None and day_max is None:
        return

    # Build updates if we have new records
    update_ops = {}
    set_on_insert = {'month_day': month_day, 'location': location}

    current = records.find_one({'month_day': month_day, 'location': location})

    # Highest temp for this calendar date
    if day_max is not None:
        if current is None or 'high' not in current or day_max > current['high']['value']:
            update_ops['high'] = {
                'value': day_max,
                'date': day_dt.strftime('%Y-%m-%d'),
                'day_timestamp': int(day_dt.timestamp() * 1e9)
            }

    # Lowest temp for this calendar date
    if day_min is not None:
        if current is None or 'low' not in current or day_min < current['low']['value']:
            update_ops['low'] = {
                'value': day_min,
                'date': day_dt.strftime('%Y-%m-%d'),
                'day_timestamp': int(day_dt.timestamp() * 1e9)
            }

    if not update_ops and current is not None:
        return  # No change needed

    update_ops['updated_at'] = datetime.utcnow()

    records.update_one(
        {'month_day': month_day, 'location': location},
        {
            '$set': update_ops,
            '$setOnInsert': set_on_insert
        },
        upsert=True
    )


def update_highest_daily_rain_record(db, daily_data):
    """Maintain the highest daily rain total ever seen per location/sensor in records.

    Uses daily aggregate totals (fields.rain.sum) so records reflect full-day rainfall,
    not instantaneous rain-rate samples.
    """
    records = db['records']

    day_dt = daily_data.get('timestamp_ms')
    if not isinstance(day_dt, datetime):
        return

    tags = daily_data.get('tags', {})
    location = tags.get('location', 'unknown')
    sensor_type = tags.get('sensor_type', 'weatherhat')

    rain_fields = daily_data.get('fields', {}).get('rain', {})
    daily_total = rain_fields.get('sum')
    if daily_total is None:
        return

    try:
        daily_total = float(daily_total)
    except (TypeError, ValueError):
        return

    query = {
        'field': 'rain_daily_total',
        'record_type': 'highest',
        'location': location,
        'sensor_type': sensor_type,
    }

    existing = records.find_one(query)
    if existing is not None and daily_total <= float(existing.get('value', float('-inf'))):
        return

    now_utc = datetime.now(timezone.utc)

    records.update_one(
        query,
        {
            '$set': {
                'value': daily_total,
                'timestamp': int(day_dt.timestamp() * 1e9),
                'date': day_dt.strftime('%Y-%m-%d'),
                'updated_at': now_utc,
            },
            '$setOnInsert': {
                'field': 'rain_daily_total',
                'record_type': 'highest',
                'location': location,
                'sensor_type': sensor_type,
                'created_at': now_utc,
            },
        },
        upsert=True,
    )


def backfill_daily_date_records(db):
    """Backfill calendar-date records from existing daily_measurements.

    Idempotent; safe to run at startup or maintenance. Only temperature min/max is used.
    """
    try:
        cursor = db.daily_measurements.find({}, {
            'timestamp_ms': 1,
            'fields.temperature.min': 1,
            'fields.temperature.max': 1,
            'tags.location': 1
        })

        count = 0
        for doc in cursor:
            update_daily_date_records(db, {
                'timestamp_ms': doc.get('timestamp_ms'),
                'fields': {'temperature': doc.get('fields', {}).get('temperature', {})},
                'tags': doc.get('tags', {})
            })
            count += 1

        print(f"Backfilled daily_date_records from {count} daily_measurements", file=sys.stderr)
    except Exception as e:
        print(f"Error backfilling daily date records: {e}", file=sys.stderr)

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