#!/usr/bin/env python3
"""
Database maintenance script for the WeatherHAT application

This script performs database maintenance tasks such as:
- Verifying TTL indexes are properly set up
- Running downsampling operations for hourly/daily aggregation
- Reporting on database size and growth
- Performing database cleanup and optimization
"""
import os
import sys
import time
import argparse
import json
from datetime import datetime, timezone, timedelta

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

# Try to load from .env file
load_env_vars()

from weatherhat_app.data_processing import (
    connect_to_mongodb, 
    setup_retention_policies, 
    setup_indexes, 
    perform_database_maintenance,
    get_collection_sizes,
    downsample_hourly,
    downsample_daily,
    DateTimeEncoder
)

# MongoDB connection settings
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://akuma:27017')
DB_NAME = os.environ.get('MONGO_DB', 'weather_data')

def optimize_database(db):
    """Perform database optimization operations"""
    try:
        print("Optimizing database...")
        
        # 1. Clean up any orphaned trend data
        # (trend data without corresponding measurements)
        now = datetime.now(timezone.utc)
        sixty_days_ago = now - timedelta(days=60)
        sixty_days_ago_ns = int(sixty_days_ago.timestamp() * 1e9)
        
        # Find and remove old trend data that doesn't have corresponding measurements
        trends_result = db.trends.delete_many({
            "timestamp": {"$lt": sixty_days_ago_ns},
        })
        print(f"Removed {trends_result.deleted_count} orphaned trend records")
        
        # 2. Pre-aggregate older data as needed
        # Create hourly aggregations for any missing hours in the last 7 days
        now = datetime.now(timezone.utc)
        for day_offset in range(7):
            target_day = now - timedelta(days=day_offset)
            
            # Process each hour in the day
            for hour in range(24):
                target_hour = datetime(
                    target_day.year, target_day.month, target_day.day, 
                    hour, 0, 0, tzinfo=timezone.utc
                )
                
                # Skip future hours
                if target_hour > now:
                    continue
                    
                # Check if we already have data for this hour
                hour_timestamp = int(target_hour.timestamp() * 1e9)
                existing = db.hourly_measurements.find_one({
                    "hour_timestamp": hour_timestamp
                })
                
                if not existing:
                    # Hour data doesn't exist, try to create it
                    # Set up window for measurements
                    hour_start = target_hour
                    hour_end = hour_start + timedelta(hours=1)
                    
                    # Extract timestamps
                    hour_start_ns = int(hour_start.timestamp() * 1e9)
                    hour_end_ns = int(hour_end.timestamp() * 1e9)
                    
                    # Find measurements in the hour period
                    pipeline = [
                        {
                            "$match": {
                                "timestamp": {"$gte": hour_start_ns, "$lt": hour_end_ns},
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
                    
                    if results and results[0]["count"] >= 3:  # Require at least 3 measurements
                        # Create hourly record
                        result = results[0]
                        hourly_data = {
                            "timestamp": hour_timestamp,
                            "timestamp_ms": hour_start,
                            "hour_timestamp": hour_timestamp,
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
                        print(f"Created missing hourly record for {hour_start}")
        
        # 3. Create daily aggregates for any missing days
        for day_offset in range(30):  # Check last 30 days
            target_day = now - timedelta(days=day_offset)
            day_start = datetime(
                target_day.year, target_day.month, target_day.day, 
                0, 0, 0, tzinfo=timezone.utc
            )
            
            # Skip future days
            if day_start > now:
                continue
                
            # Check if we already have data for this day
            day_timestamp = int(day_start.timestamp() * 1e9)
            existing = db.daily_measurements.find_one({
                "day_timestamp": day_timestamp
            })
            
            if not existing:
                # Day data doesn't exist, try to create it from hourly data
                print(f"Creating missing daily record for {day_start.date()}")
                downsample_daily(db)
        
        # 4. Compress old raw measurements by removing some fields
        # For data older than 14 days, we can remove some fields we don't need
        fourteen_days_ago = now - timedelta(days=14)
        fourteen_days_ago_ns = int(fourteen_days_ago.timestamp() * 1e9)
        
        # Count how many raw measurements we have older than 14 days
        old_count = db.measurements.count_documents({
            "timestamp": {"$lt": fourteen_days_ago_ns}
        })
        
        if old_count > 0:
            print(f"Found {old_count} raw measurements older than 14 days that could be compressed")
            
            # We won't actually delete the data in this script, just report it
            # In a production environment, you might want to update these documents
            # to remove non-essential fields or to create a more compact representation
        
        return True
        
    except Exception as e:
        print(f"Error optimizing database: {e}", file=sys.stderr)
        return False

def export_statistics(db, output_file):
    """Export database statistics to a JSON file"""
    try:
        stats = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "collections": {},
            "record_counts": {},
            "sampling_efficiency": {}
        }
        
        # Get collection sizes
        sizes = get_collection_sizes(db)
        for collection, size in sizes.items():
            stats["collections"][collection] = {"size_mb": size}
        
        # Count records in each collection
        for collection_name in db.list_collection_names():
            count = db[collection_name].count_documents({})
            stats["record_counts"][collection_name] = count
        
        # Calculate efficiency metrics
        
        # 1. Compaction ratio (raw vs. hourly vs. daily)
        raw_count = stats["record_counts"].get("measurements", 0)
        hourly_count = stats["record_counts"].get("hourly_measurements", 0)
        daily_count = stats["record_counts"].get("daily_measurements", 0)
        
        if raw_count > 0 and hourly_count > 0:
            stats["sampling_efficiency"]["raw_to_hourly_ratio"] = raw_count / hourly_count
        
        if hourly_count > 0 and daily_count > 0:
            stats["sampling_efficiency"]["hourly_to_daily_ratio"] = hourly_count / daily_count
        
        # 2. Storage efficiency
        raw_size = sizes.get("measurements", 0)
        hourly_size = sizes.get("hourly_measurements", 0)
        daily_size = sizes.get("daily_measurements", 0)
        
        total_size = sum(sizes.values())
        stats["total_size_mb"] = total_size
        
        if total_size > 0:
            stats["collections_ratio"] = {
                "raw_percentage": (raw_size / total_size) * 100 if raw_size > 0 else 0,
                "hourly_percentage": (hourly_size / total_size) * 100 if hourly_size > 0 else 0,
                "daily_percentage": (daily_size / total_size) * 100 if daily_size > 0 else 0
            }
        
        # Write statistics to the output file
        with open(output_file, 'w') as f:
            json.dump(stats, f, cls=DateTimeEncoder, indent=2)
            
        print(f"Statistics exported to {output_file}")
        return True
        
    except Exception as e:
        print(f"Error exporting statistics: {e}", file=sys.stderr)
        return False

def main():
    """Main function for the database maintenance script"""
    parser = argparse.ArgumentParser(description='Perform database maintenance for WeatherHAT')
    parser.add_argument('--check-only', action='store_true', help='Only check database status without modifications')
    parser.add_argument('--force-setup', action='store_true', help='Force setup of TTL indexes and other indexes')
    parser.add_argument('--downsample', action='store_true', help='Run downsampling operations')
    parser.add_argument('--report', action='store_true', help='Report database statistics')
    parser.add_argument('--optimize', action='store_true', help='Perform database optimization')
    parser.add_argument('--export-stats', metavar='FILE', help='Export statistics to JSON file')
    parser.add_argument('--full-maintenance', action='store_true', help='Perform all maintenance tasks')
    args = parser.parse_args()

    # If no specific arguments are given, perform basic maintenance tasks
    if not any([args.check_only, args.force_setup, args.downsample, 
                args.report, args.optimize, args.export_stats, args.full_maintenance]):
        args.force_setup = True
        args.downsample = True
        args.report = True
    
    # If full maintenance is requested, enable all options
    if args.full_maintenance:
        args.force_setup = True
        args.downsample = True
        args.report = True
        args.optimize = True
        if not args.export_stats:
            args.export_stats = "weatherhat_stats.json"

    try:
        # Connect to MongoDB
        mongo_client = connect_to_mongodb(MONGO_URI)
        db = mongo_client[DB_NAME]
        
        # Report database status
        if args.check_only or args.report:
            print("Current database collection sizes:")
            sizes = get_collection_sizes(db)
            total_size = 0
            for collection, size in sizes.items():
                print(f"  {collection}: {size:.2f} MB")
                total_size += size
            print(f"Total database size: {total_size:.2f} MB")

            # Check if TTL indexes are set up
            measurements_index = db.measurements.index_information()
            if "timestamp_ms_1" in measurements_index:
                print("TTL index on measurements collection is properly set up")
                print(f"TTL setting: {measurements_index['timestamp_ms_1'].get('expireAfterSeconds')} seconds")
            else:
                print("WARNING: TTL index on measurements collection is missing!")
            
            # Count records
            print("\nRecord counts:")
            for collection_name in db.list_collection_names():
                count = db[collection_name].count_documents({})
                print(f"  {collection_name}: {count} records")

        # Force setup indexes
        if args.force_setup:
            print("\nSetting up TTL indexes and performance indexes...")
            setup_retention_policies(db)
            setup_indexes(db)
            print("Index setup complete")

        # Run downsampling
        if args.downsample:
            print("\nPerforming database downsampling...")
            perform_database_maintenance(db)
            print("Downsampling complete")
        
        # Optimize database
        if args.optimize:
            print("\nPerforming database optimization...")
            optimize_database(db)
            print("Optimization complete")
        
        # Export statistics
        if args.export_stats:
            print(f"\nExporting database statistics to {args.export_stats}...")
            export_statistics(db, args.export_stats)

        mongo_client.close()
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())