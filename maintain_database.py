#!/usr/bin/env python3
"""
Database maintenance script for the WeatherHAT application

This script performs database maintenance tasks such as:
- Verifying TTL indexes are properly set up
- Running downsampling operations for hourly/daily aggregation
- Reporting on database size and growth
"""
import os
import sys
import time
import argparse

from weatherhat_app.data_processing import (
    connect_to_mongodb, 
    setup_retention_policies, 
    setup_indexes, 
    perform_database_maintenance,
    get_collection_sizes
)

# MongoDB connection settings
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://akuma:27017')
DB_NAME = os.environ.get('MONGO_DB', 'weather_data')

def main():
    """Main function for the database maintenance script"""
    parser = argparse.ArgumentParser(description='Perform database maintenance for WeatherHAT')
    parser.add_argument('--check-only', action='store_true', help='Only check database status without modifications')
    parser.add_argument('--force-setup', action='store_true', help='Force setup of TTL indexes and other indexes')
    parser.add_argument('--downsample', action='store_true', help='Run downsampling operations')
    parser.add_argument('--report', action='store_true', help='Report database statistics')
    args = parser.parse_args()

    # If no specific arguments are given, perform all maintenance tasks
    if not (args.check_only or args.force_setup or args.downsample or args.report):
        args.force_setup = True
        args.downsample = True
        args.report = True

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
            else:
                print("WARNING: TTL index on measurements collection is missing!")

        # Force setup indexes
        if args.force_setup:
            print("Setting up TTL indexes and performance indexes...")
            setup_retention_policies(db)
            setup_indexes(db)
            print("Index setup complete")

        # Run downsampling
        if args.downsample:
            print("Performing database maintenance...")
            perform_database_maintenance(db)
            print("Maintenance complete")

        mongo_client.close()
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())