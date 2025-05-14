#!/usr/bin/env python3
"""
Reporting functions for the WeatherHAT application
"""
import sys
import traceback
import statistics
from datetime import datetime, timedelta

def generate_daily_report(db):
    """Generate and store a daily weather report"""
    try:
        reports_collection = db['daily_reports']
        measurements_collection = db['measurements']
        
        # Get the current date and yesterday's date
        now = datetime.now()
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)
        
        # Convert to timestamps in nanoseconds
        yesterday_timestamp = int(yesterday.timestamp() * 1e9)
        today_timestamp = int(today.timestamp() * 1e9)
        
        # Check if we already have a report for yesterday
        existing_report = reports_collection.find_one({
            'date': yesterday.strftime('%Y-%m-%d')
        })
        
        # Only generate if we don't have a report yet and it's at least 1 AM
        if existing_report is None and now.hour >= 1:
            # Query for all measurements from yesterday
            daily_data = list(measurements_collection.find(
                {
                    'timestamp': {'$gte': yesterday_timestamp, '$lt': today_timestamp}
                }
            ).sort('timestamp', 1))
            
            # Only create report if we have data
            if not daily_data:
                print(f"No data available for daily report on {yesterday.strftime('%Y-%m-%d')}", file=sys.stderr)
                return None
            
            # Extract fields we want to analyze
            report_fields = ['temperature', 'humidity', 'pressure', 'wind_speed', 'rain', 'lux']
            location = daily_data[0].get('tags', {}).get('location', 'unknown')
            
            # Initialize report structure
            report = {
                "date": yesterday.strftime('%Y-%m-%d'),
                "location": location,
                "data_points": len(daily_data),
                "summary": {},
                "hourly": {}
            }
            
            # Process each field
            for field in report_fields:
                field_values = [entry.get('fields', {}).get(field) for entry in daily_data 
                                if field in entry.get('fields', {})]
                
                if field_values:
                    report["summary"][field] = {
                        "min": min(field_values),
                        "max": max(field_values),
                        "avg": statistics.mean(field_values),
                        "median": statistics.median(field_values) if len(field_values) > 0 else None
                    }
            
            # Calculate hourly averages
            for hour in range(24):
                hour_start = yesterday.replace(hour=hour, minute=0, second=0, microsecond=0)
                hour_end = hour_start + timedelta(hours=1)
                
                hour_start_ts = int(hour_start.timestamp() * 1e9)
                hour_end_ts = int(hour_end.timestamp() * 1e9)
                
                # Get data for this hour
                hour_data = [entry for entry in daily_data 
                             if entry['timestamp'] >= hour_start_ts and entry['timestamp'] < hour_end_ts]
                
                # Initialize hourly entry
                report["hourly"][str(hour)] = {"data_points": len(hour_data)}
                
                # Calculate hourly stats for each field
                for field in report_fields:
                    hour_field_values = [entry.get('fields', {}).get(field) for entry in hour_data 
                                        if field in entry.get('fields', {})]
                    
                    if hour_field_values:
                        report["hourly"][str(hour)][field] = {
                            "min": min(hour_field_values),
                            "max": max(hour_field_values),
                            "avg": statistics.mean(hour_field_values)
                        }
            
            # Store the report
            reports_collection.insert_one(report)
            print(f"Generated daily report for {yesterday.strftime('%Y-%m-%d')}", file=sys.stderr)
            
            # Note: LLM prediction functionality has been moved to a standalone microservice
            # The microservice will handle retrieving this report and generating predictions
            
            return report
        
        return None
    except Exception as e:
        print(f"Error in generate_daily_report: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None