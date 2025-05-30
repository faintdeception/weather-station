#!/usr/bin/env python3
"""
Maintenance task tracker for short-lived application runs

This module replaces the long-running MaintenanceScheduler with a stateful
approach suitable for applications that run periodically (e.g., via Telegraf)
rather than continuously.
"""
import time
import sys
from datetime import datetime, timezone

from weatherhat_app.data_processing import (
    downsample_hourly, 
    downsample_daily, 
    perform_database_maintenance
)


class MaintenanceTracker:
    """Track and execute maintenance tasks during short application runs"""
    
    def __init__(self, db):
        self.db = db
        self.maintenance_collection = db['maintenance_status']
    
    def should_run_hourly_maintenance(self):
        """Check if hourly maintenance should run"""
        last_run = self.maintenance_collection.find_one({'task': 'hourly_downsample'})
        if not last_run:
            return True
        
        last_run_time = last_run.get('last_run', 0)
        current_time = time.time()
        
        # Run if more than 1 hour has passed
        return (current_time - last_run_time) >= 3600
    
    def should_run_daily_maintenance(self):
        """Check if daily maintenance should run"""
        last_run = self.maintenance_collection.find_one({'task': 'daily_maintenance'})
        if not last_run:
            return True
        
        last_run_time = last_run.get('last_run', 0)
        current_time = time.time()
        
        # Run if more than 24 hours has passed
        return (current_time - last_run_time) >= 86400
    
    def run_hourly_maintenance(self):
        """Execute hourly maintenance and update timestamp"""
        try:
            print("Running on-demand hourly maintenance", file=sys.stderr)
            result = downsample_hourly(self.db)
            
            # Update last run timestamp
            self.maintenance_collection.update_one(
                {'task': 'hourly_downsample'},
                {'$set': {
                    'last_run': time.time(),
                    'last_result': result,
                    'last_run_date': datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            return result
        except Exception as e:
            print(f"Error in hourly maintenance: {e}", file=sys.stderr)
            # Still update the timestamp to avoid repeated failures
            self.maintenance_collection.update_one(
                {'task': 'hourly_downsample'},
                {'$set': {
                    'last_run': time.time(),
                    'last_error': str(e),
                    'last_run_date': datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            return None
    
    def run_daily_maintenance(self):
        """Execute daily maintenance and update timestamp"""
        try:
            print("Running on-demand daily maintenance", file=sys.stderr)
            result = perform_database_maintenance(self.db)
            
            # Update last run timestamp
            self.maintenance_collection.update_one(
                {'task': 'daily_maintenance'},
                {'$set': {
                    'last_run': time.time(),
                    'last_result': result,
                    'last_run_date': datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            return result
        except Exception as e:
            print(f"Error in daily maintenance: {e}", file=sys.stderr)
            # Still update the timestamp to avoid repeated failures
            self.maintenance_collection.update_one(
                {'task': 'daily_maintenance'},
                {'$set': {
                    'last_run': time.time(),
                    'last_error': str(e),
                    'last_run_date': datetime.now(timezone.utc).isoformat()
                }},
                upsert=True
            )
            return None
    
    def check_and_run_maintenance(self):
        """Check and run any needed maintenance tasks"""
        tasks_run = []
        
        if self.should_run_hourly_maintenance():
            if self.run_hourly_maintenance() is not None:
                tasks_run.append('hourly')
        
        if self.should_run_daily_maintenance():
            if self.run_daily_maintenance() is not None:
                tasks_run.append('daily')
        
        return tasks_run
    
    def get_maintenance_status(self):
        """Get the status of maintenance tasks"""
        status = {}
        
        for task in ['hourly_downsample', 'daily_maintenance']:
            task_status = self.maintenance_collection.find_one({'task': task})
            if task_status:
                status[task] = {
                    'last_run': task_status.get('last_run', 0),
                    'last_run_date': task_status.get('last_run_date', 'Never'),
                    'last_result': task_status.get('last_result'),
                    'last_error': task_status.get('last_error')
                }
            else:
                status[task] = {
                    'last_run': 0,
                    'last_run_date': 'Never',
                    'last_result': None,
                    'last_error': None
                }
        
        return status
