#!/usr/bin/env python3
"""
Scheduler for periodic database maintenance tasks
"""
import time
import sys
import threading
import traceback
from datetime import datetime, timedelta, timezone

from weatherhat_app.data_processing import perform_database_maintenance, downsample_hourly

class MaintenanceScheduler:
    """Scheduler for database maintenance tasks"""
    
    def __init__(self, db, hourly_interval=3600, daily_interval=86400):
        """Initialize the scheduler"""
        self.db = db
        self.hourly_interval = hourly_interval  # seconds between hourly jobs
        self.daily_interval = daily_interval    # seconds between daily jobs
        self.running = False
        self.thread = None
    
    def start(self):
        """Start the scheduler thread"""
        if self.thread and self.thread.is_alive():
            print("Scheduler already running", file=sys.stderr)
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._scheduler_loop)
        self.thread.daemon = True
        self.thread.start()
        print("Maintenance scheduler started", file=sys.stderr)
    
    def stop(self):
        """Stop the scheduler thread"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)
        print("Maintenance scheduler stopped", file=sys.stderr)
    
    def _scheduler_loop(self):
        """Main scheduler loop"""
        last_hourly = 0
        last_daily = 0
        
        while self.running:
            current_time = time.time()
            
            # Check if it's time for hourly maintenance
            if current_time - last_hourly >= self.hourly_interval:
                try:
                    print("Running hourly maintenance task", file=sys.stderr)
                    # Run hourly downsampling
                    downsample_hourly(self.db)
                    last_hourly = current_time
                except Exception as e:
                    print(f"Error in hourly maintenance: {e}", file=sys.stderr)
                    traceback.print_exc(file=sys.stderr)
            
            # Check if it's time for daily maintenance
            if current_time - last_daily >= self.daily_interval:
                try:
                    print("Running daily maintenance task", file=sys.stderr)
                    # Run full database maintenance
                    perform_database_maintenance(self.db)
                    last_daily = current_time
                except Exception as e:
                    print(f"Error in daily maintenance: {e}", file=sys.stderr)
                    traceback.print_exc(file=sys.stderr)
            
            # Sleep for a bit to avoid busy-waiting
            time.sleep(60)