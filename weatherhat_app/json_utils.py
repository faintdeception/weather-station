#!/usr/bin/env python3
"""
JSON utilities for the WeatherHAT application
"""
import json
from datetime import datetime

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            # Convert datetime to ISO format string
            return obj.isoformat()
        return super().default(obj)