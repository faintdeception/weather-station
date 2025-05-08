#!/usr/bin/env python3
"""
Utility functions for working with the WeatherHAT sensor
"""
import sys
import time
import math
from weatherhat import WeatherHAT

def initialize_sensor():
    """Initialize the WeatherHAT sensor"""
    try:
        print("Initializing Weather HAT sensor...", file=sys.stderr)
        sensor = WeatherHAT()
        print("Weather HAT sensor initialized", file=sys.stderr)
        return sensor
    except Exception as e:
        print(f"Error initializing Weather HAT sensor: {e}", file=sys.stderr)
        raise

def take_readings(sensor, num_readings=3, discard_first=True):
    """
    Take multiple readings from the sensor, optionally discarding the first one.
    Returns a list of reading dictionaries.
    """
    readings = []
    
    # Take first reading and discard (warm-up)
    if discard_first:
        print("Taking initial warm-up reading (will be discarded)...", file=sys.stderr)
        sensor.update(interval=1.0)
        time.sleep(1)  # Short delay after warm-up reading
    
    # Take valid readings
    for i in range(num_readings):
        print(f"Taking sensor reading {i+1}/{num_readings}...", file=sys.stderr)
        sensor.update(interval=1.0)
        
        # Store the current values
        reading = {
            "device_temperature": float(sensor.device_temperature),
            "temperature": float(sensor.temperature),
            "humidity": float(sensor.humidity),
            "dewpoint": float(sensor.dewpoint),
            "lux": float(sensor.lux),
            "pressure": float(sensor.pressure),
            "wind_speed": float(sensor.wind_speed),
            "rain": float(sensor.rain),
            "wind_direction": float(sensor.wind_direction)
        }
        readings.append(reading)
        
        # Print current values for debugging
        print(f"  Reading {i+1}: Temp={sensor.temperature}°C, Humidity={sensor.humidity}%", file=sys.stderr)
        time.sleep(1)  # Short delay between readings
    
    return readings

def calculate_average_readings(readings):
    """
    Calculate the average values from multiple sensor readings,
    handling wind direction as a special case with circular averaging.
    """
    if not readings:
        return {}
    
    # Calculate averages for all fields except wind_direction
    avg_fields = {}
    for field in readings[0].keys():
        if field != "wind_direction":
            avg_fields[field] = sum(r[field] for r in readings) / len(readings)
    
    # Handle wind direction separately (circular average)
    sin_sum = sum(math.sin(math.radians(r["wind_direction"])) for r in readings)
    cos_sum = sum(math.cos(math.radians(r["wind_direction"])) for r in readings)
    avg_direction = math.degrees(math.atan2(sin_sum, cos_sum))
    if avg_direction < 0:
        avg_direction += 360
    avg_fields["wind_direction"] = avg_direction
    
    return avg_fields

def cleanup_sensor(sensor):
    """Clean up the sensor resources"""
    if sensor:
        try:
            # Explicitly stop the background thread to allow clean exit
            sensor._polling = False
            if hasattr(sensor, '_poll_thread'):
                sensor._poll_thread.join()
            if hasattr(sensor, '_i2c_dev'):
                sensor._i2c_dev.close()
        except Exception:
            pass