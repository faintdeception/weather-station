#!/usr/bin/env python3
"""
Utility functions for working with the WeatherHAT sensor
"""
import sys
import time
import math
from weatherhat import WeatherHAT
OFFSET = -13.5  # Offset for temperature calibration
def initialize_sensor():
    """Initialize the WeatherHAT sensor"""
    try:
        print("Initializing Weather HAT sensor...", file=sys.stderr)
        sensor = WeatherHAT()
        sensor.temperature_offset = OFFSET  # Set temperature offset
        print("Weather HAT sensor initialized", file=sys.stderr)
        return sensor
    except Exception as e:
        print(f"Error initializing Weather HAT sensor: {e}", file=sys.stderr)
        raise

def take_readings(sensor, num_readings=3, discard_first=True):
    """
    Take multiple readings from the sensor, optionally discarding the first one.
    Returns a list of reading dictionaries.
    
    Wind and rain measurements require longer intervals to be accurate, so we use
    a longer sampling interval for the first reading to get valid wind/rain data.
    """
    readings = []
    
    # Take first reading with a longer interval to get valid wind/rain measurements
    # Wind speed requires time to accumulate anemometer counts for accurate readings
    if discard_first:
        print("Taking initial warm-up reading (will be discarded)...", file=sys.stderr)
        sensor.update(interval=5.0)  # Longer interval for wind/rain accumulation
        time.sleep(1)  # Short delay after warm-up reading
    
    # Take the primary reading with appropriate interval for wind/rain measurements
    print("Taking primary sensor reading with wind/rain measurement...", file=sys.stderr)
    sensor.update(interval=5.0)  # 5-second interval for accurate wind speed
    
    # Store the primary reading
    primary_reading = {
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
    readings.append(primary_reading)
    
    # Log wind/rain update status for debugging
    if hasattr(sensor, 'updated_wind_rain'):
        print(f"  Wind/rain updated: {sensor.updated_wind_rain}", file=sys.stderr)
    
    print(f"  Primary reading: Wind={sensor.wind_speed:.2f}m/s, Rain={sensor.rain:.2f}mm", file=sys.stderr)
    
    # Take additional readings for temperature/pressure/humidity averaging
    # These don't need long intervals and we reuse wind/rain from the primary reading
    for i in range(num_readings - 1):
        print(f"Taking supplementary reading {i+2}/{num_readings}...", file=sys.stderr)
        sensor.update(interval=1.0)  # Short interval for temperature/pressure/humidity only
        
        # Store reading but reuse wind/rain values from primary reading
        reading = {
            "device_temperature": float(sensor.device_temperature),
            "temperature": float(sensor.temperature),
            "humidity": float(sensor.humidity),
            "dewpoint": float(sensor.dewpoint),
            "lux": float(sensor.lux),
            "pressure": float(sensor.pressure),
            "wind_speed": primary_reading["wind_speed"],  # Reuse from primary reading
            "rain": primary_reading["rain"],              # Reuse from primary reading
            "wind_direction": primary_reading["wind_direction"]  # Reuse from primary reading
        }
        readings.append(reading)
        
        # Print current values for debugging
        print(f"  Reading {i+2}: Temp={sensor.temperature:.1f}°C, Humidity={sensor.humidity:.1f}%", file=sys.stderr)
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

def accumulate_rainfall(readings, accumulated_rain=0, last_reset_time=None):
    """
    Simple function to extract rain gauge reading from sensor data.
    
    The rain gauge provides a cumulative count of bucket tips since device startup.
    The main application handles the difference calculation and accumulation logic.
    
    Args:
        readings: List of reading dictionaries containing rain measurements
        accumulated_rain: Not used - kept for compatibility
        last_reset_time: Not used - kept for compatibility
    
    Returns:
        tuple: (current_rain_count, current_timestamp)
    """
    if not readings:
        return 0, time.time()
    
    # Get the current rain count (use the last reading since it's most recent)
    raw_rain_count = readings[-1]["rain"] if readings else 0
    
    # Round to nearest whole number since rain gauge should provide integer counts
    # We're seeing fractional values which suggests a WeatherHAT library issue
    current_rain_count = round(raw_rain_count)
    current_time = time.time()
    
    # Enhanced logging to diagnose rain gauge issues
    print(f"Raw rain gauge reading: {raw_rain_count} tips (rounded to {current_rain_count})", file=sys.stderr)
    
    # Check if rain gauge appears to be stuck at zero
    if raw_rain_count == 0.0:
        print(f"WARNING: Rain gauge reading exactly 0.0 - possible hardware reset or sensor issue", file=sys.stderr)
    
    # Log all raw readings for debugging
    all_rain_readings = [r["rain"] for r in readings]
    print(f"All rain readings this cycle: {all_rain_readings}", file=sys.stderr)
    
    # Return the rounded count - main application handles the rest
    return current_rain_count, current_time

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