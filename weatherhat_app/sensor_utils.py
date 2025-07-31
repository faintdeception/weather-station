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
    
    Based on WeatherHAT examples, wind and rain are measured over intervals and should
    use a sufficient interval for accurate readings. We'll take one primary reading
    with appropriate interval, then additional readings for temperature averaging.
    """
    readings = []
    
    # Take first reading and discard (warm-up) - use longer interval to ensure wind/rain update
    if discard_first:
        print("Taking initial warm-up reading (will be discarded)...", file=sys.stderr)
        sensor.update(interval=5.0)  # Use 5-second interval as per WeatherHAT examples
        time.sleep(1)  # Short delay after warm-up reading
    
    # Take primary reading with sufficient interval for wind/rain measurements
    print("Taking primary sensor reading with 5-second interval for wind/rain...", file=sys.stderr)
    sensor.update(interval=5.0)  # 5-second interval for accurate wind/rain measurements
    
    # Debug: Check if wind/rain data was updated
    if hasattr(sensor, 'updated_wind_rain'):
        print(f"  Wind/rain updated this cycle: {sensor.updated_wind_rain}", file=sys.stderr)
    
    # Store the primary reading with actual wind/rain values
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
    
    print(f"  Primary reading: Temp={sensor.temperature:.1f}°C, Wind={sensor.wind_speed:.2f}m/s, Rain={sensor.rain:.2f}mm", file=sys.stderr)
    
    # Take additional readings for averaging other measurements (but keep wind/rain from primary)
    for i in range(num_readings - 1):
        print(f"Taking supplementary reading {i+2}/{num_readings}...", file=sys.stderr)
        sensor.update(interval=1.0)  # Short interval for other measurements
        
        # Store reading - use current values for everything except wind/rain
        reading = {
            "device_temperature": float(sensor.device_temperature),
            "temperature": float(sensor.temperature),
            "humidity": float(sensor.humidity),
            "dewpoint": float(sensor.dewpoint),
            "lux": float(sensor.lux),
            "pressure": float(sensor.pressure),
            "wind_speed": primary_reading["wind_speed"],      # Use primary reading
            "rain": primary_reading["rain"],                  # Use primary reading  
            "wind_direction": primary_reading["wind_direction"]  # Use primary reading
        }
        readings.append(reading)
        
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
    
    # DON'T round - preserve the exact value from the sensor
    # The WeatherHAT library may use fractional values for its calculations
    current_rain_count = raw_rain_count
    current_time = time.time()
    
    # Enhanced logging to diagnose rain gauge issues
    print(f"Rain gauge reading: {raw_rain_count} (preserving exact value)", file=sys.stderr)
    
    # Check if rain gauge appears to be stuck at zero
    if raw_rain_count == 0.0:
        print(f"INFO: Rain gauge reading 0.0 - no rain detected or possible hardware reset", file=sys.stderr)
    
    # Log all raw readings for debugging
    all_rain_readings = [r["rain"] for r in readings]
    print(f"All rain readings this cycle: {all_rain_readings}", file=sys.stderr)
    
    # Return the exact count - main application handles the rest
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