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
    
    Based on the working WeatherHAT averaging.py example, we should use a single
    5-second interval update and check updated_wind_rain for valid wind/rain data.
    """
    readings = []
    
    # Take first reading and discard (warm-up)
    if discard_first:
        print("Taking initial warm-up reading (will be discarded)...", file=sys.stderr)
        sensor.update(interval=5.0)  # Use 5-second interval like working example
        time.sleep(1)  # Short delay after warm-up reading
    
    # Take readings using the same pattern as the working averaging.py example
    for i in range(num_readings):
        print(f"Taking sensor reading {i+1}/{num_readings}...", file=sys.stderr)
        
        # Update sensor with 5-second interval (like working example)
        sensor.update(interval=5.0)
        
        # Debug: Check if wind/rain data was updated
        wind_rain_updated = False
        if hasattr(sensor, 'updated_wind_rain'):
            wind_rain_updated = sensor.updated_wind_rain
            print(f"  Wind/rain updated this cycle: {wind_rain_updated}", file=sys.stderr)
        
        # Store the current values - use actual sensor readings
        # Important: Only use rain values when updated_wind_rain is True (like working example)
        # Wind direction seems to work continuously, so we'll keep using it always
        reading = {
            "device_temperature": float(sensor.device_temperature),
            "temperature": float(sensor.temperature),
            "humidity": float(sensor.humidity),
            "dewpoint": float(sensor.dewpoint),
            "lux": float(sensor.lux),
            "pressure": float(sensor.pressure),
            "wind_speed": float(sensor.wind_speed),  # Always use wind_speed
            "rain": float(sensor.rain) if wind_rain_updated else 0.0,  # Only use rain when updated
            "wind_direction": float(sensor.wind_direction)  # Always use (seems to work continuously)
        }
        readings.append(reading)
        
        # Print current values for debugging
        rain_status = "VALID" if wind_rain_updated else "STALE"
        print(f"  Reading {i+1}: Temp={sensor.temperature:.1f}°C, Wind={sensor.wind_speed:.2f}m/s, Rain={reading['rain']:.3f}mm/s ({rain_status}), Direction={sensor.wind_direction:.0f}°", file=sys.stderr)
        
        # Add delay between readings (like working example)
        if i < num_readings - 1:  # Don't sleep after last reading
            time.sleep(5.0)  # 5-second delay like working example
    
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
    Extract rain gauge reading from sensor data.
    
    NOTE: Based on working WeatherHAT examples, sensor.rain provides mm/sec rate,
    not cumulative tip counts as we originally thought. This function is kept
    for compatibility but may not be needed.
    
    Args:
        readings: List of reading dictionaries containing rain measurements
        accumulated_rain: Not used - kept for compatibility
        last_reset_time: Not used - kept for compatibility
    
    Returns:
        tuple: (current_rain_rate, current_timestamp)
    """
    if not readings:
        return 0, time.time()
    
    # Get the current rain rate (sensor.rain gives mm/sec, not tip counts)
    rain_rate_mm_sec = readings[-1]["rain"] if readings else 0
    current_time = time.time()
    
    # Log for debugging
    print(f"Rain rate from sensor: {rain_rate_mm_sec:.3f} mm/sec", file=sys.stderr)
    
    # Log all rain readings for debugging
    all_rain_readings = [r["rain"] for r in readings]
    print(f"All rain readings this cycle: {all_rain_readings}", file=sys.stderr)
    
    # Return the rain rate - this matches the working example format
    return rain_rate_mm_sec, current_time

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