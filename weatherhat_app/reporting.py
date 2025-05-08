#!/usr/bin/env python3
"""
Reporting and prediction functions for the WeatherHAT application
"""
import sys
import traceback
import json
import os
import time
import statistics
from datetime import datetime, timedelta
import requests

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
            
            # Generate weather prediction based on the report
            generate_weather_prediction(db, report)
            
            return report
        
        return None
    except Exception as e:
        print(f"Error in generate_daily_report: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None

def generate_weather_prediction(db, report_data):
    """Generate weather predictions using LLM based on daily report data"""
    try:
        predictions_collection = db['weather_predictions']
        
        # Check if we already have a prediction for this date
        existing_prediction = predictions_collection.find_one({
            'date': report_data['date']
        })
        
        if existing_prediction is not None:
            print(f"Prediction already exists for {report_data['date']}", file=sys.stderr)
            return existing_prediction
        
        # Prepare the data for sending to an LLM
        prompt_data = {
            "date": report_data['date'],
            "location": report_data['location'],
            "summary": report_data['summary'],
            "recent_trends": {}
        }
        
        # Get trend data for the last day
        trends_collection = db['trends']
        latest_trend = trends_collection.find_one(
            {'location': report_data['location']},
            sort=[('timestamp', -1)]
        )
        
        if latest_trend and 'trends' in latest_trend:
            prompt_data["recent_trends"] = latest_trend['trends']
        
        # Call the LLM API to generate predictions
        prediction_result = call_prediction_api(prompt_data)
        
        # Store the prediction
        if prediction_result:
            prediction_doc = {
                "date": report_data['date'],
                "location": report_data['location'],
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "prediction_12h": prediction_result.get('prediction_12h', {}),
                "prediction_24h": prediction_result.get('prediction_24h', {}),
                "reasoning": prediction_result.get('reasoning', ""),
                "confidence": prediction_result.get('confidence', 0.0)
            }
            
            # Insert the prediction
            predictions_collection.insert_one(prediction_doc)
            print(f"Stored weather prediction for {report_data['date']}", file=sys.stderr)
            return prediction_doc
        
        return None
    except Exception as e:
        print(f"Error in generate_weather_prediction: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None

def call_prediction_api(weather_data):
    """Call an external LLM API to generate weather predictions"""
    try:
        # Get API key from environment variable
        api_key = os.environ.get('LLM_API_KEY')
        if not api_key:
            print("LLM_API_KEY environment variable not set", file=sys.stderr)
            return None
        
        # Construct the prompt for the LLM
        prompt = f"""
Based on the following weather data from {weather_data['location']} on {weather_data['date']}, please provide:
1. A 12-hour weather prediction
2. A 24-hour weather prediction
3. Your reasoning based on the trends
4. A confidence score (0.0-1.0)

Current weather summary:
Temperature: Min {weather_data['summary']['temperature']['min']}°C, Max {weather_data['summary']['temperature']['max']}°C, Avg {weather_data['summary']['temperature']['avg']}°C
Humidity: Min {weather_data['summary']['humidity']['min']}%, Max {weather_data['summary']['humidity']['max']}%, Avg {weather_data['summary']['humidity']['avg']}%
Pressure: Min {weather_data['summary']['pressure']['min']} hPa, Max {weather_data['summary']['pressure']['max']} hPa, Avg {weather_data['summary']['pressure']['avg']} hPa
Wind Speed: Min {weather_data['summary']['wind_speed']['min']} mph, Max {weather_data['summary']['wind_speed']['max']} mph, Avg {weather_data['summary']['wind_speed']['avg']} mph
"""
        
        # Add trend data if available
        if weather_data.get('recent_trends'):
            prompt += "\nRecent trends:"
            for param, trends in weather_data['recent_trends'].items():
                prompt += f"\n{param.capitalize()}"
                for period, data in trends.items():
                    prompt += f"\n  {period}: Change {data.get('change', 0):.2f}, Rate {data.get('rate_per_hour', 0):.2f}/hour"
        
        prompt += "\n\nPlease format your response as JSON with keys: prediction_12h, prediction_24h, reasoning, confidence"
        
        # Call the LLM API
        # This is a placeholder - you'll need to replace with your specific LLM API call
        response = requests.post(
            "https://api.your-llm-provider.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "your-chosen-model",
                "messages": [
                    {"role": "system", "content": "You are a weather forecasting assistant that analyzes weather data and provides predictions in JSON format."},
                    {"role": "user", "content": prompt}
                ],
                "response_format": {"type": "json_object"}
            }
        )
        
        # Parse the response
        if response.status_code == 200:
            response_data = response.json()
            prediction_text = response_data['choices'][0]['message']['content']
            
            # Parse the JSON response from the LLM
            prediction = json.loads(prediction_text)
            return prediction
        else:
            print(f"API request failed with status code {response.status_code}: {response.text}", file=sys.stderr)
            return None
            
    except Exception as e:
        print(f"Error in call_prediction_api: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None