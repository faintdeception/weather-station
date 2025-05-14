#!/usr/bin/env python3
"""
LLM Service for Weather Predictions

This module handles all interactions with the LLM API for generating
weather predictions based on collected weather data.
"""
import os
import sys
import json
import logging
import traceback
from datetime import datetime
import requests
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("llm-service")

def connect_to_mongodb(mongo_uri, max_retries=5, retry_interval=5):
    """Connect to MongoDB with retry logic"""
    retry_count = 0
    while retry_count < max_retries:
        try:
            logger.info(f"Connecting to MongoDB at {mongo_uri}")
            mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            # Force a connection to verify it works
            mongo_client.server_info()
            logger.info("Successfully connected to MongoDB")
            return mongo_client
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                raise Exception(f"Failed to connect to MongoDB after {max_retries} attempts: {e}")
            logger.warning(f"MongoDB connection attempt {retry_count} failed: {e}. Retrying in {retry_interval} seconds...")
            import time
            time.sleep(retry_interval)
    
    # Should never reach here due to exception in loop
    return None

def get_daily_report(db, date=None):
    """
    Retrieve a daily report from the database
    
    Args:
        db: MongoDB database connection
        date: Date string in YYYY-MM-DD format, defaults to yesterday
        
    Returns:
        The daily report document or None if not found
    """
    try:
        if date is None:
            # Default to yesterday
            from datetime import datetime, timedelta
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            date = yesterday
            
        report = db['daily_reports'].find_one({'date': date})
        return report
    except Exception as e:
        logger.error(f"Error retrieving daily report: {e}")
        traceback.print_exc()
        return None

def get_trend_data(db, location):
    """
    Retrieve the latest trend data for a location
    
    Args:
        db: MongoDB database connection
        location: Location name
        
    Returns:
        The trend data document or None if not found
    """
    try:
        trend_data = db['trends'].find_one(
            {'location': location},
            sort=[('timestamp', -1)]
        )
        return trend_data
    except Exception as e:
        logger.error(f"Error retrieving trend data: {e}")
        traceback.print_exc()
        return None

def call_prediction_api(weather_data):
    """
    Call an external LLM API to generate weather predictions
    
    Args:
        weather_data: Dictionary with weather summary data
        
    Returns:
        Dictionary with prediction results or None if failed
    """
    try:
        # Get API key from environment variable
        api_key = os.environ.get('LLM_API_KEY')
        if not api_key:
            logger.error("LLM_API_KEY environment variable not set")
            return None
        
        # Get LLM API URL from environment or use default
        api_url = os.environ.get('LLM_API_URL', 'https://api.openai.com/v1/chat/completions')
        
        # Get LLM model from environment or use default
        model_name = os.environ.get('LLM_MODEL', 'gpt-4')
        
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
        
        # Log the API request (without the key)
        logger.info(f"Calling LLM API: {api_url}")
        logger.debug(f"Prompt: {prompt}")
        
        # Call the LLM API
        response = requests.post(
            api_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": model_name,
                "messages": [
                    {"role": "system", "content": "You are a weather forecasting assistant that analyzes weather data and provides predictions in JSON format."},
                    {"role": "user", "content": prompt}
                ],
                "response_format": {"type": "json_object"}
            },
            timeout=60  # Add timeout to prevent hanging requests
        )
        
        # Parse the response
        if response.status_code == 200:
            response_data = response.json()
            prediction_text = response_data['choices'][0]['message']['content']
            
            # Parse the JSON response from the LLM
            prediction = json.loads(prediction_text)
            logger.info(f"Successfully received prediction: {json.dumps(prediction)[:100]}...")
            return prediction
        else:
            logger.error(f"API request failed with status code {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error in call_prediction_api: {e}")
        traceback.print_exc()
        return None

def generate_weather_prediction(db, report_data=None, date=None, force=False):
    """
    Generate weather predictions using LLM based on daily report data
    
    Args:
        db: MongoDB database connection
        report_data: Daily report data (optional, will be fetched if not provided)
        date: Date to generate prediction for (optional, defaults to yesterday)
        force: Whether to force regeneration of prediction even if one exists
        
    Returns:
        The prediction document or None if failed
    """
    try:
        predictions_collection = db['weather_predictions']
        
        # If no report data provided, fetch it
        if report_data is None:
            report_data = get_daily_report(db, date)
            
        if report_data is None:
            logger.error(f"No daily report found for date: {date}")
            return None
            
        # Check if we already have a prediction for this date
        existing_prediction = predictions_collection.find_one({
            'date': report_data['date']
        })
        
        if existing_prediction is not None and not force:
            logger.info(f"Prediction already exists for {report_data['date']}")
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
            
            # Insert or update the prediction
            if existing_prediction and force:
                predictions_collection.replace_one(
                    {"_id": existing_prediction["_id"]},
                    prediction_doc
                )
                logger.info(f"Updated weather prediction for {report_data['date']}")
            else:
                predictions_collection.insert_one(prediction_doc)
                logger.info(f"Stored new weather prediction for {report_data['date']}")
                
            return prediction_doc
        
        return None
    except Exception as e:
        logger.error(f"Error in generate_weather_prediction: {e}")
        traceback.print_exc()
        return None