# Weather HAT MongoDB Logger

This project records temperature and other weather data from a Weather HAT sensor into MongoDB, tracking all-time highs and lows.

## Setup

1. Make sure you have Docker and Docker Compose installed
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Starting the MongoDB Container

Start the MongoDB container using Docker Compose:

```bash
docker-compose up -d
```

This will start MongoDB in the background, accessible at localhost:27017.

## Running the Weather Logger

Run the script to start collecting and recording data:

```bash
python weatherhat-telegraf.py
```

## Environment Variables

You can customize the behavior using environment variables:

- `MONGO_URI`: MongoDB connection string (default: mongodb://localhost:27017)
- `MONGO_DB`: Database name (default: weather_data)
- `STARTUP_DELAY`: Delay in seconds before connecting to MongoDB (useful when starting containers together)

## Data Structure

The application stores data in two collections:

1. `measurements`: All raw measurements with timestamps
2. `records`: All-time high and low records for each measurement type

## Viewing Records

You can connect to MongoDB and view your records using:

```bash
docker exec -it weather-mongodb mongosh weather_data
```

Then query the records:

```javascript
db.records.find()  // View all records
db.records.find({"record_type": "highest"})  // View highest records only
db.records.find({"record_type": "lowest"})   // View lowest records only
```