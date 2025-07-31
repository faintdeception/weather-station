# WeatherHAT Long-Running Service

This project has been updated to use a long-running service instead of Telegraf for more reliable sensor readings, especially for rain detection which requires persistent state.

## Service Architecture

The WeatherHAT service (`weatherhat_service.py`) maintains continuous sensor state for proper rain accumulation detection. Unlike the previous Telegraf approach that restarted the Python process every minute, this service runs continuously and maintains the internal sensor state required for accurate rain measurements.

## Deployment

### Quick Deploy
```bash
# Pull latest changes
git pull origin main

# Make deploy script executable
chmod +x deploy_service.sh

# Run deployment (stops Telegraf, installs and starts WeatherHAT service)
./deploy_service.sh
```

### Manual Deployment

1. Stop Telegraf:
   ```bash
   sudo systemctl stop telegraf
   sudo systemctl disable telegraf  # Optional: prevent auto-start
   ```

2. Install the service:
   ```bash
   sudo cp systemd/weatherhat.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable weatherhat
   sudo systemctl start weatherhat
   ```

3. Check status:
   ```bash
   sudo systemctl status weatherhat
   sudo journalctl -u weatherhat -f
   ```

## Configuration

Environment variables can be set in `/home/pi/weather-station/.env`:
```bash
WEATHER_LOCATION=backyard
WEATHER_INTERVAL=60
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB=weather_data
```

## Service Management

- **View logs**: `sudo journalctl -u weatherhat -f`
- **Check status**: `sudo systemctl status weatherhat`
- **Restart**: `sudo systemctl restart weatherhat`
- **Stop**: `sudo systemctl stop weatherhat`

## Why the Change?

The WeatherHAT library maintains internal state for rain detection - it tracks the time between measurements to calculate rain rate. When Telegraf restarts the Python process every minute, this state is lost, causing rain readings to always be zero. The long-running service maintains this state continuously for accurate rain measurements.

## Monitoring

The service logs all measurements and errors to the systemd journal. Use `journalctl` to monitor:

```bash
# Follow logs in real-time
sudo journalctl -u weatherhat -f

# View recent logs
sudo journalctl -u weatherhat -n 50

# View logs from today
sudo journalctl -u weatherhat --since today
```
