#!/bin/bash

# WeatherHAT Service Deployment Script
# Usage: ./deploy_service.sh

set -e

SERVICE_NAME="weatherhat"
SERVICE_FILE="systemd/${SERVICE_NAME}.service"
REPO_DIR="/home/pi/weather-station"
USER="pi"

echo "Starting WeatherHAT service deployment..."

# Check if running as root
if [ "$EUID" -eq 0 ]; then
    echo "This script should not be run as root. Please run as user 'pi'."
    exit 1
fi

# Stop Telegraf if running
echo "Stopping Telegraf service if running..."
sudo systemctl stop telegraf || echo "Telegraf was not running"

# Stop existing weatherhat service if running
echo "Stopping existing weatherhat service if running..."
sudo systemctl stop $SERVICE_NAME || echo "WeatherHAT service was not running"

# Install the systemd service file
echo "Installing systemd service file..."
sudo cp "$SERVICE_FILE" "/etc/systemd/system/$SERVICE_NAME.service"

# Reload systemd daemon
echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

# Set proper permissions
echo "Setting file permissions..."
sudo chown root:root "/etc/systemd/system/$SERVICE_NAME.service"
sudo chmod 644 "/etc/systemd/system/$SERVICE_NAME.service"

# Make sure the Python script is executable
chmod +x "$REPO_DIR/weatherhat_service.py"

# Enable the service to start on boot
echo "Enabling service to start on boot..."
sudo systemctl enable $SERVICE_NAME

# Start the service
echo "Starting WeatherHAT service..."
sudo systemctl start $SERVICE_NAME

# Show service status
echo "Service status:"
sudo systemctl status $SERVICE_NAME --no-pager

echo ""
echo "Deployment complete!"
echo ""
echo "Useful commands:"
echo "  View logs: sudo journalctl -u $SERVICE_NAME -f"
echo "  Check status: sudo systemctl status $SERVICE_NAME"
echo "  Stop service: sudo systemctl stop $SERVICE_NAME"
echo "  Start service: sudo systemctl start $SERVICE_NAME"
echo "  Restart service: sudo systemctl restart $SERVICE_NAME"
echo "  Disable on boot: sudo systemctl disable $SERVICE_NAME"
