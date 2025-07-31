#!/bin/bash

# WeatherHAT Service Deployment Script
# Usage: ./deploy_service.sh

set -e

SERVICE_NAME="weatherhat"
SERVICE_FILE="systemd/${SERVICE_NAME}.service"
REPO_DIR="$(pwd)"
USER="$(whoami)"

echo "Starting WeatherHAT service deployment..."
echo "Repository directory: $REPO_DIR"
echo "Running as user: $USER"

# Detect Python environment
PYTHON_PATH=""
CONDA_ENV=""

if [ -n "$CONDA_DEFAULT_ENV" ]; then
    echo "Detected conda environment: $CONDA_DEFAULT_ENV"
    CONDA_ENV="$CONDA_DEFAULT_ENV"
    PYTHON_PATH="$(which python)"
    echo "Using Python from: $PYTHON_PATH"
elif command -v conda &> /dev/null; then
    echo "Conda available but no active environment detected"
    echo "Please activate your pimoroni environment first:"
    echo "  conda activate pimoroni"
    echo "  ./deploy_service.sh"
    exit 1
else
    echo "No conda detected, using system Python"
    PYTHON_PATH="/usr/bin/python3"
fi

# Check if running as root
if [ "$EUID" -eq 0 ]; then
    echo "This script should not be run as root. Please run as your regular user."
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
TEMP_SERVICE_FILE="/tmp/${SERVICE_NAME}.service"
HOME_DIR="$(eval echo ~$USER)"

# Create service file with correct paths
sed -e "s|REPLACE_USER|$USER|g" \
    -e "s|REPLACE_REPO_DIR|$REPO_DIR|g" \
    -e "s|REPLACE_HOME|$HOME_DIR|g" \
    -e "s|REPLACE_PYTHON|$PYTHON_PATH|g" \
    -e "s|REPLACE_CONDA_ENV|$CONDA_ENV|g" \
    "$SERVICE_FILE" > "$TEMP_SERVICE_FILE"

sudo cp "$TEMP_SERVICE_FILE" "/etc/systemd/system/$SERVICE_NAME.service"
rm "$TEMP_SERVICE_FILE"

# Reload systemd daemon
echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

# Set proper permissions
echo "Setting file permissions..."
sudo chown root:root "/etc/systemd/system/$SERVICE_NAME.service"
sudo chmod 644 "/etc/systemd/system/$SERVICE_NAME.service"

# Make sure the Python script is executable
echo "Making Python script executable..."
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
