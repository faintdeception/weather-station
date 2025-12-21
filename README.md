# WeatherHAT Service

Long-running WeatherHAT sensor service that writes measurements and records to MongoDB. Uses systemd for supervision and keeps the WeatherHAT library state alive for accurate rain readings.

## Prerequisites (Pi)

- Raspberry Pi OS with I2C enabled (Preferences → Raspberry Pi Configuration → Interfaces → I2C → Enable → reboot, or `sudo raspi-config` → Interface Options → I2C).
- Python 3 with `pip` available.
- Access to MongoDB (local or remote) and network connectivity to it.
- Optional but recommended: virtual environment for Python packages.

## Setup

```bash
git clone <repo> weatherhat
cd weatherhat

# Create and activate a venv
python3 -m venv env
source env/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Configure environment (edit .env in repo root)
cat > .env <<'EOF'
MONGO_URI=mongodb://localhost:27017
MONGO_DB=weather_data
WEATHER_INTERVAL=60
WEATHER_LOCATION=backyard
EOF
```

## Deploy the systemd service (recommended)

The deploy helper sets paths and installs the service under your current user.

```bash
chmod +x deploy_service.sh
./deploy_service.sh
```

What it does:
- Verifies `weatherhat` is importable in the active Python.
- Stops any running Telegraf instance.
- Renders and installs [systemd/weatherhat.service](systemd/weatherhat.service) with the correct user, paths, and interpreter.
- Enables and starts the `weatherhat` service.

## Service management

- Status: `sudo systemctl status weatherhat`
- Logs (follow): `sudo journalctl -u weatherhat -f`
- Restart: `sudo systemctl restart weatherhat`
- Stop: `sudo systemctl stop weatherhat`
- Disable on boot: `sudo systemctl disable weatherhat`

## Troubleshooting

- Missing `/dev/i2c-1`: ensure I2C is enabled and the WeatherHAT is seated. On Bookworm, check `/boot/firmware/config.txt` for `dtparam=i2c_arm=on` and reboot.
- Mongo unreachable: verify `MONGO_URI` in `.env` and network/firewall settings.
- Wrong Python environment: activate the venv then rerun [deploy_service.sh](deploy_service.sh) so the service points at the correct interpreter.

## Data

- MongoDB collections: `measurements` (all readings) and `records` (highs/lows).