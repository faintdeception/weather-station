#!/usr/bin/env python3
"""
WeatherHAT Telegraf Integration Script

This script collects data from the Pimoroni Weather HAT, processes it,
and outputs it in a format suitable for Telegraf to ingest.

The script is designed to be run by Telegraf's exec plugin at regular intervals.
"""
import sys
from weatherhat_app.main import run

if __name__ == "__main__":
    sys.exit(run())