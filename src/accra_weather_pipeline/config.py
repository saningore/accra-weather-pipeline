"""Settings in one place. Values come from the environment or .env, with safe defaults."""

import os

from dotenv import load_dotenv

load_dotenv()  # Reads .env from the folder you run the command in, if it exists.

DATABASE = os.getenv("DATABASE_URL") or "data/pipeline.sqlite3"
BRONZE_DIR = os.getenv("BRONZE_DIR") or "data/bronze"

# Latitude and longitude for each place we collect.
LOCATIONS = {
    "accra": (5.56, -0.20),
    "kumasi": (6.69, -1.62),
}
FIRST_LOAD_DAYS = 30  # How far back the very first run reaches.
OVERLAP_DAYS = 3  # Later runs re-read this many recent days, because the source revises them.
