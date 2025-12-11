"""
Extract step for Urban Air Quality Monitoring ETL.

- Calls Open-Meteo Air Quality API (no auth) for a list of Indian metro cities.
- Implements retry with exponential backoff (default 3 attempts).
- Saves each raw response to data/raw/<city>_raw_<timestamp>.json
- Returns a list of saved file paths.

Usage:
    from extract import fetch_all_cities
    saved = fetch_all_cities()  # returns list of dicts [{'city':'Delhi','raw_path':'...'}, ...]
    # Or run from CLI: python extract.py
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------
# Configuration (can be overridden via .env)
# ------------------------------------------------------------------
RAW_DIR = Path(os.getenv("RAW_DIR", Path(__file__).resolve().parents[0] / "data" / "raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "10"))
SLEEP_BETWEEN_CALLS = float(os.getenv("SLEEP_BETWEEN_CALLS", "0.5"))

# Open-Meteo API base (parameters added dynamically per city)
API_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Default Indian metro cities
CITY_COORDS = {
    "Delhi": (28.7041, 77.1025),
    "Mumbai": (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867),
    "Kolkata": (22.5726, 88.3639),
}

DEFAULT_CITIES = list(CITY_COORDS.keys())


# ------------------------------------------------------------------
# Utility: Timestamp for filenames
# ------------------------------------------------------------------
def _now_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


# ------------------------------------------------------------------
# Utility: Save raw payload
# ------------------------------------------------------------------
def _save_raw(payload: object, city: str) -> str:
    """
    Save JSON payload to RAW_DIR. Return absolute file path.
    Falls back to writing plain text if JSON serialization fails.
    """
    ts = _now_ts()
    filename = f"{city.replace(' ', '_').lower()}_raw_{ts}.json"
    path = RAW_DIR / filename

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    except Exception:
        # fallback to .txt
        fallback = RAW_DIR / f"{city.replace(' ', '_').lower()}_raw_{ts}.txt"
        with open(fallback, "w", encoding="utf-8") as f:
            f.write(repr(payload))
        return str(fallback.resolve())

    return str(path.resolve())


# ------------------------------------------------------------------
# API fetch for a single city with retry logic
# ------------------------------------------------------------------
def _fetch_city(city: str, max_retries: int = MAX_RETRIES, timeout: int = TIMEOUT_SECONDS) -> Dict[str, Optional[str]]:
    lat, lon = CITY_COORDS[city]

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,uv_index",
    }

    attempt = 0
    last_error = None

    while attempt < max_retries:
        attempt += 1
        try:
            resp = requests.get(API_BASE, params=params, timeout=timeout)
            resp.raise_for_status()

            try:
                payload = resp.json()
            except ValueError:
                payload = {"raw_text": resp.text}

            saved_path = _save_raw(payload, city)
            print(f"[{city}] fetched & saved: {saved_path}")
            return {"city": city, "success": "true", "raw_path": saved_path}

        except requests.RequestException as e:
            last_error = str(e)
            print(f"[{city}] attempt {attempt}/{max_retries} failed: {e}")

        # exponential backoff
        backoff = 2 ** (attempt - 1)
        print(f"[{city}] retrying in {backoff}s ...")
        time.sleep(backoff)

    # All retries failed
    print(f"[{city}] FAILED after {max_retries} attempts → {last_error}")
    return {"city": city, "success": "false", "error": last_error}


# ------------------------------------------------------------------
# Fetch data for all cities
# ------------------------------------------------------------------
def fetch_all_cities(cities: Optional[List[str]] = None) -> List[Dict[str, Optional[str]]]:
    if cities is None:
        cities = DEFAULT_CITIES

    results = []
    for city in cities:
        res = _fetch_city(city)
        results.append(res)
        time.sleep(SLEEP_BETWEEN_CALLS)

    return results


# ------------------------------------------------------------------
# CLI execution
# ------------------------------------------------------------------
if __name__ == "__main__":
    print("Starting Air Quality Extraction (Open-Meteo API)")
    print(f"Cities: {DEFAULT_CITIES}")

    result = fetch_all_cities(DEFAULT_CITIES)

    print("\nExtraction complete. Summary:")
    for r in result:
        if r.get("success") == "true":
            print(f"[OK] {r['city']} saved: {r['raw_path']}")
        else:
            print(f"[ERROR] {r['city']} failed: {r.get('error')}")

