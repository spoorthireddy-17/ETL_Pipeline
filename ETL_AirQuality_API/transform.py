"""
Transform step for Urban Air Quality Monitoring ETL.

- Reads raw JSON files saved by extract.py
- Flattens hourly pollutant data into tabular format (1 row per hour per city)
- Computes derived features:
      * AQI Category (based on PM2.5)
      * Pollution Severity Score
      * Risk Classification
      * Hour of Day
- Saves final cleaned dataset to data/staged/air_quality_transformed.csv
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Dict

import pandas as pd
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR / "data" / "raw"
STAGED_DIR = BASE_DIR / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = STAGED_DIR / "air_quality_transformed.csv"


# ---------------------------------------------------------
# AQI Mapping
# ---------------------------------------------------------
def classify_aqi(pm25: float) -> str:
    if pm25 <= 50:
        return "Good"
    elif pm25 <= 100:
        return "Moderate"
    elif pm25 <= 200:
        return "Unhealthy"
    elif pm25 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"


# ---------------------------------------------------------
# Risk Classification
# ---------------------------------------------------------
def classify_risk(score: float) -> str:
    if score > 400:
        return "High Risk"
    elif score > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"


# ---------------------------------------------------------
# Read & flatten a single JSON file
# ---------------------------------------------------------
def process_file(filepath: Path) -> pd.DataFrame:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    # infer city name from filename
    city = filepath.name.split("_raw_")[0].replace("_", " ").title()

    # Open-Meteo stores times and pollutant arrays in hourly structure
    hourly = data.get("hourly", {})
    times = hourly.get("time")

    if not times:
        return pd.DataFrame()  # skip corrupted files

    df = pd.DataFrame({
        "city": city,
        "time": pd.to_datetime(times),
        "pm10": hourly.get("pm10"),
        "pm2_5": hourly.get("pm2_5"),
        "carbon_monoxide": hourly.get("carbon_monoxide"),
        "nitrogen_dioxide": hourly.get("nitrogen_dioxide"),
        "sulphur_dioxide": hourly.get("sulphur_dioxide"),
        "ozone": hourly.get("ozone"),
        "uv_index": hourly.get("uv_index"),
    })

    return df


# ---------------------------------------------------------
# Main Transform Function
# ---------------------------------------------------------
def transform_all():
    print("🔄 Transform step started...")

    all_files = RAW_DIR.glob("*.json")
    frames = []

    for file in all_files:
        print(f"Processing → {file}")
        df = process_file(file)
        if not df.empty:
            frames.append(df)

    if not frames:
        print("❌ No valid JSON files found in raw folder.")
        return

    df = pd.concat(frames, ignore_index=True)

    # -----------------------------------------------------
    # Convert pollutant columns to numeric
    # -----------------------------------------------------
    pollutant_cols = [
        "pm10", "pm2_5", "carbon_monoxide",
        "nitrogen_dioxide", "sulphur_dioxide",
        "ozone", "uv_index"
    ]

    for col in pollutant_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # -----------------------------------------------------
    # Remove rows where ALL pollutant values are missing
    # -----------------------------------------------------
    df = df.dropna(subset=pollutant_cols, how="all")

    # -----------------------------------------------------
    # Derived Features
    # -----------------------------------------------------
    df["aqi_category"] = df["pm2_5"].apply(lambda x: classify_aqi(x) if pd.notna(x) else None)

    df["severity_score"] = (
        (df["pm2_5"] * 5) +
        (df["pm10"] * 3) +
        (df["nitrogen_dioxide"] * 4) +
        (df["sulphur_dioxide"] * 4) +
        (df["carbon_monoxide"] * 2) +
        (df["ozone"] * 3)
    )

    df["risk_flag"] = df["severity_score"].apply(classify_risk)

    # Hour-of-day feature
    df["hour"] = df["time"].dt.hour

    # -----------------------------------------------------
    # Save output
    # -----------------------------------------------------
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Transform complete → Saved to {OUTPUT_FILE}")


# ---------------------------------------------------------
# Run as script
# ---------------------------------------------------------
if __name__ == "__main__":
    transform_all()
