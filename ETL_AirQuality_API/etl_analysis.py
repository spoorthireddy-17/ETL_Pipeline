"""
Analysis step for Air Quality ETL Pipeline.

Tasks:
A. KPI Metrics
B. City Pollution Trend Report
C. Export processed CSVs
D. Visualizations (PNG)

Inputs: Data loaded in Supabase table `air_quality_data`
Outputs stored in: data/processed/
"""

import os
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "air_quality_data"

# Output directories
BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def fetch_data():
    print("📥 Fetching data from Supabase...")
    rows = supabase.table(TABLE_NAME).select("*").execute()

    if not rows.data:
        raise SystemExit("❌ No data found in Supabase. Load step incomplete.")

    df = pd.DataFrame(rows.data)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    print(f"📊 Retrieved {len(df)} records from Supabase")

    return df

def compute_kpis(df: pd.DataFrame):
    print("📌 Computing KPI metrics...")

    highest_pm_city = df.groupby("city")["pm2_5"].mean().idxmax()
    highest_severity_city = df.groupby("city")["severity_score"].mean().idxmax()

    risk_percentages = df["risk_flag"].value_counts(normalize=True) * 100
    high_pct = risk_percentages.get("High Risk", 0)
    mod_pct = risk_percentages.get("Moderate Risk", 0)
    low_pct = risk_percentages.get("Low Risk", 0)

    worst_hour = df.groupby("hour")["pm2_5"].mean().idxmax()

    summary = pd.DataFrame({
        "metric": ["Highest Avg PM2.5", "Highest Avg Severity Score", 
                   "% High Risk Hours", "% Moderate Risk Hours", 
                   "% Low Risk Hours", "Hour With Worst AQI"],
        "value": [highest_pm_city, highest_severity_city, 
                  high_pct, mod_pct, low_pct, worst_hour]
    })

    summary.to_csv(PROCESSED_DIR / "summary_metrics.csv", index=False)
    print("✅ summary_metrics.csv saved")

    # City-wise risk distribution
    city_risk = df.groupby(["city", "risk_flag"]).size().reset_index(name="count")

    city_risk.to_csv(PROCESSED_DIR / "city_risk_distribution.csv", index=False)
    print("✅ city_risk_distribution.csv saved")

    return summary, city_risk

def compute_trends(df: pd.DataFrame):
    print("📈 Generating pollution trend dataset...")

    trends = df[["time", "city", "pm2_5", "pm10", "ozone"]].sort_values("time")

    trends.to_csv(PROCESSED_DIR / "pollution_trends.csv", index=False)
    print("✅ pollution_trends.csv saved")

    return trends

def create_visualizations(df: pd.DataFrame):
    print("📊 Creating visualizations...")

    #PM2.5 Histogram
    plt.figure(figsize=(8, 5))
    plt.hist(df["pm2_5"].dropna(), bins=30)
    plt.title("Distribution of PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Frequency")
    plt.savefig(PROCESSED_DIR / "hist_pm25.png")
    plt.close()
    print("📌 hist_pm25.png saved")

    #Bar chart: Risk flags per city
    plt.figure(figsize=(10, 5))
    df.groupby(["city", "risk_flag"]).size().unstack().plot(kind="bar", figsize=(10, 5))
    plt.title("Risk Flags Per City")
    plt.ylabel("Count")
    plt.savefig(PROCESSED_DIR / "risk_flags_per_city.png")
    plt.close()
    print("📌 risk_flags_per_city.png saved")

    #Line Chart: hourly PM2.5 Trends
    plt.figure(figsize=(12, 5))
    for city in df["city"].unique():
        subset = df[df["city"] == city]
        plt.plot(subset["time"], subset["pm2_5"], label=city)

    plt.title("Hourly PM2.5 Trends")
    plt.xlabel("Time")
    plt.ylabel("PM2.5")
    plt.legend()
    plt.savefig(PROCESSED_DIR / "pm25_trend.png")
    plt.close()
    print("📌 pm25_trend.png saved")

    # Scatter Plot: severity_score vs pm2_5
    plt.figure(figsize=(8, 6))
    plt.scatter(df["pm2_5"], df["severity_score"], alpha=0.5)
    plt.title("Severity Score vs PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Severity Score")
    plt.savefig(PROCESSED_DIR / "severity_vs_pm25.png")
    plt.close()
    print("📌 severity_vs_pm25.png saved")

def run_analysis():
    df = fetch_data()

    compute_kpis(df)
    compute_trends(df)
    create_visualizations(df)

    print("\n🎯 Analysis complete. All outputs saved to data/processed/")


if __name__ == "__main__":
    run_analysis()
