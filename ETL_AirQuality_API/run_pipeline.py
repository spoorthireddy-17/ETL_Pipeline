"""
Combined ETL Runner for Air Quality Monitoring Pipeline.

Order:
1) Extract
2) Transform
3) Load
4) Analysis

Usage:
    python run_pipeline.py
"""

import time
from pathlib import Path

from extract import fetch_all_cities
from transform import transform_all, OUTPUT_FILE
from load import create_table_if_not_exists, load_to_supabase
from etl_analysis import run_analysis


def run_full_pipeline():
    print("\n===============================")
    print("STEP 1: EXTRACT")
    print("===============================")

    extract_results = fetch_all_cities()
    print("Extract step completed.")
    time.sleep(1)

    print("\n===============================")
    print("STEP 2: TRANSFORM")
    print("===============================")

    transform_all()

    staged_csv_path = Path(OUTPUT_FILE)

    if not staged_csv_path.exists():
        raise FileNotFoundError(f"Staged CSV missing at {staged_csv_path}")

    print(f"Transform step completed. File saved at: {staged_csv_path}")
    time.sleep(1)

    print("\n===============================")
    print("STEP 3: LOAD")
    print("===============================")

    create_table_if_not_exists()
    load_to_supabase(str(staged_csv_path), batch_size=200)

    print("Load step completed.")
    time.sleep(1)

    print("\n===============================")
    print("STEP 4: ANALYSIS")
    print("===============================")

    run_analysis()
    print("Analysis step completed.")

    print("\n====================================")
    print("🎉 FULL ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("====================================\n")


if __name__ == "__main__":
    run_full_pipeline()
