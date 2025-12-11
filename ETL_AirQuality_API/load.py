"""
Load step for Air Quality ETL Pipeline.

- Loads the transformed air_quality_transformed.csv file
- Inserts records into Supabase table air_quality_data
- Handles:
    * Batch insert (200 rows)
    * NaN → NULL conversion
    * Datetime → ISO string
    * Retry on failed batches (2 attempts)
    * Automatic table creation via RPC (if available)

This script follows the structure/pattern from the reference weather load.py.
"""

from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
from time import sleep

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0]
STAGED_DIR = BASE_DIR / "data" / "staged"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Please set SUPABASE_URL and SUPABASE_KEY in your .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "air_quality_data"


CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    time TIMESTAMP,
    pm10 DOUBLE PRECISION,
    pm2_5 DOUBLE PRECISION,
    carbon_monoxide DOUBLE PRECISION,
    nitrogen_dioxide DOUBLE PRECISION,
    sulphur_dioxide DOUBLE PRECISION,
    ozone DOUBLE PRECISION,
    uv_index DOUBLE PRECISION,
    aqi_category TEXT,
    severity_score DOUBLE PRECISION,
    risk_flag TEXT,
    hour INTEGER
);
"""


def create_table_if_not_exists():
    """
    Try to create table via RPC.
    If RPC disabled, print SQL so user can manually run it.
    """
    try:
        print("🔧 Attempting to create table in Supabase...")
        supabase.rpc("execute_sql", {"query": CREATE_TABLE_SQL}).execute()
        print("✅ Table creation RPC executed.")
    except Exception as e:
        print(f"⚠️ RPC create_table failed: {e}")
        print("➡️ Run this SQL in Supabase if table does not exist:")
        print(CREATE_TABLE_SQL)


def _read_staged_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # Convert time into ISO strings for Supabase
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce").astype(str)

    return df


def load_to_supabase(staged_csv_path: str, batch_size: int = 200):
    staged_file = Path(staged_csv_path)
    if not staged_file.exists():
        raise FileNotFoundError(f"Staged CSV missing at {staged_csv_path}")

    df = _read_staged_csv(staged_csv_path)
    total = len(df)
    print(f"📦 Preparing to load {total} rows → Supabase table '{TABLE_NAME}'")

    # Convert NaN → None
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")

    batch_num = 1

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]

        for attempt in range(1, 3):  # 2 retries max
            try:
                res = supabase.table(TABLE_NAME).insert(batch).execute()
                print(f"✅ Batch {batch_num} inserted ({len(batch)} rows)")
                break
            except Exception as e:
                print(f"⚠️ Batch {batch_num} attempt {attempt} failed: {e}")
                sleep(2)
        else:
            print(f"❌ Batch {batch_num} failed after retries — skipping.")

        batch_num += 1

    print("🎯 Load complete!")


if __name__ == "__main__":
    csv_files = sorted([str(p) for p in STAGED_DIR.glob("air_quality_transformed.csv")])
    if not csv_files:
        raise SystemExit("❌ No staged Air Quality CSV found. Run transform.py first.")

    create_table_if_not_exists()
    load_to_supabase(csv_files[-1], batch_size=200)
