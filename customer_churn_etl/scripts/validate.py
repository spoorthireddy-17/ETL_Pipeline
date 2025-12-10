# ============================
# validate.py
# ============================
# Validates the loaded churn dataset in Supabase after ETL

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


def validate_data(original_csv_path=None,supabase_table="churn_data"):
    print("RUNNING DATA VALIDATION")
    if original_csv_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        original_csv_path = os.path.abspath(
            os.path.join(script_dir, "..", "data", "raw", "churn.csv")
        )

    print(f"📄 Looking for original CSV at: {original_csv_path}")

    if not os.path.exists(original_csv_path):
        print(f"❌ File not found: {original_csv_path}")
        return
    
    supabase = get_supabase_client()
    df_original = pd.read_csv(original_csv_path)
    original_count = len(df_original)
    print(f"Original dataset rows: {original_count}")
    print("Fetching rows from Supabase...")

    response = supabase.table(supabase_table).select("*").execute()
    db_rows = response.data
    df_db = pd.DataFrame(db_rows)
    db_count = len(df_db)

    print(f"Supabase table rows: {db_count}")
    required_cols = ["tenure", "monthlycharges", "totalcharges"]
    missing_issues = {}
    for col in required_cols:
        if col not in df_db.columns:
            missing_issues[col] = "Column not found in table"
        else:
            missing_count = df_db[col].isna().sum()
            missing_issues[col] = missing_count
    unique_rows = len(df_db.drop_duplicates())
    
    expected_tenure_groups = {"0-12", "13-24", "25-48", "49-72", "73+"}
    expected_charge_segments = {"Low", "Medium", "High"}

    tenure_group_valid = True
    charge_segment_valid = True

    if "tenure_group" in df_db.columns:
        tenure_group_valid = set(df_db["tenure_group"].unique()) <= expected_tenure_groups

    if "monthly_charge_segment" in df_db.columns:
        charge_segment_valid = set(df_db["monthly_charge_segment"].unique()) <= expected_charge_segments

    contract_valid = True
    if "contract_type_code" in df_db.columns:
        contract_valid = set(df_db["contract_type_code"].unique()) <= {0, 1, 2}

    print(" VALIDATION SUMMARY")
    print(" Missing Value Check (tenure, MonthlyCharges, TotalCharges):")
    for col, val in missing_issues.items():
        if val == 0:
            print(f"{col}: OK (no missing values)")
        else:
            print(f"{col}: {val} missing values")

    print("\nRow Count Validation:")
    print(f"Original rows: {original_count}")
    print(f"Supabase rows: {db_count}")
    print(f"Unique rows in Supabase: {unique_rows}")

    if db_count == original_count:
        print("Row count matches original dataset")
    else:
        print("Row count mismatch")

    print("\nSegment Validation:")
    print(f"tenure_group valid: {tenure_group_valid}")
    print(f"monthly_charge_segment valid: {charge_segment_valid}")

    print("\nContract Code Validation:")
    print(f"contract_type_code valid: {contract_valid}")

if __name__ == "__main__":
    validate_data()
