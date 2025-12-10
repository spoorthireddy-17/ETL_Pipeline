# ===========================
# load.py (FIXED)
# ===========================
# Purpose: Load transformed customer churn dataset into Supabase using Supabase client
     
import os
import pandas as pd
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv
     
# Initialize Supabase client
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
       
    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")
           
    return create_client(url, key)
     
# ------------------------------------------------------
# Step 1: Create table if not exists
# ------------------------------------------------------
def create_table_if_not_exists():
    try:
        supabase = get_supabase_client()
           
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS public.churn_data(
            id BIGSERIAL PRIMARY KEY,
            tenure INTEGER,
            monthlycharges FLOAT,
            totalcharges FLOAT,
            churn TEXT,
            internetservice TEXT,
            contract TEXT,
            paymentmethod TEXT,
            tenure_group TEXT,
            monthly_charge_segment TEXT,
            has_internet_service INTEGER,
            is_multi_line_user INTEGER,
            contract_type_code INTEGER
        );
        """
           
        try:
            supabase.rpc('execute_sql', {'query': create_table_sql}).execute()
            print("✅ Table 'churn_data' created or already exists")
        except Exception as e:
            print(f"ℹ️  Note: {e}")
            print("ℹ️  Table will be created on first insert")
     
    except Exception as e:
        print(f"⚠️  Error checking/creating table: {e}")
        print("ℹ️  Trying to continue with data insertion...")
     
# ------------------------------------------------------
# Step 2: Load CSV data into Supabase table
# ------------------------------------------------------
def load_to_supabase(staged_path: str, table_name: str = "churn_data"):
       
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))
       
    print(f"🔍 Looking for data file at: {staged_path}")
       
    if not os.path.exists(staged_path):
        print(f"❌ Error: File not found at {staged_path}")
        print("ℹ️  Please run transform.py first")
        return
     
    try:
        supabase = get_supabase_client()
           
        df = pd.read_csv(staged_path)

        # 🔥 FIX 1: Make columns lowercase to match DB
        df.columns = df.columns.str.lower()

        # 🔥 FIX 2: Convert NaN → None safely
        df = df.replace({np.nan: None, pd.NA: None})

        batch_size = 200
        total_rows = len(df)
           
        print(f"📊 Loading {total_rows} rows into '{table_name}'...")
           
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size].copy()

            records = batch.to_dict('records')
               
            try:
                response = supabase.table(table_name).insert(records).execute()
                   
                if hasattr(response, 'error') and response.error:
                    print(f"⚠️  Error in batch {i//batch_size + 1}: {response.error}")
                else:
                    end = min(i + batch_size, total_rows)
                    print(f"✅ Inserted rows {i+1}-{end} of {total_rows}")
                   
            except Exception as e:
                print(f"⚠️  Error in batch {i//batch_size + 1}: {str(e)}")
                continue
     
        print(f"🎯 Finished loading data into '{table_name}'.")
     
    except Exception as e:
        print(f"❌ Error loading data: {e}")
     
# ------------------------------------------------------
# Step 3: Run script
# ------------------------------------------------------
if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "churn_transformed.csv")
    create_table_if_not_exists()
    load_to_supabase(staged_csv_path)
