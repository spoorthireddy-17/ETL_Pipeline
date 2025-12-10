# ===========================
# transform.py
# ===========================
 
import os
import pandas as pd
 
# Purpose: Clean and transform Titanic dataset
def transform_data(raw_path):
    # Ensure the path is relative to project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # go up one level
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
 
    df = pd.read_csv(raw_path)
 
    # --- 1️⃣ Handle missing values ---
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    numeric_cols = ["tenure", "MonthlyCharges", "TotalCharges"]

    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())

    categorical_cols = df.select_dtypes(include=["object"]).columns
    df[categorical_cols] = df[categorical_cols].fillna("Unknown")
 
    # --- 2️⃣ Feature engineering ---
    df['tenure_group'] = pd.cut(
    df['tenure'],
    bins=[0, 12, 36, 60, float('inf')],
    labels=['New', 'Regular', 'Loyal', 'Champion'],
    right=True)
    
    df['monthly_charge_segment']=pd.cut(
    df['MonthlyCharges'],
    bins=[-float('inf'),30,70,float('inf')],
    labels=['Low','Medium','High'])
    
    df['has_internet_service'] = df['InternetService'].map({
    'DSL': 1,
    'Fiber optic': 1,
    'No': 0}).fillna(0)
    
    df['is_multi_line_user']=df['MultipleLines'].map({
    'Yes':1,
    'No':0,
    'No phone service':0})
    
    df['contract_type_code']=df['Contract'].map({
    'Month-to-month':0,
    'One year':1,
    'Two year':2})
 
    # --- 3️⃣ Drop unnecessary columns ---
    df.drop(columns=["customerID","gender",
    "Dependents",
    "Partner",
    "PhoneService",
    "MultipleLines",
    "OnlineSecurity",
    "OnlineBackup",
    "DeviceProtection",
    "TechSupport",
    "StreamingTV",
    "StreamingMovies",
    "SeniorCitizen",
    "PaperlessBilling"],inplace=True)


 
    # --- 4️⃣ Save transformed data ---
    staged_path = os.path.join(staged_dir, "churn_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path
 
 
if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_data(raw_path)