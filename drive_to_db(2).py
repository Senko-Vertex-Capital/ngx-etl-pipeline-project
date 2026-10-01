"""
Drive to DB Pipeline
====================
Reads all worksheets from the three financial Excel files
(income_statement, balance_sheet, cash_flow), cleans and transforms
each sheet to long format, then loads into three separate Neon
PostgreSQL tables.

Tables created:
    {NEON_TABLE}_income_statement
    {NEON_TABLE}_balance_sheet
    {NEON_TABLE}_cash_flow

Environment variables (set as GitHub Secrets):
    NEON_CONN  — PostgreSQL connection string
    NEON_TABLE — base table name

Usage:
    python drive_to_db.py
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# ── Secret ──────────────────────────────────────────────────────────────────
# Only using the connection string; targeting existing master tables
neon_conn = os.getenv("NEON_CONN")

TABLE_MAP = {
    "income_statement": "master_income_statement",
    "balance_sheet":    "master_balance_sheet",
    "cash_flow":        "master_cash_flow"
}

# ══════════════════════════════════════════════════════════════════════════════
# 1. DATABASE LOADING (Fast Staging Method)
# ══════════════════════════════════════════════════════════════════════════════

def load_data_with_upsert(df, table_name, engine):
    """Fast Batch Upsert into existing master tables."""
    staging_table = f"temp_staging_{table_name}"
    
    with engine.begin() as conn:
        # 1. Deduplicate before staging (keep last occurrence per PK)
        before = len(df)
        df = df.drop_duplicates(subset=['company', 'variable', 'period'], keep='last')
        dropped = before - len(df)
        if dropped:
            print(f"   ⚠️  Dropped {dropped} duplicate rows before upsert.")

        # 2. Batch upload to a temporary staging table
        df.to_sql(staging_table, conn, if_exists='replace', index=False)
        
        # 3. Merge staging to master (Targets existing PK: company, variable, period)
        merge_query = text(f"""
            INSERT INTO {table_name} 
            (company, variable, period, date, value, currency, unit, source, loaded_at)
            SELECT company, variable, period, date, value, currency, unit, source, loaded_at 
            FROM {staging_table}
            ON CONFLICT (company, variable, period) 
            DO UPDATE SET 
                value = EXCLUDED.value,
                date = EXCLUDED.date,
                source = EXCLUDED.source,
                loaded_at = EXCLUDED.loaded_at;
        """)
        conn.execute(merge_query)
        
        # 4. Drop staging table
        conn.execute(text(f"DROP TABLE {staging_table}"))
# ══════════════════════════════════════════════════════════════════════════════
# 2. FILE DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def detect_statement_type(filename):
    fname = filename.lower()
    if "income" in fname: return "income_statement"
    if "balance" in fname: return "balance_sheet"
    if "cash" in fname: return "cash_flow"
    return None

# ══════════════════════════════════════════════════════════════════════════════
# 3. MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_db_pipeline():
    if not neon_conn:
        print("❌ ERROR: NEON_CONN environment variable is not set.")
        return

    # Targeting the project's data folder
    data_folder = os.path.join(os.path.dirname(__file__), "data")
    engine = create_engine(neon_conn)
    
    # Discovery
    files = [f for f in os.listdir(data_folder) if f.endswith(('.xlsx', '.xls'))]
    
    if not files:
        print(f"⚠️ No Excel files found in {data_folder}")
        return

    for file in files:
        statement_type = detect_statement_type(file)
        if not statement_type:
            continue
            
        target_table = TABLE_MAP[statement_type]
        filepath = os.path.join(data_folder, file)
        
        print(f"\n🚀 Processing {file} -> {target_table}")
        
        try:
            xl = pd.ExcelFile(filepath)
            for sheet_name in xl.sheet_names:
                # Basic sheet filtering
                if sheet_name.lower() in ["sheet1", "cover", "contents", "readme"]:
                    continue

                df = pd.read_excel(xl, sheet_name=sheet_name)
                if df.empty:
                    continue

                # Standardize data for the master table
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                
                df['loaded_at'] = datetime.now()
                
                if 'source' not in df.columns:
                    df['source'] = file

                # Batch Upsert
                load_data_with_upsert(df, target_table, engine)
                print(f"   ✅ {sheet_name}: Upserted {len(df)} rows.")

        except Exception as e:
            print(f"   ❌ Failed to process {file}: {e}")

if __name__ == "__main__":
    run_db_pipeline()
