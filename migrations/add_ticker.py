"""
One-off migration: add `ticker` to the three master tables
===========================================================
Run ONCE (it is safe to re-run; every step is idempotent).

What it does
    1. Adds a `ticker` column to the three master tables
    2. Repairs company names that were stored incorrectly
    3. Backfills `ticker` from reference/ticker_map.csv
    4. Creates an index on `ticker`
    5. Prints verification reports (null tickers, duplicate keys)

Usage
    export NEON_CONN="postgresql://user:password@host/db?sslmode=require"
    python migrations/add_ticker.py

After this has run, drive_to_db.py keeps tickers and names correct on every
load, so this script does not need to run again.
"""

import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TICKER_MAP_PATH = os.path.join(BASE_DIR, "reference", "ticker_map.csv")

TABLES = ["master_income_statement", "master_balance_sheet", "master_cash_flow"]

# Keep these in sync with NAME_FIXES / NAME_PREFIX_FIXES in drive_to_db.py
NAME_FIXES = {
    "Transnational Corporation Plc": "Transnational Corporation of Nigeria Plc",
}
NAME_PREFIX_FIXES = {
    "Veritas Kapital Assurance Plc": "Veritas Kapital Assurance Plc",
}


def rename_company(conn, table, wrong, correct):
    """
    Renames `wrong` -> `correct`. If a row already exists under the correct
    name for the same (variable, period), the wrong-name row is deleted first
    so the primary key is not violated (the correct-name row is kept).
    """
    conn.execute(text(f"""
        DELETE FROM {table} w
        WHERE w.company = :wrong
          AND EXISTS (
              SELECT 1 FROM {table} c
              WHERE c.company  = :correct
                AND c.variable = w.variable
                AND c.period   = w.period
          )
    """), {"wrong": wrong, "correct": correct})

    result = conn.execute(text(f"""
        UPDATE {table} SET company = :correct WHERE company = :wrong
    """), {"wrong": wrong, "correct": correct})

    if result.rowcount:
        print(f"   ✅ {table}: '{wrong}' -> '{correct}' ({result.rowcount} rows)")


def main():
    neon_conn = os.getenv("NEON_CONN")
    if not neon_conn:
        sys.exit("❌ NEON_CONN environment variable is not set.")

    engine = create_engine(neon_conn)
    ticker_map = pd.read_csv(TICKER_MAP_PATH, dtype=str)
    print(f"Loaded {len(ticker_map)} tickers from {TICKER_MAP_PATH}")

    # ── 1. Add the column ────────────────────────────────────────────────────
    print("\n[1] Adding ticker column")
    with engine.begin() as conn:
        for t in TABLES:
            conn.execute(text(f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS ticker VARCHAR(20)"))
            print(f"   ✅ {t}")

    # ── 2. Repair company names ──────────────────────────────────────────────
    print("\n[2] Repairing company names")
    with engine.begin() as conn:
        for t in TABLES:
            for wrong, correct in NAME_FIXES.items():
                rename_company(conn, t, wrong, correct)

            for prefix, canonical in NAME_PREFIX_FIXES.items():
                variants = conn.execute(text(f"""
                    SELECT DISTINCT company FROM {t}
                    WHERE company LIKE :pat AND company <> :canonical
                """), {"pat": prefix + "%", "canonical": canonical}).scalars().all()
                for wrong in variants:
                    rename_company(conn, t, wrong, canonical)

    # ── 3. Backfill tickers ──────────────────────────────────────────────────
    print("\n[3] Backfilling tickers")
    params = [
        {"name": r.company_name, "ticker": r.ticker}
        for r in ticker_map.itertuples(index=False)
    ]
    with engine.begin() as conn:
        for t in TABLES:
            conn.execute(text(f"""
                UPDATE {t}
                SET ticker = :ticker
                WHERE LOWER(TRIM(company)) = LOWER(TRIM(:name))
            """), params)
            print(f"   ✅ {t}")

    # ── 4. Indexes ───────────────────────────────────────────────────────────
    print("\n[4] Creating indexes")
    with engine.begin() as conn:
        for t in TABLES:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{t}_ticker ON {t} (ticker)"))
            print(f"   ✅ idx_{t}_ticker")

    # ── 5. Verification ──────────────────────────────────────────────────────
    print("\n[5] Verification")
    for t in TABLES:
        counts = pd.read_sql(f"""
            SELECT COUNT(*) AS total_rows,
                   COUNT(ticker) AS rows_with_ticker,
                   COUNT(*) - COUNT(ticker) AS rows_still_null
            FROM {t}
        """, engine)
        print(f"\n   {t}\n{counts.to_string(index=False)}")

        unmatched = pd.read_sql(f"""
            SELECT company, COUNT(*) AS row_count FROM {t}
            WHERE ticker IS NULL GROUP BY company ORDER BY row_count DESC
        """, engine)
        if unmatched.empty:
            print("   ✅ every company has a ticker")
        else:
            print("   ⚠️  companies without a ticker (add them to reference/ticker_map.csv):")
            print(unmatched.to_string(index=False))

        dupes = pd.read_sql(f"""
            SELECT COUNT(*) AS duplicate_keys FROM (
                SELECT 1 FROM {t}
                GROUP BY company, variable, period HAVING COUNT(*) > 1
            ) d
        """, engine).iloc[0, 0]
        print("   ✅ no duplicate (company, variable, period) keys" if dupes == 0
              else f"   ⚠️  {dupes} duplicate keys found")


if __name__ == "__main__":
    main()
