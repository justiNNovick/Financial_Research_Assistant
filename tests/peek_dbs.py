# tests/peek_dbs.py
import os
import sys
import sqlite3
from typing import List, Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.scripts.connect_or_create_sql_db import connect_or_create_sql_db
from src.utils.path_helpers import project_root


print(" DB path used:", os.path.join(project_root(), "data", "sqlite", "financials.db"))


# preview a few rows from the balance_sheet table
def peek_balance_sheet_entries(limit: int = 135) -> None:
    db_path = os.path.join(project_root(), "data", "sqlite", "financials.db")
    conn = connect_or_create_sql_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM balance_sheet LIMIT ?", (limit,))
        rows = cursor.fetchall()

        if not rows:
            print(" No entries found in the balance_sheet table.")
        else:
            print(f" Showing first {limit} entries from 'balance_sheet':\n")
            for row in rows:
                print(row)
    except Exception as e:
        print(f"Failed to fetch entries: {e}")
    finally:
        conn.close()

# get schema and row count for sanity check
def show_table_info() -> None:
    db_path = os.path.join(project_root(), "data", "sqlite", "financials.db")
    conn = connect_or_create_sql_db()
    cursor = conn.cursor()
    
    print(" All tables in DB:")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cursor.fetchall())

    print("\n Schema for balance_sheet:")
    cursor.execute("PRAGMA table_info(balance_sheet);")
    for col in cursor.fetchall():
        print(col)

    print("\n Checking if constraint exists:")
    cursor.execute("SELECT sql FROM sqlite_master WHERE name='balance_sheet';")
    print(cursor.fetchone())

    print("\n Total rows:")
    cursor.execute("SELECT COUNT(*) FROM balance_sheet;")
    print(cursor.fetchone())

    conn.close()



if __name__ == "__main__":
    # To see what's inside the relational db
    peek_balance_sheet_entries()
    show_table_info()

