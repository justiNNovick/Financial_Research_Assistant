#src/scripts/clear_sql_db.py
import sqlite3
import os
import shutil

from src.scripts.connect_or_create_sql_db import connect_or_create_sql_db
from src.utils.path_helpers import project_root
import os

def clear_sql_database(conn, should_clear):
    """
    Clears all data from the balance_sheet table in the connected database if should_clear is True.
    """
    if not should_clear:
        print("Database clear skipped (should_clear is False).")
        return

    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS balance_sheet")
        conn.commit()
        print("Cleared the balance_sheet table (if it existed).")
    except sqlite3.Error as e:
        print(f"Error clearing database: {e}")


if __name__ == "__main__":
    db_path = os.path.join(project_root(), "data", "sqlite", "financials.db")
    conn = connect_or_create_sql_db()
    clear_sql_database(conn, True)