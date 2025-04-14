#src/scripts/connect_or_create_sql_db.py
import os
import sqlite3
from src.utils.path_helpers import project_root

# returns a connection to the sqlite db and creates the schema if needed
def connect_or_create_sql_db() -> sqlite3.Connection:
    """
    Connects to the SQLite database (or creates it if it doesn't exist),
    and ensures that the balance_sheet table exists with a composite primary key.
    """
    db_path = os.path.join(project_root(), "data", "sqlite", "financials.db")
    print(f"Connected to SQLite database at: {db_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS balance_sheet (
        uid TEXT PRIMARY KEY,
        as_of_date TEXT NOT NULL,
        company TEXT NOT NULL,
        statement_type TEXT NOT NULL,
        section TEXT,
        label TEXT NOT NULL,
        year TEXT NOT NULL,
        value REAL
    )
    """)

    conn.commit()
    return conn