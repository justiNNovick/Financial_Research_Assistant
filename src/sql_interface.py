
import sqlite3
import os
import pandas as pd
import hashlib
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime

from src.utils.path_helpers import project_root
from src.utils.extract_and_normalize import parse_balance_sheet_from_pdf, normalize_balance_sheet, extract_as_of_date_from_filename
from src.scripts.connect_or_create_sql_db import connect_or_create_sql_db
from src.scripts.clear_sql_db import clear_sql_database
from src.scripts.connect_or_create_document_db import connect_or_create_doc_store
from src.scripts.clear_document_db import clear_doc_store

load_dotenv()

DB_PATH = os.path.join(project_root(), "data", "sqlite", "financials.db")
print("DB path used:", DB_PATH)

client = OpenAI()

# generate a unique ID based on all the identifying columns
def generate_uid(row: pd.Series) -> str:
    uid_str = f"{row['as_of_date']}_{row['company']}_{row['statement_type']}_{row['section']}_{row['label']}_{row['year']}"
    return hashlib.md5(uid_str.encode()).hexdigest()

# insert rows into the balance_sheet table, skipping any that already exist
def insert_balance_sheet(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS balance_sheet (
                uid TEXT PRIMARY KEY,
                as_of_date TEXT,
                company TEXT,
                statement_type TEXT,
                section TEXT,
                label TEXT,
                year TEXT,
                value REAL
            );
        """)
        conn.commit()

        df["uid"] = df.apply(generate_uid, axis=1)

        placeholders = ','.join('?' for _ in df["uid"])
        existing_uids = set()
        if placeholders:
            cursor.execute(f"SELECT uid FROM balance_sheet WHERE uid IN ({placeholders})", tuple(df["uid"]))
            existing_uids = {row[0] for row in cursor.fetchall()}

        df_filtered = df[~df["uid"].isin(existing_uids)]
        if df_filtered.empty:
            print("No new rows to insert (all duplicates).")
            return

        df_filtered.to_sql("balance_sheet", conn, if_exists="append", index=False)
        print(f"Inserted {len(df_filtered)} new rows into 'balance_sheet' table.")

    except Exception as e:
        print(f"Failed to insert balance sheet data: {e}")

# removes markdown formatting from generated SQL
def clean_generated_sql(sql: str) -> str:
    lines = sql.strip().splitlines()
    lines = [line for line in lines if not line.strip().startswith("```")]
    return "\n".join(lines).strip()

# uses the OpenAI client to generate SQL from a user question and schema
def generate_sql_query(schema_description: str, user_question: str) -> str:
    few_shot_examples = """
Example rows in the table:
('2020-09-26', 'AAPL', 'balance_sheet', 'Current liabilities', 'Total current liabilities', 'current_year', 105392.0, 'uid1')
('2021-09-25', 'MSFT', 'balance_sheet', 'Non-current liabilities', 'Total liabilities', 'current_year', 120000.0, 'uid2')
('2022-09-24', 'GOOG', 'balance_sheet', 'Shareholders’ equity', 'Total shareholders’ equity', 'current_year', 97000.0, 'uid3')
('2023-09-30', 'TSLA', 'balance_sheet', 'Current assets', 'Cash and cash equivalents', 'previous_year', 40000.0, 'uid4')
('2020-09-26', 'META', 'balance_sheet', 'Non-current liabilities', 'Total non-current liabilities', 'current_year', 153157.0, 'uid5')
('2021-09-25', 'NFLX', 'balance_sheet', 'Current assets', 'Inventories', 'current_year', 8300.0, 'uid6')
('2022-09-24', 'AMZN', 'balance_sheet', 'Current liabilities', 'Accounts payable', 'previous_year', 62200.0, 'uid7')
('2023-09-30', 'INTC', 'balance_sheet', 'Current liabilities', 'Total current liabilities', 'current_year', 103000.0, 'uid8')
('2024-09-28', 'NVDA', 'balance_sheet', 'Non-current liabilities', 'Total liabilities', 'current_year', 308030.0, 'uid9')
('2024-09-28', 'TSLA', 'balance_sheet', 'Current liabilities', 'Deferred revenue', 'current_year', 8249.0, 'uid10')

Example questions and SQL queries:
Q: What is the average of total liabilities over the past 5 years?
A: SELECT AVG(value) FROM balance_sheet WHERE label = 'Total liabilities' AND CAST(substr(as_of_date, 1, 4) AS INTEGER) >= CAST(strftime('%Y', 'now', '-5 years') AS INTEGER);

Q: Show the total current assets of AAPL for the year 2020.
A: SELECT value FROM balance_sheet WHERE company = 'AAPL' AND label = 'Total current assets' AND as_of_date = '2020-09-26';

Q: How much was retained earnings for MSFT in 2021?
A: SELECT value FROM balance_sheet WHERE company = 'MSFT' AND label = 'Retained earnings' AND as_of_date LIKE '2021%';

Q: What companies had more than 100000 in total liabilities in 2024?
A: SELECT DISTINCT company FROM balance_sheet WHERE label = 'Total liabilities' AND CAST(substr(as_of_date, 1, 4) AS INTEGER) = 2024 AND value > 100000;
    """

    messages = [
        {
            "role": "system",
            "content": (
                "You are a SQL assistant. Given a schema, a few example rows from a table, and a user question, output a single SQLite-compatible SQL query. "
                "Never include anything other than the SQL query."
                f"\n\nSchema:\n{schema_description}\n\n{few_shot_examples}"
            )
        },
        {"role": "user", "content": user_question}
    ]

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0
    )
    sql_raw = response.choices[0].message.content.strip()
    return sql_raw

# makes sure the query is safe, then executes it
def execute_sql_query(query: str, conn: sqlite3.Connection) -> list:
    lowered = query.lower().strip()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError(f"Only SELECT or WITH queries are allowed for safety. Generated query: {query}")

    forbidden = ("insert", "update", "delete", "drop", "alter", "create", "replace")
    if any(word in lowered for word in forbidden):
        raise ValueError("Query contains potentially unsafe operations.")

    print("Executing SQL:", query)
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

# runs a full pipeline from NL -> SQL -> DB result -> final answer
def answer_question_from_db(schema: str, question: str, conn: sqlite3.Connection) -> str:
    sql_query = generate_sql_query(schema, question)
    cleaned_query = clean_generated_sql(sql_query)
    results = execute_sql_query(cleaned_query, conn)

    answer_prompt = (
        f"Question: {question}\n"
        f"SQL: {cleaned_query}\n"
        f"Results: {results}\n"
        "Provide a concise answer using this data."
    )

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": answer_prompt}],
        temperature=0
    )

    return response.choices[0].message.content.strip()

# REPL for asking the LLM balance sheet questions
def run_interactive_research_assistant(conn: sqlite3.Connection) -> None:
    schema = """
    Table: balance_sheet(
        uid TEXT PRIMARY KEY,
        as_of_date TEXT,
        company TEXT,
        statement_type TEXT,
        section TEXT,
        label TEXT,
        year TEXT,
        value REAL
    )
    """
    print("Welcome to the financial research assistant.\n")
    while True:
        user_input = input("Ask a question about the balance sheet (or type 'exit' to quit): ")
        if user_input.lower() in ("exit", "quit"):
            print("Goodbye!")
            break

        try:
            answer = answer_question_from_db(schema, user_input, conn)
            print("\nAnswer:", answer, "\n")
        except Exception as e:
            print("\nError:", str(e), "\n")



if __name__ == "__main__":
    """
    # for testing with aapl files downloaded
    conn = connect_or_create_sql_db()
    clear_sql_database(conn, False)

    pdfs = [
        "aapl-20200926.pdf",
        "aapl-20210925.pdf",
        "aapl-20220924.pdf",
        "aapl-20230930.pdf",
        "aapl-20240928.pdf"
    ]
    pdfs = [os.path.join(project_root(), "data", "pdfs", pdf) for pdf in pdfs]

    for pdf_path in pdfs:
        print(f"\nProcessing: {pdf_path}")
        df = parse_balance_sheet_from_pdf(pdf_path)
        if df is not None:
            as_of_date = extract_as_of_date_from_filename(pdf_path)
            df = normalize_balance_sheet(df, "AAPL", as_of_date, "balance_sheet")
            print(f"\nCleaned Balance Sheet: {os.path.basename(pdf_path)}")
            print(df)
            insert_balance_sheet(df, conn)
        else:
            print("Failed to extract data from PDF.")

    run_interactive_research_assistant(conn)
    """

