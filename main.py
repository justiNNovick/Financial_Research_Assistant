#main.py
import argparse
import os
import shutil
import pandas as pd

from src.scripts import clear_sql_db, connect_or_create_sql_db
from src.utils import extract_and_normalize, path_helpers
from src.sql_interface import run_interactive_research_assistant, insert_balance_sheet

PDF_STORE_DIR = "data/pdfs"

# deletes and recreates the PDF storage directory
def clear_pdf_store() -> None:
    if os.path.exists(PDF_STORE_DIR):
        shutil.rmtree(PDF_STORE_DIR)
    os.makedirs(PDF_STORE_DIR, exist_ok=True)

# main orchestration logic for downloading, parsing, and storing balance sheets
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear_sql_database", action="store_true", help="Clear the SQL database before starting")
    parser.add_argument("--clear_doc_store", action="store_true", help="Clear the PDF document store before starting")
    parser.add_argument("--ticker", type=str, default="AAPL", help="Company ticker symbol")
    parser.add_argument("--years_back", type=int, default=5, help="How many years back to fetch filings")
    parser.add_argument("--make_csv", action="store_true", help="Flag to store csv in home directory")
    args = parser.parse_args()



    conn = connect_or_create_sql_db.connect_or_create_sql_db()
    if args.clear_sql_database:
        print(" Clearing SQL database...")
        clear_sql_db.clear_sql_database(conn, True)
        print(" SQL database cleared.")
    
    
    if args.clear_doc_store:
        print(" Clearing PDF document store...")
        clear_pdf_store()
        print(" PDF store cleared.")

    filing_urls = extract_and_normalize.get_10k_filing_urls(args.ticker.upper(), args.years_back)
    pdfs = extract_and_normalize.save_10k_htmls_as_pdfs(filing_urls)

    for pdf_path in pdfs:
        print(f"\n Processing: {pdf_path}")
        df = extract_and_normalize.parse_balance_sheet_from_pdf(pdf_path)

        # Handle balance sheet parsing failure
        if df is None or df.empty:
            print("Skipping insert: Empty or invalid balance sheet.")
            continue

        # Attempt to extract date from filename
        as_of_date = extract_and_normalize.extract_as_of_date_from_filename(pdf_path, args.ticker.upper())
        if not as_of_date:
            print(f"Skipping file due to missing as_of_date: {pdf_path}")
            continue

        # Proceed with normalization and insert
        df = extract_and_normalize.normalize_balance_sheet(df, args.ticker.upper(), as_of_date, "balance_sheet")
        print(f"\nCleaned Balance Sheet: {os.path.basename(pdf_path)}")
        print(df)
        insert_balance_sheet(df, conn)

    if args.make_csv:
        df = pd.read_sql("SELECT * FROM balance_sheet", conn)
        df.to_csv("sqlite_export_balance_sheet.csv", index=False)
        print("CSV file created in home directory.")
    run_interactive_research_assistant(conn)


if __name__ == "__main__":
    main()
