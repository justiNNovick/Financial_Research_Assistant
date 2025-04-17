#src/utils/extract_and_normalize.py
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import os
import pdfkit
from urllib.parse import urljoin
import pdfplumber
import re
import pandas as pd
import logging
from typing import List, Optional

from src.utils.path_helpers import project_root

logging.getLogger("pdfminer").setLevel(logging.ERROR)

# grabs all 10-K index page URLs for a company
def get_10k_filing_urls(ticker: str, years_back: int) -> List[str]:
    headers = {"User-Agent": "Justin Novick (justinnovick2@gmail.com)"}
    cik_lookup_url = "https://www.sec.gov/files/company_tickers.json"
    res = requests.get(cik_lookup_url, headers=headers)
    res.raise_for_status()
    ticker_data = res.json()

    cik = None
    for k, v in ticker_data.items():
        if v['ticker'].lower() == ticker.lower():
            cik = str(v['cik_str']).zfill(10)
            break
    if not cik:
        raise ValueError(f"CIK not found for ticker: {ticker}")

    submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    res = requests.get(submissions_url, headers=headers)
    res.raise_for_status()
    data = res.json()

    current_year = datetime.now().year
    target_years = {str(y) for y in range(current_year - years_back, current_year + 1)}

    recent = data["filings"]["recent"]
    filing_urls = []

    for i in range(len(recent["form"])):
        if recent["form"][i] != "10-K":
            continue

        filing_date = recent["filingDate"][i]
        if filing_date[:4] not in target_years:
            continue

        accession_raw = recent["accessionNumber"][i]
        accession_clean = accession_raw.replace("-", "")
        index_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_clean}/{accession_raw}-index.htm"
        filing_urls.append(index_url)

    return filing_urls

# converts the linked HTML filings into PDFs
def save_10k_htmls_as_pdfs(index_urls: List[str], output_dir: Optional[str] = None) -> List[str]:
    if output_dir is None:
        output_dir = os.path.join(project_root(), "data", "pdfs")

    os.makedirs(output_dir, exist_ok=True)
    headers = {"User-Agent": "Justin Novick (justinnovick2@gmail.com)"}
    saved_pdfs = []

    for index_url in index_urls:
        print(f"Scanning index page: {index_url}")
        try:
            response = requests.get(index_url, headers=headers)
            response.raise_for_status()
        except Exception as e:
            print(f"Failed to fetch {index_url}: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", class_="tableFile", summary="Document Format Files")
        if not table:
            print("No document table found.")
            continue

        rows = table.find_all("tr")
        filing_htm_url = None

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) >= 4:
                description = cells[1].text.strip().lower()
                doc_link = cells[2].find("a")
                if doc_link and "10-k" in description:
                    raw_href = doc_link["href"]
                    filing_htm_url = urljoin("https://www.sec.gov", raw_href)
                    if filing_htm_url.startswith("https://www.sec.gov/ix?doc="):
                        filing_htm_url = filing_htm_url.replace("https://www.sec.gov/ix?doc=", "https://www.sec.gov")
                    break

        if not filing_htm_url:
            print("10-K HTML document not found.")
            continue

        file_name = os.path.basename(filing_htm_url).replace(".htm", ".pdf")
        pdf_path = os.path.join(output_dir, file_name)

        try:
            print(f"Converting to PDF: {filing_htm_url} → {pdf_path}")
            pdfkit.from_url(filing_htm_url, pdf_path)
            saved_pdfs.append(pdf_path)
        except Exception as e:
            print(f"PDF conversion failed: {e}")

    return saved_pdfs

# helper to find which page the table of contents is on
def find_toc_page_index(pdf_path: str, max_search_pages: int = 10) -> Optional[int]:
    with pdfplumber.open(pdf_path) as pdf:
        for i in range(min(max_search_pages, len(pdf.pages))):
            text = pdf.pages[i].extract_text()
            if text and "table of contents" in text.lower():
                print(f"TOC likely found on pdf.pages[{i}]")
                return i
    print("TOC not found in first few pages.")
    return None

# uses TOC to locate the Balance Sheet page
def find_balance_sheet_page_by_toc(pdf_path: str) -> Optional[int]:
    toc_index = find_toc_page_index(pdf_path)
    if toc_index is None:
        return None

    page_offset = toc_index + 1
    toc_pattern = re.compile(r"item\s+8[\.\s]+.*?(\d{1,3})", re.IGNORECASE)

    with pdfplumber.open(pdf_path) as pdf:
        toc_text = pdf.pages[toc_index].extract_text()
        if not toc_text:
            print("TOC page had no extractable text.")
            return None

        for line in toc_text.split("\n"):
            match = toc_pattern.search(line)
            if match:
                logical_page = int(match.group(1))
                actual_index = logical_page + page_offset - 1
                print(f"'Item 8' points to page {logical_page} → pdf.pages[{actual_index}]")
                return actual_index

    print("Could not find 'Item 8' in TOC.")
    return None

def is_likely_toc_table(df: pd.DataFrame) -> bool:
    first_column_text = " ".join(str(cell).lower() for cell in df[0] if pd.notnull(cell))
    return (
        df.shape[0] <= 10 and
        "consolidated balance sheets" in first_column_text and
        "comprehensive income" in first_column_text
    )

# scans nearby pages for a balance-sheet-looking table
def extract_table_near_page(pdf_path: str, page_number: int, max_offset: int = 6) -> Optional[pd.DataFrame]:
    if page_number is None:
        print("Cannot extract without a valid page number.")
        return None

    with pdfplumber.open(pdf_path) as pdf:
        for offset in range(max_offset + 1):
            try_page = page_number + offset
            if try_page >= len(pdf.pages):
                break

            page = pdf.pages[try_page]
            tables = page.extract_tables()
            for idx, table in enumerate(tables):
                df = pd.DataFrame(table)

                if df.shape[1] >= 2:
                    if is_likely_toc_table(df):
                        print(f"Skipping TOC-like table on page {try_page}")
                        continue

                    keywords = ["assets", "liabilities", "equity", "shareholders’ equity", "stockholders’ equity"]
                    flat_text = " ".join(str(cell).lower() for row in df.values for cell in row if cell)
                    if any(keyword in flat_text for keyword in keywords):
                        print(f"Found likely Balance Sheet on pdf.pages[{try_page}]")
                        df = df.dropna(how="all").dropna(axis=1, how="all")
                        return df

    print("No balance sheet-like table found.")
    return None

# standard cleaner for scraped tables
def clean_balance_sheet(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df = df.iloc[1:]  # drop the header row
    df[0] = df[0].astype(str).str.strip()
    df = df[df[0].notnull() & (df[0] != '')]

    cols = list(df.columns)
    label_col = cols[0]
    value_cols = cols[1:]

    if len(value_cols) >= 2:
        value_col_1, value_col_2 = value_cols[0], value_cols[1]
    elif len(value_cols) == 1:
        value_col_1 = value_col_2 = value_cols[0]
    else:
        raise ValueError("Not enough value columns found in balance sheet table.")

    df_clean = df[[label_col, value_col_1, value_col_2]].copy()
    df_clean.columns = ["label", "current_year", "previous_year"]

    for col in ["current_year", "previous_year"]:
        df_clean[col] = (
            df_clean[col]
            .astype(str)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", None)
            .astype(float)
        )

    return df_clean.reset_index(drop=True)

# parses the PDF to extract and clean the balance sheet
def parse_balance_sheet_from_pdf(pdf_path: str) -> Optional[pd.DataFrame]:
    bs_page = find_balance_sheet_page_by_toc(pdf_path)
    df = extract_table_near_page(pdf_path, bs_page)
    if df is not None:
        return clean_balance_sheet(df)
    else:
        print("Failed to extract Balance Sheet.")
        return None

# extracts date from filename
def extract_as_of_date_from_filename(pdf_path: str, ticker: str) -> Optional[str]:
    import shutil

    filename = os.path.basename(pdf_path)
    match = re.search(r'(\d{8})', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y%m%d").date().isoformat()
        except ValueError:
            print(f"Invalid 8-digit date in filename: {filename}")

    # Fallback: use TOC to extract "For the Fiscal Year Ended ..."
    toc_page_idx = find_toc_page_index(pdf_path)
    if toc_page_idx is not None:
        with pdfplumber.open(pdf_path) as pdf:
            text = pdf.pages[toc_page_idx].extract_text()
            if text:
                date_match = re.search(r"For the Fiscal Year Ended (.+?)\n", text)
                if date_match:
                    try:
                        parsed = dateparser.parse(date_match.group(1).strip())
                        if parsed:
                            as_of_date = parsed.date().isoformat()
                            date_str = parsed.strftime("%Y%m%d")
                            print(f"Extracted date from TOC: {as_of_date}")

                            # Standardize filename
                            dir_path = os.path.dirname(pdf_path)
                            new_filename = f"{ticker.lower()}-{date_str}.pdf"
                            new_path = os.path.join(dir_path, new_filename)

                            if os.path.basename(pdf_path) != new_filename:
                                shutil.move(pdf_path, new_path)
                                print(f"Renamed file: {filename} → {new_filename}")

                            return as_of_date
                    except Exception as e:
                        print(f" Date parse failed: {e}")

    print(f"Could not extract date for: {pdf_path}")
    return None

# reshapes cleaned balance sheet into long format for storage
def normalize_balance_sheet(df: pd.DataFrame, company: str, as_of_date: str, statement_type: str = "balance_sheet") -> pd.DataFrame:
    rows = []
    current_section = None
    filing_year = pd.to_datetime(as_of_date).year

    for _, row in df.iterrows():
        label = row['label']
        if label.endswith(":"):
            current_section = label.rstrip(":")
            continue

        value = row.get('current_year')
        if pd.notna(value):
            rows.append({
                'as_of_date': as_of_date,
                'company': company,
                'statement_type': statement_type,
                'section': current_section,
                'label': label,
                'year': filing_year,
                'value': value
            })

    return pd.DataFrame(rows)



if __name__ == "__main__":
    """
    # for testing with aapl files already downloaded
    pdfs = [
        "../data/pdfs/aapl-20200926.pdf",
        "../data/pdfs/aapl-20210925.pdf",
        "../data/pdfs/aapl-20220924.pdf",
        "../data/pdfs/aapl-20230930.pdf",
        "../data/pdfs/aapl-20240928.pdf"
    ]

    for pdf_path in pdfs:
        print(f"\nProcessing: {pdf_path}")
        df = parse_balance_sheet_from_pdf(pdf_path)
        if df is not None:
            print(f"\nCleaned Balance Sheet: {os.path.basename(pdf_path)}")
            print(df)
    """

