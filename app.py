import io
import logging
import gc

import pdfplumber
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials


# =========================
# HARDEN STREAMLIT BEHAVIOUR
# =========================

# Hide detailed error tracebacks in the UI
st.set_option("client.showErrorDetails", False)

# Make Streamlit + friends as quiet as possible
for logger_name in [
    "streamlit",
    "tornado",
    "py.warnings",
    "asyncio",
]:
    logging.getLogger(logger_name).setLevel(logging.ERROR)


# =========================
# CONFIG — EDIT IF NEEDED
# =========================

# Use ONLY the ID part between /d/ and /edit in your Sheet URL
SPREADSHEET_ID = "1UcjF-L0GWBetcJqA1t1UMyrt54ATUAR-22ozQEnVjQM"
WORKSHEET_NAME = "Raw_Data"
SERVICE_ACCOUNT_FILE = "service_account.json"

# Scopes for Google Sheets + Drive
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# How many PDF pages to process before flushing a chunk to Google Sheets
CHUNK_PAGES = 50  # you can lower this (e.g. 25) if still hitting limits


# Standard column order for the sheet
ORDERED_COLS = [
    "Posting Date",
    "Value Date",
    "Transaction Branch",
    "Reference Number",
    "Description",
    "Debit",
    "Credit",
    "Balance",
]


# =========================
# GOOGLE SHEETS HELPER
# =========================

def get_gspread_client():
    """Create an authorized gspread client using the service account JSON."""
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
    )
    client = gspread.authorize(creds)
    return client


def get_or_create_worksheet(client):
    """Open the target sheet and get/create the target worksheet."""
    sh = client.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(
            title=WORKSHEET_NAME,
            rows="1000",
            cols=str(len(ORDERED_COLS) + 5),
        )

    return worksheet


def init_worksheet_with_header(worksheet):
    """
    Clear existing data and write the header row only.
    Returns the next row index where data should be written (2).
    """
    worksheet.clear()
    worksheet.update("A1", [ORDERED_COLS])
    return 2  # next row after header


def append_chunk_to_worksheet(worksheet, df_chunk: pd.DataFrame, start_row: int) -> int:
    """
    Append a DataFrame chunk to the worksheet starting at the given row.
    Returns the next row index after the written block.
    """
    if df_chunk.empty:
        return start_row

    df_chunk = df_chunk[ORDERED_COLS]
    values = df_chunk.astype(str).fillna("").values.tolist()

    # Write starting at A<start_row>
    range_str = f"A{start_row}"
    worksheet.update(range_str, values)

    next_row = start_row + len(values)
    return next_row


# =========================
# PDF EXTRACTION + STREAMING TO GSHEETS
# =========================

def process_pdf_and_stream_to_gsheet(uploaded_pdf, progress_callback=None) -> int:
    """
    Read the uploaded PDF, extract all transactions, and stream them
    to Google Sheets in chunks to avoid memory issues.

    Returns:
        total_records (int): total number of rows written to Google Sheets.
    """
    # Read bytes once
    file_bytes = uploaded_pdf.read()
    pdf_file = io.BytesIO(file_bytes)

    # Drop reference to the uploader as early as possible
    uploaded_pdf = None
    del uploaded_pdf
    gc.collect()

    # Prepare Google Sheets
    client = get_gspread_client()
    worksheet = get_or_create_worksheet(client)
    next_row_index = init_worksheet_with_header(worksheet)

    total_records = 0

    with pdfplumber.open(pdf_file) as pdf:
        total_pages = len(pdf.pages)

        chunk_records = []

        for page_idx, page in enumerate(pdf.pages, start=1):
            # Progress update
            if progress_callback is not None and total_pages > 0:
                progress_callback(page_idx, total_pages)

            table = page.extract_table()
            if not table:
                continue

            # Assume first row is header, rest are data
            for row in table[1:]:
                # Ensure row has at least 8 cells
                row = (row + [""] * 8)[:8]
                (
                    posting_date,
                    value_date,
                    branch,
                    ref_number,
                    desc_,
                    debit,
                    credit,
                    balance,
                ) = row

                chunk_records.append(
                    {
                        "Posting Date": posting_date,
                        "Value Date": value_date,
                        "Transaction Branch": branch,
                        "Reference Number": ref_number,
                        "Description": desc_,
                        "Debit": debit,
                        "Credit": credit,
                        "Balance": balance,
                    }
                )
                total_records += 1

            # Flush to Google Sheets every CHUNK_PAGES
            if page_idx % CHUNK_PAGES == 0 and chunk_records:
                df_chunk = pd.DataFrame(chunk_records)
                next_row_index = append_chunk_to_worksheet(
                    worksheet,
                    df_chunk,
                    next_row_index,
                )

                # Clear in-memory chunk
                chunk_records = []
                del df_chunk
                gc.collect()

        # Flush any remaining records after the last page
        if chunk_records:
            df_chunk = pd.DataFrame(chunk_records)
            next_row_index = append_chunk_to_worksheet(
                worksheet,
                df_chunk,
                next_row_index,
            )
            chunk_records = []
            del df_chunk
            gc.collect()

    # Drop file bytes from memory ASAP
    pdf_file.close()
    del pdf_file
    del file_bytes
    gc.collect()

    return total_records


# =========================
# STREAMLIT APP
# =========================

st.set_page_config(page_title="Bank Statement Automation", layout="wide")

st.title("Bank Statement Automation")
st.write("Upload a bank statement PDF → extract → upload to Google Sheets (chunked).")

st.markdown(
    "**Output columns (matched to PDF):** "
    "`Posting Date`, `Value Date`, `Transaction Branch`, "
    "`Reference Number`, `Description`, `Debit`, `Credit`, `Balance`."
)

# File uploader (single file, no history UI)
uploaded_pdf = st.file_uploader(
    "Upload bank statement PDF",
    type=["pdf"],
    accept_multiple_files=False,
    key="pdf_uploader",
)

if uploaded_pdf:
    if st.button("Process PDF"):
        # Progress placeholders
        status_text = st.empty()
        progress_bar = st.progress(0)

        def progress_callback(current_page, total_pages):
            if total_pages <= 0:
                return
            pct = int(current_page / total_pages * 100)
            status_text.info(
                f"Reading page {current_page} of {total_pages} "
                f"({pct}%)…"
            )
            progress_bar.progress(pct)

        try:
            with st.spinner("Extracting from PDF and uploading to Google Sheets…"):
                total_rows = process_pdf_and_stream_to_gsheet(
                    uploaded_pdf,
                    progress_callback=progress_callback,
                )

            # Clear progress UI
            progress_bar.empty()

            if total_rows == 0:
                status_text.error("No transactions were found in the PDF.")
            else:
                status_text.success(
                    f"Finished extracting and uploading {total_rows} rows to Google Sheets."
                )
                st.success(
                    "Upload complete! Check the 'Raw_Data' tab in your Google Sheet."
                )

        except Exception:
            # Generic user-friendly error. Details are hidden by showErrorDetails=False
            progress_bar.empty()
            status_text.error(
                "Unexpected error while processing the PDF. "
                "Please try again or contact the owner."
            )

        # Best-effort cleanup of data from memory
        try:
            del total_rows
        except NameError:
            pass
        gc.collect()

else:
    st.info("Please upload a PDF bank statement to get started.")
