import io
import pdfplumber
import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials

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


def upload_df_to_gsheet(df: pd.DataFrame):
    """
    Upload the given DataFrame to the configured Google Sheet and worksheet.
    Clears existing data and writes the new one.
    """
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)

    # Get or create worksheet
    try:
        worksheet = sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(
            title=WORKSHEET_NAME,
            rows=str(len(df) + 10),
            cols=str(len(df.columns) + 5),
        )

    # Clear existing data
    worksheet.clear()

    # Ensure column order (for safety)
    ordered_cols = [
        "Posting Date",
        "Value Date",
        "Transaction Branch",
        "Reference Number",
        "Description",
        "Debit",
        "Credit",
        "Balance",
    ]
    df = df[ordered_cols]

    # Prepare values: header row + data rows as list of lists
    values = [list(df.columns)] + df.astype(str).fillna("").values.tolist()

    # Write starting at A1
    worksheet.update("A1", values)


# =========================
# PDF EXTRACTION LOGIC
# =========================
def extract_transactions_from_pdf(uploaded_pdf, progress_callback=None) -> pd.DataFrame:
    """
    Read the uploaded PDF and return a DataFrame with columns:
    Posting Date, Value Date, Transaction Branch, Reference Number,
    Description, Debit, Credit, Balance
    """

    file_bytes = uploaded_pdf.read()
    pdf_file = io.BytesIO(file_bytes)

    records = []

    with pdfplumber.open(pdf_file) as pdf:
        total_pages = len(pdf.pages)

        for page_idx, page in enumerate(pdf.pages, start=1):
            # Update progress in Streamlit if callback is provided
            if progress_callback is not None:
                progress_callback(page_idx, total_pages)

            table = page.extract_table()
            if not table:
                continue

            # Assume first row is header, rest are data
            for row in table[1:]:
                # Make sure row has at least 8 cells
                row = (row + [""] * 8)[:8]

                posting_date, value_date, branch, ref_number, desc_, debit, credit, balance = row

                records.append(
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

    df = pd.DataFrame(records)
    return df


# =========================
# STREAMLIT APP
# =========================
st.set_page_config(page_title="Bank Statement Automation", layout="wide")

st.title("Bank Statement Automation")

st.write(
    "Upload a bank statement PDF → extract → upload to Google Sheets."
)

st.markdown(
    "**Output columns (matched to PDF):** "
    "`Posting Date`, `Value Date`, `Transaction Branch`, "
    "`Reference Number`, `Description`, `Debit`, `Credit`, `Balance`."
)

# File uploader
uploaded_pdf = st.file_uploader(
    "Upload bank statement PDF",
    type=["pdf"],
)

if uploaded_pdf:
    if st.button("Process PDF"):
        # Progress placeholders
        status_text = st.empty()
        progress_bar = st.progress(0)

        def progress_callback(current_page, total_pages):
            status_text.info(f"Reading page {current_page} of {total_pages}...")
            if total_pages > 0:
                progress_bar.progress(current_page / total_pages)

        with st.spinner("Extracting transactions from PDF..."):
            df_tx = extract_transactions_from_pdf(
                uploaded_pdf,
                progress_callback=progress_callback,
            )

        # Clear progress UI
        progress_bar.empty()

        if df_tx.empty:
            status_text.error("No transactions were found in the PDF.")
        else:
            status_text.success(
                f"Finished reading {len(df_tx)} rows from PDF."
            )

            st.subheader("Preview of extracted transactions")
            st.dataframe(df_tx.head(50))

            with st.spinner("Uploading to Google Sheets..."):
                upload_df_to_gsheet(df_tx)

            st.success(
                "Upload complete! Check the 'Raw_Data' tab in your Google Sheet."
            )
else:
    st.info("Please upload a PDF bank statement to get started.")

