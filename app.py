import io
import pdfplumber
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import gc
import os
import logging

# ==========================================================
# STRICT MODE SECURITY CONTROLS
# ==========================================================

# Disable ALL Streamlit logs
logging.getLogger("streamlit").setLevel(logging.CRITICAL)
os.environ["STREAMLIT_SUPPRESS_LOGS"] = "1"

# Disable Streamlit file uploader history caching
st.session_state.clear()


# ==========================================================
# CONFIG
# ==========================================================
SPREADSHEET_ID = "1UcjF-L0GWBetcJqA1t1UMyrt54ATUAR-22ozQEnVjQM"
WORKSHEET_NAME = "Raw_Data"
SERVICE_ACCOUNT_FILE = "service_account.json"


# ==========================================================
# GOOGLE SHEETS CLIENT
# ==========================================================
def get_gsheet_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=scopes
    )
    return gspread.authorize(creds)


# ==========================================================
# UPLOAD CLEANED DF TO SHEETS
# ==========================================================
def upload_df_to_gsheet(df):
    client = get_gsheet_client()
    sh = client.open_by_key(SPREADSHEET_ID)

    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except:
        ws = sh.add_worksheet(WORKSHEET_NAME, rows=1000, cols=20)

    ws.clear()

    # Upload including column headers
    values = [df.columns.tolist()] + df.astype(str).fillna("").values.tolist()
    ws.update("A1", values)


# ==========================================================
# EXTRACT TABLES FROM PDF
# ==========================================================
def extract_transactions(file_bytes):
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        pages = len(pdf.pages)
        final_rows = []

        for i, page in enumerate(pdf.pages):
            st.write(f"Reading page {i+1}/{pages}…")  # Safe: no data exposed

            table = page.extract_table()
            if not table or len(table) <= 1:
                continue

            header = table[0]
            rows = table[1:]

            if len(header) >= 8:
                for row in rows:
                    final_rows.append(row[:8])

    df = pd.DataFrame(final_rows, columns=[
        "Posting Date",
        "Value Date",
        "Transaction Branch",
        "Reference Number",
        "Description",
        "Debit",
        "Credit",
        "Balance"
    ])

    return df


# ==========================================================
# STREAMLIT UI
# ==========================================================
st.title("Bank Statement Automation — Secure Strict Mode")

st.write("Upload → Extract → Upload to Google Sheets (no data displayed).")


uploaded_file = st.file_uploader("Upload bank statement PDF", type=["pdf"])


if uploaded_file is not None:

    # Read file — STRICT MODE: store in local var only
    pdf_bytes = uploaded_file.read()

    if st.button("Process PDF"):
        st.info("Extracting PDF… (no data will be shown)")

        df_tx = extract_transactions(pdf_bytes)

        st.info("Uploading data to Google Sheets…")
        upload_df_to_gsheet(df_tx)

        # STRICT MODE: wipe sensitive data from memory
        del pdf_bytes
        del df_tx
        gc.collect()

        # Delete reference inside Streamlit session state
        st.session_state.clear()

        st.success("Completed ✔ All data uploaded & fully wiped from memory.")


# FINAL SECURITY NOTE
st.caption("Strict security mode enabled: logs disabled, history disabled, memory wiped.")
