# Bank Statement Automation

A secure Streamlit application to:
- Upload a bank statement PDF
- Extract transactions
- Upload clean data to Google Sheets (Raw_Data)

## Tech used
- Streamlit
- pdfplumber
- pandas
- gspread
- Google Service Account

## Security
- No PDF stored on disk
- No data stored in memory after processing
- No credentials inside code (handled via Streamlit Secrets)
