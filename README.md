## PB SFTP to Google Sheets (GitHub Actions)

This repo includes a scheduled workflow at `.github/workflows/pb_sftp_to_sheets.yml` that:
- selects the latest `YYYYMMDD` folder from broker SFTP,
- downloads positions/trades CSV files into `data/raw/<asof_date>/`, and
- appends raw rows into `Raw_Positions` and `Raw_Trades` tabs.

### Required GitHub Secrets
- `PB_SFTP_PRIVATE_KEY`: OpenSSH private key text (multi-line)
- `GSHEETS_SERVICE_ACCOUNT_JSON`: Google service account JSON (multi-line)
- `GSHEETS_SPREADSHEET_ID`: Target spreadsheet ID

### Google Sheets access
Share the target spreadsheet with the service account email (`client_email` in `GSHEETS_SERVICE_ACCOUNT_JSON`) and grant **Editor** access.
