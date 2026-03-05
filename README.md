## Portfolio Price Enricher Pipeline

This repo includes a scheduled workflow at `.github/workflows/daily_sync.yml` that runs daily at **01:00 UTC (09:00 SGT)** and performs:
- pulls latest PB SFTP `YYYYMMDD` folder,
- appends new rows into `Raw_Positions` / `Raw_Trades` in Google Sheets,
- waits for `Holdings_Normalized` formula recalculation,
- detects new `ticker_key` (`exchange_code:product_code`) missing from `Price`,
- appends only new rows to `Price` with derived `eodhd_symbol` and fetched latest price (falling back to `Raw_Positions.market_price` when EODHD real-time is unavailable).

Existing `Price` rows are never overwritten or reordered; manual `Strategy`/`Sector` columns remain intact.

### Required GitHub Secrets
- `PB_SFTP_PRIVATE_KEY`: OpenSSH private key text (multi-line)
- `GSHEETS_SERVICE_ACCOUNT_JSON`: Google service account JSON (multi-line)
- `GSHEETS_SPREADSHEET_ID`: target spreadsheet ID
- `EODHD_API_TOKEN`: EODHD API token

### Fixed SFTP connection settings
The workflow injects the required connection values:
- `PB_SFTP_HOST_1=118.143.128.100`
- `PB_SFTP_HOST_2=202.82.167.228`
- `PB_SFTP_PORT=22`
- `PB_SFTP_USER=spring`

### Local run
```bash
export PB_SFTP_PRIVATE_KEY="$(cat /path/to/private_key)"
export GSHEETS_SERVICE_ACCOUNT_JSON='{"type":"service_account", ... }'
export GSHEETS_SPREADSHEET_ID="<spreadsheet_id>"
export EODHD_API_TOKEN="<token>"
python scripts/sync_pb_to_gsheets.py
```

> Alternatively, set `PB_SFTP_KEY_PATH` to an existing private key file path. If omitted, the script writes `PB_SFTP_PRIVATE_KEY` to a temporary file with `chmod 600` automatically.

### Google Sheets access
Share the target spreadsheet with the service account email (`client_email` inside `GSHEETS_SERVICE_ACCOUNT_JSON`) and grant **Editor** access.
