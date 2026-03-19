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

## Trade File Generator Pipeline

This repo also includes `.github/workflows/create_trade_files.yml` to generate trade files from Google Sheets `Raw_Trades` (`Raw Trades` fallback) using `Blank Template.xlsx`.

Generation rules:
- scans latest existing file date from Google Cloud Storage bucket objects (`SpringGate-TRADE-YYYYMMDD.xlsx`),
- reads `trade_date` from `Raw_Trades` (`Raw Trades` fallback) and generates missing dates only (`latest local + 1` to the second-latest raw trade date),
- fills columns `A:AE` on `TRADE` sheet per mapping defaults (Portfolio/Fund/CMSHK/etc.) and keeps row 1 header,
- uploads output as `SpringGate-TRADE-YYYYMMDD.xlsx` into the configured GCS bucket/prefix.

### Additional requirement
- `openpyxl` and `google-cloud-storage` (already listed in `requirements.txt`).
- GitHub secret `GCS_TRADE_FILES_BUCKET`: target GCS bucket name for `Trade Files`.
- Optional GitHub secret `GCS_TRADE_FILES_PREFIX`: object prefix (folder path) inside the bucket.

> The workflow continues using `GSHEETS_SERVICE_ACCOUNT_JSON`; grant this same service account write access to the target GCS bucket (e.g., `Storage Object Admin`).

## Portfolio Intelligence Workflow

This repo now includes `.github/workflows/portfolio_intelligence.yml`, scheduled daily at **01:15 UTC (09:15 SGT)**.

It runs `scripts/portfolio_intelligence.py` to:
- read holdings from Google Sheets tab `Dashboard_data`,
- clean/normalize holdings and compute snapshot metrics,
- compute concentration, market/sector exposures, and beta/alpha blocks using EODHD historical prices,
- generate PM-facing commentary and PM questions via OpenAI,
- send an HTML email report titled `Portfolio Intelligence | YYYY-MM-DD`.

### Required GitHub Secrets
- `GSHEETS_SERVICE_ACCOUNT_JSON`
- `GSHEETS_SPREADSHEET_ID`
- `EODHD_API_TOKEN`
- `OPENAI_API_KEY`
- `PORTFOLIO_INTELLIGENCE_EMAIL_LIST` (comma-separated recipients)
- `SMTP_USERNAME`
- `SMTP_PASSWORD`

### Optional GitHub Secrets
- `SMTP_HOST` (default `smtp.gmail.com`)
- `SMTP_PORT` (default `465`)
- `SMTP_SENDER` (default `SMTP_USERNAME`)
- `PORTFOLIO_INTELLIGENCE_OPENAI_MODEL` (default `gpt-5.2`)
- benchmark overrides:
  - `PORTFOLIO_INTELLIGENCE_SPX_TICKER`
  - `PORTFOLIO_INTELLIGENCE_NDX_TICKER`
  - `PORTFOLIO_INTELLIGENCE_HSI_TICKER`
  - `PORTFOLIO_INTELLIGENCE_HSTECH_TICKER`
