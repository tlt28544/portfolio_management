#!/usr/bin/env python3
import csv
import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from stat import S_ISDIR
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote

import paramiko
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

PB_SFTP_HOST_1 = os.environ.get("PB_SFTP_HOST_1", "118.143.128.100")
PB_SFTP_HOST_2 = os.environ.get("PB_SFTP_HOST_2", "202.82.167.228")
PB_SFTP_PORT = int(os.environ.get("PB_SFTP_PORT", "22"))
PB_SFTP_USER = os.environ.get("PB_SFTP_USER", "spring")

POSITIONS_PREFIX = "D0400_Client_Position_Report_"
TRADES_PREFIX = "C0311_Daily_securities_transaction_"

PRICE_COLUMNS = [
    "ticker_key",
    "exchange_code",
    "product_code",
    "product_name",
    "eodhd_symbol",
    "price",
    "last_update_utc",
    "Strategy",
    "Sector",
]

TRADES_COLUMNS = [
    "is_avg_price", "channel_code", "company_code", "bs_type", "exchange_code", "product_code", "product_shortname", "input_date", "trade_date", "settle_date", "ref_no", "row_no", "account_no", "account_name", "account_name2", "ae_code", "ae_name", "ae_team_id", "total_qty", "trade_ccy", "avg_price", "gross_amount", "net_amount", "commission", "stamp_fee", "trade_fee", "trade_levy", "frc_levy", "clearing_fee", "bond_interest", "other_fees", "trade_price_dec_pt", "trade_qty_dec_pt", "avg_price_dec_pt", "rebate_amount", "sum_trade_ccy", "sum_gross_amount", "sum_net_amount", "sum_commission", "sum_rebate_amount", "min_row_no"
]

POSITIONS_COLUMNS = [
    "row_no", "chk_status", "company_code", "company_name", "account_no", "account_name", "account_name2", "account_tel1", "account_tel2", "account_mobile", "ae_code", "ae_name", "ae_name2", "ae_team_id", "ae_team_name", "ae_team_name2", "trade_limit", "trade_ccy", "credit_limit", "credit_ccy", "exposure_limit", "exposure_ccy", "ccy", "avail_bal", "pending_withdrawal", "t1_ledger_bal", "tn_ledger_bal", "acc_dr_int", "acc_cr_int", "exchange_code", "product_code", "product_name", "is_suspend", "avail_qty", "undue_qty", "on_hold_qty", "ledger_qty", "product_ccy", "avg_cost", "market_price", "ledger_market_val", "margin_rate", "margin_value", "sys_ccy", "fx_rate", "main_account_type", "unrealized_pl", "avg_price_dec_pt", "trade_price_dec_pt", "trade_qty_dec_pt", "invest_suitability_class", "invest_suit_en", "invest_suit_tc", "invest_suit_remark", "product_invest_suitability_class", "product_invest_suitability_label", "bank_acct_no", "bank_acct_name", "tax_bracket"
]

FILE_ACCOUNT_RE = re.compile(r"_(\d+)\.csv$", re.IGNORECASE)
FOLDER_RE = re.compile(r"^\d{8}$")


def get_sheets_service(service_account_json: str):
    credentials_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def connect_sftp(private_key_path: str) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient, str]:
    errors = []
    for host in (PB_SFTP_HOST_1, PB_SFTP_HOST_2):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                hostname=host,
                port=PB_SFTP_PORT,
                username=PB_SFTP_USER,
                key_filename=private_key_path,
                look_for_keys=False,
                allow_agent=False,
                timeout=30,
            )
            sftp = client.open_sftp()
            print(f"Connected to SFTP host: {host}")
            return client, sftp, host
        except Exception as exc:
            errors.append(f"{host}: {exc}")
            client.close()
    raise RuntimeError("Failed to connect to both SFTP hosts. " + " | ".join(errors))


def select_latest_asof_folder(sftp: paramiko.SFTPClient) -> Optional[str]:
    folders = []
    for entry in sftp.listdir_attr("."):
        if FOLDER_RE.match(entry.filename) and S_ISDIR(entry.st_mode):
            folders.append(entry.filename)
    if not folders:
        return None
    return max(folders)


def list_matching_files(sftp: paramiko.SFTPClient, folder: str, prefix: str) -> List[str]:
    names = []
    for entry in sftp.listdir_attr(folder):
        filename = entry.filename
        if filename.startswith(prefix) and filename.lower().endswith(".csv"):
            names.append(filename)
    return sorted(names)


def get_values(sheets_service, spreadsheet_id: str, cell_range: str) -> List[List[str]]:
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=cell_range,
    ).execute()
    return result.get("values", [])


def append_values(sheets_service, spreadsheet_id: str, cell_range: str, rows: List[List[str]]) -> None:
    if not rows:
        return
    sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=cell_range,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def update_values(sheets_service, spreadsheet_id: str, cell_range: str, rows: List[List[str]]) -> None:
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=cell_range,
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()


def ensure_tab_header(sheets_service, spreadsheet_id: str, tab_name: str, header: List[str]) -> None:
    rows = get_values(sheets_service, spreadsheet_id, f"{tab_name}!1:1")
    if not rows:
        append_values(sheets_service, spreadsheet_id, f"{tab_name}!A1", [header])
        print(f"Header initialized for {tab_name}")
        return

    existing = rows[0]
    if existing[: len(header)] != header:
        update_values(sheets_service, spreadsheet_id, f"{tab_name}!A1", [header])
        print(f"Header updated for {tab_name}")


def fetch_existing_file_keys(sheets_service, spreadsheet_id: str, tab_name: str) -> Set[Tuple[str, str]]:
    rows = get_values(sheets_service, spreadsheet_id, f"{tab_name}!A:C")
    existing = set()
    for row in rows[1:]:
        asof = row[0].strip() if len(row) > 0 else ""
        source_file = row[2].strip() if len(row) > 2 else ""
        if asof and source_file:
            existing.add((asof, source_file))
    return existing


def parse_account_no(row: Dict[str, str], source_file: str) -> str:
    from_csv = (row.get("account_no") or "").strip()
    if from_csv:
        return from_csv
    match = FILE_ACCOUNT_RE.search(source_file)
    return match.group(1) if match else ""


def normalize_row(row: Dict[str, str], ordered_columns: Sequence[str]) -> List[str]:
    return [str(row.get(col, "")) for col in ordered_columns]


def batched(values: List[List[str]], size: int = 500) -> Iterable[List[List[str]]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def download_and_prepare_rows(
    sftp: paramiko.SFTPClient,
    asof_date: str,
    folder: str,
    filenames: List[str],
    expected_columns: List[str],
    target_tab: str,
    existing_keys: Set[Tuple[str, str]],
    out_dir: Path,
) -> Tuple[List[List[str]], int]:
    all_rows: List[List[str]] = []
    skipped_files = 0
    for filename in filenames:
        key = (asof_date, filename)
        if key in existing_keys:
            skipped_files += 1
            print(f"Skipping duplicate file for {target_tab}: {filename}")
            continue

        remote_path = f"{folder}/{filename}"
        local_path = out_dir / filename
        sftp.get(remote_path, str(local_path))
        print(f"Downloaded: {remote_path} -> {local_path}")

        with local_path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != expected_columns:
                raise RuntimeError(
                    f"Unexpected CSV header in {filename}.\nExpected: {expected_columns}\nActual: {reader.fieldnames}"
                )
            for row in reader:
                account_no = parse_account_no(row, filename)
                all_rows.append([asof_date, account_no, filename, *normalize_row(row, expected_columns)])

    return all_rows, skipped_files


def holdings_snapshot(sheets_service, spreadsheet_id: str) -> Tuple[int, str]:
    rows = get_values(sheets_service, spreadsheet_id, "Holdings_Normalized!A:Z")
    if len(rows) <= 1:
        return 0, ""

    header = [h.strip() for h in rows[0]]
    try:
        asof_idx = header.index("asof_date")
    except ValueError:
        return len(rows) - 1, ""

    max_asof = ""
    for row in rows[1:]:
        if asof_idx < len(row):
            value = row[asof_idx].strip()
            if value and value > max_asof:
                max_asof = value
    return len(rows) - 1, max_asof


def wait_for_holdings_recalc(sheets_service, spreadsheet_id: str, expected_asof: str, timeout_seconds: int = 60) -> None:
    initial_count, initial_asof = holdings_snapshot(sheets_service, spreadsheet_id)
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        count, asof = holdings_snapshot(sheets_service, spreadsheet_id)
        if count != initial_count or (expected_asof and asof >= expected_asof and asof != initial_asof):
            print(f"Holdings_Normalized refreshed (count={count}, max_asof={asof})")
            return
        time.sleep(5)
    print("Holdings recalculation wait timeout reached; proceeding with current sheet values")


def build_ticker_key(exchange_code: str, product_code: str) -> str:
    return f"{exchange_code}:{product_code}"


def derive_eodhd_symbol(exchange_code: str, product_code: str) -> str:
    exchange_code = (exchange_code or "").strip().upper()
    product_code = (product_code or "").strip().upper()

    if not exchange_code or not product_code:
        return ""
    if exchange_code == "USA":
        return f"{product_code}.US"
    if exchange_code == "HKEX":
        digits = re.sub(r"\D", "", product_code)
        if not digits:
            return ""
        return f"{int(digits)}.HK"
    if exchange_code == "MAMK":
        return f"{product_code}.SH"
    if exchange_code == "SZMK":
        return f"{product_code}.SZ"
    if exchange_code == "JPY":
        digits = re.sub(r"\D", "", product_code)
        return f"{digits}.T" if digits else ""
    if exchange_code == "KRW":
        digits = re.sub(r"\D", "", product_code)
        return f"{digits}.KO" if digits else ""
    return ""


def fetch_eodhd_price(symbol: str, token: str) -> Optional[float]:
    if not symbol:
        return None
    url = f"https://eodhd.com/api/real-time/{quote(symbol)}"
    try:
        response = requests.get(url, params={"api_token": token, "fmt": "json"}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        value = payload.get("close")
        if value is None:
            value = payload.get("price")
        if value is None:
            return None
        return float(value)
    except Exception as exc:
        print(f"Warning: failed to fetch EODHD for {symbol}: {exc}")
        return None


def get_latest_holdings_tickers(sheets_service, spreadsheet_id: str) -> List[Tuple[str, str, str, str]]:
    rows = get_values(sheets_service, spreadsheet_id, "Holdings_Normalized!A:Z")
    if len(rows) <= 1:
        return []

    header = [h.strip() for h in rows[0]]

    def idx(name: str) -> Optional[int]:
        return header.index(name) if name in header else None

    exch_i = idx("exchange_code")
    code_i = idx("product_code")
    name_i = idx("product_name")
    asof_i = idx("asof_date")

    if exch_i is None or code_i is None:
        raise RuntimeError("Holdings_Normalized must contain exchange_code and product_code columns")

    latest_asof = ""
    if asof_i is not None:
        for row in rows[1:]:
            if asof_i < len(row):
                value = row[asof_i].strip()
                if value and value > latest_asof:
                    latest_asof = value

    unique: Dict[str, Tuple[str, str, str, str]] = {}
    for row in rows[1:]:
        if asof_i is not None and latest_asof:
            if asof_i >= len(row) or row[asof_i].strip() != latest_asof:
                continue

        exchange_code = row[exch_i].strip() if exch_i < len(row) else ""
        product_code = row[code_i].strip() if code_i < len(row) else ""
        product_name = row[name_i].strip() if (name_i is not None and name_i < len(row)) else ""
        if not exchange_code or not product_code:
            continue
        ticker_key = build_ticker_key(exchange_code, product_code)
        unique[ticker_key] = (ticker_key, exchange_code, product_code, product_name)

    return sorted(unique.values(), key=lambda item: item[0])


def get_existing_price_tickers(sheets_service, spreadsheet_id: str) -> Set[str]:
    rows = get_values(sheets_service, spreadsheet_id, "Price!A:A")
    return {row[0].strip() for row in rows[1:] if row and row[0].strip()}


def enrich_price_tab(sheets_service, spreadsheet_id: str, eodhd_token: str) -> int:
    ensure_tab_header(sheets_service, spreadsheet_id, "Price", PRICE_COLUMNS)

    holdings_tickers = get_latest_holdings_tickers(sheets_service, spreadsheet_id)
    existing_tickers = get_existing_price_tickers(sheets_service, spreadsheet_id)

    utc_now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    rows_to_append: List[List[str]] = []
    for ticker_key, exchange_code, product_code, product_name in holdings_tickers:
        if ticker_key in existing_tickers:
            continue

        symbol = derive_eodhd_symbol(exchange_code, product_code)
        price = fetch_eodhd_price(symbol, eodhd_token) if symbol and eodhd_token else None
        rows_to_append.append([
            ticker_key,
            exchange_code,
            product_code,
            product_name,
            symbol,
            "" if price is None else price,
            utc_now,
            "",
            "",
        ])

    if not rows_to_append:
        print("No new tickers to append to Price tab")
        return 0

    append_values(sheets_service, spreadsheet_id, "Price!A1", rows_to_append)
    print(f"Price tab: appended {len(rows_to_append)} new ticker rows")
    return len(rows_to_append)


def sync_raw_tabs(
    sheets_service,
    spreadsheet_id: str,
    private_key_path: str,
) -> Optional[str]:
    ssh_client = None
    sftp = None
    try:
        ssh_client, sftp, host = connect_sftp(private_key_path)
        asof_date = select_latest_asof_folder(sftp)
        if not asof_date:
            print("No YYYYMMDD folders found in SFTP root; skipping raw sync")
            return None

        print(f"Selected asof_date folder: {asof_date} (host={host})")
        positions_files = list_matching_files(sftp, asof_date, POSITIONS_PREFIX)
        trades_files = list_matching_files(sftp, asof_date, TRADES_PREFIX)
        print(f"Positions files: {positions_files}")
        print(f"Trades files: {trades_files}")

        out_dir = Path("data/raw") / asof_date
        out_dir.mkdir(parents=True, exist_ok=True)

        ensure_tab_header(sheets_service, spreadsheet_id, "Raw_Positions", ["asof_date", "account_no", "source_file", *POSITIONS_COLUMNS])
        ensure_tab_header(sheets_service, spreadsheet_id, "Raw_Trades", ["asof_date", "account_no", "source_file", *TRADES_COLUMNS])

        existing_positions = fetch_existing_file_keys(sheets_service, spreadsheet_id, "Raw_Positions")
        pos_rows, pos_skipped = download_and_prepare_rows(
            sftp, asof_date, asof_date, positions_files, POSITIONS_COLUMNS, "Raw_Positions", existing_positions, out_dir
        )

        existing_trades = fetch_existing_file_keys(sheets_service, spreadsheet_id, "Raw_Trades")
        tr_rows, tr_skipped = download_and_prepare_rows(
            sftp, asof_date, asof_date, trades_files, TRADES_COLUMNS, "Raw_Trades", existing_trades, out_dir
        )

        for chunk in batched(pos_rows, 500):
            append_values(sheets_service, spreadsheet_id, "Raw_Positions!A1", chunk)
        for chunk in batched(tr_rows, 500):
            append_values(sheets_service, spreadsheet_id, "Raw_Trades!A1", chunk)

        print(f"Raw_Positions: appended_rows={len(pos_rows)}, skipped_files={pos_skipped}")
        print(f"Raw_Trades: appended_rows={len(tr_rows)}, skipped_files={tr_skipped}")

        return asof_date
    finally:
        if sftp:
            sftp.close()
        if ssh_client:
            ssh_client.close()


def ensure_private_key_file() -> str:
    path = os.environ.get("PB_SFTP_KEY_PATH", "").strip()
    if path:
        return path

    private_key = os.environ.get("PB_SFTP_PRIVATE_KEY", "").strip()
    if not private_key:
        raise RuntimeError("Either PB_SFTP_KEY_PATH or PB_SFTP_PRIVATE_KEY must be provided")

    temp = tempfile.NamedTemporaryFile(mode="w", prefix="pb_sftp_", suffix=".key", delete=False)
    temp.write(private_key + "\n")
    temp.flush()
    temp.close()
    os.chmod(temp.name, 0o600)
    return temp.name


def main() -> None:
    gsheet_sa_json = os.environ.get("GSHEETS_SERVICE_ACCOUNT_JSON", "").strip()
    spreadsheet_id = os.environ.get("GSHEETS_SPREADSHEET_ID", "").strip()
    eodhd_token = os.environ.get("EODHD_API_TOKEN", "").strip()

    if not gsheet_sa_json:
        raise RuntimeError("GSHEETS_SERVICE_ACCOUNT_JSON env var is required")
    if not spreadsheet_id:
        raise RuntimeError("GSHEETS_SPREADSHEET_ID env var is required")

    private_key_path = ensure_private_key_file()
    sheets_service = get_sheets_service(gsheet_sa_json)

    asof_date = sync_raw_tabs(sheets_service, spreadsheet_id, private_key_path)
    if asof_date:
        wait_for_holdings_recalc(sheets_service, spreadsheet_id, asof_date)

    enrich_price_tab(sheets_service, spreadsheet_id, eodhd_token)


if __name__ == "__main__":
    main()
