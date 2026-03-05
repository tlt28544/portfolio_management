#!/usr/bin/env python3
import csv
import json
import os
import re
from pathlib import Path
from stat import S_ISDIR
from typing import Iterable, List, Set, Tuple

import paramiko
from google.oauth2 import service_account
from googleapiclient.discovery import build

PB_SFTP_HOST_1 = "118.143.128.100"
PB_SFTP_HOST_2 = "202.82.167.228"
PB_SFTP_PORT = 22
PB_SFTP_USER = "spring"

POSITIONS_PREFIX = "D0400_Client_Position_Report_"
TRADES_PREFIX = "C0311_Daily_securities_transaction_"

TRADES_COLUMNS = [
    "is_avg_price", "channel_code", "company_code", "bs_type", "exchange_code", "product_code", "product_shortname", "input_date", "trade_date", "settle_date", "ref_no", "row_no", "account_no", "account_name", "account_name2", "ae_code", "ae_name", "ae_team_id", "total_qty", "trade_ccy", "avg_price", "gross_amount", "net_amount", "commission", "stamp_fee", "trade_fee", "trade_levy", "frc_levy", "clearing_fee", "bond_interest", "other_fees", "trade_price_dec_pt", "trade_qty_dec_pt", "avg_price_dec_pt", "rebate_amount", "sum_trade_ccy", "sum_gross_amount", "sum_net_amount", "sum_commission", "sum_rebate_amount", "min_row_no"
]

POSITIONS_COLUMNS = [
    "row_no", "chk_status", "company_code", "company_name", "account_no", "account_name", "account_name2", "account_tel1", "account_tel2", "account_mobile", "ae_code", "ae_name", "ae_name2", "ae_team_id", "ae_team_name", "ae_team_name2", "trade_limit", "trade_ccy", "credit_limit", "credit_ccy", "exposure_limit", "exposure_ccy", "ccy", "avail_bal", "pending_withdrawal", "t1_ledger_bal", "tn_ledger_bal", "acc_dr_int", "acc_cr_int", "exchange_code", "product_code", "product_name", "is_suspend", "avail_qty", "undue_qty", "on_hold_qty", "ledger_qty", "product_ccy", "avg_cost", "market_price", "ledger_market_val", "margin_rate", "margin_value", "sys_ccy", "fx_rate", "main_account_type", "unrealized_pl", "avg_price_dec_pt", "trade_price_dec_pt", "trade_qty_dec_pt", "invest_suitability_class", "invest_suit_en", "invest_suit_tc", "invest_suit_remark", "product_invest_suitability_class", "product_invest_suitability_label", "bank_acct_no", "bank_acct_name", "tax_bracket"
]


FILE_ACCOUNT_RE = re.compile(r"_(\d+)\.csv$", re.IGNORECASE)
FOLDER_RE = re.compile(r"^\d{8}$")


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


def select_latest_asof_folder(sftp: paramiko.SFTPClient) -> str:
    folders = []
    for entry in sftp.listdir_attr("."):
        if FOLDER_RE.match(entry.filename) and S_ISDIR(entry.st_mode):
            folders.append(entry.filename)
    if not folders:
        raise RuntimeError("No YYYYMMDD folders found in SFTP root")
    return max(folders)


def list_matching_files(sftp: paramiko.SFTPClient, folder: str, prefix: str) -> List[str]:
    names = []
    for entry in sftp.listdir_attr(folder):
        filename = entry.filename
        if filename.startswith(prefix) and filename.lower().endswith(".csv"):
            names.append(filename)
    return sorted(names)


def parse_account_no(row: dict, source_file: str) -> str:
    from_csv = (row.get("account_no") or "").strip()
    if from_csv:
        return from_csv
    match = FILE_ACCOUNT_RE.search(source_file)
    return match.group(1) if match else ""


def get_sheets_service(service_account_json: str):
    credentials_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def fetch_existing_file_keys(sheets_service, spreadsheet_id: str, tab_name: str) -> Set[Tuple[str, str]]:
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!A:C")
        .execute()
    )
    rows = result.get("values", [])
    existing = set()
    for row in rows[1:]:
        asof = row[0] if len(row) > 0 else ""
        source_file = row[2] if len(row) > 2 else ""
        if asof and source_file:
            existing.add((asof, source_file))
    return existing


def is_tab_empty(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!1:1")
        .execute()
    )
    return len(result.get("values", [])) == 0


def write_header_if_needed(sheets_service, spreadsheet_id: str, tab_name: str, csv_columns: List[str]) -> None:
    if is_tab_empty(sheets_service, spreadsheet_id, tab_name):
        header = [["asof_date", "account_no", "source_file", *csv_columns]]
        (
            sheets_service.spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": header},
            )
            .execute()
        )
        print(f"Header written to {tab_name}")


def batched(values: List[List[str]], size: int = 500) -> Iterable[List[List[str]]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def append_rows(sheets_service, spreadsheet_id: str, tab_name: str, rows: List[List[str]]) -> int:
    total = 0
    for chunk in batched(rows, 500):
        (
            sheets_service.spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": chunk},
            )
            .execute()
        )
        total += len(chunk)
    return total


def normalize_row(row: dict, ordered_columns: List[str]) -> List[str]:
    return [str(row.get(col, "")) for col in ordered_columns]


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
    all_rows = []
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
                    f"Unexpected CSV header in {filename}.\n"
                    f"Expected: {expected_columns}\n"
                    f"Actual:   {reader.fieldnames}"
                )
            for row in reader:
                account_no = parse_account_no(row, filename)
                out_row = [asof_date, account_no, filename, *normalize_row(row, expected_columns)]
                all_rows.append(out_row)

    return all_rows, skipped_files


def main() -> None:
    private_key_path = os.environ.get("PB_SFTP_KEY_PATH", "").strip()
    gsheet_sa_json = os.environ.get("GSHEETS_SERVICE_ACCOUNT_JSON", "").strip()
    spreadsheet_id = os.environ.get("GSHEETS_SPREADSHEET_ID", "").strip()

    if not private_key_path:
        raise RuntimeError("PB_SFTP_KEY_PATH env var is required")
    if not gsheet_sa_json:
        raise RuntimeError("GSHEETS_SERVICE_ACCOUNT_JSON env var is required")
    if not spreadsheet_id:
        raise RuntimeError("GSHEETS_SPREADSHEET_ID env var is required")

    ssh_client = None
    sftp = None
    try:
        ssh_client, sftp, host = connect_sftp(private_key_path)
        asof_date = select_latest_asof_folder(sftp)
        print(f"Selected asof_date folder: {asof_date} (host={host})")

        positions_files = list_matching_files(sftp, asof_date, POSITIONS_PREFIX)
        trades_files = list_matching_files(sftp, asof_date, TRADES_PREFIX)

        if not positions_files:
            raise RuntimeError(f"No required positions files found in {asof_date}")

        print(f"Positions files: {positions_files}")
        print(f"Trades files (optional): {trades_files}")

        out_dir = Path("data/raw") / asof_date
        out_dir.mkdir(parents=True, exist_ok=True)

        sheets_service = get_sheets_service(gsheet_sa_json)

        # Positions
        write_header_if_needed(sheets_service, spreadsheet_id, "Raw_Positions", POSITIONS_COLUMNS)
        existing_positions = fetch_existing_file_keys(sheets_service, spreadsheet_id, "Raw_Positions")
        pos_rows, pos_skipped = download_and_prepare_rows(
            sftp,
            asof_date,
            asof_date,
            positions_files,
            POSITIONS_COLUMNS,
            "Raw_Positions",
            existing_positions,
            out_dir,
        )
        pos_appended = append_rows(sheets_service, spreadsheet_id, "Raw_Positions", pos_rows) if pos_rows else 0

        # Trades (optional)
        write_header_if_needed(sheets_service, spreadsheet_id, "Raw_Trades", TRADES_COLUMNS)
        existing_trades = fetch_existing_file_keys(sheets_service, spreadsheet_id, "Raw_Trades")
        tr_rows, tr_skipped = download_and_prepare_rows(
            sftp,
            asof_date,
            asof_date,
            trades_files,
            TRADES_COLUMNS,
            "Raw_Trades",
            existing_trades,
            out_dir,
        )
        tr_appended = append_rows(sheets_service, spreadsheet_id, "Raw_Trades", tr_rows) if tr_rows else 0

        print(f"Raw_Positions: appended_rows={pos_appended}, skipped_files={pos_skipped}")
        print(f"Raw_Trades: appended_rows={tr_appended}, skipped_files={tr_skipped}")

    finally:
        if sftp:
            sftp.close()
        if ssh_client:
            ssh_client.close()


if __name__ == "__main__":
    main()
