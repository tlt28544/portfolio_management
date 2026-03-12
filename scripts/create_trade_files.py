#!/usr/bin/env python3
import json
import os
import re
from copy import copy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from openpyxl import load_workbook

PORTFOLIO_DEFAULT = "759668"
FUND_DEFAULT = "759668S"
TRADE_STATUS_DEFAULT = "New"
INVESTMENT_TYPE_DEFAULT = "Equity"
BROKER_CODE_DEFAULT = "CMSHK"
CUSTODIAN_CODE_DEFAULT = "CMSHK"

DATE_PATTERNS = [
    "%Y%m%d",
    "%Y-%m-%d",
    "%d-%b-%Y %H:%M:%S",
    "%d-%b-%Y",
    "%m/%d/%Y",
]

ASOF_RE = re.compile(r"SpringGate-TRADE-(\d{8})\.xlsx$")


def get_sheets_service(service_account_json: str):
    credentials_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def get_values(sheets_service, spreadsheet_id: str, cell_range: str) -> List[List[str]]:
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=cell_range,
    ).execute()
    return result.get("values", [])


def parse_date(value: str) -> datetime.date:
    value = (value or "").strip()
    if not value:
        raise ValueError("Empty date value")

    for pattern in DATE_PATTERNS:
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            continue

    raise ValueError(f"Unsupported date format: {value}")


def choose_existing(*candidates: Path) -> Path:
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"None of the candidate paths exists: {[str(p) for p in candidates]}")


def choose_existing_or_default(default_path: Path, *candidates: Path) -> Path:
    for path in candidates:
        if path.exists():
            return path
    return default_path


def detect_latest_local_trade_date(trade_files_dir: Path) -> Optional[datetime.date]:
    latest: Optional[datetime.date] = None
    if not trade_files_dir.exists():
        return None

    for entry in trade_files_dir.iterdir():
        if not entry.is_file():
            continue
        match = ASOF_RE.match(entry.name)
        if not match:
            continue
        date_value = datetime.strptime(match.group(1), "%Y%m%d").date()
        if latest is None or date_value > latest:
            latest = date_value
    return latest


def get_raw_trades_rows(
    sheets_service,
    spreadsheet_id: str,
) -> Tuple[List[str], List[Dict[str, str]], str]:
    for tab in ("Raw Trades", "Raw_Trades"):
        rows = get_values(sheets_service, spreadsheet_id, f"{tab}!A:AZ")
        if not rows:
            continue
        header = [h.strip() for h in rows[0]]
        body = rows[1:]
        dict_rows = []
        for row in body:
            if not any(cell.strip() for cell in row):
                continue
            record = {header[i]: row[i] if i < len(row) else "" for i in range(len(header))}
            dict_rows.append(record)
        return header, dict_rows, tab
    raise RuntimeError("No rows found in either 'Raw Trades' or 'Raw_Trades' tab")


def market_suffix(exchange_code: str) -> str:
    mapping = {
        "HKEX": "HK",
        "SZMK": "CN",
        "SHMK": "CN",
        "SSE": "CN",
        "SZSE": "CN",
        "NYSE": "US",
        "NASDAQ": "US",
        "AMEX": "US",
        "TSE": "JP",
        "TOSE": "JP",
        "JPX": "JP",
        "KRX": "KS",
        "KOSPI": "KS",
        "KOSDAQ": "KS",
    }
    return mapping.get((exchange_code or "").strip().upper(), "")


def to_float(raw: str) -> float:
    raw = (raw or "").strip().replace(",", "")
    if raw == "":
        return 0.0
    return float(raw)


def copy_first_row_style(template_path: Path, output_path: Path) -> None:
    wb = load_workbook(template_path)
    ws = wb.active

    for col in range(1, 32):  # A..AE
        src = ws.cell(row=1, column=col)
        dst = ws.cell(row=2, column=col)
        if src.has_style:
            dst._style = copy(src._style)
        if src.number_format:
            dst.number_format = src.number_format
        if src.font:
            dst.font = copy(src.font)
        if src.fill:
            dst.fill = copy(src.fill)
        if src.border:
            dst.border = copy(src.border)
        if src.alignment:
            dst.alignment = copy(src.alignment)

    wb.save(output_path)


def write_trade_file(template_path: Path, output_path: Path, rows: Sequence[Dict[str, str]], trade_date: datetime.date) -> None:
    if not output_path.exists():
        copy_first_row_style(template_path, output_path)

    wb = load_workbook(output_path)
    ws = wb.active

    for idx, row in enumerate(rows, start=2):
        t_date = parse_date(row.get("trade_date", ""))
        s_date = parse_date(row.get("settle_date", ""))
        suffix = market_suffix(row.get("exchange_code", ""))
        product_code = (row.get("product_code") or "").strip()
        investment_code = f"{product_code} {suffix}".strip()

        bs_type = (row.get("bs_type") or "").strip().upper()
        if bs_type == "B":
            trade_type = "Buy"
        elif bs_type == "S":
            trade_type = "Sell"
        else:
            raise ValueError(f"Unsupported bs_type: {bs_type}")

        seq = f"{idx - 1:03d}"
        external_ref = f"{trade_date:%Y%m%d}_{seq}"

        other_expenses = (
            to_float(row.get("stamp_fee", ""))
            + to_float(row.get("trade_fee", ""))
            + to_float(row.get("trade_levy", ""))
            + to_float(row.get("frc_levy", ""))
            + to_float(row.get("clearing_fee", ""))
        )

        values = {
            "A": PORTFOLIO_DEFAULT,
            "B": FUND_DEFAULT,
            "C": t_date,
            "D": s_date,
            "E": s_date,
            "F": trade_type,
            "G": TRADE_STATUS_DEFAULT,
            "H": investment_code,
            "K": investment_code,
            "N": external_ref,
            "O": (row.get("product_shortname") or "").strip(),
            "P": INVESTMENT_TYPE_DEFAULT,
            "R": (row.get("trade_ccy") or "").strip(),
            "S": (row.get("trade_ccy") or "").strip(),
            "T": (row.get("trade_ccy") or "").strip(),
            "V": to_float(row.get("total_qty", "")),
            "X": to_float(row.get("avg_price", "")),
            "Y": to_float(row.get("gross_amount", "")),
            "Z": to_float(row.get("commission", "")),
            "AA": other_expenses,
            "AB": to_float(row.get("net_amount", "")),
            "AD": BROKER_CODE_DEFAULT,
            "AE": CUSTODIAN_CODE_DEFAULT,
        }

        for col, value in values.items():
            ws[f"{col}{idx}"] = value

        ws[f"A{idx}"].number_format = "@"
        ws[f"B{idx}"].number_format = "@"
        ws[f"C{idx}"].number_format = "yyyy-mm-dd"
        ws[f"D{idx}"].number_format = "yyyy-mm-dd"
        ws[f"E{idx}"].number_format = "yyyy-mm-dd"

    wb.save(output_path)


def main() -> None:
    spreadsheet_id = os.environ["GSHEETS_SPREADSHEET_ID"]
    service_account_json = os.environ["GSHEETS_SERVICE_ACCOUNT_JSON"]

    repo_root = Path(__file__).resolve().parent.parent
    template_path = choose_existing(repo_root / "main" / "Blank Template.xlsx", repo_root / "Blank Template.xlsx")
    trade_files_dir = choose_existing_or_default(
        repo_root / "Trade Files",
        repo_root / "main" / "Trade Files",
        repo_root / "Trade Files",
    )
    trade_files_dir.mkdir(parents=True, exist_ok=True)

    sheets = get_sheets_service(service_account_json)
    header, raw_rows, source_tab = get_raw_trades_rows(sheets, spreadsheet_id)
    required = [
        "trade_date",
        "settle_date",
        "bs_type",
        "exchange_code",
        "product_code",
        "product_shortname",
        "trade_ccy",
        "total_qty",
        "avg_price",
        "gross_amount",
        "commission",
        "stamp_fee",
        "trade_fee",
        "trade_levy",
        "frc_levy",
        "clearing_fee",
        "net_amount",
    ]
    missing = [c for c in required if c not in header]
    if missing:
        raise RuntimeError(f"Missing required columns in {source_tab}: {missing}")

    grouped: Dict[datetime.date, List[Dict[str, str]]] = {}
    for row in raw_rows:
        t_date = parse_date(row.get("trade_date", ""))
        grouped.setdefault(t_date, []).append(row)

    if not grouped:
        print("No raw trade rows found. Nothing to generate.")
        return

    local_latest = detect_latest_local_trade_date(trade_files_dir)
    remote_latest = max(grouped.keys())

    if local_latest is None:
        start_date = min(grouped.keys())
    else:
        start_date = local_latest + timedelta(days=1)

    target_dates = sorted(d for d in grouped.keys() if start_date <= d <= remote_latest)
    if not target_dates:
        print(f"No new dates to generate. local_latest={local_latest}, remote_latest={remote_latest}")
        return

    print(f"Generating trade files for dates: {[d.isoformat() for d in target_dates]}")

    for t_date in target_dates:
        records = grouped[t_date]
        records_sorted = sorted(
            records,
            key=lambda r: (
                parse_date(r.get("trade_date", "")).isoformat(),
                (r.get("ref_no") or "").strip(),
                int((r.get("row_no") or "0").strip() or 0),
            ),
        )
        output_name = f"SpringGate-TRADE-{t_date:%Y%m%d}.xlsx"
        output_path = trade_files_dir / output_name
        write_trade_file(template_path, output_path, records_sorted, t_date)
        print(f"Created: {output_path}")


if __name__ == "__main__":
    main()
