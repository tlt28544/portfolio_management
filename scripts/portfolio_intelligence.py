#!/usr/bin/env python3
import json
import logging
import math
import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build


DASHBOARD_TAB = os.getenv("PORTFOLIO_INTELLIGENCE_DASHBOARD_TAB", "Dashboard_data")
OPENAI_MODEL = os.getenv("PORTFOLIO_INTELLIGENCE_OPENAI_MODEL", "gpt-5.2")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

BENCHMARKS = {
    "SPX": os.getenv("PORTFOLIO_INTELLIGENCE_SPX_TICKER", "GSPC.INDX"),
    "NDX": os.getenv("PORTFOLIO_INTELLIGENCE_NDX_TICKER", "NDX.INDX"),
    "HSI": os.getenv("PORTFOLIO_INTELLIGENCE_HSI_TICKER", "HSI.INDX"),
    "HSTECH": os.getenv("PORTFOLIO_INTELLIGENCE_HSTECH_TICKER", "HSTECH.INDX"),
}


def get_sheets_service(service_account_json: str):
    credentials_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def load_dashboard_data() -> pd.DataFrame:
    spreadsheet_id = os.environ["GSHEETS_SPREADSHEET_ID"]
    service = get_sheets_service(os.environ["GSHEETS_SERVICE_ACCOUNT_JSON"])
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{DASHBOARD_TAB}!A:ZZ")
        .execute()
    )
    rows = result.get("values", [])
    if not rows:
        logging.warning("No rows returned from tab %s", DASHBOARD_TAB)
        return pd.DataFrame()

    header = rows[0]
    data = rows[1:]
    df = pd.DataFrame(data, columns=header)
    logging.info("Loaded %s raw rows from %s", len(df), DASHBOARD_TAB)
    return df


def _to_numeric(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
        .replace({"": None, "nan": None, "None": None})
        .pipe(pd.to_numeric, errors="coerce")
    )


def clean_holdings(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    required_cols = [
        "Sector",
        "Company",
        "Ticker",
        "Value (USD)",
        "Weight(Portfolio)",
        "Exchange",
        "EODHD_Ticker",
    ]

    for col in required_cols:
        if col not in df.columns:
            raise RuntimeError(f"Required column missing in {DASHBOARD_TAB}: {col}")

    trimmed = df.copy()
    for col in ["Sector", "Company", "Ticker", "Exchange", "EODHD_Ticker"]:
        trimmed[col] = trimmed[col].astype(str).str.strip()

    trimmed["Value (USD)"] = _to_numeric(trimmed["Value (USD)"])
    trimmed["Weight(Portfolio)"] = _to_numeric(trimmed["Weight(Portfolio)"])

    cleaned = trimmed.dropna(subset=["Value (USD)"]).copy()
    cleaned = cleaned[cleaned["Value (USD)"] > 0]
    cleaned = cleaned[cleaned["Company"] != ""]

    if cleaned.empty:
        logging.warning("No valid holdings after cleaning")
        return cleaned

    total_value = cleaned["Value (USD)"].sum()
    cleaned["calc_weight"] = cleaned["Value (USD)"] / total_value
    cleaned["Sector"] = cleaned["Sector"].replace({"Tech": "Technology"})
    cleaned["Exchange"] = cleaned["Exchange"].str.upper()

    logging.info("Valid holdings after cleaning: %s", len(cleaned))
    return cleaned


def fetch_eodhd_close_series(ticker: str, start_date: datetime.date, end_date: datetime.date) -> Optional[pd.Series]:
    api_token = os.environ["EODHD_API_TOKEN"]
    url = f"https://eodhd.com/api/eod/{ticker}"
    params = {
        "api_token": api_token,
        "from": start_date.isoformat(),
        "to": end_date.isoformat(),
        "period": "d",
        "fmt": "json",
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or not data:
            return None
        p = pd.DataFrame(data)
        if "date" not in p.columns:
            return None
        price_col = "adjusted_close" if "adjusted_close" in p.columns else "close"
        p["date"] = pd.to_datetime(p["date"])
        p[price_col] = pd.to_numeric(p[price_col], errors="coerce")
        s = p.set_index("date")[price_col].dropna().sort_index()
        return s if not s.empty else None
    except Exception as exc:
        logging.warning("Failed to fetch EODHD history for %s: %s", ticker, exc)
        return None


def compute_exposures(holdings: pd.DataFrame, group_col: str) -> str:
    grouped = holdings.groupby(group_col, dropna=False)["Value (USD)"].sum().sort_values(ascending=False)
    total = grouped.sum()
    if total <= 0:
        return "N/A"
    return "; ".join([f"{k} {v / total * 100:.1f}%" for k, v in grouped.items()])


def compute_top_weights(holdings: pd.DataFrame) -> Tuple[float, float, str]:
    sorted_h = holdings.sort_values("calc_weight", ascending=False)
    top5 = sorted_h.head(5)["calc_weight"].sum() * 100
    top10 = sorted_h.head(10)["calc_weight"].sum() * 100
    top10_holdings = "; ".join(
        [
            f"{row['Company']} {row['calc_weight'] * 100:.2f}%"
            for _, row in sorted_h.head(10).iterrows()
        ]
    )
    return top5, top10, top10_holdings


def compute_regression_metrics(port_ret: pd.Series, bench_ret: pd.Series) -> Tuple[float, float, float]:
    aligned = pd.concat([port_ret, bench_ret], axis=1, join="inner").dropna()
    aligned.columns = ["port", "bench"]
    if len(aligned) < 30:
        raise RuntimeError(f"Insufficient aligned history for regression: {len(aligned)} rows")

    x = aligned["bench"]
    y = aligned["port"]
    beta = float(x.cov(y) / x.var()) if x.var() and not math.isclose(float(x.var()), 0.0) else float("nan")
    alpha_daily = float(y.mean() - beta * x.mean())
    r2 = float(x.corr(y) ** 2) if len(aligned) >= 2 else float("nan")
    return beta, alpha_daily, r2


def compute_universe_return_series(
    holdings: pd.DataFrame,
    start_date: datetime.date,
    end_date: datetime.date,
) -> Tuple[Optional[pd.Series], List[str], List[str]]:
    valid_tickers: List[str] = []
    missing_tickers: List[str] = []
    dropped_history: List[str] = []
    returns = []

    for _, row in holdings.iterrows():
        ticker = row["EODHD_Ticker"]
        if not ticker:
            missing_tickers.append(row["Company"])
            continue
        series = fetch_eodhd_close_series(ticker, start_date, end_date)
        if series is None or len(series) < 40:
            dropped_history.append(f"{row['Company']} ({ticker})")
            continue
        ret = series.pct_change().dropna().rename(ticker)
        returns.append(ret)
        valid_tickers.append(ticker)

    if missing_tickers:
        logging.warning("Missing EODHD tickers: %s", ", ".join(missing_tickers))
    if dropped_history:
        logging.warning("Insufficient history excluded: %s", ", ".join(dropped_history))

    if not returns:
        return None, missing_tickers, dropped_history

    returns_df = pd.concat(returns, axis=1, join="inner").dropna()
    if len(returns_df) < 30:
        return None, missing_tickers, dropped_history

    weight_map = holdings.set_index("EODHD_Ticker")["Value (USD)"]
    weight_map = weight_map[weight_map.index.isin(returns_df.columns)]
    if weight_map.empty:
        return None, missing_tickers, dropped_history

    norm_weights = weight_map / weight_map.sum()
    weighted = returns_df.mul(norm_weights, axis=1).sum(axis=1)

    if len(weighted) > 90:
        weighted = weighted.tail(90)
    return weighted, missing_tickers, dropped_history


def compute_beta_alpha_block(holdings: pd.DataFrame) -> Dict[str, str]:
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=180)

    universes = [
        (
            "US shares to SPX",
            holdings[holdings["Exchange"] == "USA"],
            BENCHMARKS["SPX"],
        ),
        (
            "US tech shares to NDX",
            holdings[(holdings["Exchange"] == "USA") & (holdings["Sector"].isin(["Technology", "Platforms"]))],
            BENCHMARKS["NDX"],
        ),
        (
            "HK shares to HSI",
            holdings[holdings["Exchange"] == "HKEX"],
            BENCHMARKS["HSI"],
        ),
        (
            "HK tech shares to HSTECH",
            holdings[(holdings["Exchange"] == "HKEX") & (holdings["Sector"].isin(["Technology", "Platforms"]))],
            BENCHMARKS["HSTECH"],
        ),
    ]

    metrics: Dict[str, str] = {}
    for label, universe, benchmark in universes:
        try:
            if universe.empty:
                metrics[label] = "N/A"
                continue
            portfolio_series, _, _ = compute_universe_return_series(universe, start_date, end_date)
            if portfolio_series is None:
                metrics[label] = "N/A"
                continue
            benchmark_price = fetch_eodhd_close_series(benchmark, start_date, end_date)
            if benchmark_price is None or len(benchmark_price) < 40:
                metrics[label] = "N/A"
                continue
            benchmark_ret = benchmark_price.pct_change().dropna()
            beta, alpha_daily, r2 = compute_regression_metrics(portfolio_series, benchmark_ret)
            alpha_annualized = ((1 + alpha_daily) ** 252 - 1) * 100
            metrics[label] = f"beta {beta:.2f} / alpha {alpha_annualized:.2f}% (annualized)"
            logging.info("%s calculated successfully (R2=%.3f)", label, r2)
        except Exception as exc:
            logging.warning("%s calculation failed: %s", label, exc)
            metrics[label] = "N/A"
    return metrics


def derive_theme_summary(holdings: pd.DataFrame) -> str:
    top = holdings.sort_values("calc_weight", ascending=False).head(12)
    lower = " ".join(top["Company"].astype(str).str.lower().tolist())
    themes = []
    if any(k in lower for k in ["nvidia", "tsmc", "broadcom", "amd", "asml"]):
        themes.append("AI infrastructure / semiconductor capex")
    if any(k in lower for k in ["tencent", "alibaba", "meituan", "jd"]):
        themes.append("China platform recovery")
    if top[top["Sector"].isin(["Technology", "Platforms"])]["calc_weight"].sum() > 0.5:
        themes.append("Growth and tech factor concentration")
    return "; ".join(themes) if themes else "No dominant deterministic theme detected"


def call_openai(prompt: str, fallback: str) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logging.warning("OPENAI_API_KEY not set; using fallback text")
        return fallback

    url = f"{OPENAI_BASE_URL.rstrip('/')}/responses"
    payload = {
        "model": OPENAI_MODEL,
        "input": prompt,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        text = data.get("output_text", "").strip()
        return text if text else fallback
    except Exception as exc:
        logging.warning("OpenAI call failed: %s", exc)
        return fallback


def build_llm_prompt_for_observations(
    holdings: pd.DataFrame,
    top10_holdings: str,
    market_exposure: str,
    sector_exposure: str,
    beta_alpha_metrics: Dict[str, str],
    themes: str,
) -> str:
    sample = holdings.sort_values("calc_weight", ascending=False).head(20)[
        ["Company", "Ticker", "Sector", "Exchange", "Value (USD)", "calc_weight"]
    ].to_dict(orient="records")
    return (
        "You are preparing a buy-side PM-facing portfolio note. English only, concise, no fluff, no exaggerated certainty. "
        "Provide 5-10 bullet points total covering: key observations, hidden bets, and risk flags.\n\n"
        f"Top holdings: {top10_holdings}\n"
        f"Market exposure: {market_exposure}\n"
        f"Sector exposure: {sector_exposure}\n"
        f"Beta/alpha metrics: {beta_alpha_metrics}\n"
        f"Deterministic theme hints: {themes}\n"
        f"Holdings sample JSON: {json.dumps(sample)}"
    )


def build_llm_prompt_for_pm_questions(
    holdings: pd.DataFrame,
    market_exposure: str,
    sector_exposure: str,
    beta_alpha_metrics: Dict[str, str],
) -> str:
    sample = holdings.sort_values("calc_weight", ascending=False).head(15)[
        ["Company", "Sector", "Exchange", "calc_weight"]
    ].to_dict(orient="records")
    return (
        "Generate 3-6 PM questions specific to this portfolio. English only. Focus on sizing, concentration, diversification, "
        "hidden factor overlap, sleeve intentionality, and fit versus intended book. Bullet points only.\n\n"
        f"Market exposure: {market_exposure}\n"
        f"Sector exposure: {sector_exposure}\n"
        f"Beta/alpha metrics: {beta_alpha_metrics}\n"
        f"Top holdings sample JSON: {json.dumps(sample)}"
    )


def render_email_html(
    asof_date: str,
    snapshot_rows: List[Tuple[str, str]],
    key_observations: str,
    pm_questions: str,
) -> str:
    table_rows = "\n".join([f"<tr><td><b>{k}</b></td><td>{v}</td></tr>" for k, v in snapshot_rows])

    def to_html_list(text: str) -> str:
        lines = [ln.strip(" -•\t") for ln in text.splitlines() if ln.strip()]
        if not lines:
            return "<p>N/A</p>"
        return "<ul>" + "".join([f"<li>{ln}</li>" for ln in lines]) + "</ul>"

    return f"""
    <html>
      <body style=\"font-family: Arial, sans-serif; font-size: 14px; color: #222;\">
        <h2>Portfolio Intelligence | {asof_date}</h2>
        <h3>1. Portfolio Snapshot</h3>
        <table border=\"1\" cellpadding=\"6\" cellspacing=\"0\" style=\"border-collapse: collapse;\">
          <tr><th align=\"left\">Item</th><th align=\"left\">Value</th></tr>
          {table_rows}
        </table>

        <h3>2. Key Observations</h3>
        {to_html_list(key_observations)}

        <h3>3. Questions for PM</h3>
        {to_html_list(pm_questions)}
      </body>
    </html>
    """.strip()


def send_email_smtp(subject: str, html_body: str) -> None:
    recipients_raw = os.getenv("PORTFOLIO_INTELLIGENCE_EMAIL_LIST", "")
    recipients = [email.strip() for email in recipients_raw.split(",") if email.strip()]
    if not recipients:
        raise RuntimeError("PORTFOLIO_INTELLIGENCE_EMAIL_LIST is empty")

    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_username = os.environ["SMTP_USERNAME"]
    smtp_password = os.environ["SMTP_PASSWORD"]
    sender = os.getenv("SMTP_SENDER", smtp_username)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    if smtp_port == 465:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as server:
            server.login(smtp_username, smtp_password)
            server.sendmail(sender, recipients, msg.as_string())
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender, recipients, msg.as_string())


def compute_snapshot_metrics(holdings: pd.DataFrame) -> Dict[str, str]:
    top5, top10, top10_holdings = compute_top_weights(holdings)
    beta_alpha = compute_beta_alpha_block(holdings)
    market_exposure = compute_exposures(holdings, "Exchange")
    sector_exposure = compute_exposures(holdings, "Sector")
    return {
        "Top 5 Weight": f"{top5:.1f}%",
        "Top 10 Weight": f"{top10:.1f}%",
        "US shares to SPX": beta_alpha.get("US shares to SPX", "N/A"),
        "US tech shares to NDX": beta_alpha.get("US tech shares to NDX", "N/A"),
        "HK shares to HSI": beta_alpha.get("HK shares to HSI", "N/A"),
        "HK tech shares to HSTECH": beta_alpha.get("HK tech shares to HSTECH", "N/A"),
        "Market Exposure": market_exposure,
        "Sector Exposure": sector_exposure,
        "Top 10 Holdings": top10_holdings,
        "_top10_holdings_raw": top10_holdings,
        "_market_exposure_raw": market_exposure,
        "_sector_exposure_raw": sector_exposure,
        "_beta_alpha_raw": beta_alpha,
    }


def run_portfolio_intelligence() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s [%(levelname)s] %(message)s")
    asof_date = datetime.now(timezone.utc).astimezone().date().isoformat()

    raw_df = load_dashboard_data()
    holdings = clean_holdings(raw_df)

    if holdings.empty:
        raise RuntimeError("No valid holdings from Dashboard_data after cleaning.")

    snapshot = compute_snapshot_metrics(holdings)
    themes = derive_theme_summary(holdings)

    observations_prompt = build_llm_prompt_for_observations(
        holdings=holdings,
        top10_holdings=snapshot["_top10_holdings_raw"],
        market_exposure=snapshot["_market_exposure_raw"],
        sector_exposure=snapshot["_sector_exposure_raw"],
        beta_alpha_metrics=snapshot["_beta_alpha_raw"],
        themes=themes,
    )

    questions_prompt = build_llm_prompt_for_pm_questions(
        holdings=holdings,
        market_exposure=snapshot["_market_exposure_raw"],
        sector_exposure=snapshot["_sector_exposure_raw"],
        beta_alpha_metrics=snapshot["_beta_alpha_raw"],
    )

    key_observations = call_openai(
        observations_prompt,
        fallback="- Key observation generation unavailable (OpenAI/API error).\n- Review concentration and factor overlap manually.",
    )
    pm_questions = call_openai(
        questions_prompt,
        fallback="- Which top positions are intentionally oversized?\n- Where is unintended factor overlap highest?\n- Which sleeve should be reduced if risk budget tightens?",
    )

    display_rows = [(k, v) for k, v in snapshot.items() if not k.startswith("_")]
    subject = f"Portfolio Intelligence | {asof_date}"
    html = render_email_html(asof_date, display_rows, key_observations, pm_questions)

    try:
        send_email_smtp(subject, html)
        logging.info("Portfolio intelligence email sent successfully")
    except Exception as exc:
        logging.error("Failed to send portfolio intelligence email: %s", exc)
        raise


if __name__ == "__main__":
    run_portfolio_intelligence()
