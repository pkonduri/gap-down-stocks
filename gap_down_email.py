#!/usr/bin/env python3
"""
Daily Gap-Down Emailer
----------------------
Finds stocks that gapped down by X% or more at the regular session open and emails you a list.

Data sources:
- Polygon.io (recommended; accurate open/prev close, broad universe)
- Yahoo Finance fallback via yfinance (free; use your own ticker universe)

Email:
- SendGrid (simple; requires SENDGRID_API_KEY)

Suggested schedule:
- Run at 9:35 AM US/Eastern (6:35 AM Pacific) so the official open price is available.

Usage:
  1) Copy .env.example to .env and fill values.
  2) pip install -r requirements.txt
  3) python gap_down_email.py

Environment (.env):
  DATA_SOURCE=polygon|yahoo
  # If DATA_SOURCE=polygon
  POLYGON_API_KEY=pk_xxx
  # If DATA_SOURCE=yahoo (fallback), provide tickers via CSV or use all exchanges
  TICKERS_CSV=tickers_sample.csv|sp500_tickers.csv|all_exchanges
  # For all_exchanges, you need:
  FINANCIAL_MODELING_PREP_API_KEY=your_fmp_api_key

  MIN_GAP_DOWN_PCT=-5    # negative number, e.g., -5 means -5% or worse
  MIN_GAP_UP_PCT=1      # positive number, e.g., 1 means +1% or better
  MIN_MARKET_CAP=3000000000   # e.g., 3B (only used when possible; polygon ref data)
  ONLY_OPTIONABLE=false   # true/false; polygon-only filter

  RESEND_API_KEY=re_xxxxxx
  EMAIL_FROM=you@example.com
  EMAIL_TO=you@example.com
  EMAIL_SUBJECT_PREFIX=[Gap Down]

Notes:
- Polygon filters (market cap, optionable) applied when possible.
- Yahoo fallback cannot filter market cap/optionable unless you pre-filter your CSV universe.
- Use TICKERS_CSV=all_exchanges to scan all NYSE/NASDAQ stocks (~11k tickers) via Financial Modeling Prep API.
- Large ticker lists are processed with progress tracking and rate limiting to avoid API issues.
- Email includes 3 sections: Gap Down stocks, Gap Up stocks, and CSV attachment with all data.
"""
import os
import sys
import math
import time
import json
import csv
from datetime import datetime, date, timedelta, timezone

from dotenv import load_dotenv

MINUTE = 60

def pct(x):
    return f"{x:.2f}%"

def load_env():
    load_dotenv()
    print("Loading environment variables...")
    
    cfg = {
        "DATA_SOURCE": os.getenv("DATA_SOURCE", "polygon").lower(),
        "POLYGON_API_KEY": os.getenv("POLYGON_API_KEY", ""),
        "TICKERS_CSV": os.getenv("TICKERS_CSV", "tickers_sample.csv"),
        "MIN_GAP_DOWN_PCT": float(os.getenv("MIN_GAP_DOWN_PCT", "-5")),
        "MIN_GAP_UP_PCT": float(os.getenv("MIN_GAP_UP_PCT", "1")),
        "MIN_MARKET_CAP": float(os.getenv("MIN_MARKET_CAP", "3000000000")),
        "ONLY_OPTIONABLE": os.getenv("ONLY_OPTIONABLE", "false").lower() == "true",
        "RESEND_API_KEY": os.getenv("RESEND_API_KEY", ""),
        "FINANCIAL_MODELING_PREP_API_KEY": os.getenv("FINANCIAL_MODELING_PREP_API_KEY", ""),
        "EMAIL_FROM": os.getenv("EMAIL_FROM", ""),
        "EMAIL_TO": os.getenv("EMAIL_TO", ""),
        "EMAIL_SUBJECT_PREFIX": os.getenv("EMAIL_SUBJECT_PREFIX", "[Gap Down]"),
    }
    
    # Print config status (without revealing actual keys)
    print(f"DATA_SOURCE: {cfg['DATA_SOURCE']}")
    print(f"TICKERS_CSV: {cfg['TICKERS_CSV']}")
    print(f"RESEND_API_KEY: {'SET' if cfg['RESEND_API_KEY'] else 'NOT SET'}")
    print(f"EMAIL_FROM: {'SET' if cfg['EMAIL_FROM'] else 'NOT SET'}")
    print(f"EMAIL_TO: {'SET' if cfg['EMAIL_TO'] else 'NOT SET'}")
    
    # Basic checks
    if not cfg["RESEND_API_KEY"] or not cfg["EMAIL_FROM"] or not cfg["EMAIL_TO"]:
        print("ERROR: Missing RESEND_API_KEY/EMAIL_FROM/EMAIL_TO in .env", file=sys.stderr)
        print(f"RESEND_API_KEY present: {bool(cfg['RESEND_API_KEY'])}")
        print(f"EMAIL_FROM present: {bool(cfg['EMAIL_FROM'])}")
        print(f"EMAIL_TO present: {bool(cfg['EMAIL_TO'])}")
        sys.exit(2)
    return cfg

def get_all_exchange_tickers(cfg):
    """Fetch all tickers from major US exchanges using Financial Modeling Prep API"""
    import requests

    api_key = cfg["FINANCIAL_MODELING_PREP_API_KEY"]
    if not api_key:
        print("ERROR: FINANCIAL_MODELING_PREP_API_KEY required for fetching all exchange tickers", file=sys.stderr)
        sys.exit(2)

    # Important exchanges (from your stocks repo)
    important_exchanges = [
        "NASDAQ",
        "Nasdaq",
        "NASDAQ Global Select",
        "NASDAQ Global Market",
        "NASDAQ Capital Market",
        "New York Stock Exchange",
        "New York Stock Exchange Arca",
    ]

    url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={api_key}"

    try:
        print("Fetching all exchange tickers from Financial Modeling Prep...")
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            # Filter stocks from important exchanges and by ticker criteria
            filtered_stocks = [
                item["symbol"] for item in data
                if item["exchange"] in important_exchanges and item["symbol"].isalpha() and len(item["symbol"]) <= 4
            ]
            print(f"Found {len(filtered_stocks)} tickers from major exchanges")
            return filtered_stocks
        else:
            print(f"Error fetching stock data: {response.status_code}")
            return []

    except Exception as e:
        print(f"Error fetching exchange tickers: {e}")
        return []

def polygon_gap_scan(cfg):
    import requests

    key = cfg["POLYGON_API_KEY"]
    if not key:
        print("ERROR: POLYGON_API_KEY required for polygon data source", file=sys.stderr)
        sys.exit(2)

    # Get previous trading day
    # Use Polygon "marketstatus" or just compute yesterday and adjust for weekends/holidays.
    # For simplicity, ask Polygon for previous trading day via /v1/marketstatus/now
    now = datetime.now(timezone.utc)
    status = requests.get("https://api.polygon.io/v1/marketstatus/now", params={"apiKey": key}).json()
    prev_trading_day = status.get("prev", {}).get("market", None)
    # If not provided, fallback to yesterday
    if not prev_trading_day:
        prev_trading_day = (now - timedelta(days=1)).date().isoformat()

    today_str = now.date().isoformat()

    # Step 1: Get today's daily open for all tickers that have data today
    # Polygon aggregates: /v2/aggs/grouped/locale/us/market/stocks/{date}
    # This gives open/close/high/low/volume for each ticker for *today* (after close).
    # But at open time, we can get 1-minute aggregates at 9:30 and compute open ourselves.
    # Simpler approach: use "open" from daily once available (post-open it may still be present).
    # We'll use grouped aggregates for PRELIM today; at 9:35 it should be populated.
    grp_today = requests.get(
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{today_str}",
        params={"adjusted": "true", "apiKey": key},
    ).json()

    results_today = grp_today.get("results", []) or []
    # Build map: ticker -> today's open
    open_today = {r["T"]: r.get("o") for r in results_today if r.get("o") is not None}

    # Step 2: Get previous day's grouped to fetch previous close
    grp_prev = requests.get(
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{prev_trading_day}",
        params={"adjusted": "true", "apiKey": key},
    ).json()
    results_prev = grp_prev.get("results", []) or []
    prev_close = {r["T"]: r.get("c") for r in results_prev if r.get("c") is not None}

    # Optional metadata for filtering (market cap, optionable)
    meta = {}
    if cfg["MIN_MARKET_CAP"] > 0 or cfg["ONLY_OPTIONABLE"]:
        # Reference tickers with market cap in pages
        page_url = "https://api.polygon.io/v3/reference/tickers"
        next_url = f"{page_url}?market=stocks&active=true&apiKey={key}&limit=1000"
        while next_url:
            resp = requests.get(next_url).json()
            for t in resp.get("results", []) or []:
                meta[t["ticker"]] = {
                    "market_cap": t.get("market_cap", 0) or 0,
                    "optionable": bool(t.get("options", False)),
                    "name": t.get("name", ""),
                }
            next_url = resp.get("next_url", None)
            if next_url:
                # next_url is relative, need apiKey again
                sep = "&" if "?" in next_url else "?"
                next_url = f"https://api.polygon.io{next_url}{sep}apiKey={key}"

    rows = []
    for t, o in open_today.items():
        c_prev = prev_close.get(t)
        if not c_prev or not o:
            continue
        gap_pct = (o - c_prev) / c_prev * 100.0
        if gap_pct <= cfg["MIN_GAP_PCT"]:
            # Filter by market cap / optionable if present
            if t in meta:
                mc = meta[t]["market_cap"]
                op = meta[t]["optionable"]
                if mc < cfg["MIN_MARKET_CAP"]:
                    continue
                if cfg["ONLY_OPTIONABLE"] and not op:
                    continue
                name = meta[t]["name"]
            else:
                mc = None
                op = None
                name = ""

            rows.append({
                "ticker": t,
                "name": name,
                "prev_close": c_prev,
                "today_open": o,
                "gap_pct": gap_pct,
                "market_cap": mc,
                "optionable": op,
            })

    rows.sort(key=lambda x: x["gap_pct"])  # most negative first
    return rows

def yahoo_gap_scan(cfg):
    # Use yfinance with tickers from CSV or Financial Modeling Prep API
    import yfinance as yf
    import pandas as pd

    # Check if we should use Financial Modeling Prep API for all exchanges
    if cfg["TICKERS_CSV"] == "all_exchanges" or cfg["TICKERS_CSV"] == "all":
        print("Using Financial Modeling Prep API to fetch all exchange tickers...")
        tickers = get_all_exchange_tickers(cfg)
        if not tickers:
            print("ERROR: Failed to fetch tickers from Financial Modeling Prep API", file=sys.stderr)
            sys.exit(2)
    else:
        # Use CSV file
        csv_path = cfg["TICKERS_CSV"]
        if not os.path.exists(csv_path):
            print(f"ERROR: TICKERS_CSV not found: {csv_path}", file=sys.stderr)
            sys.exit(2)

        tickers = []
        with open(csv_path, "r", newline="") as f:
            for row in csv.reader(f):
                if not row:
                    continue
                t = row[0].strip().upper()
                if t and t != "TICKER":
                    tickers.append(t)

    print(f"Scanning {len(tickers)} tickers for gap opportunities...")

    all_data = []  # Store all stock data for CSV
    gap_down_rows = []  # Store gap-down stocks
    gap_up_rows = []  # Store gap-up stocks
    successful_downloads = 0
    failed_downloads = 0

    # Pull last two daily candles per ticker and compute open vs prev close
    for i, t in enumerate(tickers, 1):
        try:
            # Progress indicator (less frequent to see individual ticker output)
            if i % 100 == 0 or i == len(tickers):
                print(f"\n--- Progress: {i}/{len(tickers)} ({i/len(tickers)*100:.1f}%) - Gap Downs: {len(gap_down_rows)}, Gap Ups: {len(gap_up_rows)} ---\n")

            # Add a small delay to avoid rate limiting
            time.sleep(0.2)

            df = yf.download(t, period="5d", interval="1d", auto_adjust=False, progress=False)
            if df is None or len(df) < 2:
                failed_downloads += 1
                continue

            prev_close = float(df["Close"].iloc[-2].iloc[0])
            today_open = float(df["Open"].iloc[-1].iloc[0])
            gap_pct = (today_open - prev_close) / prev_close * 100.0

            successful_downloads += 1

            # Print gap information for debugging
            gap_direction = "GAP DOWN" if gap_pct < 0 else "GAP UP" if gap_pct > 0 else "NO GAP"
            print(f"{t}: {prev_close:.2f} → {today_open:.2f} ({gap_pct:+.2f}%) - {gap_direction}")

            # Create stock data entry
            stock_data = {
                "ticker": t,
                "name": "",
                "prev_close": prev_close,
                "today_open": today_open,
                "gap_pct": gap_pct,
                "market_cap": None,
                "optionable": None,
            }

            # Store all data for CSV
            all_data.append(stock_data)

            # Categorize based on gap thresholds
            if gap_pct <= cfg["MIN_GAP_DOWN_PCT"]:
                gap_down_rows.append(stock_data)
            elif gap_pct >= cfg["MIN_GAP_UP_PCT"]:
                gap_up_rows.append(stock_data)

        except Exception as e:
            failed_downloads += 1
            # Only print errors for first few failures to avoid spam
            if failed_downloads <= 5:
                print(f"Skipping {t}: {e}")
            elif failed_downloads == 6:
                print("... (suppressing further error messages)")

    print(f"\nScan complete:")
    print(f"- Successfully processed: {successful_downloads} tickers")
    print(f"- Failed downloads: {failed_downloads} tickers")
    print(f"- Gap-down stocks found: {len(gap_down_rows)}")
    print(f"- Gap-up stocks found: {len(gap_up_rows)}")

    # Sort the data
    gap_down_rows.sort(key=lambda x: x["gap_pct"])  # most negative first
    gap_up_rows.sort(key=lambda x: x["gap_pct"], reverse=True)  # most positive first
    all_data.sort(key=lambda x: x["gap_pct"])  # most negative first

    return {
        "gap_downs": gap_down_rows,
        "gap_ups": gap_up_rows,
        "all_data": all_data
    }

def send_email(cfg, data):
    # Use Resend to send HTML tables and CSV attachment
    import resend
    import csv
    import io
    from datetime import datetime

    gap_downs = data.get("gap_downs", [])
    gap_ups = data.get("gap_ups", [])
    all_data = data.get("all_data", [])

    def build_html_table(rows, title, color_threshold=0):
        if not rows:
            return f"<h3>{title}</h3><p>No stocks found.</p>"

        ths = ["Ticker", "Prev Close", "Today Open", "$ Change", "Gap %"]
        trs = []
        for r in rows:
            dollar_change = r['today_open'] - r['prev_close']
            gap_color = "red" if r['gap_pct'] < color_threshold else "green" if r['gap_pct'] > color_threshold else "black"
            change_color = "red" if dollar_change < 0 else "green" if dollar_change > 0 else "black"
            trs.append(
                f"<tr>"
                f"<td><b>{r['ticker']}</b></td>"
                f"<td>${r['prev_close']:.2f}</td>"
                f"<td>${r['today_open']:.2f}</td>"
                f"<td style='color:{change_color}'>${dollar_change:+.2f}</td>"
                f"<td style='color:{gap_color}'>{r['gap_pct']:+.2f}%</td>"
                f"</tr>"
            )
        html_table = (
            "<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse; margin-bottom:20px;'>"
            "<thead><tr>" + "".join([f"<th>{h}</th>" for h in ths]) + "</tr></thead>"
            "<tbody>" + "".join(trs) + "</tbody></table>"
        )
        return f"<h3>{title} ({len(rows)} stocks)</h3>" + html_table

    # Build HTML content
    html_parts = [
        f"<h2>Daily Gap Analysis - {datetime.now().strftime('%Y-%m-%d')}</h2>",
        f"<p><strong>Gap Down Threshold:</strong> ≤ {cfg['MIN_GAP_DOWN_PCT']}%</p>",
        f"<p><strong>Gap Up Threshold:</strong> ≥ {cfg['MIN_GAP_UP_PCT']}%</p>",
        build_html_table(gap_downs, "Gap Down Stocks", 0),
        build_html_table(gap_ups, "Gap Up Stocks", 0),
        "<p><em>Complete data attached as CSV file.</em></p>"
    ]

    html = "".join(html_parts)

    # Create subject line
    total_gaps = len(gap_downs) + len(gap_ups)
    subject = f"{cfg['EMAIL_SUBJECT_PREFIX']} {len(gap_downs)} gap-down, {len(gap_ups)} gap-up stocks"

    try:
        if not cfg["RESEND_API_KEY"] or cfg["RESEND_API_KEY"] == "your_resend_api_key_here":
            print("ERROR: RESEND_API_KEY not configured in .env file", file=sys.stderr)
            print("Please create a .env file with your Resend API key", file=sys.stderr)
            sys.exit(3)

        # Create CSV attachment
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)

        # Write CSV header
        csv_writer.writerow(["Ticker", "Prev_Close", "Today_Open", "Dollar_Change", "Gap_Pct"])

        # Write all data to CSV
        for row in all_data:
            dollar_change = row['today_open'] - row['prev_close']
            csv_writer.writerow([
                row["ticker"],
                f"{row['prev_close']:.2f}",
                f"{row['today_open']:.2f}",
                f"{dollar_change:.2f}",
                f"{row['gap_pct']:.2f}"
            ])

        csv_content = csv_buffer.getvalue()
        csv_buffer.close()

        # Set the API key
        resend.api_key = cfg["RESEND_API_KEY"]

        # Send the email using Resend with attachment
        params = {
            "from": cfg["EMAIL_FROM"],
            "to": [cfg["EMAIL_TO"]],
            "subject": subject,
            "html": html,
            "attachments": [
                {
                    "filename": f"gap_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
                    "content": csv_content,
                    "type": "text/csv"
                }
            ]
        }

        email = resend.Emails.send(params)
        print(f"Email sent successfully! ID: {email['id']}")
        print(f"CSV attachment included with {len(all_data)} stocks")

    except Exception as e:
        print(f"ERROR sending email: {e}", file=sys.stderr)
        if "401" in str(e) or "Unauthorized" in str(e):
            print("This is likely due to an invalid Resend API key", file=sys.stderr)
            print("Please check your RESEND_API_KEY in the .env file", file=sys.stderr)
        sys.exit(3)

def main():
    print("Starting gap down email script...")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Script started at: {datetime.now()}")
    
    try:
        cfg = load_env()
        print(f"Configuration loaded successfully. DATA_SOURCE: {cfg['DATA_SOURCE']}")
        
        if cfg["DATA_SOURCE"] == "polygon":
            print("Using Polygon data source...")
            # For polygon, we need to adapt the old format to new format
            rows = polygon_gap_scan(cfg)
            data = {
                "gap_downs": rows,
                "gap_ups": [],  # Polygon scan only finds gap downs
                "all_data": rows
            }
        elif cfg["DATA_SOURCE"] == "yahoo":
            print("Using Yahoo data source...")
            data = yahoo_gap_scan(cfg)
        else:
            print("ERROR: DATA_SOURCE must be 'polygon' or 'yahoo'", file=sys.stderr)
            sys.exit(2)

        print(f"Data collection complete. Sending email...")
        send_email(cfg, data)
        print("Email sent successfully!")
        
    except Exception as e:
        print(f"FATAL ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
