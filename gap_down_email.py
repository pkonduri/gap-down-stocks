#!/usr/bin/env python3
"""
Daily Gap-Down Emailer
----------------------
Finds stocks that gapped down by X% or more at the regular session open and emails you a list.

Data source:
- Yahoo Finance via yfinance

Email:
- Resend API for sending emails

Suggested schedule:
- Run at configurable time (default 9:15 AM US/Eastern) to capture pre-market/open price data.

Usage:
  1) Create .env file with required variables
  2) pip install -r requirements.txt
  3) python gap_down_email.py

Environment (.env):
  TICKERS_CSV=sp500_tickers.csv
  OPEN_HOUR=9           # Hour in ET (24-hour format)
  OPEN_MINUTE=15        # Minute
  MIN_GAP_DOWN_PCT=-5   # negative number, e.g., -5 means -5% or worse
  MIN_GAP_UP_PCT=1      # positive number, e.g., 1 means +1% or better
  RESEND_API_KEY=re_xxxxxx
  EMAIL_FROM=you@example.com
  EMAIL_TO=you@example.com
  EMAIL_SUBJECT_PREFIX=[Gap Down]

Notes:
- Compares previous day's close with configured morning timestamp
- Email includes Gap Down stocks, Gap Up stocks, and CSV attachment with all data
- Large ticker lists are processed with progress tracking and rate limiting
"""
import os
import sys
import math
import time
import json
import csv
import base64
from datetime import datetime, date, timedelta, timezone
import pandas as pd
import pytz

from dotenv import load_dotenv

def pct(x):
    return f"{x:.2f}%"

def load_env():
    load_dotenv()
    print("Loading environment variables...")

    cfg = {
        "TICKERS_CSV": os.getenv("TICKERS_CSV", "sp500_tickers.csv"),
        "OPEN_HOUR": int(os.getenv("OPEN_HOUR", "9")),
        "OPEN_MINUTE": int(os.getenv("OPEN_MINUTE", "15")),
        "MIN_GAP_DOWN_PCT": float(os.getenv("MIN_GAP_DOWN_PCT", "-5")),
        "MIN_GAP_UP_PCT": float(os.getenv("MIN_GAP_UP_PCT", "1")),
        "RESEND_API_KEY": os.getenv("RESEND_API_KEY", ""),
        "EMAIL_FROM": os.getenv("EMAIL_FROM", ""),
        "EMAIL_TO": os.getenv("EMAIL_TO", ""),
        "EMAIL_SUBJECT_PREFIX": os.getenv("EMAIL_SUBJECT_PREFIX", "[Gap Down]"),
    }

    # Print config status (without revealing actual keys)
    print(f"TICKERS_CSV: {cfg['TICKERS_CSV']}")
    print(f"OPEN_TIME: {cfg['OPEN_HOUR']:02d}:{cfg['OPEN_MINUTE']:02d} ET")
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



def get_market_open_price(ticker, open_hour, open_minute):
    """
    Get the most recent market open price for a ticker at the configured time.
    Works regardless of when script is run (weekend, after hours, etc.)
    """
    try:
        import yfinance as yf
        import pytz

        et_tz = pytz.timezone('US/Eastern')
        now_et = datetime.now(et_tz)

        # Find the most recent weekday (Monday=0, Sunday=6)
        days_back = 0
        current_date = now_et.date()
        while current_date.weekday() >= 5:  # Weekend
            days_back += 1
            current_date = (now_et - timedelta(days=days_back)).date()

        # If today is a weekday but it's before the configured open time, use yesterday
        if current_date == now_et.date() and (now_et.hour < open_hour or (now_et.hour == open_hour and now_et.minute < open_minute)):
            days_back += 1
            current_date = (now_et - timedelta(days=days_back)).date()
            # Skip weekends
            while current_date.weekday() >= 5:
                days_back += 1
                current_date = (now_et - timedelta(days=days_back)).date()

        # Get several days of 1-minute data to ensure we capture the target date
        start_date = (current_date - timedelta(days=2)).isoformat()
        end_date = (current_date + timedelta(days=1)).isoformat()

        df_minute = yf.download(ticker, start=start_date, end=end_date,
                               interval="1m", auto_adjust=False,
                               progress=False, prepost=True)

        if df_minute is None or len(df_minute) == 0:
            return None

        # Look for exactly the configured open time on the target date
        target_datetime = et_tz.localize(datetime.combine(current_date, datetime.min.time().replace(hour=open_hour, minute=open_minute)))

        for timestamp, row in df_minute.iterrows():
            if timestamp.tz is None:
                timestamp = pytz.UTC.localize(timestamp)
            timestamp_et = timestamp.astimezone(et_tz)

            # Look for exactly the configured open time on target date
            if (timestamp_et.date() == current_date and
                timestamp_et.hour == open_hour and
                timestamp_et.minute == open_minute):

                try:
                    # Handle MultiIndex columns when downloading single ticker
                    if isinstance(df_minute.columns, pd.MultiIndex):
                        close_price = row[('Close', ticker)]
                    else:
                        close_price = row['Close']

                    if pd.notna(close_price):
                        return {
                            'price': float(close_price),
                            'timestamp': timestamp_et,
                            'source': 'market-open',
                            'date': current_date
                        }
                except (KeyError, IndexError):
                    continue

        return None

    except Exception as e:
        print(f"Market open price fetch error for {ticker}: {e}")
        return None

def yahoo_gap_scan(cfg):
    # Use yfinance with tickers from CSV file
    import yfinance as yf
    import pandas as pd

    # Use CSV file
    csv_path = cfg["TICKERS_CSV"]
    if not os.path.exists(csv_path):
        print(f"ERROR: TICKERS_CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(2)

    tickers = []
    seen_tickers = set()
    with open(csv_path, "r", newline="") as f:
        for row in csv.reader(f):
            if not row:
                continue
            t = row[0].strip().upper()
            if t and t != "TICKER" and t not in seen_tickers:
                tickers.append(t)
                seen_tickers.add(t)

    # TEMPORARY: Limit to first 100 tickers for testing
    original_count = len(tickers)
    tickers = tickers[:50]
    print(f"TESTING MODE: Using first {len(tickers)} tickers out of {original_count} total")

    print(f"Scanning {len(tickers)} tickers for gap opportunities... (TESTING MODE - LIMITED SET)")

    all_data = []  # Store all stock data for CSV
    gap_down_rows = []  # Store gap-down stocks
    gap_up_rows = []  # Store gap-up stocks
    successful_downloads = 0
    failed_downloads = 0

    # Process each ticker: get yesterday's close vs today's configured time price
    for i, t in enumerate(tickers, 1):
        try:
            # Progress indicator
            if i % 100 == 0 or i == len(tickers):
                print(f"Progress: {i}/{len(tickers)} ({i/len(tickers)*100:.1f}%) - Gap Downs: {len(gap_down_rows)}, Gap Ups: {len(gap_up_rows)}")

            # Add a small delay to avoid rate limiting
            time.sleep(0.1)

            # Get recent daily data to extract yesterday's close
            df = yf.download(t, period="5d", interval="1d", auto_adjust=False, progress=False)
            if df is None or len(df) < 2:
                failed_downloads += 1
                continue

            # Extract yesterday's close price
            prev_close_price = float(df["Close"].iloc[-2].iloc[0] if hasattr(df["Close"].iloc[-2], 'iloc') else df["Close"].iloc[-2])

            # Try to get configured time price, fallback to regular open
            today_open_price = float(df["Open"].iloc[-1].iloc[0] if hasattr(df["Open"].iloc[-1], 'iloc') else df["Open"].iloc[-1])
            data_source = 'regular-open'

            # Try to get market open price at configured time
            market_open_data = get_market_open_price(t, cfg['OPEN_HOUR'], cfg['OPEN_MINUTE'])
            if market_open_data:
                today_open_price = market_open_data['price']
                data_source = 'market-open'

            # Calculate gap percentage
            gap_pct = (today_open_price - prev_close_price) / prev_close_price * 100.0
            successful_downloads += 1

            # Create stock data entry
            stock_data = {
                "ticker": t,
                "name": "",
                "prev_close": prev_close_price,
                "today_open": today_open_price,
                "gap_pct": gap_pct,
                "data_source": data_source,
            }

            all_data.append(stock_data)

            # Categorize based on gap thresholds
            if gap_pct <= cfg["MIN_GAP_DOWN_PCT"]:
                gap_down_rows.append(stock_data)
                if len(gap_down_rows) <= 10:  # Show first 10 gap downs
                    print(f"GAP DOWN: {t} ${prev_close_price:.2f} → ${today_open_price:.2f} ({gap_pct:+.2f}%)")
            elif gap_pct >= cfg["MIN_GAP_UP_PCT"]:
                gap_up_rows.append(stock_data)
                if len(gap_up_rows) <= 10:  # Show first 10 gap ups
                    print(f"GAP UP: {t} ${prev_close_price:.2f} → ${today_open_price:.2f} ({gap_pct:+.2f}%)")

        except Exception as e:
            failed_downloads += 1
            if failed_downloads <= 5:
                print(f"Skipping {t}: {e}")

    print(f"\n{'='*80}")
    print(f"SCAN RESULTS SUMMARY")
    print(f"{'='*80}")
    print(f"Successfully processed: {successful_downloads} tickers")
    print(f"Failed downloads: {failed_downloads} tickers")
    print(f"Gap-down stocks found (≤{cfg['MIN_GAP_DOWN_PCT']}%): {len(gap_down_rows)}")
    print(f"Gap-up stocks found (≥{cfg['MIN_GAP_UP_PCT']}%): {len(gap_up_rows)}")
    print(f"{'='*80}\n")

    # Remove duplicates based on ticker (keep first occurrence)
    seen_tickers = set()
    unique_all_data = []
    for stock in all_data:
        if stock['ticker'] not in seen_tickers:
            unique_all_data.append(stock)
            seen_tickers.add(stock['ticker'])

    # Remove duplicates from gap lists too
    seen_gap_down = set()
    unique_gap_downs = []
    for stock in gap_down_rows:
        if stock['ticker'] not in seen_gap_down:
            unique_gap_downs.append(stock)
            seen_gap_down.add(stock['ticker'])

    seen_gap_up = set()
    unique_gap_ups = []
    for stock in gap_up_rows:
        if stock['ticker'] not in seen_gap_up:
            unique_gap_ups.append(stock)
            seen_gap_up.add(stock['ticker'])

    # Sort the data
    unique_gap_downs.sort(key=lambda x: x["gap_pct"])  # most negative first
    unique_gap_ups.sort(key=lambda x: x["gap_pct"], reverse=True)  # most positive first
    unique_all_data.sort(key=lambda x: x["gap_pct"])  # most negative first

    # Display summary of market open data
    market_open_count = len([s for s in unique_all_data if s.get('data_source') == 'market-open'])
    regular_count = len(unique_all_data) - market_open_count
    print(f"Data source breakdown: {cfg['OPEN_HOUR']:02d}:{cfg['OPEN_MINUTE']:02d} ET data: {market_open_count} | Regular open: {regular_count}")

    return {
        "gap_downs": unique_gap_downs,
        "gap_ups": unique_gap_ups,
        "all_data": unique_all_data
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

    # Build HTML content with proper trading day timestamps
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)

    # Determine the actual trading day for "today's" open timestamp
    today_trading_day = now_et.date()
    # If today is weekend, the "today" data is actually from Friday
    while today_trading_day.weekday() >= 5:  # Weekend
        today_trading_day = today_trading_day - timedelta(days=1)

    # If today is a weekday but before market open time, we're looking at "today's" pre-market
    # If it's after market open time, we might still be looking at "today" depending on when script runs
    # The key insight: the script gets the most recent trading day's open price

    # For "yesterday's" close, find the trading day before the "today" trading day
    yesterday_trading_day = today_trading_day - timedelta(days=1)
    while yesterday_trading_day.weekday() >= 5:  # Skip weekends
        yesterday_trading_day = yesterday_trading_day - timedelta(days=1)

    # Format the trading days for display
    today_day_name = today_trading_day.strftime('%A')
    yesterday_day_name = yesterday_trading_day.strftime('%A')

    html_parts = [
        f"<h2>Daily Gap Analysis - {datetime.now().strftime('%Y-%m-%d')}</h2>",
        f"<p><strong>Data Source:</strong> Yahoo Finance</p>",
        f"<p><strong>Today's Open Timestamp:</strong> {today_day_name}, {today_trading_day.strftime('%Y-%m-%d')} at {cfg['OPEN_HOUR']:02d}:{cfg['OPEN_MINUTE']:02d} ET</p>",
        f"<p><strong>Previous Close Timestamp:</strong> {yesterday_day_name}, {yesterday_trading_day.strftime('%Y-%m-%d')} at ~4:00 PM ET</p>",
        f"<p><strong>Gap Down Threshold:</strong> ≤ {cfg['MIN_GAP_DOWN_PCT']}%</p>",
        f"<p><strong>Gap Up Threshold:</strong> ≥ {cfg['MIN_GAP_UP_PCT']}%</p>",
        f"<p><strong>Total Stocks Analyzed:</strong> {len(all_data)}</p>",
        build_html_table(gap_downs, f"Gap Down Stocks", 0),
        build_html_table(gap_ups, f"Gap Up Stocks", 0),
        "<p><em>Complete data with all stocks attached as Excel file with highlighting.</em></p>"
    ]

    html = "".join(html_parts)

    # Create subject line with proper format: [Daily Gaps] [MM/DD/YYYY] [x gap down, x gap up, stocks]
    today_formatted = datetime.now().strftime('%m/%d/%Y')
    total_stocks = len(all_data)
    subject = f"[Daily Gaps] [{today_formatted}] [{len(gap_downs)} gap down, {len(gap_ups)} gap up, {total_stocks} stocks]"

    try:
        if not cfg["RESEND_API_KEY"] or cfg["RESEND_API_KEY"] == "your_resend_api_key_here":
            print("ERROR: RESEND_API_KEY not configured in .env file", file=sys.stderr)
            print("Please create a .env file with your Resend API key", file=sys.stderr)
            sys.exit(3)

        # Create Excel attachment with highlighted rows
        import pandas as pd

        # Sort all data by gap percentage (most negative first) for better organization
        sorted_data = sorted(all_data, key=lambda x: x['gap_pct'])

        # Prepare data for DataFrame
        excel_data = []
        print(f"\nPreparing Excel data for {len(sorted_data)} stocks...")

        for i, row in enumerate(sorted_data):
            dollar_change = row['today_open'] - row['prev_close']

            excel_row = {
                "Ticker": row["ticker"],
                "Previous_Close": round(row['prev_close'], 2),
                "Today_Open": round(row['today_open'], 2),
                "Dollar_Change": round(dollar_change, 2),
                "Gap_Percent": round(row['gap_pct'], 2),
                "Data_Source": row.get('data_source', 'regular-open').replace('-', '_').upper(),
                "_gap_raw": row['gap_pct']  # Keep for highlighting logic
            }
            excel_data.append(excel_row)

            # Debug: Print first few rows
            if i < 5:
                gap_status = "GAP_DOWN" if row['gap_pct'] <= cfg['MIN_GAP_DOWN_PCT'] else "GAP_UP" if row['gap_pct'] >= cfg['MIN_GAP_UP_PCT'] else "NORMAL"
                print(f"Row {i}: {row['ticker']} - Gap: {row['gap_pct']:+.2f}% ({gap_status})")

        # Create DataFrame
        df = pd.DataFrame(excel_data)

        # Remove the helper column for display
        display_df = df.drop('_gap_raw', axis=1)

        print(f"\nExcel DataFrame shape: {display_df.shape}")
        print(f"Excel DataFrame columns: {list(display_df.columns)}")
        print(f"\nFirst 5 rows:")
        print(display_df.head().to_string(index=False))

        # Define highlighting function
        def highlight_gaps(row):
            # Get the raw gap value for this row
            gap_pct = df.loc[row.name, '_gap_raw']

            if gap_pct <= cfg['MIN_GAP_DOWN_PCT']:
                # Red background for gap downs
                return ['background-color: #ffcccc'] * len(row)
            elif gap_pct >= cfg['MIN_GAP_UP_PCT']:
                # Green background for gap ups
                return ['background-color: #ccffcc'] * len(row)
            else:
                # White background for normal gaps
                return ['background-color: white'] * len(row)

        # Apply styling to the display DataFrame
        styled_df = display_df.style.apply(highlight_gaps, axis=1)

        # Count highlighted rows
        gap_down_count = len([s for s in sorted_data if s['gap_pct'] <= cfg['MIN_GAP_DOWN_PCT']])
        gap_up_count = len([s for s in sorted_data if s['gap_pct'] >= cfg['MIN_GAP_UP_PCT']])
        print(f"\nHighlighting: {gap_down_count} red rows (gap down), {gap_up_count} green rows (gap up)")

        # Save to Excel file with highlighting
        excel_filename = f"gap_analysis_{datetime.now().strftime('%Y%m%d')}.xlsx"
        styled_df.to_excel(excel_filename, index=False, engine='openpyxl')

        # Also save debug CSV
        csv_content = display_df.to_csv(index=False)
        with open(f"debug_gap_analysis_{datetime.now().strftime('%Y%m%d')}.csv", 'w', encoding='utf-8') as debug_file:
            debug_file.write(csv_content)

        print(f"Excel file with highlighting saved as: {excel_filename}")
        print(f"Debug CSV also saved for reference")

        # Read the Excel file as bytes for email attachment
        with open(excel_filename, 'rb') as f:
            excel_bytes = f.read()
        excel_base64 = base64.b64encode(excel_bytes).decode('utf-8')

        # csv_content is already created above

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
                    "filename": f"gap_analysis_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    "content": excel_base64,
                    "type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                }
            ]
        }

        email = resend.Emails.send(params)
        print(f"Email sent successfully! ID: {email['id']}")
        print(f"Excel attachment included with ALL {len(all_data)} stocks")
        print(f"Row highlighting: {gap_down_count} red (gap-down), {gap_up_count} green (gap-up)")
        print(f"Local Excel and CSV files created for verification")

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
        print(f"Configuration loaded successfully. Using Yahoo Finance data source.")

        print("Scanning for gap opportunities...")
        data = yahoo_gap_scan(cfg)

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
