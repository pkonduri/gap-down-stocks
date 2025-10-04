#!/usr/bin/env python3
"""
Daily Gap Analysis Emailer
-------------------------
Finds stocks with significant price gaps by comparing current prices vs previous market close.

Data source:
- Yahoo Finance via yfinance

Email:
- Resend API for sending emails

Timing Logic:
- Current Timestamp: Uses the exact time when the script is run to get current pricing
- Previous Close: Uses the most recent market close (4pm ET) from the last trading day

Examples:
- Script run Sunday 9pm ET: Current = Sunday 9pm overnight price, Previous = Friday 4pm close
- Script run Monday 9am ET: Current = Monday 9am pre-market, Previous = Friday 4pm close  
- Script run Tuesday 2pm ET: Current = Tuesday 2pm intraday, Previous = Monday 4pm close
- Script run Wednesday 8pm ET: Current = Wednesday 8pm after-hours, Previous = Tuesday 4pm close

Usage:
  1) Create .env file with required variables
  2) pip install -r requirements.txt
  3) python gap_down_email.py [command]
  
Commands:
  email       - Send to personal recipients (default, uses PERSONAL_EMAILS)
  email-all   - Send to all recipients (uses RECEIVER_EMAIL_ADDRESS)

Environment (.env):
  TICKERS_CSV=sp500_tickers.csv
  TESTING_MODE=false    # true to limit to first 50 tickers for testing
  MIN_GAP_DOWN_PCT=-5   # negative number, e.g., -5 means -5% or worse
  MIN_GAP_UP_PCT=1      # positive number, e.g., 1 means +1% or better
  RESEND_API_KEY=re_xxxxxx
  EMAIL_FROM=you@example.com
  PERSONAL_EMAILS=you@example.com  # personal/default recipient for 'email' command
  RECEIVER_EMAIL_ADDRESS=email1@example.com,email2@example.com  # all recipients for 'email-all' command
  EMAIL_SUBJECT_PREFIX=[Gap Down]

Notes:
- Gap calculation: (Current Price - Previous Close) / Previous Close * 100
- Automatically handles weekends, holidays, and extended trading sessions
- Current price source: minute-level data when available, daily close as fallback
- Email includes Gap Down stocks, Gap Up stocks, and Excel attachment with highlighting
- Excel file highlights gap-down stocks in red, gap-up stocks in green
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

def get_personal_emails(cfg):
    """Get personal/default email recipients from PERSONAL_EMAILS environment variable"""
    personal_emails = cfg.get("PERSONAL_EMAILS", "")
    if not personal_emails:
        return []
    return [email.strip() for email in personal_emails.split(",") if email.strip()]

def get_all_recipients(cfg):
    """Get all email recipients from RECEIVER_EMAIL_ADDRESS environment variable"""
    receiver_emails = cfg.get("RECEIVER_EMAIL_ADDRESS", "")
    if not receiver_emails:
        return []
    return [email.strip() for email in receiver_emails.split(",") if email.strip()]

def pct(x):
    return f"{x:.2f}%"

def load_env():
    load_dotenv()
    print("Loading environment variables...")

    cfg = {
        "TICKERS_CSV": os.getenv("TICKERS_CSV", "sp500_tickers.csv"),
        "MIN_GAP_DOWN_PCT": float(os.getenv("MIN_GAP_DOWN_PCT", "-5")),
        "MIN_GAP_UP_PCT": float(os.getenv("MIN_GAP_UP_PCT", "1")),
        "TESTING_MODE": os.getenv("TESTING_MODE", "false").lower() == "true",
        "RESEND_API_KEY": os.getenv("RESEND_API_KEY", ""),
        "EMAIL_FROM": os.getenv("EMAIL_FROM", ""),
        "PERSONAL_EMAILS": os.getenv("PERSONAL_EMAILS", ""),
        "RECEIVER_EMAIL_ADDRESS": os.getenv("RECEIVER_EMAIL_ADDRESS", ""),
        "EMAIL_SUBJECT_PREFIX": os.getenv("EMAIL_SUBJECT_PREFIX", "[Gap Down]"),
    }

    # Print config status (without revealing actual keys)
    print(f"TICKERS_CSV: {cfg['TICKERS_CSV']}")
    print(f"TESTING_MODE: {cfg['TESTING_MODE']}")
    print(f"GAP_DOWN_THRESHOLD: {cfg['MIN_GAP_DOWN_PCT']}%")
    print(f"GAP_UP_THRESHOLD: {cfg['MIN_GAP_UP_PCT']}%")
    print(f"RESEND_API_KEY: {'SET' if cfg['RESEND_API_KEY'] else 'NOT SET'}")
    print(f"EMAIL_FROM: {'SET' if cfg['EMAIL_FROM'] else 'NOT SET'}")
    print(f"PERSONAL_EMAILS: {'SET' if cfg['PERSONAL_EMAILS'] else 'NOT SET'}")

    # Basic checks
    if not cfg["RESEND_API_KEY"] or not cfg["EMAIL_FROM"]:
        print("ERROR: Missing RESEND_API_KEY/EMAIL_FROM in .env", file=sys.stderr)
        print(f"RESEND_API_KEY present: {bool(cfg['RESEND_API_KEY'])}")
        print(f"EMAIL_FROM present: {bool(cfg['EMAIL_FROM'])}")
        sys.exit(2)
    
    return cfg



def get_current_price(ticker):
    """
    Get the current price for a ticker at the current timestamp.
    If markets are closed (weekends, after hours), gets the most recent available price.
    """
    try:
        import yfinance as yf
        import pytz

        # Set User-Agent to avoid being blocked by Yahoo Finance
        yf.utils.get_user_agent = lambda: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

        et_tz = pytz.timezone('US/Eastern')
        now_et = datetime.now(et_tz)

        # Get recent data to find the most current price available
        # Use 1-minute data for current day, and daily data as fallback

        # Try to get recent minute data first (for intraday/overnight pricing)
        try:
            # Get last 5 days of minute data to capture weekend/overnight sessions
            df_minute = yf.download(ticker, period="5d", interval="1m",
                                   auto_adjust=False, progress=False, prepost=True)

            if df_minute is not None and len(df_minute) > 0:
                # Get the most recent price from minute data
                latest_idx = df_minute.index[-1]
                latest_row = df_minute.iloc[-1]

                # Convert timestamp to ET
                if latest_idx.tz is None:
                    latest_idx = pytz.UTC.localize(latest_idx)
                latest_timestamp_et = latest_idx.astimezone(et_tz)

                # Get the close price from the most recent minute
                if isinstance(df_minute.columns, pd.MultiIndex):
                    current_price = latest_row[('Close', ticker)]
                else:
                    current_price = latest_row['Close']

                if pd.notna(current_price):
                    return {
                        'price': float(current_price),
                        'timestamp': latest_timestamp_et,
                        'source': 'current-minute',
                        'date': latest_timestamp_et.date()
                    }

        except Exception:
            # Fallback to daily data if minute data fails
            pass

        # Fallback: Get daily data for most recent close
        df_daily = yf.download(ticker, period="5d", interval="1d",
                              auto_adjust=False, progress=False)

        if df_daily is not None and len(df_daily) > 0:
            # Get the most recent trading day's close
            latest_close = float(df_daily["Close"].iloc[-1].iloc[0] if hasattr(df_daily["Close"].iloc[-1], 'iloc') else df_daily["Close"].iloc[-1])
            latest_date = df_daily.index[-1].date()

            return {
                'price': latest_close,
                'timestamp': now_et,  # Current time when we fetched it
                'source': 'daily-close',
                'date': latest_date
            }

        return None

    except Exception as e:
        print(f"Current price fetch error for {ticker}: {e}")
        return None

def yahoo_gap_scan(cfg):
    # Use yfinance with tickers from CSV file
    import yfinance as yf
    import pandas as pd

    # Set User-Agent to avoid being blocked by Yahoo Finance
    yf.utils.get_user_agent = lambda: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

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
            # Replace . with - for Yahoo Finance compatibility (BRK.B -> BRK-B)
            t = t.replace('.', '-')
            if t and t != "TICKER" and t not in seen_tickers:
                tickers.append(t)
                seen_tickers.add(t)

    # Apply testing mode if enabled
    if cfg.get('TESTING_MODE', False):
        original_count = len(tickers)
        tickers = tickers[:50]
        print(f"TESTING MODE: Using first {len(tickers)} tickers out of {original_count} total")

    mode_text = " (TESTING MODE - LIMITED SET)" if cfg.get('TESTING_MODE', False) else ""
    print(f"Scanning {len(tickers)} tickers for gap opportunities...{mode_text}")

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

            # Get recent daily data to extract the most recent market close
            df = yf.download(t, period="5d", interval="1d", auto_adjust=False, progress=False)
            if df is None or len(df) < 1:
                failed_downloads += 1
                continue

            # The most recent row in daily data represents the most recent market close
            # For Sunday evening, this would be Friday's close (since no Saturday trading)
            # For Monday, this would still be Friday's close until Monday's market closes
            prev_close_price = float(df["Close"].iloc[-1].iloc[0] if hasattr(df["Close"].iloc[-1], 'iloc') else df["Close"].iloc[-1])

            # Get current price (most recent available price)
            current_price_data = get_current_price(t)
            if current_price_data:
                today_current_price = current_price_data['price']
                data_source = current_price_data['source']
            else:
                # Fallback to daily open if current price unavailable
                today_current_price = float(df["Open"].iloc[-1].iloc[0] if hasattr(df["Open"].iloc[-1], 'iloc') else df["Open"].iloc[-1])
                data_source = 'daily-fallback'

            # Calculate gap percentage (current price vs previous close)
            gap_pct = (today_current_price - prev_close_price) / prev_close_price * 100.0
            successful_downloads += 1

            # Create stock data entry
            stock_data = {
                "ticker": t,
                "name": "",
                "prev_close": prev_close_price,
                "today_current": today_current_price,
                "gap_pct": gap_pct,
                "data_source": data_source,
            }

            all_data.append(stock_data)

            # Categorize based on gap thresholds
            if gap_pct <= cfg["MIN_GAP_DOWN_PCT"]:
                gap_down_rows.append(stock_data)
                if len(gap_down_rows) <= 10:  # Show first 10 gap downs
                    print(f"GAP DOWN: {t} ${prev_close_price:.2f} → ${today_current_price:.2f} ({gap_pct:+.2f}%)")
            elif gap_pct >= cfg["MIN_GAP_UP_PCT"]:
                gap_up_rows.append(stock_data)
                if len(gap_up_rows) <= 10:  # Show first 10 gap ups
                    print(f"GAP UP: {t} ${prev_close_price:.2f} → ${today_current_price:.2f} ({gap_pct:+.2f}%)")

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

    # Early exit check
    if successful_downloads == 0:
        print("ERROR: No stocks were successfully processed. Cannot send email.", file=sys.stderr)
        return None

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

    # Display summary of current price data sources
    current_minute_count = len([s for s in unique_all_data if s.get('data_source') == 'current-minute'])
    daily_close_count = len([s for s in unique_all_data if s.get('data_source') == 'daily-close'])
    daily_fallback_count = len([s for s in unique_all_data if s.get('data_source') == 'daily-fallback'])
    print(f"Data source breakdown: Current minute: {current_minute_count} | Daily close: {daily_close_count} | Daily fallback: {daily_fallback_count}")

    return {
        "gap_downs": unique_gap_downs,
        "gap_ups": unique_gap_ups,
        "all_data": unique_all_data
    }


def send_email(cfg, data, to_emails=None):
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

        ths = ["Ticker", "Prev Close", "Today Current", "$ Change", "Gap %"]
        trs = []
        for r in rows:
            dollar_change = r['today_current'] - r['prev_close']
            gap_color = "red" if r['gap_pct'] < color_threshold else "green" if r['gap_pct'] > color_threshold else "black"
            change_color = "red" if dollar_change < 0 else "green" if dollar_change > 0 else "black"
            trs.append(
                f"<tr>"
                f"<td><b>{r['ticker']}</b></td>"
                f"<td>${r['prev_close']:.2f}</td>"
                f"<td>${r['today_current']:.2f}</td>"
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

    # Build HTML content with current timestamp info
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)

    # Find the most recent market close day
    # This should be the most recent day when markets actually closed at 4pm ET
    # For Sunday: Friday close (no Saturday trading)
    # For Monday before 4pm: Friday close (Monday hasn't closed yet)
    # For Monday after 4pm: Monday close (Monday has closed)

    # Start from today and work backwards to find the most recent trading day that has closed
    previous_trading_day = now_et.date()

    # If today is a weekend, go back to the most recent trading day
    while previous_trading_day.weekday() >= 5:  # Saturday=5, Sunday=6
        previous_trading_day = previous_trading_day - timedelta(days=1)

    # If today is a weekday, we need to decide:
    # - If markets haven't closed today yet (before 4pm), use previous day's close
    # - If markets have closed today (after 4pm), use today's close
    # For simplicity, let's always use the previous trading day's close for consistency
    # since we're comparing current price vs "previous close"
    if now_et.weekday() < 5:  # If today is a weekday
        previous_trading_day = previous_trading_day - timedelta(days=1)
        while previous_trading_day.weekday() >= 5:  # Skip weekends
            previous_trading_day = previous_trading_day - timedelta(days=1)

    # Format the timestamps for display
    current_time_str = now_et.strftime('%A, %Y-%m-%d at %I:%M %p ET')
    previous_day_name = previous_trading_day.strftime('%A')

    html_parts = [
        f"<h2>Daily Gap Analysis - {datetime.now().strftime('%Y-%m-%d')}</h2>",
        f"<p><strong>Data Source:</strong> Yahoo Finance</p>",
        f"<p><strong>Current Timestamp:</strong> {current_time_str}</p>",
        f"<p><strong>Previous Close Timestamp:</strong> {previous_day_name}, {previous_trading_day.strftime('%Y-%m-%d')} at ~4:00 PM ET</p>",
        f"<p><strong>Gap Calculation:</strong> Current price vs Previous close price</p>",
        f"<p><strong>Gap Down Threshold:</strong> ≤ {cfg['MIN_GAP_DOWN_PCT']}%</p>",
        f"<p><strong>Gap Up Threshold:</strong> ≥ {cfg['MIN_GAP_UP_PCT']}%</p>",
        f"<p><strong>Total Stocks Analyzed:</strong> {len(all_data)}</p>",
        build_html_table(gap_downs, f"Gap Down Stocks", 0),
        build_html_table(gap_ups, f"Gap Up Stocks", 0),
        "<p><em>Complete data with all stocks attached as Excel file with highlighting.</em></p>"
    ]

    html = "".join(html_parts)

    # Create subject line with proper format: [Daily Gaps] [DD/MM/YYYY] [x gap down, x gap up, stocks]
    today_formatted = datetime.now().strftime('%d/%m/%Y')
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
            dollar_change = row['today_current'] - row['prev_close']

            excel_row = {
                "Ticker": row["ticker"],
                "Previous_Close": round(row['prev_close'], 2),
                "Today_Current": round(row['today_current'], 2),
                "Dollar_Change": round(dollar_change, 2),
                "Gap_Percent": round(row['gap_pct'], 2),
                "Data_Source": row.get('data_source', 'current-minute').replace('-', '_').upper(),
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
            "to": to_emails,
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

    # Default command is 'email' if no arguments provided
    command = sys.argv[1] if len(sys.argv) > 1 else 'email'

    try:
        cfg = load_env()
        print(f"Configuration loaded successfully. Using Yahoo Finance data source.")

        print("Scanning for gap opportunities...")
        data = yahoo_gap_scan(cfg)

        if data is None:
            print("ERROR: Data collection failed. Exiting without sending email.", file=sys.stderr)
            sys.exit(1)

        print(f"Data collection complete. Sending email...")

        if command == 'email':
            # Send to personal recipients (PERSONAL_EMAILS)
            to_emails = get_personal_emails(cfg)
            if not to_emails:
                print("ERROR: No personal email recipients specified. Set PERSONAL_EMAILS in .env", file=sys.stderr)
                sys.exit(2)
            print(f"Sending email to personal recipients: {len(to_emails)} recipient(s): {', '.join(to_emails)}")
            send_email(cfg, data, to_emails)
        elif command == 'email-all':
            # Send to all recipients (RECEIVER_EMAIL_ADDRESS)
            to_emails = get_all_recipients(cfg)
            if not to_emails:
                print("ERROR: No all email recipients specified. Set RECEIVER_EMAIL_ADDRESS in .env", file=sys.stderr)
                sys.exit(2)
            print(f"Sending email to all recipients: {len(to_emails)} recipient(s): {', '.join(to_emails)}")
            send_email(cfg, data, to_emails)
        else:
            print(f"ERROR: Invalid command '{command}'. Use 'email' or 'email-all'", file=sys.stderr)
            sys.exit(2)
            
        print("Email sent successfully!")

    except Exception as e:
        print(f"FATAL ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
