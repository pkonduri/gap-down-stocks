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
import pandas as pd
import pytz

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

    # Step 1: Try to get pre-market data at 8 AM ET for today
    # We'll use 1-minute aggregates to get 8 AM ET price
    et_tz = pytz.timezone('US/Eastern')
    target_time_et = datetime.now(et_tz).replace(hour=8, minute=0, second=0, microsecond=0)
    
    # Convert 8 AM ET to the format Polygon expects (milliseconds since epoch)
    target_timestamp = int(target_time_et.timestamp() * 1000)
    
    print(f"\nFetching pre-market data around 8 AM ET ({target_time_et.strftime('%Y-%m-%d %H:%M %Z')})...")
    
    # First, get list of all tickers from previous day to know what to fetch
    grp_prev_for_tickers = requests.get(
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{prev_trading_day}",
        params={"adjusted": "true", "apiKey": key},
    ).json()
    available_tickers = [r["T"] for r in grp_prev_for_tickers.get("results", []) or []]
    
    print(f"Found {len(available_tickers)} tickers from previous day to check for pre-market data")
    
    # Try to get pre-market prices using minute aggregates
    # We'll batch requests for efficiency
    open_today = {}
    premarket_found = 0
    
    # Sample a subset for testing (first 50 tickers to avoid rate limits)
    sample_tickers = available_tickers[:50] if len(available_tickers) > 50 else available_tickers
    
    for i, ticker in enumerate(sample_tickers):
        if i % 10 == 0:
            print(f"Checking pre-market data: {i}/{len(sample_tickers)} tickers...")
            
        # Get minute data for today around 8 AM ET
        # Polygon format: /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
        try:
            minute_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{today_str}/{today_str}"
            minute_response = requests.get(minute_url, params={"adjusted": "true", "apiKey": key})
            minute_data = minute_response.json()
            
            if minute_data.get("results"):
                # Look for data around 8 AM ET (convert timestamps)
                for bar in minute_data["results"]:
                    bar_time = datetime.fromtimestamp(bar["t"] / 1000, tz=pytz.UTC).astimezone(et_tz)
                    # Check if this is around 8 AM ET (+/- 30 minutes)
                    if (bar_time.hour == 8 and 0 <= bar_time.minute <= 30) or \
                       (bar_time.hour == 7 and bar_time.minute >= 30):
                        open_today[ticker] = bar["c"]  # Use close price of the minute bar
                        premarket_found += 1
                        if i < 3:  # Debug first few
                            print(f"  {ticker}: Pre-market price at {bar_time.strftime('%H:%M ET')}: ${bar['c']:.2f}")
                        break
            
            # Small delay to avoid rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            if i < 3:
                print(f"  {ticker}: Error fetching pre-market data: {e}")
            continue
    
    print(f"\nPre-market data found for {premarket_found} out of {len(sample_tickers)} tickers")
    
    # Fallback: Get regular daily data for tickers without pre-market data
    if premarket_found < len(sample_tickers) * 0.1:  # If less than 10% have pre-market data
        print("Low pre-market data availability, falling back to regular market open prices...")
        grp_today = requests.get(
            f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{today_str}",
            params={"adjusted": "true", "apiKey": key},
        ).json()
        
        results_today = grp_today.get("results", []) or []
        # Add regular open prices for tickers we don't have pre-market data
        for r in results_today:
            if r["T"] not in open_today and r.get("o") is not None:
                open_today[r["T"]] = r.get("o")
    else:
        results_today = []  # We're using pre-market data instead

    # Step 2: Get previous day's grouped to fetch previous close
    grp_prev = requests.get(
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{prev_trading_day}",
        params={"adjusted": "true", "apiKey": key},
    ).json()
    results_prev = grp_prev.get("results", []) or []
    prev_close = {r["T"]: r.get("c") for r in results_prev if r.get("c") is not None}
    
    print(f"\nDATA COLLECTION RESULTS:")
    print(f"Pre-market/Today's data ({today_str}): {premarket_found} pre-market + {len(open_today) - premarket_found} regular = {len(open_today)} total with prices")
    print(f"Previous day data ({prev_trading_day}): {len(results_prev)} tickers total, {len(prev_close)} with close prices")
    
    # Show sample of today's data
    if results_today:
        print(f"\nSample of today's data:")
        for i, r in enumerate(results_today[:3]):
            print(f"  {r['T']}: Open={r.get('o')}, Close={r.get('c')}, High={r.get('h')}, Low={r.get('l')}, Volume={r.get('v')}")
    
    # Show sample of previous day data
    if results_prev:
        print(f"\nSample of previous day data:")
        for i, r in enumerate(results_prev[:3]):
            print(f"  {r['T']}: Open={r.get('o')}, Close={r.get('c')}, High={r.get('h')}, Low={r.get('l')}, Volume={r.get('v')}")
    print()

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
        # Store all data for debugging regardless of thresholds
        stock_data = {
            "ticker": t,
            "name": meta.get(t, {}).get("name", ""),
            "prev_close": c_prev,
            "today_open": o,
            "gap_pct": gap_pct,
            "market_cap": meta.get(t, {}).get("market_cap"),
            "optionable": meta.get(t, {}).get("optionable"),
        }
        
        rows.append(stock_data)

    # Print debugging table for all stocks
    print(f"\n{'='*80}")
    print(f"POLYGON DATA DEBUGGING - All Stocks with Data")
    print(f"Previous Trading Day: {prev_trading_day}")
    print(f"Today: {today_str}")
    print(f"UTC Now: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Total stocks with today's open data: {len(open_today)}")
    print(f"Total stocks with previous close data: {len(prev_close)}")
    print(f"Stocks with both open and close data: {len(rows)}")
    print(f"{'='*80}")
    print(f"{'Ticker':<8} {'Prev Close':<10} {'Pre/Open':<10} {'Gap %':<8} {'Gap $':<8} {'Status':<15} {'Data Type':<12}")
    print("-" * 92)
    
    gap_downs = []
    gap_ups = []
    
    for row in rows:
        gap_dollar = row['today_open'] - row['prev_close']
        
        # Apply filters for final results
        skip_reason = None
        if row['ticker'] in meta:
            if meta[row['ticker']]['market_cap'] < cfg['MIN_MARKET_CAP']:
                skip_reason = "Low Market Cap"
            elif cfg['ONLY_OPTIONABLE'] and not meta[row['ticker']]['optionable']:
                skip_reason = "Not Optionable"
        
        if not skip_reason:
            if row['gap_pct'] <= cfg['MIN_GAP_DOWN_PCT']:
                gap_downs.append(row)
                status = "GAP DOWN ✓"
            elif row['gap_pct'] >= cfg['MIN_GAP_UP_PCT']:
                gap_ups.append(row)
                status = "GAP UP ✓"
            else:
                status = "No Gap"
        else:
            status = f"Filtered: {skip_reason}"
            
        data_type = "Pre-market" if row.get('data_source') == 'pre-market' else "Regular"
        print(f"{row['ticker']:<8} ${row['prev_close']:<9.2f} ${row['today_open']:<9.2f} {row['gap_pct']:<7.2f}% ${gap_dollar:<7.2f} {status:<15} {data_type:<12}")
    
    print(f"\nSUMMARY:")
    print(f"Total stocks with data: {len(rows)}")
    print(f"Gap-down stocks (≤{cfg['MIN_GAP_DOWN_PCT']}%): {len(gap_downs)}")
    print(f"Gap-up stocks (≥{cfg['MIN_GAP_UP_PCT']}%): {len(gap_ups)}")
    print(f"{'='*80}\n")
    
    rows.sort(key=lambda x: x["gap_pct"])  # most negative first
    gap_downs.sort(key=lambda x: x["gap_pct"])  # most negative first
    gap_ups.sort(key=lambda x: x["gap_pct"], reverse=True)  # most positive first
    
    return {
        "gap_downs": gap_downs,
        "gap_ups": gap_ups,
        "all_data": rows
    }

def get_premarket_price(ticker, target_hour=8):
    """
    Attempt to get pre-market price at specified hour (default 8 AM ET).
    Returns None if no pre-market data available.
    Only works during actual market hours when pre-market is active.
    """
    try:
        import yfinance as yf
        import pytz
        
        et_tz = pytz.timezone('US/Eastern')
        today = datetime.now(et_tz).date()
        
        # Only try to get intraday data if it's a weekday and during extended hours
        now_et = datetime.now(et_tz)
        if now_et.weekday() >= 5:  # Weekend
            return None
            
        # Get 1-minute data for today with pre/post market
        df_minute = yf.download(ticker, start=today.isoformat(), 
                               interval="1m", auto_adjust=False, 
                               progress=False, prepost=True)
        
        if df_minute is None or len(df_minute) == 0:
            return None
            
        # Look for data around target hour ET
        for idx, timestamp in enumerate(df_minute.index):
            if timestamp.tz is None:
                timestamp = pytz.UTC.localize(timestamp)
            timestamp_et = timestamp.astimezone(et_tz)
            
            # Check if this is around target hour ET (+/- 30 minutes)
            if (timestamp_et.hour == target_hour and 0 <= timestamp_et.minute <= 30) or \
               (timestamp_et.hour == target_hour - 1 and timestamp_et.minute >= 30):
                try:
                    # Handle MultiIndex columns when downloading single ticker
                    if isinstance(df_minute.columns, pd.MultiIndex):
                        close_price = df_minute[('Close', ticker)].iloc[idx]
                    else:
                        close_price = df_minute["Close"].iloc[idx]
                    
                    if pd.notna(close_price):
                        return {
                            'price': float(close_price),
                            'timestamp': timestamp_et,
                            'source': 'pre-market'
                        }
                except (KeyError, IndexError):
                    continue
        return None
        
    except Exception as e:
        print(f"Pre-market fetch error for {ticker}: {e}")
        return None

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

            # For now, let's use regular daily data but we can enhance this later
            # when the market is actually open and pre-market data is available
            df = yf.download(t, period="5d", interval="1d", auto_adjust=False, progress=False)
            if df is None or len(df) < 2:
                failed_downloads += 1
                continue

            # Add timestamp debugging for first few stocks
            if i <= 3:
                print(f"\n--- TIMESTAMP DEBUG for {t} ---")
                print(f"DataFrame shape: {df.shape}")
                print(f"DataFrame index (dates):")
                for idx, date in enumerate(df.index):
                    close_val = df['Close'].iloc[idx].iloc[0] if hasattr(df['Close'].iloc[idx], 'iloc') else df['Close'].iloc[idx]
                    open_val = df['Open'].iloc[idx].iloc[0] if hasattr(df['Open'].iloc[idx], 'iloc') else df['Open'].iloc[idx]
                    print(f"  {idx}: {date.strftime('%Y-%m-%d %A')} - Close: {close_val:.2f}, Open: {open_val:.2f}")
                print(f"Using prev_close from: {df.index[-2].strftime('%Y-%m-%d %A')}")
                print(f"Using today_open from: {df.index[-1].strftime('%Y-%m-%d %A')}")
                print("--- END DEBUG ---\n")

            prev_close_price = float(df["Close"].iloc[-2].iloc[0] if hasattr(df["Close"].iloc[-2], 'iloc') else df["Close"].iloc[-2])
            today_open_price = float(df["Open"].iloc[-1].iloc[0] if hasattr(df["Open"].iloc[-1], 'iloc') else df["Open"].iloc[-1])
            
            # Try to get pre-market data if we're during trading hours
            premarket_data = None
            try:
                premarket_data = get_premarket_price(t, target_hour=8)
            except Exception as e:
                if i <= 3:
                    print(f"Pre-market fetch failed for {t}: {e}")
            
            if premarket_data:
                today_open_price = premarket_data['price']
                if i <= 3:
                    print(f"Using pre-market price for {t} at {premarket_data['timestamp'].strftime('%H:%M ET')}: ${today_open_price:.2f}")
            elif i <= 3:
                print(f"No pre-market data available for {t}, using regular open: ${today_open_price:.2f}")
            if df is None or len(df) < 2:
                failed_downloads += 1
                continue

            # Assign the prices we found
            prev_close = prev_close_price
            today_open = today_open_price
            gap_pct = (today_open - prev_close) / prev_close * 100.0

            successful_downloads += 1

            # Print concise gap information during processing
            if i % 50 == 0 or gap_pct <= cfg['MIN_GAP_DOWN_PCT'] or gap_pct >= cfg['MIN_GAP_UP_PCT']:
                gap_direction = "GAP DOWN" if gap_pct <= cfg['MIN_GAP_DOWN_PCT'] else "GAP UP" if gap_pct >= cfg['MIN_GAP_UP_PCT'] else "No Gap"
                print(f"{t}: ${prev_close:.2f} → ${today_open:.2f} ({gap_pct:+.2f}%) - {gap_direction}")

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

            # Store all data for CSV with source info
            stock_data['data_source'] = premarket_data['source'] if premarket_data else 'regular-open'
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

    print(f"\n{'='*80}")
    print(f"YAHOO FINANCE DATA DEBUGGING - All Stocks Summary")
    print(f"{'='*80}")
    print(f"Successfully processed: {successful_downloads} tickers")
    print(f"Failed downloads: {failed_downloads} tickers")
    print(f"Gap-down stocks found (≤{cfg['MIN_GAP_DOWN_PCT']}%): {len(gap_down_rows)}")
    print(f"Gap-up stocks found (≥{cfg['MIN_GAP_UP_PCT']}%): {len(gap_up_rows)}")
    print(f"\nDetailed breakdown of all processed stocks:")
    print(f"{'Ticker':<8} {'Prev Close':<10} {'Pre/Open':<10} {'Gap %':<8} {'Gap $':<8} {'Status':<15} {'Data Type':<12}")
    print("-" * 92)
    
    # Print all data in sorted order
    for row in sorted(all_data, key=lambda x: x['gap_pct']):
        gap_dollar = row['today_open'] - row['prev_close']
        if row['gap_pct'] <= cfg['MIN_GAP_DOWN_PCT']:
            status = "GAP DOWN ✓"
        elif row['gap_pct'] >= cfg['MIN_GAP_UP_PCT']:
            status = "GAP UP ✓"
        else:
            status = "No Gap"
        data_type = "Pre-market" if row.get('data_source') == 'pre-market' else "Regular"
        print(f"{row['ticker']:<8} ${row['prev_close']:<9.2f} ${row['today_open']:<9.2f} {row['gap_pct']:<7.2f}% ${gap_dollar:<7.2f} {status:<15} {data_type:<12}")
    
    print(f"{'='*80}\n")

    # Sort the data
    gap_down_rows.sort(key=lambda x: x["gap_pct"])  # most negative first
    gap_up_rows.sort(key=lambda x: x["gap_pct"], reverse=True)  # most positive first
    all_data.sort(key=lambda x: x["gap_pct"])  # most negative first

    # Create and display DataFrame for verification
    display_premarket_dataframe(all_data)

    return {
        "gap_downs": gap_down_rows,
        "gap_ups": gap_up_rows,
        "all_data": all_data
    }

def display_premarket_dataframe(all_data):
    """Display a pandas DataFrame of all stocks with 8AM ET and previous close prices"""
    print(f"\n{'='*80}")
    print("8AM ET PRE-MARKET PRICES VERIFICATION DATAFRAME")
    print(f"{'='*80}")
    
    if not all_data:
        print("No data available to display")
        return
    
    # Create DataFrame
    df_data = []
    for stock in all_data:
        df_data.append({
            'Ticker': stock['ticker'],
            'Prev_Close': f"${stock['prev_close']:.2f}",
            '8AM_ET_Price': f"${stock['today_open']:.2f}",
            'Gap_$': f"${(stock['today_open'] - stock['prev_close']):+.2f}",
            'Gap_%': f"{stock['gap_pct']:+.2f}%",
            'Data_Source': stock.get('data_source', 'regular-open')
        })
    
    # Create pandas DataFrame
    df = pd.DataFrame(df_data)
    
    # Count pre-market vs regular data
    premarket_count = len([s for s in all_data if s.get('data_source') == 'pre-market'])
    regular_count = len(all_data) - premarket_count
    
    print(f"Total stocks: {len(all_data)} | Pre-market data: {premarket_count} | Regular open: {regular_count}")
    print("\nComplete price data:")
    print(df.to_string(index=False))
    print(f"{'='*80}\n")

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

    # Build HTML content with debugging info
    html_parts = [
        f"<h2>Daily Gap Analysis - {datetime.now().strftime('%Y-%m-%d')}</h2>",
        f"<p><strong>Data Source:</strong> {cfg.get('DATA_SOURCE', 'Unknown').upper()}</p>",
        f"<p><strong>Timing:</strong> Pre-market data (8:00 AM ET) vs Previous Close</p>",
        f"<p><strong>Gap Down Threshold:</strong> ≤ {cfg['MIN_GAP_DOWN_PCT']}%</p>",
        f"<p><strong>Gap Up Threshold:</strong> ≥ {cfg['MIN_GAP_UP_PCT']}%</p>",
        f"<p><strong>Total Stocks Analyzed:</strong> {len(all_data)}</p>",
        build_html_table(gap_downs, "Gap Down Stocks (Pre-market)", 0),
        build_html_table(gap_ups, "Gap Up Stocks (Pre-market)", 0),
        "<p><em>Complete data with all stocks attached as CSV file for debugging.</em></p>"
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
            data = polygon_gap_scan(cfg)
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
