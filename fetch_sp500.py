#!/usr/bin/env python3
"""
Script to fetch S&P 500 ticker symbols and save to CSV
"""
import pandas as pd
import os

def fetch_sp500_symbols():
    """Fetch S&P 500 symbols from Wikipedia"""
    try:
        print("Fetching S&P 500 symbols from Wikipedia...")

        # URL of the Wikipedia page containing S&P 500 companies
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

        # Read the HTML tables from the page
        tables = pd.read_html(url)

        # The first table contains the list of companies
        sp500_table = tables[0]

        # Extract the 'Symbol' column and clean it
        sp500_symbols = sp500_table['Symbol'].tolist()

        # Remove any symbols with dots (like BRK.B) and replace with hyphens for yfinance
        cleaned_symbols = []
        for symbol in sp500_symbols:
            if '.' in symbol:
                # Replace dots with hyphens for yfinance compatibility
                cleaned_symbol = symbol.replace('.', '-')
                cleaned_symbols.append(cleaned_symbol)
            else:
                cleaned_symbols.append(symbol)

        print(f"Found {len(cleaned_symbols)} S&P 500 symbols")

        # Save to CSV
        output_file = 'sp500_tickers.csv'
        with open(output_file, 'w', newline='') as f:
            f.write('TICKER\n')
            for symbol in cleaned_symbols:
                f.write(f'{symbol}\n')

        print(f"S&P 500 symbols saved to {output_file}")
        return cleaned_symbols

    except Exception as e:
        print(f"Error fetching S&P 500 symbols: {e}")
        return None

def fetch_nasdaq_nyse_symbols():
    """Fetch all NASDAQ and NYSE symbols from NASDAQ FTP"""
    try:
        import ftplib
        from io import StringIO

        print("Fetching NASDAQ and NYSE symbols from NASDAQ FTP...")

        # FTP server details
        ftp_server = 'ftp.nasdaqtrader.com'
        ftp_path = '/SymbolDirectory'
        files = ['nasdaqlisted.txt', 'otherlisted.txt']

        all_symbols = []

        # Connect to the FTP server
        ftp = ftplib.FTP(ftp_server)
        ftp.login()

        for file in files:
            print(f"Downloading {file}...")

            # Change to the directory containing the files
            ftp.cwd(ftp_path)

            # Retrieve the file contents
            r = StringIO()
            ftp.retrlines(f'RETR {file}', lambda line: r.write(line + '\n'))
            r.seek(0)

            # Read the file into a DataFrame
            df = pd.read_csv(r, sep='|')

            # Filter for common stocks (exclude test issues and certain financial statuses)
            if 'Test Issue' in df.columns:
                df = df[df['Test Issue'] == 'N']
            if 'Financial Status' in df.columns:
                df = df[df['Financial Status'].isin(['N', 'D'])]

            # Extract the symbols
            symbols = df['Symbol'].tolist()
            all_symbols.extend(symbols)

        # Close the FTP connection
        ftp.quit()

        # Remove duplicates and sort
        unique_symbols = sorted(list(set(all_symbols)))

        print(f"Found {len(unique_symbols)} total NASDAQ/NYSE symbols")

        # Save to CSV
        output_file = 'nasdaq_nyse_tickers.csv'
        with open(output_file, 'w', newline='') as f:
            f.write('TICKER\n')
            for symbol in unique_symbols:
                f.write(f'{symbol}\n')

        print(f"NASDAQ/NYSE symbols saved to {output_file}")
        return unique_symbols

    except Exception as e:
        print(f"Error fetching NASDAQ/NYSE symbols: {e}")
        return None

if __name__ == "__main__":
    print("Stock Symbol Fetcher")
    print("===================")

    # Fetch S&P 500 symbols
    sp500_symbols = fetch_sp500_symbols()

    if sp500_symbols:
        print(f"\nS&P 500 symbols fetched successfully!")
        print(f"First 10 symbols: {sp500_symbols[:10]}")

    # Uncomment the line below to also fetch all NASDAQ/NYSE symbols
    # nasdaq_nyse_symbols = fetch_nasdaq_nyse_symbols()
