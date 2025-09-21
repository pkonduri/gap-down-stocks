# Gap Down Stocks Emailer

A Python application that scans S&P 500 stocks for gap-down and gap-up opportunities and sends daily email reports.

## Features

- Scans S&P 500 stocks for gap opportunities
- Configurable gap thresholds for both up and down movements
- Sends HTML email reports with tables and CSV attachments
- Runs nightly at midnight EST via Fly.io deployment
- Uses Resend for email delivery

## Setup

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Create a `.env` file with your configuration:

```env
# Data source
DATA_SOURCE=yahoo
TICKERS_CSV=sp500_tickers.csv

# Gap thresholds
MIN_GAP_DOWN_PCT=-1
MIN_GAP_UP_PCT=1

# Email configuration
RESEND_API_KEY=your_resend_api_key
EMAIL_FROM=your_email@domain.com
EMAIL_TO=recipient@domain.com
EMAIL_SUBJECT_PREFIX=[Gap Analysis]

# Optional: Financial Modeling Prep API for all exchanges
FINANCIAL_MODELING_PREP_API_KEY=your_fmp_api_key
```

## Usage

### Local Development
```bash
python gap_down_email.py
```

### Scheduled Runs
```bash
python scheduler.py
```

## Deployment

This app is designed to run on Fly.io with nightly email reports.

### Deploy to Fly.io

1. Install Fly CLI: https://fly.io/docs/hands-on/install-flyctl/
2. Login: `fly auth login`
3. Deploy: `fly deploy`

The app will automatically run gap analysis at midnight EST daily.

## Email Format

The email includes:
- Gap Down Stocks table (stocks that gapped down by threshold or more)
- Gap Up Stocks table (stocks that gapped up by threshold or more)  
- CSV attachment with complete data for all stocks

## Configuration

- `MIN_GAP_DOWN_PCT`: Negative number for gap-down threshold (e.g., -1 for -1% or worse)
- `MIN_GAP_UP_PCT`: Positive number for gap-up threshold (e.g., 1 for +1% or better)
- `TICKERS_CSV`: CSV file with ticker symbols, or "all_exchanges" for all NYSE/NASDAQ stocks