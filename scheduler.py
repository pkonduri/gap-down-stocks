#!/usr/bin/env python3
"""
Nightly Gap Analysis Scheduler
Runs the gap analysis at midnight EST and sends email reports
"""
import schedule
import time
import os
import sys
from datetime import datetime
import pytz

# Add current directory to path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from gap_down_email import main as run_gap_analysis

def job():
    """Run the gap analysis and send email"""
    try:
        print(f"Starting gap analysis at {datetime.now()}")
        run_gap_analysis()
        print(f"Gap analysis completed at {datetime.now()}")
    except Exception as e:
        print(f"Error running gap analysis: {e}")
        # You could add error notification here if needed

def main():
    """Main scheduler loop"""
    print("Starting Gap Analysis Scheduler...")
    print("Scheduling gap analysis for 8:00 AM CT daily")

    # Schedule the job to run at 8:00 AM CT
    # Note: Using CT timezone handling
    ct_tz = pytz.timezone('US/Central')
    schedule.every().day.at("08:00").do(job)

    # Also run immediately on startup for testing (remove this in production)
    print("Running initial gap analysis...")
    job()

    print("Scheduler started. Waiting for scheduled runs...")

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
