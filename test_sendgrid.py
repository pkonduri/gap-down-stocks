#!/usr/bin/env python3
"""
Simple test script to verify Resend API key
"""
import os
from dotenv import load_dotenv
import resend

def test_resend():
    load_dotenv()

    api_key = os.getenv('RESEND_API_KEY')
    email_from = os.getenv('EMAIL_FROM')
    email_to = os.getenv('EMAIL_TO')

    if not api_key or not email_from or not email_to:
        print("ERROR: Missing required environment variables")
        return False

    print(f"Testing Resend API key: {api_key[:10]}...")
    print(f"From: {email_from}")
    print(f"To: {email_to}")

    try:
        # Set the API key
        resend.api_key = api_key

        # Send a simple test email
        params = {
            "from": email_from,
            "to": [email_to],
            "subject": "Resend API Test",
            "html": "<p>This is a test email to verify your Resend API key is working.</p>"
        }

        email = resend.Emails.send(params)
        print(f"SUCCESS: Email sent! ID: {email['id']}")
        return True
    except Exception as e:
        print(f"ERROR: {e}")
        if "401" in str(e) or "Unauthorized" in str(e):
            print("\nThis suggests your Resend API key is invalid or expired.")
            print("Please check your Resend dashboard and create a new API key.")
        return False

if __name__ == "__main__":
    test_resend()
