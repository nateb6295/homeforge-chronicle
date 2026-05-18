#!/usr/bin/env python3
"""Send SMS to Nate via Verizon email-to-SMS gateway.

Usage:
  python3 sms.py "Your message here"
  python3 sms.py  # reads from stdin

Requires GMAIL_APP_PASSWORD in chronicle.env.
To generate: Google Account → Security → 2-Step Verification → App passwords
"""
import smtplib, sys, os
from email.mime.text import MIMEText
from pathlib import Path

NATE_SMS = "2533121887@vtext.com"
GMAIL_USER = "bradfordnathaniel92@gmail.com"
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

env_path = Path.home() / "chronicle" / "chronicle.env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        if line.startswith("GMAIL_APP_PASSWORD="):
            os.environ["GMAIL_APP_PASSWORD"] = line.split("=", 1)[1].strip().strip('"').strip("'")

APP_PASS = os.environ.get("GMAIL_APP_PASSWORD", "")


def send_sms(message: str) -> bool:
    if not APP_PASS:
        print("ERROR: GMAIL_APP_PASSWORD not set in chronicle.env")
        print("Generate at: Google Account → Security → App passwords")
        return False

    msg = MIMEText(message)
    msg["From"] = GMAIL_USER
    msg["To"] = NATE_SMS

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(GMAIL_USER, APP_PASS)
            server.send_message(msg)
        print(f"SMS sent ({len(message)} chars)")
        return True
    except Exception as e:
        print(f"SMS failed: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        text = " ".join(sys.argv[1:])
    else:
        text = sys.stdin.read().strip()

    if not text:
        print("Usage: sms.py 'message'")
        sys.exit(1)

    ok = send_sms(text)
    sys.exit(0 if ok else 1)
