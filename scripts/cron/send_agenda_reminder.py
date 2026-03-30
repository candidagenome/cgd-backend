#!/usr/bin/env python3
"""
Send agenda brief reminder email to curators.

Meeting is Thursday 12:30PM, so reminder is sent Monday morning.

Usage:
    python send_agenda_reminder.py
    ./slack-cron.sh ./send_agenda_reminder.py

Environment Variables:
    SMTP_HOST: SMTP server (default: localhost)
    CURATOR_EMAIL: Curator mailing list email
    ADMIN_EMAIL: Admin email address
    PROJECT_ACRONYM: Project acronym (default: CGD)
"""
from __future__ import annotations

import os
import smtplib
import sys
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables
load_dotenv(PROJECT_ROOT / ".env")

# Configuration
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "cgd-curators@lists.stanford.edu")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "cgd-admin@lists.stanford.edu")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")


def send_reminder() -> int:
    """Send agenda brief reminder email."""
    subject = "Agenda Item Briefs"

    body = f"""Hi all,

Please send your briefs by Wednesday 5pm for Thursday's meeting.

Cordially,
On behalf of the {PROJECT_ACRONYM} Curators
"""

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = f"{PROJECT_ACRONYM} Admin <{ADMIN_EMAIL}>"
    msg["To"] = f"{PROJECT_ACRONYM} Curators <{CURATOR_EMAIL}>"
    msg["Reply-To"] = f"{PROJECT_ACRONYM} Curators <{CURATOR_EMAIL}>"

    try:
        with smtplib.SMTP(SMTP_HOST) as smtp:
            smtp.send_message(msg)
        print(f"Agenda reminder sent to {CURATOR_EMAIL}")
        return 0
    except Exception as e:
        print(f"ERROR: Failed to send email: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(send_reminder())
