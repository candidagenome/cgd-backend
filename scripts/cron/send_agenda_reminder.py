#!/usr/bin/env python3
"""
Send agenda brief reminder to Slack channel.

Meeting is Thursday 12:30PM, so reminder is sent Monday morning.

Usage:
    python send_agenda_reminder.py
    ./slack-cron.sh ./cgd_send_agenda_reminder.sh

Environment Variables:
    SLACK_WEBHOOK_URL: Slack incoming webhook URL (required)
    PROJECT_ACRONYM: Project acronym (default: CGD)
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables
load_dotenv(PROJECT_ROOT / ".env")

# Configuration
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")


def send_reminder() -> int:
    """Send agenda brief reminder to Slack."""
    if not SLACK_WEBHOOK_URL:
        print("ERROR: SLACK_WEBHOOK_URL not set in environment", file=sys.stderr)
        return 1

    message = {
        "text": f"*Agenda Item Briefs Reminder*\n\nHi all,\n\nPlease send your briefs by Wednesday 5pm for Thursday's meeting.\n\nCordially,\nOn behalf of the {PROJECT_ACRONYM} Curators"
    }

    try:
        data = json.dumps(message).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                print("Agenda reminder sent to Slack")
                return 0
            else:
                print(f"ERROR: Slack returned status {response.status}", file=sys.stderr)
                return 1
    except Exception as e:
        print(f"ERROR: Failed to send Slack message: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(send_reminder())
