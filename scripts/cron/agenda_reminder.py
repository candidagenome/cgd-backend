#!/usr/bin/env python3
"""
Send agenda item brief reminder email.

This script sends a reminder email to curators about submitting
their agenda item briefs.

Original Perl: agenda.pl
Converted to Python: 2024

Usage:
    python agenda_reminder.py
    python agenda_reminder.py --dry-run
    python agenda_reminder.py --to custom@email.com
"""

import argparse
import logging
import os
import smtplib
import sys
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Default configuration (can be overridden by environment variables)
DEFAULT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
DEFAULT_ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "cgd-admin@stanford.edu")
DEFAULT_CURATORS_EMAIL = os.getenv("CURATORS_EMAIL", "cgd-curators@lists.stanford.edu")
DEFAULT_SMTP_SERVER = os.getenv("SMTP_SERVER", "localhost")


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def send_agenda_reminder(
    acronym: str = DEFAULT_ACRONYM,
    admin_email: str = DEFAULT_ADMIN_EMAIL,
    curators_email: str = DEFAULT_CURATORS_EMAIL,
    smtp_server: str = DEFAULT_SMTP_SERVER,
    dry_run: bool = False,
) -> bool:
    """
    Send agenda item brief reminder email.

    Args:
        acronym: Project acronym (e.g., CGD)
        admin_email: Admin email address (From)
        curators_email: Curators email address (To)
        smtp_server: SMTP server hostname
        dry_run: If True, print email instead of sending

    Returns:
        True if successful
    """
    # Compose message
    body = f"""Hi all,

Please send your briefs by Monday 3pm.

Cordially,
On behalf of the {acronym} Curators
"""

    msg = MIMEText(body)
    msg['Subject'] = 'Agenda Item Briefs'
    msg['From'] = f'{acronym} Admin <{admin_email}>'
    msg['To'] = f'{acronym} Curators <{curators_email}>'
    msg['Reply-To'] = f'{acronym} Curators <{curators_email}>'

    if dry_run:
        logger.info("Dry run - would send email:")
        print("-" * 50)
        print(f"From: {msg['From']}")
        print(f"To: {msg['To']}")
        print(f"Reply-To: {msg['Reply-To']}")
        print(f"Subject: {msg['Subject']}")
        print()
        print(body)
        print("-" * 50)
        return True

    try:
        with smtplib.SMTP(smtp_server) as server:
            server.sendmail(admin_email, [curators_email], msg.as_string())
        logger.info(f"Sent agenda reminder to {curators_email}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Send agenda item brief reminder email"
    )
    parser.add_argument(
        "--acronym",
        default=DEFAULT_ACRONYM,
        help=f"Project acronym (default: {DEFAULT_ACRONYM})",
    )
    parser.add_argument(
        "--from-email",
        default=DEFAULT_ADMIN_EMAIL,
        help=f"Admin email address (default: {DEFAULT_ADMIN_EMAIL})",
    )
    parser.add_argument(
        "--to",
        default=DEFAULT_CURATORS_EMAIL,
        help=f"Curators email address (default: {DEFAULT_CURATORS_EMAIL})",
    )
    parser.add_argument(
        "--smtp-server",
        default=DEFAULT_SMTP_SERVER,
        help=f"SMTP server (default: {DEFAULT_SMTP_SERVER})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print email instead of sending",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    success = send_agenda_reminder(
        acronym=args.acronym,
        admin_email=args.from_email,
        curators_email=args.to,
        smtp_server=args.smtp_server,
        dry_run=args.dry_run,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
