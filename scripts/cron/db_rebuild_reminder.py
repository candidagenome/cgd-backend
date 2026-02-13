#!/usr/bin/env python3
"""
Send monthly reminder to rebuild development database from production.

This is a simple cron job that sends an email reminder to curators
to rebuild the development database from production.

Environment Variables:
    CURATOR_EMAIL: Email address for curators
    ADMIN_EMAIL: Admin email address (sender)
    PROJECT_ACRONYM: Project acronym (e.g., CGD, AspGD)
    SMTP_HOST: SMTP server host (default: localhost)
    SMTP_PORT: SMTP server port (default: 25)
"""

import logging
import os
import smtplib
import sys
from email.message import EmailMessage

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def send_reminder_email() -> bool:
    """
    Send the database rebuild reminder email.

    Returns:
        True on success, False on failure
    """
    curator_email = os.getenv("CURATOR_EMAIL")
    admin_email = os.getenv("ADMIN_EMAIL", "admin@localhost")
    acronym = os.getenv("PROJECT_ACRONYM", "CGD")
    smtp_host = os.getenv("SMTP_HOST", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))

    if not curator_email:
        logger.error("CURATOR_EMAIL environment variable not set")
        return False

    # Compose email
    msg = EmailMessage()
    msg["From"] = f"{acronym} Admin <{admin_email}>"
    msg["To"] = f"{acronym} Curators <{curator_email}>"
    msg["Reply-To"] = f"{acronym} Curators <{curator_email}>"
    msg["Subject"] = f"Reminder to Rebuild Development DB from {acronym} Production"

    body = f"""
This is your monthly reminder to rebuild the development database from {acronym} production database.

Cordially,
On behalf of the {acronym} Curators
"""
    msg.set_content(body.strip())

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.send_message(msg)
        logger.info(f"Reminder email sent to {curator_email}")
        return True

    except smtplib.SMTPException as e:
        logger.error(f"Failed to send email: {e}")
        return False

    except Exception as e:
        logger.error(f"Unexpected error sending email: {e}")
        return False


def main() -> int:
    """Main entry point."""
    logger.info("Sending DB rebuild reminder email")
    success = send_reminder_email()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
