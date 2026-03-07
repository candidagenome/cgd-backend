#!/usr/bin/env python3
"""
Check Pathway Tools (ptools) service health.

This script monitors the Pathway Tools service by requesting a test pathway.
If the service doesn't respond within the timeout, it attempts to restart it.

Environment Variables:
    PTOOLS_TEST_URL: URL template for testing ptools (with _SUBSTITUTE_THIS_ placeholder)
    PTOOLS_TEST_PATHWAY: Test pathway ID (default: POLYAMSYN-YEAST-PWY)
    PTOOLS_RESTART_CMD: Command to restart ptools (default: /etc/init.d/pathway restart)
    PTOOLS_TIMEOUT: Timeout in seconds (default: 60)
    LOG_DIR: Directory for log files (default: /tmp)
    CURATOR_EMAIL: Email for notifications
"""

import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
PTOOLS_TEST_URL = os.getenv(
    "PTOOLS_TEST_URL",
    "http://localhost/cgi-bin/pathway/pathway.pl?object=_SUBSTITUTE_THIS_"
)
PTOOLS_TEST_PATHWAY = os.getenv("PTOOLS_TEST_PATHWAY", "POLYAMSYN-YEAST-PWY")
PTOOLS_RESTART_CMD = os.getenv("PTOOLS_RESTART_CMD", "/etc/init.d/pathway restart")
PTOOLS_TIMEOUT = int(os.getenv("PTOOLS_TIMEOUT", "60"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
LOG_FILE = LOG_DIR / "ptools_restart.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def send_email(message: str) -> None:
    """Send notification email."""
    curator_email = os.getenv("CURATOR_EMAIL")
    if curator_email:
        logger.info(f"Would send email to {curator_email}: {message}")
        # In production, implement actual email sending here


def restart_ptools() -> bool:
    """
    Restart the Pathway Tools service.

    Returns:
        True on success, False on failure
    """
    logger.info("Attempting to restart Pathway Tools")

    try:
        result = subprocess.run(
            PTOOLS_RESTART_CMD.split(),
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode != 0:
            error_msg = f"Error restarting ptools: {result.stderr}"
            logger.error(error_msg)
            send_email(error_msg)
            return False

        logger.info(f"Ptools has been restarted at: {datetime.now()}")
        return True

    except subprocess.TimeoutExpired:
        error_msg = "Timeout while restarting ptools"
        logger.error(error_msg)
        send_email(error_msg)
        return False

    except Exception as e:
        error_msg = f"Exception while restarting ptools: {e}"
        logger.error(error_msg)
        send_email(error_msg)
        return False


def check_ptools() -> bool:
    """
    Check if Pathway Tools is responding.

    Returns:
        True if healthy, False if needs restart
    """
    test_url = PTOOLS_TEST_URL.replace("_SUBSTITUTE_THIS_", PTOOLS_TEST_PATHWAY)
    logger.info(f"Testing ptools at: {test_url}")

    try:
        response = requests.get(test_url, timeout=PTOOLS_TIMEOUT)

        # Check if response contains error indicators
        if response.status_code != 200:
            logger.warning(f"Ptools returned status code: {response.status_code}")
            return False

        # Check for lynx-style errors in response (legacy check)
        if "lynx:" in response.text.lower() or "error" in response.text[:500].lower():
            logger.warning("Ptools response indicates an error")
            return False

        logger.info("Ptools is responding normally")
        return True

    except requests.exceptions.Timeout:
        logger.warning(f"Ptools request timed out after {PTOOLS_TIMEOUT} seconds")
        return False

    except requests.exceptions.ConnectionError:
        logger.warning("Could not connect to ptools")
        return False

    except Exception as e:
        logger.warning(f"Error checking ptools: {e}")
        return False


def main() -> int:
    """Main entry point."""
    logger.info("Starting ptools health check")

    if check_ptools():
        return 0

    # Ptools is not responding, try to restart
    logger.warning("Ptools appears to be down or hung")

    if restart_ptools():
        send_email("Ptools seemed to be in a hung state and has been restarted.")
        return 0
    else:
        send_email("Failed to restart ptools - manual intervention required.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
