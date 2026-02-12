#!/usr/bin/env python3
"""
Check reCAPTCHA service health.

This script verifies that the reCAPTCHA service is accessible and the API keys
are valid by making a test verification request.

Environment Variables:
    RECAPTCHA_PUBLIC_KEY: Google reCAPTCHA site key
    RECAPTCHA_PRIVATE_KEY: Google reCAPTCHA secret key
    CURATOR_EMAIL: Email address for error notifications (optional)
    LOG_DIR: Directory for log files (optional, defaults to /tmp)
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
LOG_DIR = os.getenv("LOG_DIR", "/tmp")
LOG_FILE = Path(LOG_DIR) / "check_recaptcha.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# reCAPTCHA verification URL
RECAPTCHA_VERIFY_URL = "https://www.google.com/recaptcha/api/siteverify"


def get_required_env(key: str) -> str:
    """Get a required environment variable or exit with error."""
    value = os.getenv(key)
    if not value:
        logger.error(f"Required environment variable {key} is not set")
        sys.exit(1)
    return value


def send_error_email(message: str) -> None:
    """Send error notification email."""
    curator_email = os.getenv("CURATOR_EMAIL")
    if not curator_email:
        logger.warning("CURATOR_EMAIL not set, skipping email notification")
        return

    # For now, just log the error. In production, integrate with your email service
    # You could use smtplib, SendGrid, AWS SES, etc.
    logger.info(f"Would send error email to {curator_email}: {message}")


def check_recaptcha() -> bool:
    """
    Test reCAPTCHA service connectivity and API key validity.

    Returns:
        True if service is accessible (even if verification fails due to
        invalid response token - that's expected), False if there's a
        connectivity or configuration issue.
    """
    public_key = get_required_env("RECAPTCHA_PUBLIC_KEY")
    private_key = get_required_env("RECAPTCHA_PRIVATE_KEY")

    logger.info(f"Starting reCAPTCHA check at {datetime.now()}")
    logger.info(f"Using public key: {public_key[:10]}...")

    # Test verification with a dummy response
    # This should return an error (invalid response), but confirms the API is reachable
    try:
        response = requests.post(
            RECAPTCHA_VERIFY_URL,
            data={
                "secret": private_key,
                "response": "test_invalid_response",
            },
            timeout=30,
        )
        response.raise_for_status()

        result = response.json()
        logger.info(f"reCAPTCHA API response: {result}")

        if result.get("success"):
            # This shouldn't happen with a fake response, but if it does, all is well
            logger.info("reCAPTCHA verification succeeded (unexpected with test token)")
            return True

        error_codes = result.get("error-codes", [])
        if error_codes:
            # Expected errors like "invalid-input-response" mean the API is working
            expected_errors = {"invalid-input-response", "timeout-or-duplicate"}
            if any(code in expected_errors for code in error_codes):
                logger.info(
                    f"reCAPTCHA API is accessible. Expected error: {error_codes}"
                )
                return True

            # Unexpected errors might indicate configuration issues
            if "invalid-input-secret" in error_codes:
                error_msg = "reCAPTCHA secret key is invalid!"
                logger.error(error_msg)
                send_error_email(error_msg)
                return False

            logger.warning(f"Unexpected error codes: {error_codes}")

        return True

    except requests.exceptions.Timeout:
        error_msg = "reCAPTCHA API request timed out"
        logger.error(error_msg)
        send_error_email(error_msg)
        return False

    except requests.exceptions.RequestException as e:
        error_msg = f"reCAPTCHA API request failed: {e}"
        logger.error(error_msg)
        send_error_email(error_msg)
        return False

    except Exception as e:
        error_msg = f"Unexpected error checking reCAPTCHA: {e}"
        logger.error(error_msg)
        send_error_email(error_msg)
        return False


def main() -> int:
    """Main entry point."""
    logger.info(f"Program {__file__}: Starting {datetime.now()}")

    success = check_recaptcha()

    logger.info(f"Complete {datetime.now()}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
