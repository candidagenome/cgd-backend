"""
Email and notification utilities.

This module provides functions for sending email notifications
and error alerts.
"""

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_SMTP_HOST = "localhost"
DEFAULT_SMTP_PORT = 25
DEFAULT_FROM_EMAIL = "noreply@candidagenome.org"


def send_email(
    to_email: str | list[str],
    subject: str,
    body: str,
    from_email: Optional[str] = None,
    smtp_host: Optional[str] = None,
    smtp_port: Optional[int] = None,
    html: bool = False,
) -> bool:
    """
    Send an email notification.

    Args:
        to_email: Recipient email address(es)
        subject: Email subject
        body: Email body text
        from_email: Sender email address
        smtp_host: SMTP server hostname
        smtp_port: SMTP server port
        html: Whether body is HTML format

    Returns:
        True if sent successfully, False otherwise

    Example:
        >>> send_email("curator@example.org", "Alert", "Something happened")
        True
    """
    # Get configuration
    from_email = from_email or os.getenv("FROM_EMAIL", DEFAULT_FROM_EMAIL)
    smtp_host = smtp_host or os.getenv("SMTP_HOST", DEFAULT_SMTP_HOST)
    smtp_port = smtp_port or int(os.getenv("SMTP_PORT", DEFAULT_SMTP_PORT))

    # Handle list of recipients
    if isinstance(to_email, str):
        recipients = [to_email]
    else:
        recipients = list(to_email)

    # Filter out empty addresses
    recipients = [r for r in recipients if r and r.strip()]

    if not recipients:
        logger.warning("No valid recipients for email")
        return False

    try:
        # Create message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = ", ".join(recipients)

        # Attach body
        content_type = "html" if html else "plain"
        msg.attach(MIMEText(body, content_type))

        # Send
        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.sendmail(from_email, recipients, msg.as_string())

        logger.info(f"Email sent successfully to {recipients}")
        return True

    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending email: {e}")
        return False
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        return False


def send_error_email(
    subject: str,
    message: str,
    curator_email: Optional[str] = None,
    include_traceback: bool = True,
) -> bool:
    """
    Send an error notification email.

    This is a convenience function for sending error alerts
    to curators.

    Args:
        subject: Error subject
        message: Error message
        curator_email: Recipient email (default: from CURATOR_EMAIL env var)
        include_traceback: Include Python traceback if available

    Returns:
        True if sent successfully, False otherwise
    """
    curator_email = curator_email or os.getenv("CURATOR_EMAIL", "")

    if not curator_email:
        logger.warning("CURATOR_EMAIL not set, cannot send error notification")
        logger.error(f"Error notification: {subject}")
        logger.error(f"Message: {message}")
        return False

    # Build body
    body_parts = [
        "An error occurred in a CGD script:",
        "",
        f"Subject: {subject}",
        "",
        "Message:",
        message,
    ]

    if include_traceback:
        import traceback
        tb = traceback.format_exc()
        if tb and tb != "NoneType: None\n":
            body_parts.extend([
                "",
                "Traceback:",
                tb,
            ])

    body = "\n".join(body_parts)

    return send_email(
        to_email=curator_email,
        subject=f"[CGD Error] {subject}",
        body=body,
    )


def send_completion_email(
    script_name: str,
    summary: str,
    curator_email: Optional[str] = None,
    stats: Optional[dict] = None,
) -> bool:
    """
    Send a script completion notification.

    Args:
        script_name: Name of the completed script
        summary: Summary of what was done
        curator_email: Recipient email
        stats: Optional dictionary of statistics to include

    Returns:
        True if sent successfully, False otherwise
    """
    curator_email = curator_email or os.getenv("CURATOR_EMAIL", "")

    if not curator_email:
        logger.info(f"Script completed: {script_name}")
        logger.info(f"Summary: {summary}")
        return False

    body_parts = [
        f"Script: {script_name}",
        "",
        "Summary:",
        summary,
    ]

    if stats:
        body_parts.extend([
            "",
            "Statistics:",
        ])
        for key, value in stats.items():
            body_parts.append(f"  {key}: {value}")

    body = "\n".join(body_parts)

    return send_email(
        to_email=curator_email,
        subject=f"[CGD] {script_name} completed",
        body=body,
    )


class EmailNotifier:
    """
    Email notification helper class.

    This class provides a convenient interface for sending
    notifications with consistent formatting.
    """

    def __init__(
        self,
        default_recipient: Optional[str] = None,
        default_subject_prefix: str = "[CGD]",
        smtp_host: Optional[str] = None,
        smtp_port: Optional[int] = None,
    ):
        """
        Initialize the notifier.

        Args:
            default_recipient: Default email recipient
            default_subject_prefix: Prefix for all subjects
            smtp_host: SMTP server hostname
            smtp_port: SMTP server port
        """
        self.default_recipient = default_recipient or os.getenv("CURATOR_EMAIL", "")
        self.subject_prefix = default_subject_prefix
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port

    def notify(
        self,
        subject: str,
        message: str,
        recipient: Optional[str] = None,
    ) -> bool:
        """Send a notification."""
        recipient = recipient or self.default_recipient
        full_subject = f"{self.subject_prefix} {subject}"

        return send_email(
            to_email=recipient,
            subject=full_subject,
            body=message,
            smtp_host=self.smtp_host,
            smtp_port=self.smtp_port,
        )

    def notify_error(self, subject: str, message: str) -> bool:
        """Send an error notification."""
        return self.notify(f"[Error] {subject}", message)

    def notify_success(self, subject: str, message: str) -> bool:
        """Send a success notification."""
        return self.notify(f"[Success] {subject}", message)
