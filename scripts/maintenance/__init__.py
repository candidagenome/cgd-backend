"""
CGD Maintenance Scripts

This package contains scripts for system maintenance, archiving, and
administrative tasks.

Scripts:
- archive_website.py: Archive website pages for historical record
- convert_logs_weekly_to_monthly.py: Convert weekly Apache logs to monthly

Usage:
    python scripts/maintenance/archive_website.py --help
    python scripts/maintenance/convert_logs_weekly_to_monthly.py --help

Environment Variables:
    HTML_ROOT_DIR: Root directory for HTML files
    HTML_ROOT_URL: Base URL of the website
    CGI_ROOT_URL: Base URL for CGI scripts
    WEB_LOG_DIR: Directory containing Apache log files
    LOG_DIR: Directory for log files
    CURATOR_EMAIL: Email for error notifications
"""
