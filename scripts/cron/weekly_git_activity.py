#!/usr/bin/env python3
"""
Report weekly Git repository activity.

This script reports Git activity (commits, added files, deleted files) over the
past week to help track changes and synchronize between branches.

Note: This is a modernized version of weeklyCVSactivity.pl, converted to use Git.

Usage:
    python weekly_git_activity.py /path/to/repo [--branch main] [--since 2024-01-01]

Environment Variables:
    LOG_DIR: Directory for log files (default: /tmp)
    CURATOR_EMAIL: Email for reports
    ADMIN_EMAIL: Sender email address
"""

import argparse
import logging
import os
import smtplib
import subprocess
import sys
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@localhost")
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def run_git_command(repo_path: Path, args: list[str]) -> tuple[str, str, int]:
    """
    Run a git command and return output.

    Args:
        repo_path: Path to repository
        args: Git command arguments

    Returns:
        Tuple of (stdout, stderr, return_code)
    """
    cmd = ["git", "-C", str(repo_path)] + args

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        return result.stdout, result.stderr, result.returncode

    except subprocess.TimeoutExpired:
        return "", "Command timed out", 1


def get_commits_since(repo_path: Path, since_date: str, branch: str | None = None) -> list[dict]:
    """
    Get commits since a given date.

    Args:
        repo_path: Path to repository
        since_date: ISO date string (YYYY-MM-DD)
        branch: Optional branch name

    Returns:
        List of commit dictionaries
    """
    args = [
        "log",
        f"--since={since_date}",
        "--pretty=format:%H|%an|%ae|%ad|%s",
        "--date=short",
    ]

    if branch:
        args.append(branch)

    stdout, stderr, code = run_git_command(repo_path, args)

    if code != 0:
        logger.error(f"git log failed: {stderr}")
        return []

    commits = []
    for line in stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split("|", 4)
        if len(parts) >= 5:
            commits.append({
                "hash": parts[0],
                "author": parts[1],
                "email": parts[2],
                "date": parts[3],
                "subject": parts[4],
            })

    return commits


def get_changed_files(repo_path: Path, since_date: str, branch: str | None = None) -> dict:
    """
    Get files changed since a given date.

    Args:
        repo_path: Path to repository
        since_date: ISO date string
        branch: Optional branch name

    Returns:
        Dictionary with 'added', 'modified', 'deleted' file lists
    """
    args = [
        "log",
        f"--since={since_date}",
        "--name-status",
        "--pretty=format:",
    ]

    if branch:
        args.append(branch)

    stdout, stderr, code = run_git_command(repo_path, args)

    if code != 0:
        logger.error(f"git log failed: {stderr}")
        return {"added": [], "modified": [], "deleted": []}

    added = set()
    modified = set()
    deleted = set()

    for line in stdout.split("\n"):
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t", 1)
        if len(parts) < 2:
            continue

        status, filepath = parts[0], parts[1]

        if status.startswith("A"):
            added.add(filepath)
        elif status.startswith("M"):
            modified.add(filepath)
        elif status.startswith("D"):
            deleted.add(filepath)

    return {
        "added": sorted(added),
        "modified": sorted(modified),
        "deleted": sorted(deleted),
    }


def generate_report(
    repo_path: Path,
    since_date: str,
    branch: str | None,
    commits: list[dict],
    changed_files: dict,
) -> str:
    """Generate the activity report."""
    report = []
    report.append(f"Git Activity Report for {repo_path}")
    report.append("=" * 60)
    report.append(f"Branch: {branch or 'all'}")
    report.append(f"Since: {since_date}")
    report.append(f"Generated: {datetime.now().isoformat()}")
    report.append("")

    # Summary
    report.append(f"Total commits: {len(commits)}")
    report.append(f"Files added: {len(changed_files['added'])}")
    report.append(f"Files modified: {len(changed_files['modified'])}")
    report.append(f"Files deleted: {len(changed_files['deleted'])}")
    report.append("")

    # Added files
    if changed_files["added"]:
        report.append("ADDED FILES:")
        for f in changed_files["added"]:
            report.append(f"  {f}")
        report.append("")
    else:
        report.append("No files were added.")
        report.append("")

    # Modified files
    if changed_files["modified"]:
        report.append("MODIFIED FILES:")
        for f in changed_files["modified"]:
            report.append(f"  {f}")
        report.append("")
    else:
        report.append("No files were modified.")
        report.append("")

    # Deleted files
    if changed_files["deleted"]:
        report.append("DELETED FILES:")
        for f in changed_files["deleted"]:
            report.append(f"  {f}")
        report.append("")
    else:
        report.append("No files were deleted.")
        report.append("")

    # Recent commits
    if commits:
        report.append("RECENT COMMITS:")
        for commit in commits[:20]:  # Limit to 20 most recent
            report.append(f"  {commit['date']} {commit['author']}: {commit['subject']}")
        if len(commits) > 20:
            report.append(f"  ... and {len(commits) - 20} more commits")
        report.append("")

    return "\n".join(report)


def send_report_email(report: str, repo_name: str) -> bool:
    """Send the report via email."""
    if not CURATOR_EMAIL:
        logger.warning("CURATOR_EMAIL not set, skipping email")
        return True

    msg = EmailMessage()
    msg["From"] = ADMIN_EMAIL
    msg["To"] = CURATOR_EMAIL
    msg["Subject"] = f"Weekly Git Activity Report: {repo_name}"
    msg.set_content(report)

    try:
        with smtplib.SMTP(SMTP_HOST) as server:
            server.send_message(msg)
        logger.info(f"Report sent to {CURATOR_EMAIL}")
        return True

    except smtplib.SMTPException as e:
        logger.error(f"Failed to send email: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Report weekly Git repository activity"
    )
    parser.add_argument(
        "repo",
        type=Path,
        help="Path to Git repository",
    )
    parser.add_argument(
        "--branch",
        help="Branch to report on (default: all branches)",
    )
    parser.add_argument(
        "--since",
        help="Start date (YYYY-MM-DD, default: 1 week ago)",
    )
    parser.add_argument(
        "--email",
        action="store_true",
        help="Send report via email",
    )

    args = parser.parse_args()

    # Validate repo path
    if not args.repo.exists():
        logger.error(f"Repository path does not exist: {args.repo}")
        return 1

    git_dir = args.repo / ".git"
    if not git_dir.exists():
        logger.error(f"Not a Git repository: {args.repo}")
        return 1

    # Calculate since date
    if args.since:
        since_date = args.since
    else:
        since_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    logger.info(f"Generating report for {args.repo}")
    logger.info(f"Since: {since_date}")

    # Get activity data
    commits = get_commits_since(args.repo, since_date, args.branch)
    changed_files = get_changed_files(args.repo, since_date, args.branch)

    # Generate report
    report = generate_report(args.repo, since_date, args.branch, commits, changed_files)

    # Output report
    print(report)

    # Write to log file
    log_file = LOG_DIR / "weekly_git_activity.log"
    with open(log_file, "a") as f:
        f.write("\n" + "#" * 60 + "\n\n")
        f.write(report)
    logger.info(f"Log written to {log_file}")

    # Send email if requested
    if args.email:
        send_report_email(report, args.repo.name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
