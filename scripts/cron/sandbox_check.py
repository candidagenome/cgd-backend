#!/usr/bin/env python3
"""
Check Git repository sandbox for out-of-sync files.

This script compares the state of a local Git repository against the remote
and reports files that are not in sync. This helps ensure production sandboxes
stay up-to-date with the repository.

Based on sandboxCheck.pl by Jon Binkley (Sep 2009), updated for Git.

Usage:
    python sandbox_check.py
    python sandbox_check.py --repo-dir /var/www/cgd

Environment Variables:
    PROJECT_ROOT: Root directory of the project (Git repository)
    CURATOR_EMAIL: Email for notifications
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Load environment variables
load_dotenv()

# Configuration from environment
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/var/www/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "curator@candidagenome.org")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@candidagenome.org")
SENDMAIL = os.getenv("SENDMAIL", "/usr/sbin/sendmail")

# Directories to exclude from checks
EXCLUDED_PATTERNS = [
    "logs/",
    "icons/",
    "error/",
    "tmp/",
    ".git/",
    "__pycache__/",
    "*.pyc",
    ".env",
    "*.log",
    ".DS_Store",
    "node_modules/",
    "venv/",
    ".venv/",
]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@dataclass
class FileStatus:
    """Status information for a file."""
    path: str
    status: str
    local_changes: bool = False
    needs_pull: bool = False
    untracked: bool = False
    staged: bool = False


@dataclass
class SandboxStatus:
    """Overall status of the sandbox."""
    branch: str = ""
    remote_branch: str = ""
    is_clean: bool = True
    ahead: int = 0
    behind: int = 0
    files: list[FileStatus] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def run_git_command(repo_dir: Path, *args) -> tuple[str, str, int]:
    """Run a git command and return stdout, stderr, and return code."""
    cmd = ["git", "-C", str(repo_dir)] + list(args)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", "Command timed out", 1
    except Exception as e:
        return "", str(e), 1


def fetch_remote(repo_dir: Path) -> tuple[bool, str]:
    """Fetch latest from remote."""
    stdout, stderr, code = run_git_command(repo_dir, "fetch", "--all", "--prune")
    if code != 0:
        return False, stderr
    return True, ""


def get_current_branch(repo_dir: Path) -> str:
    """Get the current branch name."""
    stdout, stderr, code = run_git_command(repo_dir, "rev-parse", "--abbrev-ref", "HEAD")
    if code != 0:
        return "unknown"
    return stdout.strip()


def get_tracking_branch(repo_dir: Path) -> str | None:
    """Get the remote tracking branch for the current branch."""
    stdout, stderr, code = run_git_command(
        repo_dir, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"
    )
    if code != 0:
        return None
    return stdout.strip()


def get_ahead_behind(repo_dir: Path) -> tuple[int, int]:
    """Get number of commits ahead and behind remote."""
    tracking = get_tracking_branch(repo_dir)
    if not tracking:
        return 0, 0

    stdout, stderr, code = run_git_command(
        repo_dir, "rev-list", "--left-right", "--count", f"HEAD...{tracking}"
    )
    if code != 0:
        return 0, 0

    parts = stdout.strip().split()
    if len(parts) == 2:
        return int(parts[0]), int(parts[1])
    return 0, 0


def get_status(repo_dir: Path) -> list[FileStatus]:
    """Get status of all files in the repository."""
    files = []

    # Get porcelain status for easy parsing
    stdout, stderr, code = run_git_command(repo_dir, "status", "--porcelain=v1")
    if code != 0:
        logger.error(f"Error getting status: {stderr}")
        return files

    for line in stdout.splitlines():
        if not line:
            continue

        # Porcelain format: XY filename
        # X = status in index, Y = status in worktree
        index_status = line[0] if len(line) > 0 else " "
        worktree_status = line[1] if len(line) > 1 else " "
        filepath = line[3:] if len(line) > 3 else ""

        # Skip excluded patterns
        skip = False
        for pattern in EXCLUDED_PATTERNS:
            if pattern.endswith("/"):
                if filepath.startswith(pattern) or f"/{pattern}" in filepath:
                    skip = True
                    break
            elif pattern.startswith("*"):
                if filepath.endswith(pattern[1:]):
                    skip = True
                    break
            elif pattern in filepath:
                skip = True
                break

        if skip:
            continue

        status = FileStatus(path=filepath, status="")

        # Determine status
        if index_status == "?" and worktree_status == "?":
            status.status = "Untracked"
            status.untracked = True
        elif index_status == "A":
            status.status = "Added (staged)"
            status.staged = True
        elif index_status == "M" or worktree_status == "M":
            status.status = "Modified"
            status.local_changes = True
            if index_status == "M":
                status.staged = True
        elif index_status == "D" or worktree_status == "D":
            status.status = "Deleted"
            status.local_changes = True
        elif index_status == "R":
            status.status = "Renamed"
            status.local_changes = True
        elif index_status == "C":
            status.status = "Copied"
            status.local_changes = True
        elif index_status == "U":
            status.status = "Unmerged (conflict)"
            status.local_changes = True
        else:
            status.status = f"Unknown ({index_status}{worktree_status})"

        files.append(status)

    return files


def check_sandbox(repo_dir: Path) -> SandboxStatus:
    """Check the status of a Git sandbox."""
    status = SandboxStatus()

    # Verify it's a git repository
    if not (repo_dir / ".git").exists():
        status.errors.append(f"{repo_dir} is not a Git repository")
        return status

    # Fetch latest from remote
    logger.info("Fetching from remote...")
    success, error = fetch_remote(repo_dir)
    if not success:
        status.errors.append(f"Error fetching: {error}")

    # Get branch info
    status.branch = get_current_branch(repo_dir)
    status.remote_branch = get_tracking_branch(repo_dir) or "none"

    # Get ahead/behind counts
    status.ahead, status.behind = get_ahead_behind(repo_dir)

    # Get file status
    status.files = get_status(repo_dir)

    # Determine if clean
    status.is_clean = (
        len(status.files) == 0 and
        status.ahead == 0 and
        status.behind == 0 and
        len(status.errors) == 0
    )

    return status


def format_report(status: SandboxStatus, repo_dir: Path) -> str:
    """Format the status as a text report."""
    lines = [
        f"{PROJECT_ACRONYM} Sandbox Status Report",
        "=" * 50,
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Repository: {repo_dir}",
        "",
        f"Branch: {status.branch}",
        f"Tracking: {status.remote_branch}",
        "",
    ]

    if status.errors:
        lines.append("ERRORS:")
        for error in status.errors:
            lines.append(f"  - {error}")
        lines.append("")

    if status.is_clean:
        lines.append("All files are up to date with the remote repository.")
    else:
        if status.ahead > 0:
            lines.append(f"Local is AHEAD of remote by {status.ahead} commit(s)")
        if status.behind > 0:
            lines.append(f"Local is BEHIND remote by {status.behind} commit(s)")

        if status.files:
            lines.append("")
            lines.append(f"Files with changes: {len(status.files)}")
            lines.append("")

            # Group by status
            by_status: dict[str, list[str]] = {}
            for f in status.files:
                if f.status not in by_status:
                    by_status[f.status] = []
                by_status[f.status].append(f.path)

            for file_status, paths in sorted(by_status.items()):
                lines.append(f"{file_status} ({len(paths)}):")
                for path in paths:
                    lines.append(f"  {path}")
                lines.append("")

    return "\n".join(lines)


def write_log(report: str, log_dir: Path):
    """Write report to log file."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "sandboxCheck.log"

    with open(log_file, "w") as f:
        f.write(report)

    logger.info(f"Log written to {log_file}")


def send_email(report: str, test_mode: bool = False):
    """Send report via email."""
    recipient = CURATOR_EMAIL if not test_mode else ADMIN_EMAIL

    try:
        proc = subprocess.Popen(
            [SENDMAIL, "-oi", "-t"],
            stdin=subprocess.PIPE,
            text=True,
        )

        email_content = f"""From: <{ADMIN_EMAIL}>
To: <{recipient}>
Reply-To: <{recipient}>
Subject: {PROJECT_ACRONYM} Sandbox Check Report

{report}
"""
        proc.communicate(email_content)
        logger.info(f"Email sent to {recipient}")

    except Exception as e:
        logger.error(f"Error sending email: {e}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check Git repository sandbox for out-of-sync files"
    )
    parser.add_argument(
        "--repo-dir",
        type=Path,
        default=PROJECT_ROOT,
        help=f"Repository directory (default: {PROJECT_ROOT})",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode - send email to admin instead of curators",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Don't send email, just print report",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Don't fetch from remote (use cached state)",
    )

    args = parser.parse_args()

    repo_dir = args.repo_dir

    if not repo_dir.exists():
        logger.error(f"Repository directory does not exist: {repo_dir}")
        return 1

    logger.info(f"Checking sandbox: {repo_dir}")

    try:
        # Check sandbox status
        status = check_sandbox(repo_dir)

        # Format report
        report = format_report(status, repo_dir)

        # Write log
        write_log(report, LOG_DIR)

        # Print report
        print(report)

        # Send email if not suppressed
        if not args.no_email:
            send_email(report, args.test)

        return 0 if status.is_clean else 1

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
