#!/usr/bin/env python3
"""
Check and restart autosuggest service.

This script monitors the autosuggest Java process (start.jar) and restarts
it if necessary. It logs all actions to a log file.

Original: check_autosuggest.sh (bash)
Converted to Python: 2024

Usage:
    python check_autosuggest.py
    python check_autosuggest.py --log-file /path/to/log
    python check_autosuggest.py --autosuggest-dir /data/autosuggest
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_PROC_PATTERN = "java -jar start.jar"
DEFAULT_LOGFILE = "/share/www-data_cgd/prod/logs/autosuggest_monitor.log"
DEFAULT_AUTOSUGGEST_DIR = "/data/autosuggest"


def setup_file_logging(log_file: Path) -> None:
    """Configure file logging with timestamps."""
    # Ensure log file exists
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.touch(exist_ok=True)

    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s : %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def find_processes(pattern: str) -> list[int]:
    """
    Find process IDs matching the given pattern.

    Args:
        pattern: Command pattern to search for

    Returns:
        List of matching PIDs
    """
    try:
        result = subprocess.run(
            ['pgrep', '-f', pattern],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return [int(pid) for pid in result.stdout.strip().split('\n')]
    except Exception as e:
        logger.error(f"Error finding processes: {e}")
    return []


def kill_processes(pattern: str, force: bool = False) -> bool:
    """
    Kill processes matching the given pattern.

    Args:
        pattern: Command pattern to match
        force: Use SIGKILL instead of SIGTERM

    Returns:
        True if successful
    """
    try:
        signal_flag = '-9' if force else ''
        cmd = ['pkill', '-f', pattern]
        if force:
            cmd.insert(1, '-9')
        subprocess.run(cmd, capture_output=True)
        return True
    except Exception as e:
        logger.error(f"Error killing processes: {e}")
        return False


def start_autosuggest(autosuggest_dir: Path) -> bool:
    """
    Start the autosuggest process.

    Args:
        autosuggest_dir: Directory containing start.jar

    Returns:
        True if started successfully
    """
    try:
        # Change to autosuggest directory
        os.chdir(autosuggest_dir)

        # Remove old nohup.out
        nohup_out = autosuggest_dir / 'nohup.out'
        if nohup_out.exists():
            nohup_out.unlink()

        # Start process with nohup
        with open('/dev/null', 'w') as devnull:
            subprocess.Popen(
                ['nohup', 'java', '-jar', 'start.jar'],
                stdout=devnull,
                stderr=devnull,
                start_new_session=True,
            )

        return True
    except Exception as e:
        logger.error(f"Error starting autosuggest: {e}")
        return False


def check_and_restart_autosuggest(
    proc_pattern: str = DEFAULT_PROC_PATTERN,
    autosuggest_dir: Path = Path(DEFAULT_AUTOSUGGEST_DIR),
) -> bool:
    """
    Check if autosuggest is running and restart if needed.

    Args:
        proc_pattern: Process pattern to match
        autosuggest_dir: Directory containing start.jar

    Returns:
        True if successful
    """
    was_running = False

    # Check if process is already running
    pids = find_processes(proc_pattern)

    if pids:
        was_running = True
        logger.info(f"start.jar is running with PID(s): {pids} – attempting to stop for restart")

        # Try graceful stop
        kill_processes(proc_pattern, force=False)
        time.sleep(5)

        # Check if still running
        if find_processes(proc_pattern):
            logger.info("WARNING: start.jar still running after pkill; sending SIGKILL")
            kill_processes(proc_pattern, force=True)
            time.sleep(2)

            # Final check
            if find_processes(proc_pattern):
                logger.error("ERROR: Unable to stop existing start.jar process(es); aborting restart")
                return False

        logger.info("Existing start.jar process(es) stopped successfully")

    # Verify autosuggest directory exists
    if not autosuggest_dir.exists():
        logger.error(f"ERROR: could not cd to {autosuggest_dir}")
        return False

    # Start the process
    if start_autosuggest(autosuggest_dir):
        if was_running:
            logger.info("start.jar was running and has been restarted")
        else:
            logger.info("start.jar was not running and has been started")
        return True
    else:
        logger.error("ERROR: failed to start start.jar")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check and restart autosuggest service"
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path(DEFAULT_LOGFILE),
        help=f"Log file path (default: {DEFAULT_LOGFILE})",
    )
    parser.add_argument(
        "--autosuggest-dir",
        type=Path,
        default=Path(DEFAULT_AUTOSUGGEST_DIR),
        help=f"Autosuggest directory (default: {DEFAULT_AUTOSUGGEST_DIR})",
    )
    parser.add_argument(
        "--proc-pattern",
        default=DEFAULT_PROC_PATTERN,
        help=f"Process pattern to match (default: {DEFAULT_PROC_PATTERN})",
    )

    args = parser.parse_args()

    # Setup logging
    setup_file_logging(args.log_file)

    # Also log to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)

    success = check_and_restart_autosuggest(
        proc_pattern=args.proc_pattern,
        autosuggest_dir=args.autosuggest_dir,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
