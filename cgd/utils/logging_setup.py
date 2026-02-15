"""
Logging configuration utilities.

This module provides standardized logging setup for CGD scripts.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logging(
    name: str = None,
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
    log_dir: Optional[Path] = None,
    console: bool = True,
    format_string: str = "%(asctime)s - %(levelname)s - %(message)s",
) -> logging.Logger:
    """
    Set up logging with optional file and console handlers.

    Args:
        name: Logger name (defaults to root logger if None)
        level: Logging level (default: INFO)
        log_file: Path to log file (optional)
        log_dir: Directory for log file (used with log_file name)
        console: Whether to add console handler (default: True)
        format_string: Log message format

    Returns:
        Configured logger instance

    Example:
        >>> logger = setup_logging(__name__, log_file=Path("app.log"))
        >>> logger.info("Application started")
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()

    formatter = logging.Formatter(format_string)

    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # File handler
    if log_file or log_dir:
        if log_dir and log_file:
            log_path = log_dir / log_file
        elif log_dir:
            log_path = log_dir / "app.log"
        else:
            log_path = log_file

        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = None) -> logging.Logger:
    """
    Get a logger instance.

    This is a convenience wrapper around logging.getLogger() that
    ensures consistent logger naming.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def add_file_handler(
    logger: logging.Logger,
    log_file: Path,
    level: int = logging.INFO,
    format_string: str = "%(asctime)s - %(levelname)s - %(message)s",
) -> None:
    """
    Add a file handler to an existing logger.

    Args:
        logger: Logger instance to modify
        log_file: Path to log file
        level: Logging level for file handler
        format_string: Log message format
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(file_handler)


def configure_basic_logging(
    level: int = logging.INFO,
    format_string: str = "%(asctime)s - %(levelname)s - %(message)s",
) -> None:
    """
    Configure basic logging to stdout.

    This replaces the common pattern of calling logging.basicConfig()
    at the module level.

    Args:
        level: Logging level
        format_string: Log message format
    """
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=[logging.StreamHandler(sys.stdout)],
    )
