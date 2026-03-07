#!/usr/bin/env python3
"""
Create and update cache files for frequently accessed pages.

This script fetches HTML content from specified URLs and caches them as static
files. It only updates the cache when the content has changed (based on checksum).

This is useful for pages that are expensive to generate but don't change often.

Usage:
    python make_cache_file.py <cache_filename>
    python make_cache_file.py genomeSnapshot.html organism=C_albicans

Environment Variables:
    CACHE_DIR: Directory for cache files
    CGI_ROOT_URL: Base URL for CGI scripts
    TMP_DIR: Temporary directory (default: /tmp)
"""

import argparse
import hashlib
import logging
import os
import shutil
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
CACHE_DIR = Path(os.getenv("CACHE_DIR", "/var/www/html/cache"))
CGI_ROOT_URL = os.getenv("CGI_ROOT_URL", "http://localhost/cgi-bin/")
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Mapping of cache filenames to URLs (relative to CGI_ROOT_URL)
FILES_TO_URL = {
    "NewPapersThisWeek.html": "reference/litGuide.pl?date=1",
    "genome-wide-analysis.html": "reference/litGuide.pl?topic=Large+Scale+Analysis",
    "Labs.html": "colleague/yeastLabs.pl",
    "communityTable.html": "communityTable",
    "genomeSnapshot.html": "genomeSnapshot.pl",
    "PhenotypeTree.html": "phenotype/phenotype.pl?rm=obs_tree",
}

# Files that accept additional parameters
FILES_ALLOWED_PARAMS = {
    "genomeSnapshot.html": {"organism"},
}

# Example params for help text
FILES_PARAMS_EXAMPLE = {
    "genomeSnapshot.html": "organism=<strain_abbrev>",
}


def get_file_checksum(filepath: Path) -> str | None:
    """Calculate MD5 checksum of a file."""
    if not filepath.exists():
        return None

    hasher = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def fetch_url(url: str, timeout: int = 60) -> str | None:
    """
    Fetch content from a URL.

    Args:
        url: URL to fetch
        timeout: Request timeout in seconds

    Returns:
        Page content or None on error
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text

    except requests.RequestException as e:
        logger.error(f"Error fetching {url}: {e}")
        return None


def make_cache_file(filename: str, params: str | None = None) -> bool:
    """
    Create or update a cache file.

    Args:
        filename: Name of the cache file
        params: Optional URL parameters (e.g., "key1=val1&key2=val2")

    Returns:
        True on success, False on failure
    """
    if filename not in FILES_TO_URL:
        logger.error(f"Unknown cache file: {filename}")
        logger.info("Available cache files:")
        for name, example in FILES_TO_URL.items():
            param_example = FILES_PARAMS_EXAMPLE.get(name, "")
            logger.info(f"  {name} {param_example}")
        return False

    # Build URL
    base_url = CGI_ROOT_URL.rstrip("/") + "/" + FILES_TO_URL[filename]
    url = base_url
    out_filename = filename

    # Handle optional parameters
    if params:
        allowed_params = FILES_ALLOWED_PARAMS.get(filename, set())
        param_parts = []

        for keyval in params.split("&"):
            if "=" in keyval:
                key, val = keyval.split("=", 1)
                if key in allowed_params:
                    param_parts.append(f"{key}={val}")

        if param_parts:
            separator = "&" if "?" in url else "?"
            url += separator + "&".join(param_parts)

            # Modify output filename to include params
            values = [p.split("=")[1] for p in param_parts]
            out_filename = "_".join(values) + "_" + filename

    logger.info(f"Fetching: {url}")

    # Fetch content
    content = fetch_url(url)
    if content is None:
        return False

    # Write to temp file
    tmp_file = TMP_DIR / f"makeCacheFile_{os.getpid()}.tmp"
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(content)

        # Calculate checksums
        cache_file = CACHE_DIR / out_filename
        new_checksum = get_file_checksum(tmp_file)
        cache_checksum = get_file_checksum(cache_file)

        # Only update if different
        if new_checksum != cache_checksum:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            shutil.move(str(tmp_file), str(cache_file))
            logger.info(f"Updated cache for {out_filename}")
            return True
        else:
            logger.info(f"Cache unchanged for {out_filename}")
            return True

    finally:
        # Clean up temp file
        if tmp_file.exists():
            tmp_file.unlink()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create and update cache files for frequently accessed pages"
    )
    parser.add_argument(
        "filename",
        nargs="?",
        help="Cache filename to create/update",
    )
    parser.add_argument(
        "params",
        nargs="?",
        help="Optional URL parameters (e.g., key1=val1&key2=val2)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available cache files",
    )

    args = parser.parse_args()

    if args.list or not args.filename:
        print("Available cache files:")
        for name in sorted(FILES_TO_URL.keys()):
            param_example = FILES_PARAMS_EXAMPLE.get(name, "")
            print(f"  {name} {param_example}")
        return 0 if args.list else 1

    success = make_cache_file(args.filename, args.params)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
