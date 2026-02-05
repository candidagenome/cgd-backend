"""
Submission file utilities.

Writes submission data to flat files for curator review,
matching the original Perl CGI system behavior.
"""
from __future__ import annotations

import os
import json
import logging
from datetime import datetime
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)


def _get_submission_dir() -> str:
    """Get submission directory from environment or use default."""
    # Check environment variable first
    env_dir = os.environ.get('CGD_SUBMISSION_DIR')
    if env_dir:
        logger.info(f"CGD_SUBMISSION_DIR env var set to: {env_dir}")
        return env_dir

    # Default: use /tmp which should always be writable
    default_dir = '/tmp/cgd_submissions/colleague'
    logger.info(f"Using default submission directory: {default_dir}")
    return default_dir


def _ensure_submission_dir() -> Path:
    """Ensure submission directory exists and is writable."""
    # Try configured directory first
    submission_dir = _get_submission_dir()
    path = Path(submission_dir)
    logger.info(f"Trying submission directory: {path}")

    try:
        path.mkdir(parents=True, exist_ok=True)
        # Test write permission
        test_file = path / '.write_test'
        test_file.touch()
        test_file.unlink()
        logger.info(f"Submission directory ready: {path}")
        return path
    except PermissionError as e:
        logger.warning(f"Permission denied for {path}: {e}")
    except Exception as e:
        logger.warning(f"Failed to use {path}: {e}")

    # Fallback to /tmp
    fallback_path = Path('/tmp/cgd_submissions/colleague')
    logger.info(f"Falling back to: {fallback_path}")
    try:
        fallback_path.mkdir(parents=True, exist_ok=True)
        # Test write permission
        test_file = fallback_path / '.write_test'
        test_file.touch()
        test_file.unlink()
        logger.info(f"Fallback directory ready: {fallback_path}")
        return fallback_path
    except Exception as e:
        logger.error(f"Failed to create fallback directory {fallback_path}: {e}")
        raise


def _generate_filename(prefix: str, pid: Optional[int] = None) -> str:
    """Generate unique filename for submission."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if pid is None:
        pid = os.getpid()
    return f"{prefix}_{pid}_{timestamp}.json"


def write_colleague_submission(
    colleague_no: Optional[int],
    data: dict,
    remote_addr: Optional[str] = None,
) -> str:
    """
    Write colleague submission to file.

    Args:
        colleague_no: Colleague ID for updates, None for new registration
        data: Colleague data
        remote_addr: Remote IP address of submitter

    Returns:
        Path to created file
    """
    submission_dir = _ensure_submission_dir()

    is_update = colleague_no is not None
    prefix = "colleague_update_entry" if is_update else "colleague_new_entry"
    filename = _generate_filename(prefix)
    filepath = submission_dir / filename

    submission_record = {
        "submission_type": "colleague_update" if is_update else "colleague_new",
        "colleague_no": colleague_no,
        "submitted_at": datetime.now().isoformat(),
        "remote_addr": remote_addr,
        "data": data,
    }

    with open(filepath, 'w') as f:
        json.dump(submission_record, f, indent=2, default=str)

    logger.info(f"Colleague submission written to: {filepath}")
    return str(filepath)


def write_gene_registry_submission(
    data: dict,
    remote_addr: Optional[str] = None,
) -> str:
    """
    Write gene registry submission to file.

    Args:
        data: Gene registry data including colleague info
        remote_addr: Remote IP address of submitter

    Returns:
        Path to created file
    """
    submission_dir = _ensure_submission_dir()

    filename = _generate_filename("gene_registry")
    filepath = submission_dir / filename

    submission_record = {
        "submission_type": "gene_registry",
        "submitted_at": datetime.now().isoformat(),
        "remote_addr": remote_addr,
        "gene_name": data.get("gene_name"),
        "orf_name": data.get("orf_name"),
        "organism": data.get("organism"),
        "colleague_no": data.get("colleague_no"),
        "data": data,
    }

    with open(filepath, 'w') as f:
        json.dump(submission_record, f, indent=2, default=str)

    logger.info(f"Gene registry submission written to: {filepath}")
    return str(filepath)


def format_colleague_submission_text(
    colleague_no: Optional[int],
    data: dict,
) -> str:
    """
    Format colleague submission as human-readable text.

    This matches the original Perl format for curator review.
    """
    lines = []
    lines.append("=" * 60)
    lines.append("COLLEAGUE SUBMISSION")
    lines.append("=" * 60)
    lines.append(f"Submitted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Type: {'Update' if colleague_no else 'New Registration'}")
    if colleague_no:
        lines.append(f"Colleague ID: {colleague_no}")
    lines.append("")

    lines.append("--- Personal Information ---")
    lines.append(f"Name: {data.get('first_name', '')} {data.get('last_name', '')}")
    if data.get('other_last_name'):
        lines.append(f"Other Last Name: {data.get('other_last_name')}")
    if data.get('suffix'):
        lines.append(f"Suffix: {data.get('suffix')}")
    lines.append(f"Email: {data.get('email', '')}")
    if data.get('profession'):
        lines.append(f"Profession: {data.get('profession')}")
    if data.get('job_title'):
        lines.append(f"Position: {data.get('job_title')}")
    lines.append("")

    lines.append("--- Organization & Address ---")
    lines.append(f"Organization: {data.get('institution', '')}")
    if data.get('address1'):
        lines.append(f"Address: {data.get('address1')}")
    if data.get('address2'):
        lines.append(f"         {data.get('address2')}")
    if data.get('address3'):
        lines.append(f"         {data.get('address3')}")
    city_line = []
    if data.get('city'):
        city_line.append(data.get('city'))
    if data.get('state'):
        city_line.append(data.get('state'))
    elif data.get('region'):
        city_line.append(data.get('region'))
    if data.get('postal_code'):
        city_line.append(data.get('postal_code'))
    if city_line:
        lines.append(f"         {', '.join(city_line)}")
    if data.get('country'):
        lines.append(f"Country: {data.get('country')}")
    lines.append("")

    lines.append("--- Contact ---")
    if data.get('work_phone'):
        lines.append(f"Work Phone: {data.get('work_phone')}")
    if data.get('other_phone'):
        lines.append(f"Other Phone: {data.get('other_phone')}")
    if data.get('fax'):
        lines.append(f"Fax: {data.get('fax')}")
    urls = data.get('urls', [])
    for url in urls:
        if isinstance(url, dict) and url.get('url'):
            lines.append(f"URL: {url.get('url')} ({url.get('url_type', '')})")
        elif isinstance(url, str):
            lines.append(f"URL: {url}")
    lines.append("")

    if data.get('research_interests') or data.get('keywords'):
        lines.append("--- Research ---")
        if data.get('research_interests'):
            lines.append(f"Interests: {data.get('research_interests')}")
        if data.get('keywords'):
            lines.append(f"Keywords: {data.get('keywords')}")
        lines.append("")

    lines.append("=" * 60)
    return "\n".join(lines)


def format_gene_registry_text(data: dict) -> str:
    """
    Format gene registry submission as human-readable text.

    This matches the original Perl format for curator review.
    """
    lines = []
    lines.append("=" * 60)
    lines.append("GENE REGISTRY SUBMISSION")
    lines.append("=" * 60)
    lines.append(f"Submitted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    lines.append("--- Gene Information ---")
    lines.append(f"Gene Name: {data.get('gene_name', '')}")
    if data.get('orf_name'):
        lines.append(f"ORF Name: {data.get('orf_name')}")
    lines.append(f"Organism: {data.get('organism', '')}")
    if data.get('description'):
        lines.append(f"Description: {data.get('description')}")
    if data.get('reference'):
        lines.append(f"Reference: {data.get('reference')}")
    if data.get('comments'):
        lines.append(f"Comments: {data.get('comments')}")
    lines.append("")

    lines.append("--- Submitter ---")
    if data.get('colleague_no'):
        lines.append(f"Colleague ID: {data.get('colleague_no')}")
    else:
        lines.append(f"Name: {data.get('first_name', '')} {data.get('last_name', '')}")
        lines.append(f"Email: {data.get('email', '')}")
        lines.append(f"Organization: {data.get('institution', '')}")
    lines.append("")

    lines.append("=" * 60)
    return "\n".join(lines)
