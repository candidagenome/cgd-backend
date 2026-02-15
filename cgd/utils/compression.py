"""
File compression and archiving utilities.

This module provides functions for compressing, decompressing,
and archiving files with date-stamped naming.
"""

import gzip
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional


def compress_file(
    input_file: Path,
    output_file: Optional[Path] = None,
    level: int = 9,
    keep_original: bool = True,
) -> Path:
    """
    Compress a file using gzip.

    Args:
        input_file: Path to file to compress
        output_file: Path for compressed file (default: input_file + ".gz")
        level: Compression level (1-9, default: 9)
        keep_original: Whether to keep the original file

    Returns:
        Path to compressed file

    Example:
        >>> compressed = compress_file(Path("data.txt"))
        >>> print(compressed)
        data.txt.gz
    """
    if output_file is None:
        output_file = input_file.with_suffix(input_file.suffix + ".gz")

    with open(input_file, "rb") as f_in:
        with gzip.open(output_file, "wb", compresslevel=level) as f_out:
            shutil.copyfileobj(f_in, f_out)

    if not keep_original:
        input_file.unlink()

    return output_file


def decompress_file(
    input_file: Path,
    output_file: Optional[Path] = None,
    keep_original: bool = True,
) -> Path:
    """
    Decompress a gzipped file.

    Args:
        input_file: Path to gzipped file
        output_file: Path for decompressed file (default: removes .gz suffix)
        keep_original: Whether to keep the original compressed file

    Returns:
        Path to decompressed file
    """
    if output_file is None:
        if str(input_file).endswith(".gz"):
            output_file = input_file.with_suffix("")
        else:
            output_file = input_file.with_suffix(input_file.suffix + ".decompressed")

    with gzip.open(input_file, "rb") as f_in:
        with open(output_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    if not keep_original:
        input_file.unlink()

    return output_file


def archive_file(
    file_path: Path,
    archive_dir: Optional[Path] = None,
    date_format: str = "%Y%m%d",
    compress: bool = True,
    keep_original: bool = True,
) -> Optional[Path]:
    """
    Archive a file with date-stamped naming.

    Args:
        file_path: Path to file to archive
        archive_dir: Directory for archives (default: file_path.parent/archive)
        date_format: strftime format for date suffix
        compress: Whether to gzip the archive
        keep_original: Whether to keep the original file

    Returns:
        Path to archived file, or None if file doesn't exist

    Example:
        >>> archived = archive_file(Path("data.tab"))
        >>> print(archived)
        archive/data.tab.20240215.gz
    """
    if not file_path.exists():
        return None

    # Default archive directory
    if archive_dir is None:
        archive_dir = file_path.parent / "archive"

    archive_dir.mkdir(parents=True, exist_ok=True)

    # Create date-stamped filename
    date_str = datetime.now().strftime(date_format)
    archive_name = f"{file_path.name}.{date_str}"
    archive_path = archive_dir / archive_name

    # Copy to archive
    shutil.copy2(file_path, archive_path)

    # Compress if requested
    if compress:
        compressed_path = compress_file(archive_path, keep_original=False)
        archive_path = compressed_path

    if not keep_original:
        file_path.unlink()

    return archive_path


def archive_monthly(
    file_path: Path,
    archive_dir: Optional[Path] = None,
    day_threshold: int = 8,
) -> Optional[Path]:
    """
    Archive a file at the first run of the month.

    This is useful for monthly archiving of data files. The archive
    is only created if the current day of month is less than the
    threshold.

    Args:
        file_path: Path to file to archive
        archive_dir: Directory for archives
        day_threshold: Only archive if day of month < this value

    Returns:
        Path to archived file, or None if not archived
    """
    if not file_path.exists():
        return None

    now = datetime.now()
    if now.day >= day_threshold:
        return None

    return archive_file(
        file_path,
        archive_dir=archive_dir,
        date_format="%Y%m",
        compress=True,
        keep_original=True,
    )


def archive_weekly(
    file_path: Path,
    archive_dir: Optional[Path] = None,
) -> Optional[Path]:
    """
    Archive a file with full date (weekly archives).

    Args:
        file_path: Path to file to archive
        archive_dir: Directory for archives

    Returns:
        Path to archived file, or None if file doesn't exist
    """
    return archive_file(
        file_path,
        archive_dir=archive_dir,
        date_format="%Y%m%d",
        compress=True,
        keep_original=True,
    )


def read_gzipped_text(file_path: Path) -> str:
    """
    Read text content from a gzipped file.

    Args:
        file_path: Path to gzipped file

    Returns:
        Text content of file
    """
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        return f.read()


def write_gzipped_text(file_path: Path, content: str, level: int = 9) -> None:
    """
    Write text content to a gzipped file.

    Args:
        file_path: Path for output file
        content: Text content to write
        level: Compression level (1-9)
    """
    with gzip.open(file_path, "wt", encoding="utf-8", compresslevel=level) as f:
        f.write(content)


def ensure_gzip_suffix(file_path: Path) -> Path:
    """
    Ensure file path has .gz suffix.

    Args:
        file_path: Path to check

    Returns:
        Path with .gz suffix
    """
    if not str(file_path).endswith(".gz"):
        return file_path.with_suffix(file_path.suffix + ".gz")
    return file_path
