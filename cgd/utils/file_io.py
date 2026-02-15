"""
General file I/O utilities.

This module provides functions for common file operations
like reading/writing tab-delimited files and directory management.
"""

import csv
import gzip
from pathlib import Path
from typing import Any, Iterator, Optional, TextIO, Union


def ensure_directory(path: Path) -> Path:
    """
    Ensure a directory exists, creating it if necessary.

    Args:
        path: Directory path

    Returns:
        The path (for chaining)

    Example:
        >>> ensure_directory(Path("/var/data/output"))
        Path('/var/data/output')
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def open_file(
    filepath: Path,
    mode: str = "r",
    gzip_aware: bool = True,
    encoding: str = "utf-8",
) -> TextIO:
    """
    Open a file, automatically handling gzipped files.

    Args:
        filepath: Path to file
        mode: File mode ('r', 'w', 'a', etc.)
        gzip_aware: Automatically detect and handle gzipped files
        encoding: Text encoding

    Returns:
        File handle

    Example:
        >>> with open_file(Path("data.txt.gz")) as f:
        ...     content = f.read()
    """
    filepath_str = str(filepath)
    is_gzip = filepath_str.endswith(".gz") or filepath_str.endswith(".gzip")

    if gzip_aware and is_gzip:
        # Convert mode for gzip (add 't' for text mode)
        if "b" not in mode and "t" not in mode:
            mode = mode + "t"
        return gzip.open(filepath, mode, encoding=encoding)
    else:
        return open(filepath, mode, encoding=encoding)


def read_tab_delimited(
    filepath: Path,
    has_header: bool = True,
    skip_comments: bool = True,
    comment_char: str = "#",
) -> list[dict[str, str]] | list[list[str]]:
    """
    Read a tab-delimited file.

    Args:
        filepath: Path to file
        has_header: First row is header (returns list of dicts)
        skip_comments: Skip lines starting with comment_char
        comment_char: Character indicating comment lines

    Returns:
        List of dictionaries (if has_header) or list of lists

    Example:
        >>> data = read_tab_delimited(Path("data.tab"))
        >>> print(data[0]["gene_name"])
    """
    rows = []

    with open_file(filepath) as fh:
        # Skip comment lines at the beginning
        header = None
        for line in fh:
            line = line.rstrip("\n\r")

            if skip_comments and line.startswith(comment_char):
                continue

            if not line.strip():
                continue

            parts = line.split("\t")

            if has_header and header is None:
                header = parts
                continue

            if has_header and header:
                # Create dict with header keys
                row_dict = {}
                for i, value in enumerate(parts):
                    key = header[i] if i < len(header) else f"col_{i}"
                    row_dict[key] = value
                rows.append(row_dict)
            else:
                rows.append(parts)

    return rows


def write_tab_delimited(
    filepath: Path,
    data: list[dict[str, Any]] | list[list[Any]],
    header: Optional[list[str]] = None,
    write_header: bool = True,
) -> int:
    """
    Write data to a tab-delimited file.

    Args:
        filepath: Path to output file
        data: List of dictionaries or list of lists
        header: Column headers (auto-detected from dicts if not provided)
        write_header: Whether to write header row

    Returns:
        Number of rows written

    Example:
        >>> write_tab_delimited(Path("output.tab"), [{"a": 1, "b": 2}])
        1
    """
    if not data:
        # Write empty file
        filepath.write_text("")
        return 0

    # Detect if data is list of dicts
    is_dict_data = isinstance(data[0], dict)

    # Auto-detect header from dict keys
    if is_dict_data and header is None:
        header = list(data[0].keys())

    with open(filepath, "w", newline="") as fh:
        if is_dict_data:
            writer = csv.DictWriter(fh, fieldnames=header, delimiter="\t")
            if write_header and header:
                writer.writeheader()
            writer.writerows(data)
        else:
            writer = csv.writer(fh, delimiter="\t")
            if write_header and header:
                writer.writerow(header)
            writer.writerows(data)

    return len(data)


def read_lines(
    filepath: Path,
    strip: bool = True,
    skip_empty: bool = True,
    skip_comments: bool = True,
    comment_char: str = "#",
) -> list[str]:
    """
    Read lines from a file.

    Args:
        filepath: Path to file
        strip: Strip whitespace from lines
        skip_empty: Skip empty lines
        skip_comments: Skip comment lines
        comment_char: Character indicating comments

    Returns:
        List of lines
    """
    lines = []

    with open_file(filepath) as fh:
        for line in fh:
            if strip:
                line = line.strip()
            else:
                line = line.rstrip("\n\r")

            if skip_empty and not line:
                continue

            if skip_comments and line.startswith(comment_char):
                continue

            lines.append(line)

    return lines


def write_lines(
    filepath: Path,
    lines: list[str],
    newline: str = "\n",
) -> int:
    """
    Write lines to a file.

    Args:
        filepath: Path to output file
        lines: Lines to write
        newline: Line ending character

    Returns:
        Number of lines written
    """
    with open(filepath, "w") as fh:
        for line in lines:
            fh.write(line)
            if not line.endswith(newline):
                fh.write(newline)

    return len(lines)


def iter_lines(
    filepath: Path,
    strip: bool = True,
    skip_empty: bool = True,
    skip_comments: bool = True,
    comment_char: str = "#",
) -> Iterator[str]:
    """
    Iterate over lines in a file (memory efficient).

    Args:
        filepath: Path to file
        strip: Strip whitespace from lines
        skip_empty: Skip empty lines
        skip_comments: Skip comment lines
        comment_char: Character indicating comments

    Yields:
        Lines from file
    """
    with open_file(filepath) as fh:
        for line in fh:
            if strip:
                line = line.strip()
            else:
                line = line.rstrip("\n\r")

            if skip_empty and not line:
                continue

            if skip_comments and line.startswith(comment_char):
                continue

            yield line


def file_exists(filepath: Path) -> bool:
    """Check if a file exists."""
    return filepath.exists() and filepath.is_file()


def directory_exists(path: Path) -> bool:
    """Check if a directory exists."""
    return path.exists() and path.is_dir()


def get_file_size(filepath: Path) -> int:
    """Get file size in bytes."""
    return filepath.stat().st_size if filepath.exists() else 0


def count_lines(filepath: Path) -> int:
    """
    Count lines in a file efficiently.

    Args:
        filepath: Path to file

    Returns:
        Number of lines
    """
    count = 0
    with open_file(filepath) as fh:
        for _ in fh:
            count += 1
    return count


def safe_write(
    filepath: Path,
    content: str,
    backup: bool = True,
) -> None:
    """
    Safely write content to a file with optional backup.

    Writes to a temporary file first, then moves to final location.

    Args:
        filepath: Path to output file
        content: Content to write
        backup: Create backup of existing file
    """
    temp_file = filepath.with_suffix(filepath.suffix + ".tmp")

    # Write to temp file
    with open(temp_file, "w") as fh:
        fh.write(content)

    # Backup existing file
    if backup and filepath.exists():
        backup_file = filepath.with_suffix(filepath.suffix + ".bak")
        filepath.rename(backup_file)

    # Move temp to final
    temp_file.rename(filepath)
