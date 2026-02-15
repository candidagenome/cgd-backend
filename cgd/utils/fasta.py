"""
FASTA file reading and writing utilities.

This module provides functions for reading and writing FASTA format
sequence files, with support for gzipped files.
"""

import gzip
from pathlib import Path
from typing import Iterator, TextIO


def read_fasta(
    filepath: Path,
    gzip_aware: bool = True,
) -> dict[str, str]:
    """
    Read sequences from a FASTA file.

    Args:
        filepath: Path to FASTA file
        gzip_aware: Automatically detect and handle gzipped files

    Returns:
        Dictionary mapping sequence IDs to sequences

    Example:
        >>> sequences = read_fasta(Path("sequences.fasta"))
        >>> print(sequences["YAL001C"])
        ATGCGATCG...
    """
    sequences = {}
    current_id = None
    current_seq_parts: list[str] = []

    with open_fasta(filepath, gzip_aware) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue

            if line.startswith(">"):
                # Save previous sequence
                if current_id is not None:
                    sequences[current_id] = "".join(current_seq_parts)

                # Start new sequence
                current_id = line[1:].split()[0]
                current_seq_parts = []
            else:
                current_seq_parts.append(line)

        # Save last sequence
        if current_id is not None:
            sequences[current_id] = "".join(current_seq_parts)

    return sequences


def read_fasta_with_descriptions(
    filepath: Path,
    gzip_aware: bool = True,
) -> dict[str, tuple[str, str]]:
    """
    Read sequences from a FASTA file, preserving full descriptions.

    Args:
        filepath: Path to FASTA file
        gzip_aware: Automatically detect and handle gzipped files

    Returns:
        Dictionary mapping sequence IDs to (description, sequence) tuples

    Example:
        >>> sequences = read_fasta_with_descriptions(Path("sequences.fasta"))
        >>> desc, seq = sequences["YAL001C"]
    """
    sequences = {}
    current_id = None
    current_desc = ""
    current_seq_parts: list[str] = []

    with open_fasta(filepath, gzip_aware) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue

            if line.startswith(">"):
                # Save previous sequence
                if current_id is not None:
                    sequences[current_id] = (current_desc, "".join(current_seq_parts))

                # Start new sequence
                header = line[1:]
                parts = header.split(None, 1)
                current_id = parts[0]
                current_desc = parts[1] if len(parts) > 1 else ""
                current_seq_parts = []
            else:
                current_seq_parts.append(line)

        # Save last sequence
        if current_id is not None:
            sequences[current_id] = (current_desc, "".join(current_seq_parts))

    return sequences


def parse_fasta_stream(fh: TextIO) -> Iterator[tuple[str, str, str]]:
    """
    Parse FASTA records from a file stream.

    This is a generator function that yields records one at a time,
    suitable for processing large files.

    Args:
        fh: File handle to read from

    Yields:
        Tuples of (sequence_id, description, sequence)
    """
    current_id = None
    current_desc = ""
    current_seq_parts: list[str] = []

    for line in fh:
        line = line.strip()
        if not line:
            continue

        if line.startswith(">"):
            # Yield previous sequence
            if current_id is not None:
                yield (current_id, current_desc, "".join(current_seq_parts))

            # Start new sequence
            header = line[1:]
            parts = header.split(None, 1)
            current_id = parts[0]
            current_desc = parts[1] if len(parts) > 1 else ""
            current_seq_parts = []
        else:
            current_seq_parts.append(line)

    # Yield last sequence
    if current_id is not None:
        yield (current_id, current_desc, "".join(current_seq_parts))


def write_fasta(
    filepath: Path,
    sequences: dict[str, str],
    descriptions: dict[str, str] = None,
    line_length: int = 60,
) -> int:
    """
    Write sequences to a FASTA file.

    Args:
        filepath: Path to output file
        sequences: Dictionary mapping sequence IDs to sequences
        descriptions: Optional dictionary mapping IDs to descriptions
        line_length: Number of characters per sequence line (default: 60)

    Returns:
        Number of sequences written

    Example:
        >>> write_fasta(Path("output.fasta"), {"seq1": "ATGC", "seq2": "GCTA"})
        2
    """
    descriptions = descriptions or {}

    with open(filepath, "w") as fh:
        for seq_id, sequence in sequences.items():
            desc = descriptions.get(seq_id, "")
            fh.write(format_fasta_entry(seq_id, desc, sequence, line_length))
            fh.write("\n")

    return len(sequences)


def format_fasta_entry(
    seq_id: str,
    description: str,
    sequence: str,
    line_length: int = 60,
) -> str:
    """
    Format a single FASTA entry.

    Args:
        seq_id: Sequence identifier
        description: Sequence description
        sequence: Sequence string
        line_length: Number of characters per line

    Returns:
        Formatted FASTA entry string
    """
    lines = []

    # Header line
    if description:
        lines.append(f">{seq_id} {description}")
    else:
        lines.append(f">{seq_id}")

    # Sequence lines
    for i in range(0, len(sequence), line_length):
        lines.append(sequence[i:i + line_length])

    return "\n".join(lines)


def open_fasta(filepath: Path, gzip_aware: bool = True) -> TextIO:
    """
    Open a FASTA file, automatically handling gzipped files.

    Args:
        filepath: Path to FASTA file
        gzip_aware: Automatically detect and handle gzipped files

    Returns:
        File handle for reading
    """
    filepath_str = str(filepath)

    if gzip_aware and (filepath_str.endswith(".gz") or filepath_str.endswith(".gzip")):
        return gzip.open(filepath, "rt", encoding="utf-8")
    else:
        return open(filepath, "r", encoding="utf-8")


def count_sequences(filepath: Path) -> int:
    """
    Count the number of sequences in a FASTA file.

    This is more memory-efficient than reading the entire file.

    Args:
        filepath: Path to FASTA file

    Returns:
        Number of sequences
    """
    count = 0
    with open_fasta(filepath) as fh:
        for line in fh:
            if line.startswith(">"):
                count += 1
    return count


def get_sequence_ids(filepath: Path) -> list[str]:
    """
    Get list of sequence IDs from a FASTA file.

    Args:
        filepath: Path to FASTA file

    Returns:
        List of sequence IDs
    """
    ids = []
    with open_fasta(filepath) as fh:
        for line in fh:
            if line.startswith(">"):
                seq_id = line[1:].strip().split()[0]
                ids.append(seq_id)
    return ids
