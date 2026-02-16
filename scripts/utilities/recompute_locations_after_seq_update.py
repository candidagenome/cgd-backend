#!/usr/bin/env python3
"""
Recompute genomic locations after a sequence update.

This script maps locations in a file (GFF, VCF) from one genome version
to the next. It takes a simplistic approach by only accepting a single
sequence change per chromosome/contig.

For all regions downstream of that seq change, it will shift positions
based on the offset. For regions that fully contain the seq change, it
will expand/retract the region based on offset. For regions that overlap
with the sequence change, it will send a warning and leave them unchanged.

Original Perl: recomputeLocationsAfterSeqUpdate.pl (Prachi Shah, Jan 2012)
Converted to Python: 2024

Usage:
    python recompute_locations_after_seq_update.py input.gff GFF changes.txt
    python recompute_locations_after_seq_update.py input.vcf VCF changes.txt

Sequence change file format (one change per line, one per chromosome):
    Chr/contig <tab> insertion/deletion/substitution <tab> details

Change details:
    - Insertion: position:length (insertion after specified position)
    - Deletion: start:end (both positions inclusive)
    - Substitution: start:end:new_length (both positions inclusive)
"""

import argparse
import logging
import shutil
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


class FileFormat(Enum):
    """Supported file formats."""
    GFF = "GFF"
    VCF = "VCF"


class UpdateType(Enum):
    """Types of sequence changes."""
    INSERTION = "insertion"
    DELETION = "deletion"
    SUBSTITUTION = "substitution"


@dataclass
class SeqChange:
    """Sequence change specification."""
    update_type: UpdateType
    details: str


@dataclass
class LocationStats:
    """Statistics for location updates."""
    downstream: int = 0
    unaffected: int = 0
    overlap: int = 0
    encompassing: int = 0


# Column indices for each file format
FILE_CHR_COLUMN = {
    FileFormat.GFF: 0,
    FileFormat.VCF: 0,
}

FILE_POS_COLUMNS = {
    FileFormat.GFF: {'start': 3, 'stop': 4},
    FileFormat.VCF: {'start': 1, 'stop': 1},
}


def parse_seq_changes(change_file: Path) -> dict[str, SeqChange]:
    """
    Parse the sequence changes file.

    Args:
        change_file: Path to changes file

    Returns:
        Dict mapping chromosome to SeqChange
    """
    changes = {}

    with open(change_file, 'r') as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 3:
                raise ValueError(
                    f"Invalid format at line {line_num}: expected 3 tab-separated fields"
                )

            chrom, upd_type_str, details = parts[:3]

            if chrom in changes:
                raise ValueError(
                    f"Only one change per chromosome is allowed. "
                    f"Second change encountered for {chrom}"
                )

            try:
                upd_type = UpdateType(upd_type_str.lower())
            except ValueError:
                raise ValueError(
                    f"Update type '{upd_type_str}' at line {line_num} is not recognized. "
                    f"Please use 'insertion', 'deletion', or 'substitution'"
                )

            changes[chrom] = SeqChange(update_type=upd_type, details=details)

    return changes


def compute_new_location(
    upd_type: UpdateType,
    details: str,
    start: int,
    stop: int,
    line_num: int,
    line_content: str,
) -> tuple[int, int, str]:
    """
    Compute new location based on sequence change.

    Args:
        upd_type: Type of update
        details: Update details string
        start: Current start position
        stop: Current stop position
        line_num: Current line number
        line_content: Original line content for warnings

    Returns:
        Tuple of (new_start, new_stop, change_type)
    """
    if upd_type == UpdateType.INSERTION:
        pos, ins_len = map(int, details.split(':'))

        if start > pos and stop > pos:
            # Downstream - shift by insertion length
            return start + ins_len, stop + ins_len, 'downstream'

        elif start <= pos and stop <= pos:
            # Unaffected by the seq change
            return start, stop, 'unaffected'

        else:
            # Region overlaps insertion
            logger.warning(
                f"Region overlaps INSERTION for line {line_num}; "
                f"New location is NOT computed.\n {line_content}"
            )
            return start, stop, 'overlap'

    elif upd_type == UpdateType.DELETION:
        del_start, del_stop = map(int, details.split(':'))
        del_len = del_stop - del_start + 1

        if start > del_stop and stop > del_stop:
            # Downstream - shift back by deletion length
            return start - del_len, stop - del_len, 'downstream'

        elif start < del_start and stop < del_start:
            # Unaffected by the seq change
            return start, stop, 'unaffected'

        elif start < del_start and stop > del_stop:
            # Region contains the deletion
            return start, stop - del_len, 'encompassing'

        else:
            # Region overlaps deletion
            logger.warning(
                f"Region overlaps DELETION for line {line_num}; "
                f"New location is NOT computed.\n {line_content}"
            )
            return start, stop, 'overlap'

    elif upd_type == UpdateType.SUBSTITUTION:
        parts = details.split(':')
        subs_start, subs_stop, subs_len = int(parts[0]), int(parts[1]), int(parts[2])
        offset = subs_len - (subs_stop - subs_start + 1)

        if start > subs_stop and stop > subs_stop:
            # Downstream - shift by offset
            return start + offset, stop + offset, 'downstream'

        elif start < subs_start and stop < subs_start:
            # Unaffected by the seq change
            return start, stop, 'unaffected'

        elif start < subs_start and stop > subs_stop:
            # Region contains the substitution
            return start, stop + offset, 'encompassing'

        else:
            # Region overlaps substitution
            logger.warning(
                f"Region overlaps SUBSTITUTION for line {line_num}; "
                f"New location is NOT computed.\n {line_content}"
            )
            return start, stop, 'overlap'

    return start, stop, 'unaffected'


def process_file(
    input_file: Path,
    file_format: FileFormat,
    seq_changes: dict[str, SeqChange],
) -> dict[str, LocationStats]:
    """
    Process input file and update locations.

    Args:
        input_file: Path to input file
        file_format: File format (GFF or VCF)
        seq_changes: Sequence changes to apply

    Returns:
        Dict mapping chromosome to LocationStats
    """
    # Backup original file
    backup_file = Path(str(input_file) + '.old')
    shutil.copy2(input_file, backup_file)

    chr_col = FILE_CHR_COLUMN[file_format]
    start_col = FILE_POS_COLUMNS[file_format]['start']
    stop_col = FILE_POS_COLUMNS[file_format]['stop']

    stats_per_chr: dict[str, LocationStats] = {}

    with open(backup_file, 'r') as infile, open(input_file, 'w') as outfile:
        for line_num, line in enumerate(infile, start=1):
            line = line.rstrip('\n\r')

            # First line is header
            if line_num == 1:
                outfile.write(line + '\n')
                continue

            cols = line.split('\t')
            chrom = cols[chr_col] if len(cols) > chr_col else None

            if not chrom or chrom not in seq_changes:
                # No seq changes for this chromosome, write as-is
                outfile.write('\t'.join(cols) + '\n')
                continue

            # Get start and stop positions
            start = int(cols[start_col]) if len(cols) > start_col and cols[start_col] else None
            stop = int(cols[stop_col]) if len(cols) > stop_col and cols[stop_col] else None

            if not start or not stop:
                logger.warning(f"No start/stop for line {line_num}: {start}, {stop} ... Skipping")
                continue

            # Get sequence change info
            change = seq_changes[chrom]

            # Compute new location
            new_start, new_stop, change_type = compute_new_location(
                change.update_type,
                change.details,
                start,
                stop,
                line_num,
                line,
            )

            # Update columns with new positions
            cols[start_col] = str(new_start)
            cols[stop_col] = str(new_stop)

            outfile.write('\t'.join(cols) + '\n')

            # Update stats
            if chrom not in stats_per_chr:
                stats_per_chr[chrom] = LocationStats()

            if change_type == 'downstream':
                stats_per_chr[chrom].downstream += 1
            elif change_type == 'unaffected':
                stats_per_chr[chrom].unaffected += 1
            elif change_type == 'overlap':
                stats_per_chr[chrom].overlap += 1
            elif change_type == 'encompassing':
                stats_per_chr[chrom].encompassing += 1

    return stats_per_chr


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Recompute genomic locations after sequence updates"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input file to convert (GFF or VCF)",
    )
    parser.add_argument(
        "format",
        choices=['GFF', 'VCF'],
        help="File format (GFF or VCF)",
    )
    parser.add_argument(
        "changes_file",
        type=Path,
        help="File with list of sequence changes",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
    )

    # Validate inputs
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    if not args.changes_file.exists():
        logger.error(f"Changes file not found: {args.changes_file}")
        sys.exit(1)

    file_format = FileFormat(args.format)

    try:
        # Parse sequence changes
        seq_changes = parse_seq_changes(args.changes_file)

        # Process file
        stats = process_file(args.input_file, file_format, seq_changes)

        # Print summary
        print("\nLocation update summary:")
        for chrom, stat in sorted(stats.items()):
            parts = []
            if stat.downstream:
                parts.append(f"{stat.downstream} downstream")
            if stat.unaffected:
                parts.append(f"{stat.unaffected} unaffected")
            if stat.overlap:
                parts.append(f"{stat.overlap} overlap")
            if stat.encompassing:
                parts.append(f"{stat.encompassing} encompassing")
            print(f"{chrom}: {', '.join(parts)}")

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
