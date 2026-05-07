#!/usr/bin/env python3
"""
Convert InterProScan TSV results to GFF format for JBrowse protein tracks.

Usage:
    python interproscan_to_gff.py --tsv FILE --output FILE

InterProScan TSV columns:
    0: Protein accession (e.g., EER30087.1)
    1: Sequence MD5 digest
    2: Sequence length
    3: Analysis (e.g., Pfam, SMART, Gene3D)
    4: Signature accession (e.g., PF00001)
    5: Signature description
    6: Start location
    7: Stop location
    8: E-value / Score
    9: Status (T=true match)
    10: Date
    11: InterPro accession (e.g., IPR000001)
    12: InterPro description
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Map InterProScan analysis names to display names
ANALYSIS_DISPLAY = {
    'Gene3D': 'CATH',
    'ProSiteProfiles': 'ProSiteProfiles',
    'ProSitePatterns': 'ProSitePatterns',
    'Pfam': 'Pfam',
    'SMART': 'SMART',
    'SUPERFAMILY': 'SUPERFAMILY',
    'CDD': 'CDD',
    'PRINTS': 'PRINTS',
    'PANTHER': 'PANTHER',
    'NCBIfam': 'NCBIfam',
    'PIRSF': 'PIRSF',
    'Hamap': 'Hamap',
    'SFLD': 'SFLD',
    'SignalP_EUK': 'SignalP',
    'SignalP_GRAM_POSITIVE': 'SignalP',
    'SignalP_GRAM_NEGATIVE': 'SignalP',
    'TMHMM': 'TMHMM',
    'Coils': 'Coils',
    'MobiDBLite': 'MobiDBLite',
}


def parse_interproscan_tsv(tsv_file: Path):
    """Parse InterProScan TSV and yield GFF lines."""
    with open(tsv_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            fields = line.split('\t')
            if len(fields) < 11:
                continue

            protein_id = fields[0]
            analysis = fields[3]
            sig_accession = fields[4]
            sig_description = fields[5] if len(fields) > 5 else ''
            start = fields[6]
            end = fields[7]
            score = fields[8] if len(fields) > 8 and fields[8] != '-' else '.'
            status = fields[9] if len(fields) > 9 else ''
            ipr_accession = fields[11] if len(fields) > 11 else ''
            ipr_description = fields[12] if len(fields) > 12 else ''

            # Only include true matches
            if status != 'T':
                continue

            # Get display name for analysis
            display_name = ANALYSIS_DISPLAY.get(analysis, analysis)

            # Build description
            description = sig_description or ipr_description or sig_accession

            # Escape special characters for GFF
            description = description.replace(';', '%3B').replace('=', '%3D').replace('&', '%26')

            # Build attributes
            attributes = f"ID={sig_accession};Name={sig_accession};description={description}"
            if ipr_accession:
                attributes += f";interpro={ipr_accession}"

            # GFF3 line: seqid source type start end score strand phase attributes
            yield f"{protein_id}\t{display_name}\t{display_name}\t{start}\t{end}\t{score}\t.\t.\t{attributes}"


def convert_to_gff(tsv_file: Path, output_file: Path):
    """Convert InterProScan TSV to GFF format."""
    logger.info(f"Converting {tsv_file} to GFF format")

    line_count = 0
    with open(output_file, 'w') as out:
        # Write GFF header
        out.write("##gff-version 3\n")

        for gff_line in parse_interproscan_tsv(tsv_file):
            out.write(gff_line + '\n')
            line_count += 1

            if line_count % 10000 == 0:
                logger.info(f"Processed {line_count} features...")

    logger.info(f"Wrote {line_count} features to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert InterProScan TSV to GFF for JBrowse"
    )
    parser.add_argument(
        "--tsv",
        type=Path,
        required=True,
        help="InterProScan TSV results file"
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output GFF file"
    )
    args = parser.parse_args()

    if not args.tsv.exists():
        logger.error(f"TSV file not found: {args.tsv}")
        sys.exit(1)

    convert_to_gff(args.tsv, args.output)
    logger.info("Done!")


if __name__ == "__main__":
    main()
