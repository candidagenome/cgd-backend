#!/usr/bin/env python3
"""
Create XML cluster file from InParanoid SQL table output.

This script creates an XML cluster file similar to the format used by
InParanoid (see http://inparanoid.sbc.su.se/download/current/xml/).
The created files can be used with cno_finder.py to find closest non-orthologs.

Original Perl: InparanoidXMLCreator.pl
Converted to Python: 2024

Usage:
    python inparanoid_xml_creator.py sqltable.txt > clusters.xml
"""

import argparse
import sys
import re
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom


def create_xml_from_sqltable(input_file: Path) -> str:
    """
    Create XML from InParanoid SQL table file.

    Args:
        input_file: Path to SQL table file from InParanoid

    Returns:
        XML string
    """
    root = Element('INPARANOID')
    current_cluster = None
    current_cluster_id = 0

    with open(input_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Parse line: cluster_id score species ortholog_score inparanoid_id ...
            match = re.match(
                r'(\d+)\s+(\d+)\s+([A-Za-z]+)\.?\w*\s+(\S+)\s+(\S+)\s+.*',
                line
            )
            if not match:
                continue

            cluster_id = int(match.group(1))
            score = int(match.group(2))
            species = match.group(3)
            ortholog_score = match.group(4)
            inparanoid_id = match.group(5)

            # New cluster
            if cluster_id != current_cluster_id:
                current_cluster_id = cluster_id
                current_cluster = SubElement(
                    root, 'CLUSTER',
                    CLUSTERNO=str(cluster_id),
                    BITSCORE=str(score)
                )

            # Add gene to cluster
            SubElement(
                current_cluster, 'GENE',
                GENEID=inparanoid_id,
                PROTID=inparanoid_id,
                SCORE=ortholog_score,
                SPECIES=species
            )

    # Pretty print XML
    rough_string = tostring(root, encoding='unicode')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ", encoding=None)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create XML cluster file from InParanoid SQL table"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="InParanoid SQL table file",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output XML file (default: stdout)",
    )

    args = parser.parse_args()

    if not args.input_file.exists():
        print(f"Error: File not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)

    xml_output = create_xml_from_sqltable(args.input_file)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(xml_output)
    else:
        print(xml_output)


if __name__ == "__main__":
    main()
