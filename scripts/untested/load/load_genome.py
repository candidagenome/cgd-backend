#!/usr/bin/env python3
"""
Load genome features from GFF/GenBank/EMBL files.

This script parses genome annotation files and compares features against
the database, generating reports of features to add, update, or delete.

It handles:
- Parsing GFF3, GenBank, and EMBL format files
- Comparing new coordinates with existing database features
- Generating summary reports of changes
- Optionally loading changes to the database

Input formats supported:
- GFF3 (Gene Feature Format)
- GenBank
- EMBL

Original Perl: loadGenome.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from Bio import SeqIO
from dotenv import load_dotenv
from sqlalchemy import and_, func, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import (
    Feature,
    FeatLocation,
    FeatProperty,
    Organism,
    Seq,
    Subfeature,
)

load_dotenv()

logger = logging.getLogger(__name__)


def setup_logging(log_file: Path = None, verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def get_organism(session: Session, organism_abbrev: str) -> Organism:
    """Get organism by abbreviation."""
    organism = session.query(Organism).filter(
        Organism.organism_abbrev == organism_abbrev
    ).first()

    if not organism:
        raise ValueError(f"Organism not found: {organism_abbrev}")

    return organism


def parse_gff3(filepath: Path) -> dict:
    """
    Parse GFF3 file and extract feature information.

    Returns:
        Dict mapping feature_name -> {type, root, strand, exons, featMin, featMax, ...}
    """
    features = {}

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split('\t')
            if len(parts) < 9:
                continue

            seqid = parts[0]
            source = parts[1]
            feat_type = parts[2]
            start = int(parts[3])
            stop = int(parts[4])
            score = parts[5]
            strand = parts[6]
            phase = parts[7]
            attributes = parts[8]

            # Parse attributes
            attrs = {}
            for attr in attributes.split(';'):
                if '=' in attr:
                    key, value = attr.split('=', 1)
                    attrs[key] = value

            # Get feature name from ID or Name attribute
            feat_name = attrs.get('ID') or attrs.get('Name')
            if not feat_name:
                continue

            # Clean up mRNA suffix
            feat_name = re.sub(r'_mRNA$', '', feat_name)

            # Handle gene/mRNA/CDS entries
            if feat_type in ('gene', 'mRNA', 'transcript'):
                if feat_name not in features:
                    features[feat_name] = {
                        'type': 'ORF',  # Default type
                        'root': seqid,
                        'strand': strand,
                        'featMin': start,
                        'featMax': stop,
                        'exons': [],
                        'alias': [],
                    }
                else:
                    features[feat_name]['featMin'] = min(
                        features[feat_name]['featMin'], start
                    )
                    features[feat_name]['featMax'] = max(
                        features[feat_name]['featMax'], stop
                    )

                # Check for type in attributes
                if 'feature_type' in attrs:
                    features[feat_name]['type'] = attrs['feature_type']

                # Check for aliases
                if 'Alias' in attrs:
                    for alias in attrs['Alias'].split(','):
                        if alias not in features[feat_name]['alias']:
                            features[feat_name]['alias'].append(alias)

            elif feat_type == 'CDS':
                parent = attrs.get('Parent', '')
                parent = re.sub(r'_mRNA$', '', parent)
                if parent and parent in features:
                    exon_str = f"{start}-{stop}"
                    if exon_str not in features[parent]['exons']:
                        features[parent]['exons'].append(exon_str)
                    if 'cdsMin' not in features[parent]:
                        features[parent]['cdsMin'] = start
                        features[parent]['cdsMax'] = stop
                    else:
                        features[parent]['cdsMin'] = min(
                            features[parent]['cdsMin'], start
                        )
                        features[parent]['cdsMax'] = max(
                            features[parent]['cdsMax'], stop
                        )

            elif feat_type == 'exon':
                parent = attrs.get('Parent', '')
                parent = re.sub(r'_mRNA$', '', parent)
                if parent and parent in features:
                    exon_str = f"{start}-{stop}"
                    if exon_str not in features[parent]['exons']:
                        features[parent]['exons'].append(exon_str)

    # Sort exons by start coordinate
    for feat_name, feat_info in features.items():
        if feat_info['exons']:
            feat_info['exons'].sort(key=lambda x: int(x.split('-')[0]))

    logger.info(f"Parsed {len(features)} features from GFF3")
    return features


def get_db_features(
    session: Session,
    organism_abbrev: str,
    seq_source: str,
) -> dict:
    """
    Get existing features from database.

    Returns:
        Dict mapping feature_name -> {feature_no, type, chr, start, stop, strand, ...}
    """
    # Get features with locations for this organism
    organism = get_organism(session, organism_abbrev)

    features = session.query(Feature).filter(
        Feature.organism_no == organism.organism_no
    ).all()

    result = {}
    for feat in features:
        # Get current location
        location = session.query(FeatLocation).filter(
            and_(
                FeatLocation.feature_no == feat.feature_no,
                FeatLocation.is_loc_current == 'Y',
            )
        ).first()

        if location:
            # Get root sequence name
            root_seq = session.query(Seq).filter(
                Seq.seq_no == location.root_seq_no
            ).first()

            chr_name = None
            if root_seq:
                chr_feat = session.query(Feature).filter(
                    Feature.feature_no == root_seq.feature_no
                ).first()
                if chr_feat:
                    chr_name = chr_feat.feature_name

            result[feat.feature_name] = {
                'feature_no': feat.feature_no,
                'type': feat.feature_type,
                'chr': chr_name,
                'start': location.start_coord,
                'stop': location.stop_coord,
                'strand': location.strand,
                'qualifier': None,  # Would need FeatProperty lookup
            }

    logger.info(f"Found {len(result)} features in database")
    return result


def compare_features(
    new_features: dict,
    db_features: dict,
    keep_features: set = None,
    orf_only: bool = False,
    no_delete: bool = False,
) -> tuple[list, list, list, list]:
    """
    Compare new features against database.

    Returns:
        Tuple of (to_add, to_update, to_delete, no_change)
    """
    to_add = []
    to_update = []
    to_delete = []
    no_change = []

    keep_features = keep_features or set()

    # Check new features
    for feat_name, new_info in new_features.items():
        if feat_name in db_features:
            db_info = db_features[feat_name]

            # Compare coordinates
            new_start = new_info.get('cdsMin') or new_info.get('featMin')
            new_stop = new_info.get('cdsMax') or new_info.get('featMax')

            # Adjust for strand
            if new_info['strand'] == '-':
                new_start, new_stop = new_stop, new_start

            db_start = db_info['start']
            db_stop = db_info['stop']

            # Check for changes
            if new_start != db_start or new_stop != db_stop:
                to_update.append({
                    'feature_name': feat_name,
                    'new': new_info,
                    'db': db_info,
                })
            else:
                no_change.append(feat_name)
        else:
            to_add.append({
                'feature_name': feat_name,
                'info': new_info,
            })

    # Check for deletions
    if not no_delete:
        for feat_name, db_info in db_features.items():
            if feat_name in new_features:
                continue
            if feat_name in keep_features:
                continue
            if orf_only and db_info['type'] != 'ORF':
                continue
            if db_info.get('qualifier') and 'deleted' in db_info['qualifier'].lower():
                continue

            to_delete.append({
                'feature_name': feat_name,
                'info': db_info,
            })

    return to_add, to_update, to_delete, no_change


def write_summary(
    output_file: Path,
    to_add: list,
    to_update: list,
    to_delete: list,
    no_change: list,
) -> None:
    """Write summary report."""
    with open(output_file, 'w') as f:
        f.write("FEATURE_NAME\tSTATUS\tNEW_CHR\tNEW_START\tNEW_STOP\t"
                "DB_CHR\tDB_START\tDB_STOP\n")

        for item in to_add:
            info = item['info']
            start = info.get('cdsMin') or info.get('featMin')
            stop = info.get('cdsMax') or info.get('featMax')
            f.write(f"{item['feature_name']}\tADD\t{info['root']}\t"
                    f"{start}\t{stop}\t\t\t\n")

        for item in to_update:
            new_info = item['new']
            db_info = item['db']
            new_start = new_info.get('cdsMin') or new_info.get('featMin')
            new_stop = new_info.get('cdsMax') or new_info.get('featMax')
            f.write(f"{item['feature_name']}\tUPDATE\t{new_info['root']}\t"
                    f"{new_start}\t{new_stop}\t{db_info['chr']}\t"
                    f"{db_info['start']}\t{db_info['stop']}\n")

        for item in to_delete:
            info = item['info']
            f.write(f"{item['feature_name']}\tDELETE\t\t\t\t"
                    f"{info['chr']}\t{info['start']}\t{info['stop']}\n")

        for feat_name in no_change:
            f.write(f"{feat_name}\tNO CHANGE\t\t\t\t\t\t\n")

    logger.info(f"Summary written to {output_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load genome features from GFF/GenBank/EMBL files"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input annotation file (GFF3, GenBank, or EMBL)",
    )
    parser.add_argument(
        "--organism", "-o",
        required=True,
        help="Organism abbreviation",
    )
    parser.add_argument(
        "--format", "-f",
        choices=['gff', 'genbank', 'embl'],
        default='gff',
        help="Input file format (default: gff)",
    )
    parser.add_argument(
        "--seq-source",
        help="Sequence source identifier",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path('.'),
        help="Output directory for reports (default: current)",
    )
    parser.add_argument(
        "--keep-file",
        type=Path,
        help="File with feature names to keep (not delete)",
    )
    parser.add_argument(
        "--orf-only",
        action="store_true",
        help="Only delete ORF features (keep other types)",
    )
    parser.add_argument(
        "--no-delete",
        action="store_true",
        help="Don't mark any features for deletion",
    )
    parser.add_argument(
        "--created-by",
        default=os.getenv("DB_USER", "SCRIPT"),
        help="Database user for created_by field",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate report only, don't modify database",
    )

    args = parser.parse_args()

    setup_logging(verbose=args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Format: {args.format}")
    logger.info(f"Organism: {args.organism}")

    # Load keep file if provided
    keep_features = set()
    if args.keep_file and args.keep_file.exists():
        with open(args.keep_file) as f:
            for line in f:
                feat = line.strip().split()[0] if line.strip() else None
                if feat:
                    keep_features.add(feat)
        logger.info(f"Loaded {len(keep_features)} features to keep")

    # Parse input file
    if args.format == 'gff':
        new_features = parse_gff3(args.input_file)
    elif args.format == 'genbank':
        # GenBank parsing would go here
        logger.error("GenBank format not yet implemented")
        sys.exit(1)
    elif args.format == 'embl':
        # EMBL parsing would go here
        logger.error("EMBL format not yet implemented")
        sys.exit(1)

    if not new_features:
        logger.warning("No features found in input file")
        return

    try:
        with SessionLocal() as session:
            # Get database features
            db_features = get_db_features(
                session, args.organism, args.seq_source
            )

            # Compare features
            to_add, to_update, to_delete, no_change = compare_features(
                new_features,
                db_features,
                keep_features,
                args.orf_only,
                args.no_delete,
            )

            # Generate summary report
            summary_file = args.output_dir / f"{args.input_file.stem}_summary.txt"
            write_summary(summary_file, to_add, to_update, to_delete, no_change)

            logger.info("=" * 50)
            logger.info("Comparison Summary:")
            logger.info(f"  Features to ADD: {len(to_add)}")
            logger.info(f"  Features to UPDATE: {len(to_update)}")
            logger.info(f"  Features to DELETE: {len(to_delete)}")
            logger.info(f"  Features with NO CHANGE: {len(no_change)}")
            logger.info("=" * 50)

            if args.dry_run:
                logger.info("DRY RUN - no database modifications")
                return

            # TODO: Implement actual database loading
            # This would involve:
            # 1. Adding new features with locations
            # 2. Updating feature coordinates
            # 3. Marking deleted features
            # 4. Updating subfeatures (CDS, introns, UTRs)
            logger.warning("Database loading not implemented - use summary report")

    except Exception as e:
        logger.error(f"Error processing genome file: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
