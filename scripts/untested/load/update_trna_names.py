#!/usr/bin/env python3
"""
Update tRNA feature names, gene names, and GO annotations.

This script processes tRNA features to:
- Update feature_name and gene_name following standard nomenclature
- Add standard headline descriptions
- Add aliases for previous names
- Add GO annotations with ISM evidence code for tRNA function
- Link references for nomenclature and tRNAscan-SE

Input format: Tab-delimited file with columns:
  1. Feature number (temp, not used for lookup)
  2. Chromosome name
  3. Current systematic name (feature_name)
  4. Current standard name (gene_name)
  5. Amino acid (3-letter code)
  6. Anticodon
  7. New systematic name (or 'auto')
  8. New standard name (or 'auto')
  9. New aliases (pipe-delimited or 'auto')
  10. New history note (or 'auto')
  11. New headline (or 'auto')

Lines not starting with a digit are skipped.

Original Perl: update_tRNA_names.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import (
    Alias,
    FeatAlias,
    Feature,
    Go,
    GoAnnotation,
    GoRef,
    Note,
    NoteLink,
    RefLink,
    Reference,
)

load_dotenv()

logger = logging.getLogger(__name__)

# tRNAscan-SE paper PMID
TRNASCAN_PMID = 9023104

# GO IDs for tRNA functions
CYTO_TRNA_GOID = 2182    # cytoplasmic translational elongation
MITO_TRNA_GOID = 70125   # mitochondrial translational elongation
SEC_TRNA_GOID = 1514     # selenocysteine incorporation

# Amino acid 3-letter to 1-letter mapping
ONE_FOR_THREE = {
    'Ala': 'A', 'Cys': 'C', 'Asp': 'D', 'Glu': 'E',
    'Phe': 'F', 'Gly': 'G', 'His': 'H', 'Ile': 'I',
    'Lys': 'K', 'Leu': 'L', 'Met': 'M', 'Asn': 'N',
    'Pro': 'P', 'Gln': 'Q', 'Arg': 'R', 'Ser': 'S',
    'Thr': 'T', 'Val': 'V', 'Trp': 'W', 'Tyr': 'Y',
    'SeC': 'U',
}

# GO IDs for specific anticodons - "NNN codon-amino acid adaptor activity"
GOID_FOR_ANTICODON = {
    'UUU': 33443, 'GUU': 33442, 'CUU': 33444, 'AUU': 33441,
    'UGU': 33439, 'GGU': 33438, 'CGU': 33440, 'AGU': 33437,
    'UCU': 33447, 'GCU': 33446, 'CCU': 33448, 'ACU': 33445,
    'UAU': 33435, 'GAU': 33434, 'CAU': 33436, 'AAU': 33433,
    'UUG': 33427, 'GUG': 33426, 'CUG': 33428, 'AUG': 33425,
    'UGG': 33423, 'GGG': 33422, 'CGG': 33424, 'AGG': 33421,
    'UCG': 33431, 'GCG': 33430, 'CCG': 33432, 'ACG': 33429,
    'UAG': 33419, 'GAG': 33418, 'CAG': 33420, 'AAG': 33417,
    'UUC': 33459, 'GUC': 33458, 'CUC': 33460, 'AUC': 33457,
    'UGC': 33455, 'GGC': 33454, 'CGC': 33456, 'AGC': 33453,
    'UCC': 33463, 'GCC': 33462, 'CCC': 33464, 'ACC': 33461,
    'UAC': 33451, 'GAC': 33450, 'CAC': 33452, 'AAC': 33449,
    'UUA': 33411, 'GUA': 33410, 'CUA': 33412, 'AUA': 33409,
    'UGA': 33407, 'GGA': 33406, 'CGA': 33408, 'AGA': 33405,
    'UCA': 33415, 'GCA': 33414, 'CCA': 33416, 'ACA': 33413,
    'UAA': 33403, 'GAA': 33402, 'CAA': 33404, 'AAA': 33401,
}


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def get_reference_by_pmid(session: Session, pmid: int) -> Reference | None:
    """Get reference by PubMed ID."""
    return session.query(Reference).filter(Reference.pubmed == pmid).first()


def get_curator_reference(session: Session) -> Reference | None:
    """Get curator reference for tRNA annotation."""
    return session.query(Reference).filter(
        Reference.citation.like('%Annotation of tRNAs%')
    ).first()


def get_feature_by_name(session: Session, feature_name: str) -> Feature | None:
    """Get feature by feature_name."""
    return session.query(Feature).filter(
        Feature.feature_name == feature_name
    ).first()


def get_go_by_goid(session: Session, goid: int) -> Go | None:
    """Get GO term by GOID."""
    return session.query(Go).filter(Go.goid == goid).first()


def get_go_annotations(session: Session, feature_no: int) -> dict:
    """
    Get existing GO annotations for a feature.

    Returns:
        Dict mapping go_no to list of (go_annotation_no, evidence, annotation_type)
    """
    annotations = session.query(GoAnnotation).filter(
        GoAnnotation.feature_no == feature_no
    ).all()

    result = defaultdict(list)
    for ann in annotations:
        result[ann.go_no].append({
            'go_annotation_no': ann.go_annotation_no,
            'evidence': ann.go_evidence,
            'annotation_type': ann.annotation_type,
        })
    return result


def get_feature_refs(session: Session, feature_no: int) -> tuple[dict, dict]:
    """
    Get reference links for a feature.

    Returns:
        Tuple of (gene_name_refs, headline_refs) as dicts of ref_no -> True
    """
    refs = session.query(RefLink).filter(
        and_(
            RefLink.tab_name == 'FEATURE',
            RefLink.primary_key == feature_no,
        )
    ).all()

    gene_refs = {}
    hl_refs = {}
    for ref in refs:
        if ref.col_name == 'GENE_NAME':
            gene_refs[ref.reference_no] = True
        elif ref.col_name == 'HEADLINE':
            hl_refs[ref.reference_no] = True

    return gene_refs, hl_refs


def check_mito(chromosome: str, mito_chromosomes: list[str]) -> bool:
    """Check if chromosome is mitochondrial."""
    return chromosome in mito_chromosomes


def get_go_nos_for_trna(
    session: Session,
    amino_acid: str,
    anticodon: str,
    is_mito: bool,
) -> list[int]:
    """
    Get relevant GO numbers for a tRNA.

    Args:
        session: Database session
        amino_acid: 3-letter amino acid code
        anticodon: Anticodon sequence
        is_mito: Whether the tRNA is mitochondrial

    Returns:
        List of go_no values
    """
    go_nos = []

    # Get anticodon-specific GO term
    if anticodon in GOID_FOR_ANTICODON:
        goid = GOID_FOR_ANTICODON[anticodon]
        go = get_go_by_goid(session, goid)
        if go:
            go_nos.append(go.go_no)

    # Get selenocysteine GO if applicable
    if amino_acid == 'SeC':
        go = get_go_by_goid(session, SEC_TRNA_GOID)
        if go:
            go_nos.append(go.go_no)

    # Get translational elongation GO (mito or cyto)
    if is_mito:
        go = get_go_by_goid(session, MITO_TRNA_GOID)
    else:
        go = get_go_by_goid(session, CYTO_TRNA_GOID)
    if go:
        go_nos.append(go.go_no)

    return go_nos


def add_go_annotation(
    session: Session,
    feature_no: int,
    go_no: int,
    reference_no: int,
    source: str,
    created_by: str,
) -> bool:
    """
    Add GO annotation with ISM evidence if not already present.

    Returns:
        True if added, False if already existed
    """
    # Check for existing ISM computational annotation
    existing = session.query(GoAnnotation).filter(
        and_(
            GoAnnotation.feature_no == feature_no,
            GoAnnotation.go_no == go_no,
            GoAnnotation.go_evidence == 'ISM',
            GoAnnotation.annotation_type == 'computational',
        )
    ).first()

    if existing:
        return False

    new_ann = GoAnnotation(
        go_no=go_no,
        feature_no=feature_no,
        go_evidence='ISM',
        annotation_type='computational',
        source=source,
        created_by=created_by[:12],
    )
    session.add(new_ann)
    session.flush()

    # Add go_ref link
    new_go_ref = GoRef(
        reference_no=reference_no,
        go_annotation_no=new_ann.go_annotation_no,
        has_qualifier='N',
        has_supporting_evidence='N',
        created_by=created_by[:12],
    )
    session.add(new_go_ref)

    logger.info(f"Added GO annotation go_no={go_no} for feature_no={feature_no}")
    return True


def add_alias(
    session: Session,
    feature_no: int,
    alias_name: str,
    created_by: str,
) -> int | None:
    """
    Add alias for a feature.

    Returns:
        feat_alias_no or None if already exists
    """
    # Check if alias already exists
    existing_alias = session.query(Alias).filter(
        Alias.alias_name == alias_name
    ).first()

    if existing_alias:
        alias_no = existing_alias.alias_no
    else:
        new_alias = Alias(
            alias_name=alias_name,
            alias_type='Non-uniform',
            created_by=created_by[:12],
        )
        session.add(new_alias)
        session.flush()
        alias_no = new_alias.alias_no

    # Check if feat_alias link already exists
    existing_link = session.query(FeatAlias).filter(
        and_(
            FeatAlias.feature_no == feature_no,
            FeatAlias.alias_no == alias_no,
        )
    ).first()

    if existing_link:
        return None

    new_link = FeatAlias(
        feature_no=feature_no,
        alias_no=alias_no,
    )
    session.add(new_link)
    session.flush()

    logger.info(f"Added alias '{alias_name}' for feature_no={feature_no}")
    return new_link.feat_alias_no


def add_ref_link(
    session: Session,
    reference_no: int,
    primary_key: int,
    tab_name: str,
    col_name: str,
    created_by: str,
) -> bool:
    """
    Add reference link if not exists.

    Returns:
        True if added, False if already existed
    """
    existing = session.query(RefLink).filter(
        and_(
            RefLink.reference_no == reference_no,
            RefLink.tab_name == tab_name,
            RefLink.col_name == col_name,
            RefLink.primary_key == primary_key,
        )
    ).first()

    if existing:
        return False

    new_link = RefLink(
        reference_no=reference_no,
        tab_name=tab_name,
        col_name=col_name,
        primary_key=primary_key,
        created_by=created_by[:12],
    )
    session.add(new_link)
    return True


def delete_gene_refs(
    session: Session,
    feature_no: int,
    ref_nos: list[int],
) -> int:
    """Delete GENE_NAME ref_links for a feature."""
    count = 0
    for ref_no in ref_nos:
        deleted = session.query(RefLink).filter(
            and_(
                RefLink.tab_name == 'FEATURE',
                RefLink.col_name == 'GENE_NAME',
                RefLink.primary_key == feature_no,
                RefLink.reference_no == ref_no,
            )
        ).delete()
        count += deleted
        if deleted:
            logger.debug(f"Deleted GENE_NAME ref link ref_no={ref_no}")
    return count


def add_note(
    session: Session,
    feature_no: int,
    note_text: str,
    created_by: str,
    note_cache: dict,
) -> int | None:
    """
    Add nomenclature history note.

    Returns:
        note_no or None if already linked
    """
    # Check if note exists
    existing_note = session.query(Note).filter(
        and_(
            Note.note == note_text,
            Note.note_type == 'Nomenclature history',
        )
    ).first()

    if existing_note:
        note_no = existing_note.note_no
    else:
        new_note = Note(
            note=note_text,
            note_type='Nomenclature history',
            created_by=created_by[:12],
        )
        session.add(new_note)
        session.flush()
        note_no = new_note.note_no

    # Check if already linked to this feature
    cache_key = (feature_no, note_no)
    if cache_key in note_cache:
        return None

    # Check database for existing link
    existing_link = session.query(NoteLink).filter(
        and_(
            NoteLink.note_no == note_no,
            NoteLink.tab_name == 'FEATURE',
            NoteLink.primary_key == feature_no,
        )
    ).first()

    if existing_link:
        note_cache[cache_key] = True
        return None

    new_link = NoteLink(
        note_no=note_no,
        tab_name='FEATURE',
        primary_key=feature_no,
        created_by=created_by[:12],
    )
    session.add(new_link)
    note_cache[cache_key] = True

    logger.info(f"Added note for feature_no={feature_no}")
    return note_no


def update_trna_names(
    session: Session,
    input_file: Path,
    trnascan_ref: Reference,
    curator_ref: Reference,
    mito_chromosomes: list[str],
    source: str,
    std_curator_note: str,
    created_by: str,
) -> dict:
    """
    Process tRNA names file and update database.

    Returns:
        Statistics dictionary
    """
    stats = {
        "features_processed": 0,
        "feature_names_updated": 0,
        "gene_names_updated": 0,
        "headlines_updated": 0,
        "aliases_added": 0,
        "go_annotations_added": 0,
        "notes_added": 0,
        "skipped_lines": 0,
    }

    anticodon_counts = defaultdict(int)
    note_cache = {}

    with open(input_file) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()

            # Skip lines not starting with digit
            if not line or not line[0].isdigit():
                continue

            parts = line.split('\t')
            if len(parts) < 6:
                logger.warning(f"Line {line_num}: Not enough columns, skipping")
                stats["skipped_lines"] += 1
                continue

            # Parse columns
            temp_feat_no = parts[0]
            chromosome = parts[1]
            sys_name = parts[2]
            std_name = parts[3] if len(parts) > 3 else ''
            amino_acid = parts[4] if len(parts) > 4 else ''
            anticodon = parts[5] if len(parts) > 5 else ''
            new_sys = parts[6] if len(parts) > 6 else ''
            new_std = parts[7] if len(parts) > 7 else ''
            new_alias = parts[8] if len(parts) > 8 else ''
            new_hist = parts[9] if len(parts) > 9 else ''
            new_hl = parts[10] if len(parts) > 10 else ''

            # Handle placeholder
            if new_hl == 'xyz':
                new_hl = ''

            # Convert T to U in anticodon
            if anticodon:
                anticodon = anticodon.replace('T', 'U')

            is_mito = check_mito(chromosome, mito_chromosomes)

            # Auto-generate standard name
            if new_std == 'auto' and amino_acid in ONE_FOR_THREE:
                anticodon_counts[anticodon] += 1
                new_std = f"t{ONE_FOR_THREE[amino_acid]}({anticodon}){anticodon_counts[anticodon]}"
                if is_mito:
                    new_std += 'mt'

                # Auto-generate alias with T instead of U
                if new_alias == 'auto' and 'U' in anticodon:
                    mod_ac = anticodon.replace('U', 'T')
                    new_alias = f"t{ONE_FOR_THREE[amino_acid]}({mod_ac}){anticodon_counts[anticodon]}"
                    if is_mito:
                        new_alias += 'mt'

            # Auto-generate headline
            if new_hl == 'auto':
                if amino_acid == 'SeC':
                    new_hl = 'Selenocysteine tRNA, predicted by tRNAscan-SE; UCA anticodon'
                else:
                    new_hl = f'tRNA-{amino_acid}, predicted by tRNAscan-SE; {anticodon} anticodon'
                if is_mito:
                    new_hl = 'Mitochondrial ' + new_hl

            # Auto-generate history note
            if new_hist == 'auto':
                new_hist = std_curator_note

            # Get feature
            feature = get_feature_by_name(session, sys_name)
            if not feature:
                logger.warning(f"Line {line_num}: Feature not found: {sys_name}")
                stats["skipped_lines"] += 1
                continue

            feat_no = feature.feature_no
            feat_type = feature.feature_type

            logger.debug(f"Processing feature_no={feat_no}, feature_name={sys_name}")
            stats["features_processed"] += 1

            # Get existing references
            gene_refs, hl_refs = get_feature_refs(session, feat_no)

            # Add GO annotations for tRNA features
            if feat_type == 'tRNA':
                existing_go = get_go_annotations(session, feat_no)
                go_nos = get_go_nos_for_trna(session, amino_acid, anticodon, is_mito)

                for go_no in go_nos:
                    # Skip if already has ISM computational annotation
                    should_add = True
                    if go_no in existing_go:
                        for ann_info in existing_go[go_no]:
                            if (ann_info['evidence'] == 'ISM' and
                                    ann_info['annotation_type'] == 'computational'):
                                should_add = False
                                break

                    if should_add:
                        if add_go_annotation(
                            session, feat_no, go_no,
                            trnascan_ref.reference_no, source, created_by
                        ):
                            stats["go_annotations_added"] += 1

            # Update feature_name if changed
            if new_sys and sys_name != new_sys:
                feature.feature_name = new_sys
                stats["feature_names_updated"] += 1
                logger.info(f"Updated feature_name to {new_sys}")

                # Add old name as alias
                fa_no = add_alias(session, feat_no, sys_name, created_by)
                if fa_no:
                    stats["aliases_added"] += 1
                    add_ref_link(session, curator_ref.reference_no, fa_no,
                                 'FEAT_ALIAS', 'FEAT_ALIAS_NO', created_by)

            # Update gene_name if changed
            if new_std and std_name != new_std:
                feature.gene_name = new_std
                stats["gene_names_updated"] += 1
                logger.info(f"Updated gene_name to {new_std}")

                # Add old name as alias if it had content
                if std_name and std_name.strip():
                    fa_no = add_alias(session, feat_no, std_name, created_by)
                    if fa_no:
                        stats["aliases_added"] += 1
                        add_ref_link(session, curator_ref.reference_no, fa_no,
                                     'FEAT_ALIAS', 'FEAT_ALIAS_NO', created_by)

                        # Move gene name refs to alias
                        if gene_refs:
                            delete_gene_refs(session, feat_no, list(gene_refs.keys()))
                            for ref_no in gene_refs:
                                add_ref_link(session, ref_no, fa_no,
                                             'FEAT_ALIAS', 'FEAT_ALIAS_NO', created_by)

            # Add additional aliases
            if new_alias and new_alias != 'auto':
                aliases = new_alias.split('|') if '|' in new_alias else [new_alias]
                for alias in aliases:
                    if alias and alias != sys_name and alias != new_sys and alias != std_name and alias != new_std:
                        fa_no = add_alias(session, feat_no, alias, created_by)
                        if fa_no:
                            stats["aliases_added"] += 1
                            add_ref_link(session, curator_ref.reference_no, fa_no,
                                         'FEAT_ALIAS', 'FEAT_ALIAS_NO', created_by)

            # Update headline
            if new_hl:
                feature.headline = new_hl
                stats["headlines_updated"] += 1

            # Add reference links
            if new_std or std_name:
                if curator_ref.reference_no not in gene_refs:
                    add_ref_link(session, curator_ref.reference_no, feat_no,
                                 'FEATURE', 'GENE_NAME', created_by)
            if trnascan_ref.reference_no not in hl_refs:
                add_ref_link(session, trnascan_ref.reference_no, feat_no,
                             'FEATURE', 'HEADLINE', created_by)

            # Add history notes
            if new_hist:
                notes = new_hist.split('|') if '|' in new_hist else [new_hist]
                for note_text in notes:
                    note_no = add_note(session, feat_no, note_text, created_by, note_cache)
                    if note_no:
                        stats["notes_added"] += 1
                        if 'tRNAscan' in note_text:
                            add_ref_link(session, trnascan_ref.reference_no, note_no,
                                         'NOTE', 'NOTE_NO', created_by)

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update tRNA feature names and GO annotations"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input tab-delimited tRNA names file",
    )
    parser.add_argument(
        "--source",
        default="CGD",
        help="Annotation source (default: CGD)",
    )
    parser.add_argument(
        "--mito-chromosomes",
        nargs="*",
        default=[],
        help="List of mitochondrial chromosome names",
    )
    parser.add_argument(
        "--nomenclature-url",
        default="/Nomenclature.shtml#trna",
        help="URL to nomenclature guide",
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
        help="Parse file but don't modify database",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Source: {args.source}")
    logger.info(f"Mito chromosomes: {args.mito_chromosomes}")

    # Build standard curator note
    std_curator_note = (
        f"Standard Name assigned by {args.source}; please see the "
        f'<a href="{args.nomenclature_url}">Nomenclature Guide</a> '
        "for more information about the naming conventions used."
    )

    if args.dry_run:
        logger.info("DRY RUN - parsing file only")
        count = 0
        with open(args.input_file) as f:
            for line in f:
                if line.strip() and line.strip()[0].isdigit():
                    count += 1
        logger.info(f"Found {count} tRNA entries")
        return

    try:
        with SessionLocal() as session:
            # Get required references
            trnascan_ref = get_reference_by_pmid(session, TRNASCAN_PMID)
            if not trnascan_ref:
                logger.error(f"tRNAscan-SE reference not found (PMID: {TRNASCAN_PMID})")
                sys.exit(1)

            curator_ref = get_curator_reference(session)
            if not curator_ref:
                logger.error("Curator reference for tRNA annotation not found")
                sys.exit(1)

            logger.info(f"tRNAscan reference_no: {trnascan_ref.reference_no}")
            logger.info(f"Curator reference_no: {curator_ref.reference_no}")

            # Process tRNA names
            stats = update_trna_names(
                session,
                args.input_file,
                trnascan_ref,
                curator_ref,
                args.mito_chromosomes,
                args.source,
                std_curator_note,
                args.created_by,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Update Summary:")
            logger.info(f"  Features processed: {stats['features_processed']}")
            logger.info(f"  Feature names updated: {stats['feature_names_updated']}")
            logger.info(f"  Gene names updated: {stats['gene_names_updated']}")
            logger.info(f"  Headlines updated: {stats['headlines_updated']}")
            logger.info(f"  Aliases added: {stats['aliases_added']}")
            logger.info(f"  GO annotations added: {stats['go_annotations_added']}")
            logger.info(f"  Notes added: {stats['notes_added']}")
            if stats["skipped_lines"] > 0:
                logger.warning(f"  Lines skipped: {stats['skipped_lines']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error updating tRNA names: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
