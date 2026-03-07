#!/usr/bin/env python3
"""
Bulk annotation loading script.

This script loads bulk annotation data from a tab-delimited file into the
database, handling feature names, gene names, headlines, aliases, notes,
and paragraphs, along with their reference associations.

Input file format (tab-delimited, NO HEADER):
- Column 1: FEATURE_NAME (value only, no tag)
- Remaining columns: TAG|VALUE pairs (pipe-separated)

Recognized tags:
- feature_name: Replaces current feature name
- gene_name: Updates gene name
- headline: Updates headline
- alias_name: Adds an alias (multiple per row OK)
- note: Adds a note (format: "TAG description", where TAG is SEQ, ANN, or NAME)
- paragraph: Adds a paragraph
- reference_no: Reference to associate with changes

Original Perl: bulk_annotation.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import (
    Alias,
    FeatAlias,
    FeatPara,
    Feature,
    Note,
    NoteLink,
    Paragraph,
    RefLink,
)

load_dotenv()

logger = logging.getLogger(__name__)

# Note type mapping
NOTE_TYPE_FOR_TAG = {
    "SEQ": "Sequence change",
    "ANN": "Annotation change",
    "NAME": "Nomenclature history",
}


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


def get_feature_info(session: Session, feature_name: str) -> tuple | None:
    """
    Get feature information by name.

    Returns:
        Tuple of (feature_no, feature_type, gene_name) or None
    """
    feature = session.query(Feature).filter(
        Feature.feature_name == feature_name
    ).first()

    if not feature:
        return None

    return (feature.feature_no, feature.feature_type, feature.gene_name)


def get_feature_refs(
    session: Session,
    feature_no: int,
) -> tuple[dict, dict, dict]:
    """
    Get references associated with a feature.

    Returns:
        Tuple of (feature_name_refs, gene_name_refs, headline_refs)
    """
    feat_name_refs = {}
    gene_name_refs = {}
    headline_refs = {}

    ref_links = session.query(RefLink).filter(
        and_(
            RefLink.tab_name == "FEATURE",
            RefLink.primary_key == feature_no,
        )
    ).all()

    for rl in ref_links:
        if rl.col_name == "FEATURE_NAME":
            feat_name_refs[rl.reference_no] = True
        elif rl.col_name == "GENE_NAME":
            gene_name_refs[rl.reference_no] = True
        elif rl.col_name == "HEADLINE":
            headline_refs[rl.reference_no] = True

    return feat_name_refs, gene_name_refs, headline_refs


def get_alias_refs(session: Session, feature_no: int) -> dict:
    """Get references associated with feature aliases."""
    alias_refs = {}

    # Join feat_alias with ref_link
    feat_aliases = session.query(FeatAlias).filter(
        FeatAlias.feature_no == feature_no
    ).all()

    for fa in feat_aliases:
        ref_links = session.query(RefLink).filter(
            and_(
                RefLink.tab_name == "FEAT_ALIAS",
                RefLink.primary_key == fa.feat_alias_no,
            )
        ).all()
        for rl in ref_links:
            alias_refs[rl.reference_no] = True

    return alias_refs


def add_alias(
    session: Session,
    feature_no: int,
    alias_name: str,
    created_by: str,
) -> int | None:
    """
    Add an alias for a feature.

    Returns:
        feat_alias_no or None if failed
    """
    # Check if alias exists
    alias = session.query(Alias).filter(
        Alias.alias_name == alias_name
    ).first()

    if not alias:
        # Create new alias
        alias = Alias(
            alias_name=alias_name,
            alias_type="Non-uniform",
            created_by=created_by[:12],
        )
        session.add(alias)
        session.flush()
        logger.info(f"Created alias '{alias_name}'")

    # Link alias to feature
    existing_link = session.query(FeatAlias).filter(
        and_(
            FeatAlias.feature_no == feature_no,
            FeatAlias.alias_no == alias.alias_no,
        )
    ).first()

    if existing_link:
        logger.debug(f"Alias '{alias_name}' already linked to feature {feature_no}")
        return existing_link.feat_alias_no

    feat_alias = FeatAlias(
        feature_no=feature_no,
        alias_no=alias.alias_no,
    )
    session.add(feat_alias)
    session.flush()

    logger.info(f"Linked alias '{alias_name}' to feature_no {feature_no}")
    return feat_alias.feat_alias_no


def add_ref_link(
    session: Session,
    reference_no: int,
    primary_key: int,
    tab_name: str,
    col_name: str,
    created_by: str,
) -> None:
    """Add a reference link if it doesn't exist."""
    existing = session.query(RefLink).filter(
        and_(
            RefLink.reference_no == reference_no,
            RefLink.tab_name == tab_name,
            RefLink.col_name == col_name,
            RefLink.primary_key == primary_key,
        )
    ).first()

    if existing:
        return

    ref_link = RefLink(
        reference_no=reference_no,
        tab_name=tab_name,
        col_name=col_name,
        primary_key=primary_key,
        created_by=created_by[:12],
    )
    session.add(ref_link)
    logger.info(f"Added ref_link: ref={reference_no}, table={tab_name}, col={col_name}, pk={primary_key}")


def delete_ref_link(
    session: Session,
    reference_no: int,
    primary_key: int,
    tab_name: str,
    col_name: str,
) -> None:
    """Delete a reference link."""
    session.query(RefLink).filter(
        and_(
            RefLink.reference_no == reference_no,
            RefLink.tab_name == tab_name,
            RefLink.col_name == col_name,
            RefLink.primary_key == primary_key,
        )
    ).delete()
    logger.info(f"Deleted ref_link: ref={reference_no}, table={tab_name}, col={col_name}, pk={primary_key}")


def update_feature_name(
    session: Session,
    feature_no: int,
    new_name: str,
) -> None:
    """Update feature name."""
    feature = session.query(Feature).filter(
        Feature.feature_no == feature_no
    ).first()
    if feature:
        feature.feature_name = new_name
        logger.info(f"Updated feature_name to '{new_name}' for feature_no {feature_no}")


def update_gene_name(
    session: Session,
    feature_no: int,
    new_name: str,
) -> None:
    """Update gene name."""
    feature = session.query(Feature).filter(
        Feature.feature_no == feature_no
    ).first()
    if feature:
        feature.gene_name = new_name
        logger.info(f"Updated gene_name to '{new_name}' for feature_no {feature_no}")


def update_headline(
    session: Session,
    feature_no: int,
    headline: str,
) -> None:
    """Update headline."""
    feature = session.query(Feature).filter(
        Feature.feature_no == feature_no
    ).first()
    if feature:
        feature.headline = headline
        logger.info(f"Updated headline for feature_no {feature_no}")


def add_note(
    session: Session,
    feature_no: int,
    note_text: str,
    note_type: str,
    created_by: str,
) -> int | None:
    """
    Add a note and link it to a feature.

    Returns:
        note_no or None
    """
    # Check if note exists
    note = session.query(Note).filter(
        and_(
            Note.note == note_text,
            Note.note_type == note_type,
        )
    ).first()

    if not note:
        note = Note(
            note=note_text,
            note_type=note_type,
            created_by=created_by[:12],
        )
        session.add(note)
        session.flush()

    # Check if note is already linked
    existing_link = session.query(NoteLink).filter(
        and_(
            NoteLink.note_no == note.note_no,
            NoteLink.tab_name == "FEATURE",
            NoteLink.primary_key == feature_no,
        )
    ).first()

    if not existing_link:
        note_link = NoteLink(
            note_no=note.note_no,
            tab_name="FEATURE",
            primary_key=feature_no,
            created_by=created_by[:12],
        )
        session.add(note_link)
        logger.info(f"Added note '{note_text[:50]}...' to feature_no {feature_no}")

    return note.note_no


def add_paragraph(
    session: Session,
    feature_no: int,
    paragraph_text: str,
    created_by: str,
) -> None:
    """Add a paragraph and link it to a feature."""
    # Check if paragraph exists
    paragraph = session.query(Paragraph).filter(
        Paragraph.paragraph_text == paragraph_text
    ).first()

    if not paragraph:
        paragraph = Paragraph(
            paragraph_text=paragraph_text,
            created_by=created_by[:12],
        )
        session.add(paragraph)
        session.flush()

    # Check if already linked
    existing_link = session.query(FeatPara).filter(
        and_(
            FeatPara.feature_no == feature_no,
            FeatPara.paragraph_no == paragraph.paragraph_no,
        )
    ).first()

    if not existing_link:
        feat_para = FeatPara(
            feature_no=feature_no,
            paragraph_no=paragraph.paragraph_no,
            paragraph_order=1,
        )
        session.add(feat_para)
        logger.info(f"Added paragraph to feature_no {feature_no}")


def parse_input_file(filepath: Path) -> list[dict]:
    """
    Parse the bulk annotation input file.

    Returns:
        List of dictionaries with feature data
    """
    entries = []

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            feature_name = parts[0]

            items = []
            for part in parts[1:]:
                if "|" in part:
                    tag, value = part.split("|", 1)
                    items.append((tag.lower(), value))

            entries.append({
                "line_num": line_num,
                "feature_name": feature_name,
                "items": items,
            })

    logger.info(f"Parsed {len(entries)} entries from input file")
    return entries


def process_entry(
    session: Session,
    entry: dict,
    created_by: str,
    acronym: str = "CGD",
) -> dict:
    """
    Process a single annotation entry.

    Returns:
        Dictionary with processing stats
    """
    stats = {
        "processed": False,
        "feature_name_updated": False,
        "gene_name_updated": False,
        "headline_updated": False,
        "aliases_added": 0,
        "notes_added": 0,
        "paragraphs_added": 0,
    }

    feature_name = entry["feature_name"]
    items = entry["items"]

    # Get feature info
    feat_info = get_feature_info(session, feature_name)
    if not feat_info:
        logger.warning(f"Feature '{feature_name}' not found in database - skipping")
        return stats

    feature_no, feature_type, current_gene_name = feat_info
    logger.info(f"Processing feature_no {feature_no}, feature_name {feature_name}")

    stats["processed"] = True

    # Get current references
    feat_name_refs, gene_name_refs, hl_refs = get_feature_refs(session, feature_no)

    # First pass: extract gene_name and reference_no
    new_gene_name = None
    reference_no = None

    for tag, value in items:
        if tag == "gene_name" and value != current_gene_name:
            new_gene_name = value
        elif tag == "reference_no":
            try:
                reference_no = int(value)
            except ValueError:
                pass

    # Process each item
    for tag, value in items:
        if value == "0":
            continue

        if tag == "feature_name" and feature_name != value:
            logger.info(f"  Changing feature_name to {value}")
            update_feature_name(session, feature_no, value)

            # Add reference for new feature name
            if reference_no:
                add_ref_link(session, reference_no, feature_no, "FEATURE", "FEATURE_NAME", created_by)

            # Delete references for old feature name
            for ref_no in feat_name_refs:
                delete_ref_link(session, ref_no, feature_no, "FEATURE", "FEATURE_NAME")

            # Make old feature name an alias (if not same as gene name)
            if feature_name != current_gene_name and feature_name != new_gene_name:
                feat_alias_no = add_alias(session, feature_no, feature_name, created_by)
                if feat_alias_no and reference_no:
                    add_ref_link(session, reference_no, feat_alias_no, "FEAT_ALIAS", "FEAT_ALIAS_NO", created_by)
                for ref_no in feat_name_refs:
                    add_ref_link(session, ref_no, feat_alias_no, "FEAT_ALIAS", "FEAT_ALIAS_NO", created_by)

            stats["feature_name_updated"] = True

        elif tag == "gene_name" and current_gene_name != value:
            logger.info(f"  Adding new gene_name {value}")
            update_gene_name(session, feature_no, value)

            # Add reference for new gene name
            if reference_no:
                add_ref_link(session, reference_no, feature_no, "FEATURE", "GENE_NAME", created_by)

            # Make old gene name an alias and transfer references
            if current_gene_name:
                feat_alias_no = add_alias(session, feature_no, current_gene_name, created_by)
                if feat_alias_no and reference_no:
                    add_ref_link(session, reference_no, feat_alias_no, "FEAT_ALIAS", "FEAT_ALIAS_NO", created_by)
                for ref_no in gene_name_refs:
                    delete_ref_link(session, ref_no, feature_no, "FEATURE", "GENE_NAME")
                    add_ref_link(session, ref_no, feat_alias_no, "FEAT_ALIAS", "FEAT_ALIAS_NO", created_by)

            stats["gene_name_updated"] = True

        elif tag == "headline":
            logger.info(f"  Adding new headline")
            update_headline(session, feature_no, value)

            # Add reference for new headline
            if reference_no:
                add_ref_link(session, reference_no, feature_no, "FEATURE", "HEADLINE", created_by)

            # Delete references for old headline
            for ref_no in hl_refs:
                delete_ref_link(session, ref_no, feature_no, "FEATURE", "HEADLINE")

            stats["headline_updated"] = True

        elif tag == "alias_name":
            logger.info(f"  Adding new alias {value}")
            feat_alias_no = add_alias(session, feature_no, value, created_by)

            # Add reference for new alias (skip A. nidulans systematic name variants)
            if feat_alias_no and reference_no and not value.startswith(("ANIA_", "ANID_")):
                add_ref_link(session, reference_no, feat_alias_no, "FEAT_ALIAS", "FEAT_ALIAS_NO", created_by)

            stats["aliases_added"] += 1

        elif tag == "note":
            # Parse note format: "TAG description"
            match = re.match(r"^([A-Z]+)\s+(.+)$", value)
            if match:
                note_tag = match.group(1)
                note_text = match.group(2)

                if note_tag in NOTE_TYPE_FOR_TAG:
                    note_type = NOTE_TYPE_FOR_TAG[note_tag]
                    logger.info(f"  Adding new note of type {note_type}")
                    note_no = add_note(session, feature_no, note_text, note_type, created_by)

                    # Add reference for note
                    if note_no and reference_no:
                        add_ref_link(session, reference_no, note_no, "NOTE", "NOTE_NO", created_by)

                    stats["notes_added"] += 1
                else:
                    logger.warning(f"No valid note_type for note: {value}")
            else:
                logger.warning(f"Invalid note format: {value}")

        elif tag == "paragraph":
            logger.info(f"  Adding new paragraph")
            add_paragraph(session, feature_no, value, created_by)
            stats["paragraphs_added"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load bulk annotation data into the database"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Standard strain abbreviation",
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input annotation file (tab-delimited)",
    )
    parser.add_argument(
        "created_by",
        help="Database user name",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Path to log file",
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

    # Setup logging
    setup_logging(args.log_file, args.verbose)

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    logger.info(f"Strain: {args.strain_abbrev}")
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Created by: {args.created_by}")

    # Parse input file
    entries = parse_input_file(args.input_file)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would process {len(entries)} entries")
        for entry in entries:
            logger.info(f"  Feature: {entry['feature_name']}, items: {len(entry['items'])}")
        return

    try:
        with SessionLocal() as session:
            total_stats = {
                "entries_processed": 0,
                "features_updated": 0,
                "feature_names_updated": 0,
                "gene_names_updated": 0,
                "headlines_updated": 0,
                "aliases_added": 0,
                "notes_added": 0,
                "paragraphs_added": 0,
            }

            for entry in entries:
                stats = process_entry(
                    session,
                    entry,
                    args.created_by,
                )

                if stats["processed"]:
                    total_stats["entries_processed"] += 1
                    total_stats["features_updated"] += 1
                    if stats["feature_name_updated"]:
                        total_stats["feature_names_updated"] += 1
                    if stats["gene_name_updated"]:
                        total_stats["gene_names_updated"] += 1
                    if stats["headline_updated"]:
                        total_stats["headlines_updated"] += 1
                    total_stats["aliases_added"] += stats["aliases_added"]
                    total_stats["notes_added"] += stats["notes_added"]
                    total_stats["paragraphs_added"] += stats["paragraphs_added"]

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Entries processed: {total_stats['entries_processed']}")
            logger.info(f"  Features updated: {total_stats['features_updated']}")
            logger.info(f"  Feature names updated: {total_stats['feature_names_updated']}")
            logger.info(f"  Gene names updated: {total_stats['gene_names_updated']}")
            logger.info(f"  Headlines updated: {total_stats['headlines_updated']}")
            logger.info(f"  Aliases added: {total_stats['aliases_added']}")
            logger.info(f"  Notes added: {total_stats['notes_added']}")
            logger.info(f"  Paragraphs added: {total_stats['paragraphs_added']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading bulk annotation: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
