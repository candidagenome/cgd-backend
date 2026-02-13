#!/usr/bin/env python3
"""
Process obsolete GO IDs and update/delete associated annotations.

This script identifies GO annotations using obsolete GO IDs and:
- Automatically replaces annotations with single replacement IDs
- Automatically deletes IEA annotations or annotations for orf19.* features
- Reports annotations with multiple replacement suggestions for curator review
- Reports annotations with no replacement suggestions for curator review

Based on process_obsolete_goids.pl

Usage:
    python process_obsolete_goids.py           # Dry run (report only)
    python process_obsolete_goids.py --update  # Actually make changes

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory containing GO files
    CURATOR_EMAIL: Email for notifications
    ADMIN_EMAIL: Admin email for sending
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def normalize_goid(goid: str) -> str:
    """
    Normalize a GO ID to numeric-only format.

    Args:
        goid: GO ID (with or without 'GO:' prefix)

    Returns:
        Numeric GO ID without prefix or leading zeros
    """
    if not goid:
        return ""
    goid = re.sub(r"^GO:", "", goid, flags=re.IGNORECASE)
    goid = goid.lstrip("0")
    return goid if goid else "0"


def classify_obsolete_go(
    goids: list[str], obo_file: Path
) -> tuple[list[str], dict[str, str], dict[str, list[str]], dict[str, str]]:
    """
    Classify obsolete GO IDs based on replacement suggestions.

    Args:
        goids: List of numeric GO IDs to check
        obo_file: Path to gene_ontology.obo file

    Returns:
        Tuple of:
        - no_suggestions: List of obsolete IDs with no replacements
        - single_replacement: Dict of obsolete ID -> single replacement ID
        - multi_suggestions: Dict of obsolete ID -> list of suggestion IDs
        - alt_mappings: Dict of alt_id -> primary_id
    """
    # Parse OBO file
    term_map: dict[str, dict] = {}  # norm_id -> {obsolete, replaced_by, consider}
    alt2primary: dict[str, str] = {}  # alt norm_id -> primary norm_id

    with open(obo_file, "r", encoding="utf-8") as f:
        in_term = False
        current_id = None
        alt_ids: list[str] = []
        obsolete = False
        replaced_by: list[str] = []
        consider: list[str] = []

        for line in f:
            line = line.strip()

            if line == "[Term]":
                # Commit previous term
                if in_term and current_id:
                    _commit_term(
                        term_map, alt2primary, current_id,
                        alt_ids, obsolete, replaced_by, consider
                    )

                # Reset for new term
                in_term = True
                current_id = None
                alt_ids = []
                obsolete = False
                replaced_by = []
                consider = []
                continue

            if not in_term:
                continue

            if line.startswith("["):
                # End of current term
                if current_id:
                    _commit_term(
                        term_map, alt2primary, current_id,
                        alt_ids, obsolete, replaced_by, consider
                    )
                in_term = line == "[Term]"
                if in_term:
                    current_id = None
                    alt_ids = []
                    obsolete = False
                    replaced_by = []
                    consider = []
                continue

            # Parse term attributes
            match = re.match(r"^id:\s*(GO:(\d+))", line)
            if match:
                current_id = match.group(1)
                continue

            match = re.match(r"^alt_id:\s*(GO:(\d+))", line)
            if match:
                alt_ids.append(match.group(1))
                continue

            if line.startswith("is_obsolete:") and "true" in line.lower():
                obsolete = True
                continue

            match = re.match(r"^replaced_by:\s*(GO:(\d+))", line)
            if match:
                replaced_by.append(match.group(1))
                continue

            match = re.match(r"^consider:\s*(GO:(\d+))", line)
            if match:
                consider.append(match.group(1))
                continue

        # Commit last term
        if in_term and current_id:
            _commit_term(
                term_map, alt2primary, current_id,
                alt_ids, obsolete, replaced_by, consider
            )

    # Classify input GOIDs
    no_suggestions: list[str] = []
    single_replacement: dict[str, str] = {}
    multi_suggestions: dict[str, list[str]] = {}
    alt_mappings: dict[str, str] = {}

    for inp in goids:
        if not inp or not re.match(r"^\d+$", str(inp)):
            continue

        inp_str = str(inp)

        # Determine primary ID
        if inp_str in term_map:
            primary = inp_str
        elif inp_str in alt2primary:
            primary = alt2primary[inp_str]
            alt_mappings[inp_str] = primary
        else:
            continue  # Unknown ID

        term_info = term_map.get(primary)
        if not term_info or not term_info.get("obsolete"):
            continue  # Not obsolete

        # Get suggestions
        suggestions = (
            term_info.get("replaced_by", []) +
            term_info.get("consider", [])
        )

        if not suggestions:
            no_suggestions.append(inp_str)
        elif len(suggestions) == 1:
            single_replacement[inp_str] = suggestions[0]
        else:
            multi_suggestions[inp_str] = suggestions

    return no_suggestions, single_replacement, multi_suggestions, alt_mappings


def _commit_term(
    term_map: dict, alt_map: dict,
    term_id: str, alt_ids: list[str],
    obsolete: bool, replaced_by: list[str], consider: list[str]
) -> None:
    """Commit a parsed term to the term_map."""
    if not term_id:
        return

    norm_id = normalize_goid(term_id)
    term_map[norm_id] = {
        "obsolete": obsolete,
        "replaced_by": [normalize_goid(r) for r in replaced_by],
        "consider": [normalize_goid(c) for c in consider],
    }

    for alt_id in alt_ids:
        alt_norm = normalize_goid(alt_id)
        alt_map[alt_norm] = norm_id


class ObsoleteGOProcessor:
    """Process obsolete GO IDs and update annotations."""

    def __init__(self, session, update: bool = False):
        self.session = session
        self.update = update

        # Report sections
        self.replace_report: list[str] = []
        self.delete_report: list[str] = []
        self.suggest_report: list[str] = []
        self.blank_report: list[str] = []

    def get_annotated_goids(self) -> list[str]:
        """Get all GO IDs that have annotations."""
        query = text(f"""
            SELECT goid
            FROM {DB_SCHEMA}.go
            WHERE go_no IN (
                SELECT DISTINCT go_no FROM {DB_SCHEMA}.go_annotation
            )
        """)

        result = self.session.execute(query)
        goids = [str(row[0]) for row in result if row[0]]

        logger.info(f"Found {len(goids)} GO IDs with annotations")
        return goids

    def get_go_info(self, goid: str) -> tuple[int | None, str | None]:
        """Get go_no and go_term for a GOID."""
        query = text(f"""
            SELECT go_no, go_term
            FROM {DB_SCHEMA}.go
            WHERE goid = :goid
        """)
        result = self.session.execute(query, {"goid": goid}).first()
        return (result[0], result[1]) if result else (None, None)

    def get_feature_name(self, feature_no: int) -> str:
        """Get feature name for a feature_no."""
        query = text(f"""
            SELECT feature_name
            FROM {DB_SCHEMA}.feature
            WHERE feature_no = :feat_no
        """)
        result = self.session.execute(query, {"feat_no": feature_no}).first()
        return result[0] if result else ""

    def get_annotation_info(self, go_no: int) -> list[tuple]:
        """Get annotation info for a go_no."""
        query = text(f"""
            SELECT go_annotation_no, go_evidence, feature_no
            FROM {DB_SCHEMA}.go_annotation
            WHERE go_no = :go_no
        """)
        result = self.session.execute(query, {"go_no": go_no})
        return [(row[0], row[1], row[2]) for row in result]

    def check_annotation_exists(
        self, go_no: int, feature_no: int, evidence: str
    ) -> int | None:
        """Check if an annotation already exists with the same parameters."""
        query = text(f"""
            SELECT go_annotation_no
            FROM {DB_SCHEMA}.go_annotation
            WHERE go_no = :go_no
            AND feature_no = :feat_no
            AND go_evidence = :evidence
        """)
        result = self.session.execute(query, {
            "go_no": go_no,
            "feat_no": feature_no,
            "evidence": evidence,
        }).first()
        return result[0] if result else None

    def delete_annotation(self, ga_no: int) -> None:
        """Delete a GO annotation and its references."""
        if not self.update:
            return

        # Delete go_ref entries
        delete_ref = text(f"""
            DELETE FROM {DB_SCHEMA}.go_ref
            WHERE go_annotation_no = :ga_no
        """)
        self.session.execute(delete_ref, {"ga_no": ga_no})

        # Delete go_annotation
        delete_annot = text(f"""
            DELETE FROM {DB_SCHEMA}.go_annotation
            WHERE go_annotation_no = :ga_no
        """)
        self.session.execute(delete_annot, {"ga_no": ga_no})

    def update_annotation(self, ga_no: int, new_go_no: int) -> None:
        """Update a GO annotation to use a new GO term."""
        if not self.update:
            return

        update_sql = text(f"""
            UPDATE {DB_SCHEMA}.go_annotation
            SET go_no = :new_go_no
            WHERE go_annotation_no = :ga_no
        """)
        self.session.execute(update_sql, {"new_go_no": new_go_no, "ga_no": ga_no})

    def process_direct_replacements(
        self, replacements: dict[str, str], type_str: str
    ) -> None:
        """
        Process annotations with direct (single) replacements.

        Args:
            replacements: Dict of obsolete goid -> replacement goid
            type_str: "Obsolete" or "Secondary"
        """
        for goid, replacement in replacements.items():
            old_no, old_term = self.get_go_info(goid)
            new_no, new_term = self.get_go_info(replacement)

            if not old_no:
                continue

            annotations = self.get_annotation_info(old_no)

            for ga_no, evidence, feature_no in annotations:
                if not ga_no:
                    continue

                feature_name = self.get_feature_name(feature_no)

                # Check if replacement annotation already exists
                existing = self.check_annotation_exists(new_no, feature_no, evidence)

                if existing:
                    # Delete this annotation since replacement exists
                    self.delete_annotation(ga_no)
                    self.delete_report.append(
                        f"{type_str} GO_ANNOTATION deleted: Feature {feature_name}, "
                        f"GOID {goid}, {old_term}, evidence {evidence} "
                        f"(replacement annotation exists, GOID {replacement}, {new_term})"
                    )
                else:
                    # Update to new GO term
                    self.update_annotation(ga_no, new_no)
                    self.replace_report.append(
                        f"{type_str} GO_ANNOTATION replaced: Feature {feature_name}, "
                        f"GOID {goid}, {old_term} REPLACED BY GOID {replacement}, {new_term}"
                    )

    def process_suggestions(
        self,
        no_suggestions: list[str],
        multi_suggestions: dict[str, list[str]]
    ) -> None:
        """
        Process annotations with multiple or no suggestions.

        Args:
            no_suggestions: List of obsolete GOIDs with no suggestions
            multi_suggestions: Dict of obsolete GOID -> list of suggestions
        """
        all_goids = list(multi_suggestions.keys()) + no_suggestions

        for goid in all_goids:
            go_no, term = self.get_go_info(goid)
            if not go_no:
                continue

            annotations = self.get_annotation_info(go_no)

            for ga_no, evidence, feature_no in annotations:
                if not ga_no:
                    continue

                feature_name = self.get_feature_name(feature_no)

                # Auto-delete IEA annotations or orf19.* features
                if evidence == "IEA" or (feature_name and feature_name.startswith("orf19")):
                    self.delete_annotation(ga_no)
                    self.delete_report.append(
                        f"Obsolete GO_ANNOTATION deleted: Feature {feature_name}, "
                        f"GOID {goid}, {term}, evidence {evidence}"
                    )
                elif goid in multi_suggestions:
                    # Multiple suggestions - report for curator review
                    sugg_text = ""
                    for sugg in multi_suggestions[goid]:
                        sugg_no, sugg_term = self.get_go_info(sugg)
                        sugg_text += f" GOID {sugg}, {sugg_term};"

                    self.suggest_report.append(
                        f"Obsolete GO_ANNOTATION: Feature {feature_name}, "
                        f"GOID {goid}, {term}, evidence {evidence}. "
                        f"Suggested replacements:{sugg_text}"
                    )
                else:
                    # No suggestions - report for curator review
                    self.blank_report.append(
                        f"Obsolete GO_ANNOTATION: Feature {feature_name}, "
                        f"GOID {goid}, {term}, evidence {evidence}. "
                        f"No suggested replacements"
                    )

    def generate_report(self) -> str:
        """Generate the full report."""
        sections = []

        if self.suggest_report:
            sections.append(
                "Obsolete GO Annotations with suggested replacements for curator review:\n\n" +
                "\n".join(self.suggest_report)
            )

        if self.blank_report:
            sections.append(
                "Obsolete GO Annotations without suggested replacements for curator review:\n\n" +
                "\n".join(self.blank_report)
            )

        if self.replace_report or self.delete_report:
            auto_section = "Obsolete GO Annotations that were handled automatically by script:\n\n"
            if self.replace_report:
                auto_section += "\n".join(self.replace_report) + "\n"
            if self.delete_report:
                auto_section += "\n".join(self.delete_report)
            sections.append(auto_section)

        return "\n\n".join(sections)


def process_obsolete_goids(
    obo_file: Path | None = None, update: bool = False
) -> tuple[bool, str]:
    """
    Main function to process obsolete GO IDs.

    Args:
        obo_file: Path to OBO file (default: DATA_DIR/GO/gene_ontology.obo)
        update: Whether to actually make changes (False = dry run)

    Returns:
        Tuple of (success, report_message)
    """
    if obo_file is None:
        obo_file = DATA_DIR / "GO" / "gene_ontology.obo"

    if not obo_file.exists():
        logger.error(f"OBO file not found: {obo_file}")
        return False, f"OBO file not found: {obo_file}"

    logger.info(f"Processing obsolete GO IDs from {obo_file}")
    logger.info(f"Update mode: {update}")

    try:
        with SessionLocal() as session:
            processor = ObsoleteGOProcessor(session, update=update)

            # Get all GOIDs with annotations
            goids = processor.get_annotated_goids()

            # Classify obsolete GOIDs
            (no_suggestions, single_replacement,
             multi_suggestions, alt_mappings) = classify_obsolete_go(goids, obo_file)

            logger.info(f"Found {len(single_replacement)} with single replacement")
            logger.info(f"Found {len(multi_suggestions)} with multiple suggestions")
            logger.info(f"Found {len(no_suggestions)} with no suggestions")
            logger.info(f"Found {len(alt_mappings)} secondary (alt_id) mappings")

            # Process direct replacements (obsolete with single replacement)
            processor.process_direct_replacements(single_replacement, "Obsolete")

            # Process secondary (alt_id) mappings
            processor.process_direct_replacements(alt_mappings, "Secondary")

            # Process those needing curator review
            processor.process_suggestions(no_suggestions, multi_suggestions)

            # Commit if in update mode
            if update:
                session.commit()
                logger.info("Changes committed to database")
            else:
                logger.info("Dry run - no changes made")

            # Generate report
            report = processor.generate_report()

            return True, report

    except Exception as e:
        logger.exception(f"Error processing obsolete GO IDs: {e}")
        return False, str(e)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Process obsolete GO IDs and update/delete annotations"
    )
    parser.add_argument(
        "--obo",
        type=Path,
        help="Path to gene_ontology.obo file",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Actually make changes to the database (default: dry run)",
    )

    args = parser.parse_args()

    success, report = process_obsolete_goids(args.obo, update=args.update)

    if report:
        print(report)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
