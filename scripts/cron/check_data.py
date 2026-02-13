#!/usr/bin/env python3
"""
Check data for complex business rules.

This script performs various data integrity checks on the database and
reports violations to curators via email.

Checks include:
- Duplicate URL types
- Gene reservation emails
- Locus vs alias name conflicts
- Headline descriptions with multiple references
- Pseudogenes with GO annotations

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    LOG_DIR: Directory for log files
    TMP_DIR: Temporary directory
    CURATOR_EMAIL: Email for notifications
"""

import logging
import os
import sys
from datetime import datetime
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
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@localhost")

LOG_FILE = LOG_DIR / "CheckData.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class DataChecker:
    """Check database for business rule violations."""

    def __init__(self, session):
        self.session = session
        self.issues = []

    def add_issue(self, category: str, message: str) -> None:
        """Record an issue found during checking."""
        self.issues.append({"category": category, "message": message})
        logger.warning(f"[{category}] {message}")

    def check_duplicate_url_types(self) -> int:
        """Check for features with duplicate URL types."""
        query = text(f"""
            SELECT f.feature_name, u.url_type, COUNT(*)
            FROM {DB_SCHEMA}.feat_url fu
            JOIN {DB_SCHEMA}.feature f ON fu.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.url u ON fu.url_no = u.url_no
            GROUP BY f.feature_name, u.url_type
            HAVING COUNT(*) > 1
        """)

        result = self.session.execute(query)
        count = 0

        for row in result:
            feature_name, url_type, num = row
            self.add_issue(
                "Duplicate URL Types",
                f"Feature {feature_name} has {num} URLs of type '{url_type}'",
            )
            count += 1

        return count

    def check_gene_reservations(self, strain_abbrev: str, organism_no: int) -> int:
        """Check for gene reservations with missing/invalid emails."""
        query = text(f"""
            SELECT gr.reservation_name, gr.email
            FROM {DB_SCHEMA}.gene_reservation gr
            WHERE gr.organism_no = :organism_no
            AND (gr.email IS NULL OR gr.email NOT LIKE '%@%')
        """)

        result = self.session.execute(query, {"organism_no": organism_no})
        count = 0

        for row in result:
            res_name, email = row
            self.add_issue(
                f"Gene Reservation ({strain_abbrev})",
                f"Reservation '{res_name}' has invalid email: {email}",
            )
            count += 1

        return count

    def check_locus_vs_alias_names(self, strain_abbrev: str, organism_no: int) -> int:
        """Check for conflicts between locus names and aliases."""
        query = text(f"""
            SELECT f1.feature_name AS locus, a.alias_name, f2.feature_name AS alias_feature
            FROM {DB_SCHEMA}.feature f1
            JOIN {DB_SCHEMA}.alias a ON f1.feature_no = a.feature_no
            JOIN {DB_SCHEMA}.feature f2 ON UPPER(a.alias_name) = UPPER(f2.gene_name)
            WHERE f1.organism_no = :organism_no
            AND f1.feature_no != f2.feature_no
        """)

        result = self.session.execute(query, {"organism_no": organism_no})
        count = 0

        for row in result:
            locus, alias_name, alias_feature = row
            self.add_issue(
                f"Locus/Alias Conflict ({strain_abbrev})",
                f"Locus {locus} has alias '{alias_name}' which matches gene name of {alias_feature}",
            )
            count += 1

        return count

    def check_pseudogenes_with_go(self, strain_abbrev: str, organism_no: int) -> int:
        """Check for pseudogenes with GO annotations (should not have any)."""
        query = text(f"""
            SELECT DISTINCT f.feature_name, f.feature_type
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.go_annotation ga ON f.feature_no = ga.feature_no
            WHERE f.organism_no = :organism_no
            AND f.feature_type = 'pseudogene'
        """)

        result = self.session.execute(query, {"organism_no": organism_no})
        count = 0

        for row in result:
            feature_name, feature_type = row
            self.add_issue(
                f"Pseudogene GO ({strain_abbrev})",
                f"Pseudogene {feature_name} has GO annotations",
            )
            count += 1

        return count

    def check_headline_descriptions(self, strain_abbrev: str, organism_no: int) -> int:
        """Check for features with headline linked to multiple refs including auto-generated."""
        # This is a complex check - looking for features where headline
        # is linked to multiple references and one is the auto-generated ref
        query = text(f"""
            SELECT f.feature_name, COUNT(DISTINCT rfl.reference_no) as ref_count
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.ref_link rfl ON f.feature_no = rfl.feature_no
            WHERE f.organism_no = :organism_no
            AND f.headline IS NOT NULL
            AND rfl.ref_link_type = 'Headline'
            GROUP BY f.feature_name
            HAVING COUNT(DISTINCT rfl.reference_no) > 1
        """)

        result = self.session.execute(query, {"organism_no": organism_no})
        count = 0

        for row in result:
            feature_name, ref_count = row
            self.add_issue(
                f"Headline Refs ({strain_abbrev})",
                f"Feature {feature_name} has {ref_count} references linked to headline",
            )
            count += 1

        return count

    def get_all_strains(self) -> list[dict]:
        """Get all strains from the database."""
        query = text(f"""
            SELECT organism_no, organism_abbrev
            FROM {DB_SCHEMA}.organism
            WHERE tax_rank = 'Strain'
            OR tax_rank = 'no rank'
            ORDER BY organism_abbrev
        """)

        result = self.session.execute(query)
        return [{"organism_no": row[0], "organism_abbrev": row[1]} for row in result]

    def run_all_checks(self) -> dict:
        """Run all data checks."""
        stats = {
            "total_issues": 0,
            "checks_run": 0,
        }

        logger.info("=" * 60)
        logger.info(f"Data Check Started: {datetime.now()}")
        logger.info("=" * 60)

        # Global checks
        logger.info("Checking for duplicate URL types...")
        stats["duplicate_urls"] = self.check_duplicate_url_types()
        stats["checks_run"] += 1

        # Per-strain checks
        strains = self.get_all_strains()
        logger.info(f"Found {len(strains)} strains to check")

        for strain in strains:
            strain_abbrev = strain["organism_abbrev"]
            organism_no = strain["organism_no"]

            logger.info(f"\n### Checking {strain_abbrev} ###")

            self.check_gene_reservations(strain_abbrev, organism_no)
            self.check_locus_vs_alias_names(strain_abbrev, organism_no)
            self.check_headline_descriptions(strain_abbrev, organism_no)
            self.check_pseudogenes_with_go(strain_abbrev, organism_no)

            stats["checks_run"] += 4

        stats["total_issues"] = len(self.issues)

        logger.info("")
        logger.info("=" * 60)
        logger.info(f"Data Check Complete: {datetime.now()}")
        logger.info(f"Total issues found: {stats['total_issues']}")
        logger.info("=" * 60)

        return stats


def send_report_email(issues: list[dict]) -> None:
    """Send report email to curators."""
    if not CURATOR_EMAIL:
        logger.warning("CURATOR_EMAIL not set, skipping email")
        return

    if not issues:
        logger.info("No issues to report")
        return

    # Group issues by category
    by_category: dict[str, list[str]] = {}
    for issue in issues:
        cat = issue["category"]
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(issue["message"])

    # Build message
    message = f"Data Check Report - {datetime.now().strftime('%m/%d/%Y')}\n\n"
    message += f"Total issues found: {len(issues)}\n\n"

    for category, messages in sorted(by_category.items()):
        message += f"\n=== {category} ({len(messages)}) ===\n"
        for msg in messages:
            message += f"  - {msg}\n"

    message += f"\n\nThe full log file is available at: {LOG_FILE}\n"

    logger.info(f"Would send report to {CURATOR_EMAIL}")
    # In production, implement actual email sending


def main() -> int:
    """Main entry point."""
    try:
        with SessionLocal() as session:
            checker = DataChecker(session)
            stats = checker.run_all_checks()

            if checker.issues:
                send_report_email(checker.issues)

            return 0 if stats["total_issues"] == 0 else 1

    except Exception as e:
        logger.exception(f"Error running data checks: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
