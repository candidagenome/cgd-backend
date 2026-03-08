#!/usr/bin/env python3
from __future__ import annotations

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
import smtplib
import sys
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
LOG_DIR = Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@localhost")
SMTP_SERVER = os.getenv("SMTP_SERVER", "localhost")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Ensure log directory exists
LOG_DIR.mkdir(parents=True, exist_ok=True)
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
        """Check for gene reservations with colleagues missing email addresses."""
        query = text(f"""
            SELECT f.feature_no, cg.colleague_no
            FROM {DB_SCHEMA}.gene_reservation g
            JOIN {DB_SCHEMA}.coll_generes cg ON g.gene_reservation_no = cg.gene_reservation_no
            JOIN {DB_SCHEMA}.colleague c ON cg.colleague_no = c.colleague_no
            JOIN {DB_SCHEMA}.feature f ON g.feature_no = f.feature_no
            WHERE f.organism_no = :organism_no
            AND c.email IS NULL
        """)

        result = self.session.execute(query, {"organism_no": organism_no})
        count = 0

        for row in result:
            feature_no, colleague_no = row
            self.add_issue(
                f"Gene Reservation ({strain_abbrev})",
                f"Feature {feature_no} reservation has colleague {colleague_no} with no email",
            )
            count += 1

        return count

    def check_locus_vs_alias_names(self, strain_abbrev: str, organism_no: int) -> int:
        """Check for features where gene_name matches an alias_name."""
        query = text(f"""
            SELECT f.feature_no, f.gene_name, a.alias_no, a.alias_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_alias fa ON f.feature_no = fa.feature_no
            JOIN {DB_SCHEMA}.alias a ON fa.alias_no = a.alias_no
            WHERE f.organism_no = :organism_no
            AND f.gene_name IS NOT NULL
            AND f.gene_name = a.alias_name
        """)

        result = self.session.execute(query, {"organism_no": organism_no})
        count = 0

        for row in result:
            feature_no, gene_name, alias_no, alias_name = row
            self.add_issue(
                f"Locus/Alias Conflict ({strain_abbrev})",
                f"Feature {feature_no} gene_name '{gene_name}' matches alias '{alias_name}' (alias_no={alias_no})",
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
        # Reference_no for internal reference: "auto-generated descriptions using orthologs and transferred GO"
        # CGD: 56824, AspGD: 3444
        auto_ref_no = 56824  # CGD default

        # Find features where headline is linked to auto-generated ref AND other refs
        query = text(f"""
            SELECT DISTINCT r1.primary_key, f.feature_name, f.headline
            FROM {DB_SCHEMA}.ref_link r1
            JOIN {DB_SCHEMA}.ref_link r2 ON r1.primary_key = r2.primary_key
            JOIN {DB_SCHEMA}.feature f ON r1.primary_key = f.feature_no
            WHERE r1.tab_name = 'FEATURE'
            AND r1.col_name = 'HEADLINE'
            AND r2.tab_name = 'FEATURE'
            AND r2.col_name = 'HEADLINE'
            AND r1.reference_no != r2.reference_no
            AND r1.reference_no = :auto_ref_no
            AND f.organism_no = :organism_no
        """)

        result = self.session.execute(
            query, {"auto_ref_no": auto_ref_no, "organism_no": organism_no}
        )
        count = 0

        for row in result:
            feature_no, feature_name, headline = row
            self.add_issue(
                f"Headline Refs ({strain_abbrev})",
                f"Feature {feature_name} (feature_no={feature_no}) has auto-generated headline with other refs",
            )
            count += 1

        return count

    def get_all_strains(self) -> list[dict]:
        """Get all strains from the database."""
        # Known CGD strains
        strain_abbrevs = [
            "C_albicans_SC5314",
            "C_dubliniensis_CD36",
            "C_glabrata_CBS138",
            "C_parapsilosis_CDC317",
            "C_auris_B8441",
        ]

        strains = []
        for abbrev in strain_abbrevs:
            query = text(f"""
                SELECT organism_no, organism_abbrev
                FROM {DB_SCHEMA}.organism
                WHERE organism_abbrev = :abbrev
            """)
            result = self.session.execute(query, {"abbrev": abbrev}).fetchone()
            if result:
                strains.append({"organism_no": result[0], "organism_abbrev": result[1]})

        return strains

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

    # Build message body
    body = f"Data Check Report - {datetime.now().strftime('%m/%d/%Y')}\n\n"
    body += f"Total issues found: {len(issues)}\n\n"

    for category, messages in sorted(by_category.items()):
        body += f"\n=== {category} ({len(messages)}) ===\n"
        for msg in messages:
            body += f"  - {msg}\n"

    body += f"\n\nThe full log file is available at: {LOG_FILE}\n"

    # Create email
    subject = f"Check {PROJECT_ACRONYM} Data for Business Rules - {datetime.now().strftime('%m/%d/%Y')}"
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = ADMIN_EMAIL
    msg["To"] = CURATOR_EMAIL

    # Send email
    try:
        with smtplib.SMTP(SMTP_SERVER) as server:
            server.send_message(msg)
        logger.info(f"Sent report email to {CURATOR_EMAIL}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


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
