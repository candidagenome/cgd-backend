#!/usr/bin/env python3
"""
Generate curator progress reports.

This script generates weekly progress reports for curation work including:
- GO annotation progress
- Literature Guide (Ref_Property) curation
- Phenotype curation
- Headlines with/without references
- Expired gene reservations
- Paragraph curation

The report is emailed to curators and written to a log file.

Usage:
    python curator_reports.py --strain C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    HTML_ROOT_DIR: Root directory for HTML files
    CURATOR_EMAIL: Email for notifications
    ADMIN_EMAIL: Admin email address
    SMTP_HOST: SMTP server host
"""

import argparse
import logging
import os
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.text import MIMEText
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
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@localhost")
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "25"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_date_range() -> tuple[str, str]:
    """Get current date and date one week ago in YYYY-MM-DD format."""
    current_date = datetime.now()
    previous_date = current_date - timedelta(days=7)
    return (
        current_date.strftime("%Y-%m-%d"),
        previous_date.strftime("%Y-%m-%d"),
    )


class CuratorReporter:
    """Generate curator progress reports."""

    def __init__(self, session, strain_abbrev: str):
        self.session = session
        self.strain_abbrev = strain_abbrev
        self.organism_no = None
        self.species_abbrev = None
        self.feature_types: list[str] = []
        self.current_date, self.previous_date = get_date_range()
        self.report_lines: list[str] = []

    def get_organism_info(self) -> bool:
        """Get organism information from database."""
        query = text(f"""
            SELECT o.organism_no, p.organism_abbrev
            FROM {DB_SCHEMA}.organism o
            LEFT JOIN {DB_SCHEMA}.organism p ON o.parent_organism_no = p.organism_no
                AND p.tax_rank = 'Species'
            WHERE o.organism_abbrev = :strain_abbrev
        """)

        result = self.session.execute(
            query, {"strain_abbrev": self.strain_abbrev}
        ).first()

        if result:
            self.organism_no = result[0]
            self.species_abbrev = result[1] or self.strain_abbrev
            return True
        return False

    def get_feature_types(self) -> None:
        """Get valid feature types from web_metadata."""
        query = text(f"""
            SELECT col_value
            FROM {DB_SCHEMA}.web_metadata
            WHERE application_name = 'Locus Page'
            AND tab_name = 'FEATURE'
            AND col_name = 'FEATURE_TYPE'
        """)

        result = self.session.execute(query)
        self.feature_types = [row[0] for row in result if row[0]]

    def log(self, message: str) -> None:
        """Add message to report log."""
        self.report_lines.append(message)
        logger.info(message)

    def get_genes_without_go(self) -> dict[str, int]:
        """Get count of genes without GO annotations by feature type."""
        counts: dict[str, int] = {}

        for feat_type in self.feature_types:
            query = text(f"""
                SELECT COUNT(DISTINCT feature_no)
                FROM {DB_SCHEMA}.feature
                WHERE feature_type = :feat_type
                AND organism_no = :organism_no
                AND feature_no NOT IN (SELECT feature_no FROM {DB_SCHEMA}.go_annotation)
                AND feature_no NOT IN (
                    SELECT feature_no FROM {DB_SCHEMA}.feat_property
                    WHERE property_type = 'feature_qualifier'
                    AND property_value LIKE 'Deleted%'
                )
            """)

            result = self.session.execute(query, {
                "feat_type": feat_type,
                "organism_no": self.organism_no,
            }).first()

            count = result[0] if result else 0
            if count > 0:
                counts[feat_type] = count

        return counts

    def get_go_progress(self, manual: bool = True) -> tuple[int, int]:
        """
        Get GO annotation progress for the date range.

        Args:
            manual: If True, count manual annotations; otherwise automatic

        Returns:
            Tuple of (genes_done, genes_reviewed)
        """
        type_filter = (
            "AND (a.annotation_type = 'manually curated' AND a.go_evidence <> 'ND')"
            if manual
            else "AND (a.annotation_type <> 'manually curated' OR a.go_evidence = 'ND')"
        )

        feat_types_str = ", ".join(f"'{ft}'" for ft in self.feature_types)

        # Genes with GO annotations done this week
        query = text(f"""
            SELECT COUNT(DISTINCT f.feature_no)
            FROM {DB_SCHEMA}.go_annotation a
            JOIN {DB_SCHEMA}.go_ref r ON r.go_annotation_no = a.go_annotation_no
            JOIN {DB_SCHEMA}.feature f ON a.feature_no = f.feature_no
            WHERE TO_CHAR(r.date_created, 'YYYY-MM-DD') <= :current_date
            AND TO_CHAR(r.date_created, 'YYYY-MM-DD') > :previous_date
            AND f.organism_no = :organism_no
            AND f.feature_type IN ({feat_types_str})
            {type_filter}
        """)

        result = self.session.execute(query, {
            "current_date": self.current_date,
            "previous_date": self.previous_date,
            "organism_no": self.organism_no,
        }).first()

        genes_done = result[0] if result else 0

        # Genes reviewed this week
        review_query = text(f"""
            SELECT COUNT(DISTINCT a.feature_no)
            FROM {DB_SCHEMA}.go_annotation a
            JOIN {DB_SCHEMA}.feature f ON a.feature_no = f.feature_no
            WHERE TO_CHAR(a.date_last_reviewed, 'YYYY-MM-DD') <= :current_date
            AND TO_CHAR(a.date_last_reviewed, 'YYYY-MM-DD') > :previous_date
            AND f.organism_no = :organism_no
            {type_filter}
        """)

        review_result = self.session.execute(review_query, {
            "current_date": self.current_date,
            "previous_date": self.previous_date,
            "organism_no": self.organism_no,
        }).first()

        genes_reviewed = review_result[0] if review_result else 0

        return genes_done, genes_reviewed

    def get_paragraph_progress(self) -> int:
        """Get count of genes with new paragraphs this week."""
        query = text(f"""
            SELECT COUNT(DISTINCT fp.feature_no)
            FROM {DB_SCHEMA}.feat_para fp
            JOIN {DB_SCHEMA}.paragraph p ON p.paragraph_no = fp.paragraph_no
            JOIN {DB_SCHEMA}.feature f ON fp.feature_no = f.feature_no
            WHERE TO_CHAR(p.date_created, 'YYYY-MM-DD') <= :current_date
            AND TO_CHAR(p.date_created, 'YYYY-MM-DD') > :previous_date
            AND f.organism_no = :organism_no
        """)

        result = self.session.execute(query, {
            "current_date": self.current_date,
            "previous_date": self.previous_date,
            "organism_no": self.organism_no,
        }).first()

        return result[0] if result else 0

    def get_lit_guide_stats(self) -> dict:
        """Get Literature Guide curation statistics."""
        stats = {}

        # Features without curated papers
        query1 = text(f"""
            SELECT COUNT(DISTINCT f.feature_no)
            FROM {DB_SCHEMA}.feature f
            WHERE f.organism_no = :organism_no
            AND f.feature_no IN (
                SELECT DISTINCT feature_no FROM {DB_SCHEMA}.refprop_feat
                MINUS
                SELECT DISTINCT rf.feature_no
                FROM {DB_SCHEMA}.ref_property rp
                JOIN {DB_SCHEMA}.refprop_feat rf ON rf.ref_property_no = rp.ref_property_no
                WHERE rp.property_value != 'Not yet curated'
            )
        """)

        result1 = self.session.execute(query1, {"organism_no": self.organism_no}).first()
        stats["features_no_curation"] = result1[0] if result1 else 0

        # References not curated for any genes (ALL ORGANISMS)
        query2 = text(f"""
            SELECT COUNT(DISTINCT reference_no)
            FROM {DB_SCHEMA}.reference
            WHERE reference_no IN (
                SELECT DISTINCT reference_no
                FROM {DB_SCHEMA}.ref_property
                WHERE property_value = 'Not yet curated'
                AND property_type = 'literature_topic'
                AND reference_no NOT IN (
                    SELECT reference_no FROM {DB_SCHEMA}.ref_property
                    WHERE property_value != 'Not yet curated'
                    AND property_type = 'literature_topic'
                )
            )
        """)

        result2 = self.session.execute(query2).first()
        stats["refs_not_curated"] = result2[0] if result2 else 0

        # References with no feature or topic association (ALL ORGANISMS)
        query3 = text(f"""
            SELECT COUNT(reference_no)
            FROM {DB_SCHEMA}.reference
            WHERE pubmed IS NOT NULL
            AND source != 'PDB script'
            AND reference_no NOT IN (
                SELECT DISTINCT reference_no FROM {DB_SCHEMA}.ref_property
                UNION
                SELECT DISTINCT reference_no FROM {DB_SCHEMA}.ref_link
            )
        """)

        result3 = self.session.execute(query3).first()
        stats["refs_no_association"] = result3[0] if result3 else 0

        # Papers partially curated (ALL ORGANISMS)
        query4 = text(f"""
            SELECT COUNT(DISTINCT reference_no)
            FROM {DB_SCHEMA}.reference
            WHERE reference_no IN (
                SELECT DISTINCT reference_no
                FROM {DB_SCHEMA}.ref_property
                WHERE property_value IN (
                    SELECT term_name FROM {DB_SCHEMA}.cv_term t
                    JOIN {DB_SCHEMA}.cv c ON c.cv_no = t.cv_no
                    WHERE c.cv_name = 'literature_topic'
                )
                AND reference_no IN (
                    SELECT DISTINCT reference_no FROM {DB_SCHEMA}.ref_property
                    WHERE property_value IN ('Not yet curated', 'High Priority')
                )
            )
        """)

        result4 = self.session.execute(query4).first()
        stats["refs_partially_curated"] = result4[0] if result4 else 0

        return stats

    def get_lit_guide_progress(self) -> dict:
        """Get Literature Guide progress for the week."""
        stats = {}

        # New papers added (ALL ORGANISMS)
        query1 = text(f"""
            SELECT COUNT(reference_no)
            FROM {DB_SCHEMA}.reference
            WHERE TO_CHAR(date_created, 'YYYY-MM-DD') >= :previous_date
        """)

        result1 = self.session.execute(query1, {
            "previous_date": self.previous_date
        }).first()
        stats["new_papers"] = result1[0] if result1 else 0

        # Papers curated this week (ALL ORGANISMS)
        query2 = text(f"""
            SELECT COUNT(DISTINCT reference_no)
            FROM {DB_SCHEMA}.reference
            WHERE reference_no IN (
                SELECT DISTINCT reference_no
                FROM {DB_SCHEMA}.ref_property
                WHERE property_type = 'literature_topic'
                AND TO_CHAR(date_last_reviewed, 'YYYY-MM-DD') <= :current_date
                AND TO_CHAR(date_last_reviewed, 'YYYY-MM-DD') > :previous_date
            )
        """)

        result2 = self.session.execute(query2, {
            "current_date": self.current_date,
            "previous_date": self.previous_date,
        }).first()
        stats["papers_curated"] = result2[0] if result2 else 0

        # Features curated this week
        query3 = text(f"""
            SELECT COUNT(DISTINCT feature_no)
            FROM {DB_SCHEMA}.feature
            WHERE organism_no = :organism_no
            AND feature_no IN (
                SELECT DISTINCT rf.feature_no
                FROM {DB_SCHEMA}.ref_property rp
                JOIN {DB_SCHEMA}.refprop_feat rf ON rf.ref_property_no = rp.ref_property_no
                WHERE property_value != 'Not yet curated'
                AND TO_CHAR(rf.date_created, 'YYYY-MM-DD') <= :current_date
                AND TO_CHAR(rf.date_created, 'YYYY-MM-DD') > :previous_date
            )
        """)

        result3 = self.session.execute(query3, {
            "organism_no": self.organism_no,
            "current_date": self.current_date,
            "previous_date": self.previous_date,
        }).first()
        stats["features_curated"] = result3[0] if result3 else 0

        # Non-gene annotations (ALL ORGANISMS)
        query4 = text(f"""
            SELECT COUNT(DISTINCT reference_no)
            FROM {DB_SCHEMA}.reference
            WHERE reference_no IN (
                SELECT DISTINCT reference_no
                FROM {DB_SCHEMA}.ref_property
                WHERE property_type = 'literature_topic'
                AND TO_CHAR(date_last_reviewed, 'YYYY-MM-DD') <= :current_date
                AND TO_CHAR(date_last_reviewed, 'YYYY-MM-DD') > :previous_date
                AND ref_property_no NOT IN (
                    SELECT DISTINCT ref_property_no FROM {DB_SCHEMA}.refprop_feat
                )
            )
        """)

        result4 = self.session.execute(query4, {
            "current_date": self.current_date,
            "previous_date": self.previous_date,
        }).first()
        stats["non_gene_annotations"] = result4[0] if result4 else 0

        return stats

    def get_phenotype_stats(self) -> dict:
        """Get phenotype curation statistics."""
        stats = {}

        feat_types_str = ", ".join(f"'{ft}'" for ft in self.feature_types)

        # Genes without phenotypes
        query1 = text(f"""
            SELECT COUNT(DISTINCT feature_no)
            FROM {DB_SCHEMA}.feature
            WHERE organism_no = :organism_no
            AND feature_type IN ({feat_types_str})
            AND feature_no NOT IN (
                SELECT feature_no FROM {DB_SCHEMA}.feat_property
                WHERE property_type = 'feature_qualifier'
                AND property_value LIKE 'Deleted%'
            )
            AND feature_no NOT IN (
                SELECT feature_no FROM {DB_SCHEMA}.pheno_annotation
            )
        """)

        result1 = self.session.execute(query1, {
            "organism_no": self.organism_no
        }).first()
        stats["genes_no_phenotype"] = result1[0] if result1 else 0

        # Genes with phenotypes added this week
        query2 = text(f"""
            SELECT COUNT(DISTINCT f.feature_no)
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.pheno_annotation pa ON pa.feature_no = f.feature_no
            WHERE f.organism_no = :organism_no
            AND TO_CHAR(pa.date_created, 'YYYY-MM-DD') <= :current_date
            AND TO_CHAR(pa.date_created, 'YYYY-MM-DD') > :previous_date
        """)

        result2 = self.session.execute(query2, {
            "organism_no": self.organism_no,
            "current_date": self.current_date,
            "previous_date": self.previous_date,
        }).first()
        stats["phenotypes_added"] = result2[0] if result2 else 0

        return stats

    def get_headline_stats(self) -> dict:
        """Get headline reference statistics."""
        stats = {}

        feat_types_str = ", ".join(f"'{ft}'" for ft in self.feature_types)

        # Headlines without references
        query1 = text(f"""
            SELECT COUNT(feature_no)
            FROM {DB_SCHEMA}.feature
            WHERE organism_no = :organism_no
            AND headline IS NOT NULL
            AND feature_no NOT IN (
                SELECT primary_key FROM {DB_SCHEMA}.ref_link
                WHERE tab_name = 'FEATURE' AND col_name = 'HEADLINE'
            )
            AND feature_type IN ({feat_types_str})
        """)

        result1 = self.session.execute(query1, {
            "organism_no": self.organism_no
        }).first()
        stats["headlines_no_ref"] = result1[0] if result1 else 0

        # Headlines with refs added this week
        query2 = text(f"""
            SELECT COUNT(DISTINCT feature_no)
            FROM {DB_SCHEMA}.feature
            WHERE organism_no = :organism_no
            AND feature_no IN (
                SELECT DISTINCT primary_key
                FROM {DB_SCHEMA}.ref_link
                WHERE tab_name = 'FEATURE'
                AND col_name = 'HEADLINE'
                AND TO_CHAR(date_created, 'YYYY-MM-DD') <= :current_date
                AND TO_CHAR(date_created, 'YYYY-MM-DD') > :previous_date
            )
        """)

        result2 = self.session.execute(query2, {
            "organism_no": self.organism_no,
            "current_date": self.current_date,
            "previous_date": self.previous_date,
        }).first()
        stats["headlines_with_ref_added"] = result2[0] if result2 else 0

        return stats

    def get_expired_reservations(self) -> list[dict]:
        """Get expired gene name reservations."""
        query = text(f"""
            SELECT f.gene_name, c.last_name, c.first_name, c.email, gr.expiration_date
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.gene_reservation gr ON f.feature_no = gr.feature_no
            JOIN {DB_SCHEMA}.coll_generes cg ON gr.gene_reservation_no = cg.gene_reservation_no
            JOIN {DB_SCHEMA}.colleague c ON cg.colleague_no = c.colleague_no
            WHERE TO_CHAR(gr.expiration_date, 'YYYY-MM-DD') <= :current_date
            AND gr.date_standardized IS NULL
            AND f.organism_no = :organism_no
            ORDER BY gr.expiration_date
        """)

        result = self.session.execute(query, {
            "current_date": self.current_date,
            "organism_no": self.organism_no,
        })

        return [
            {
                "gene_name": row[0],
                "last_name": row[1],
                "first_name": row[2],
                "email": row[3],
                "expiration_date": str(row[4]),
            }
            for row in result
        ]

    def generate_report(self) -> str:
        """Generate the full curator report."""
        if not self.get_organism_info():
            return f"Error: Strain {self.strain_abbrev} not found"

        self.get_feature_types()

        # Build report
        lines = []
        lines.append(f"Curation Progress Report for {self.species_abbrev}")
        lines.append(f"Date range: {self.previous_date} to {self.current_date}")
        lines.append("")

        # GO Curation
        lines.append("GO CURATION")
        lines.append("=" * 40)

        no_go = self.get_genes_without_go()
        total_no_go = sum(no_go.values())
        lines.append(f"Genes without any GO annotations: {total_no_go}")
        for feat_type, count in sorted(no_go.items()):
            lines.append(f"  {feat_type}: {count}")

        manual_done, manual_rev = self.get_go_progress(manual=True)
        auto_done, auto_rev = self.get_go_progress(manual=False)
        lines.append(f"Genes with Manual GO annotations this week: {manual_done}")
        lines.append(f"Genes with Automatic GO annotations this week: {auto_done}")
        lines.append("")

        # Paragraph curation
        lines.append("PARAGRAPH CURATION")
        lines.append("=" * 40)
        paragraphs = self.get_paragraph_progress()
        lines.append(f"Genes with paragraphs added this week: {paragraphs}")
        lines.append("")

        # Literature Guide
        lines.append("LITERATURE GUIDE CURATION")
        lines.append("=" * 40)

        lit_stats = self.get_lit_guide_stats()
        lit_progress = self.get_lit_guide_progress()

        lines.append(f"Features without curated papers: {lit_stats['features_no_curation']}")
        lines.append(f"References not curated [ALL ORGANISMS]: {lit_stats['refs_not_curated']}")
        lines.append(f"References with no association [ALL ORGANISMS]: {lit_stats['refs_no_association']}")
        lines.append(f"References partially curated [ALL ORGANISMS]: {lit_stats['refs_partially_curated']}")
        lines.append(f"Features curated this week: {lit_progress['features_curated']}")
        lines.append(f"Papers curated this week [ALL ORGANISMS]: {lit_progress['papers_curated']}")
        lines.append(f"Non-gene annotations this week [ALL ORGANISMS]: {lit_progress['non_gene_annotations']}")
        lines.append(f"New papers added this week [ALL ORGANISMS]: {lit_progress['new_papers']}")
        lines.append("")

        # Phenotype curation
        lines.append("PHENOTYPE CURATION")
        lines.append("=" * 40)

        pheno_stats = self.get_phenotype_stats()
        lines.append(f"Genes without phenotypes: {pheno_stats['genes_no_phenotype']}")
        lines.append(f"Genes with phenotypes added this week: {pheno_stats['phenotypes_added']}")
        lines.append("")

        # Headlines
        lines.append("INCOMPLETE REFERENCE CURATION")
        lines.append("=" * 40)

        headline_stats = self.get_headline_stats()
        lines.append(f"Headlines without a reference: {headline_stats['headlines_no_ref']}")
        lines.append(f"Headlines with refs added this week: {headline_stats['headlines_with_ref_added']}")
        lines.append("")

        # Expired reservations
        lines.append("EXPIRED GENE RESERVATIONS")
        lines.append("=" * 40)

        expired = self.get_expired_reservations()
        if expired:
            lines.append(f"Expired reservations: {len(expired)}")
            for res in expired:
                lines.append(
                    f"  {res['gene_name']} - {res['first_name']} {res['last_name']} "
                    f"({res['email']}) - {res['expiration_date']}"
                )
        else:
            lines.append("No expired gene name reservations this week.")

        return "\n".join(lines)


def send_email(subject: str, body: str, to_email: str) -> bool:
    """Send email notification."""
    if not to_email:
        logger.warning("No email recipient configured")
        return False

    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = ADMIN_EMAIL
        msg["To"] = to_email

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.sendmail(ADMIN_EMAIL, [to_email], msg.as_string())

        logger.info(f"Email sent to {to_email}")
        return True

    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def generate_curator_report(strain_abbrev: str) -> bool:
    """
    Main function to generate curator report.

    Args:
        strain_abbrev: Strain abbreviation

    Returns:
        True on success, False on failure
    """
    try:
        with SessionLocal() as session:
            reporter = CuratorReporter(session, strain_abbrev)
            report = reporter.generate_report()

            # Write to log file
            report_dir = HTML_ROOT_DIR / "reports"
            report_dir.mkdir(parents=True, exist_ok=True)

            log_file = report_dir / f"{reporter.species_abbrev}_CurationProgress.log"

            with open(log_file, "a") as f:
                f.write("\n")
                f.write("*" * 50 + "\n")
                f.write(f"Report generated: {datetime.now()}\n")
                f.write(report)
                f.write("\n")

            logger.info(f"Report written to {log_file}")

            # Send email
            if CURATOR_EMAIL:
                send_email(
                    subject=f"{reporter.species_abbrev} curation progress report",
                    body=report,
                    to_email=CURATOR_EMAIL,
                )

            return True

    except Exception as e:
        logger.exception(f"Error generating curator report: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate curator progress reports"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )

    args = parser.parse_args()

    success = generate_curator_report(args.strain)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
