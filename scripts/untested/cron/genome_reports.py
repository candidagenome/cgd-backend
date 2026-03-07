#!/usr/bin/env python3
"""
Generate genome status reports.

This script generates monthly reports on the state of the genome including:
- Total number of features by type
- Named features count
- New features added this month
- Newly named genes
- Deleted features
- Merged features
- Expired gene reservations

The report is emailed to curators and written to HTML files.

Usage:
    python genome_reports.py --strain C_albicans_SC5314

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
from datetime import datetime
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
    """Get current date and date one month ago in YYYY-MM-DD format."""
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")

    # One month ago
    year = now.year
    month = now.month - 1
    if month == 0:
        month = 12
        year -= 1

    previous_date = f"{year}-{month:02d}-{now.day:02d}"
    return current_date, previous_date


class GenomeReporter:
    """Generate genome status reports."""

    def __init__(self, session, strain_abbrev: str):
        self.session = session
        self.strain_abbrev = strain_abbrev
        self.organism_no = None
        self.seq_source = None
        self.current_date, self.previous_date = get_date_range()

    def get_organism_info(self) -> bool:
        """Get organism information from database."""
        query = text(f"""
            SELECT organism_no
            FROM {DB_SCHEMA}.organism
            WHERE organism_abbrev = :strain_abbrev
        """)

        result = self.session.execute(
            query, {"strain_abbrev": self.strain_abbrev}
        ).first()

        if result:
            self.organism_no = result[0]
            return True
        return False

    def get_seq_source(self) -> str | None:
        """Get the default sequence source for the strain."""
        query = text(f"""
            SELECT DISTINCT fl.seq_source
            FROM {DB_SCHEMA}.feat_location fl
            JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
            WHERE f.organism_no = :organism_no
            AND fl.is_loc_current = 'Y'
        """)

        result = self.session.execute(query, {
            "organism_no": self.organism_no
        }).first()

        if result:
            self.seq_source = result[0]
            return self.seq_source
        return None

    def get_feature_counts(self) -> dict[str, int]:
        """Get count of features by type."""
        query = text(f"""
            SELECT f.feature_type, COUNT(*)
            FROM {DB_SCHEMA}.feature f
            WHERE f.organism_no = :organism_no
            AND f.feature_type NOT IN (
                'chromosome', 'contig', 'intron', 'CDS',
                'adjustment', 'noncoding_exon', 'gap', 'allele'
            )
            GROUP BY f.feature_type
            ORDER BY COUNT(*) DESC
        """)

        result = self.session.execute(query, {"organism_no": self.organism_no})
        return {row[0]: row[1] for row in result}

    def get_total_features(self) -> int:
        """Get total count of features."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.feature f
            WHERE f.organism_no = :organism_no
            AND f.feature_type NOT IN (
                'chromosome', 'contig', 'intron', 'CDS',
                'adjustment', 'noncoding_exon', 'gap', 'allele'
            )
        """)

        result = self.session.execute(query, {
            "organism_no": self.organism_no
        }).first()
        return result[0] if result else 0

    def get_named_features(self) -> int:
        """Get count of features with gene names."""
        query = text(f"""
            SELECT COUNT(*)
            FROM {DB_SCHEMA}.feature f
            WHERE f.organism_no = :organism_no
            AND f.gene_name IS NOT NULL
            AND f.feature_type NOT IN (
                'chromosome', 'contig', 'intron', 'CDS',
                'adjustment', 'noncoding_exon', 'gap', 'allele'
            )
        """)

        result = self.session.execute(query, {
            "organism_no": self.organism_no
        }).first()
        return result[0] if result else 0

    def get_recent_features(self) -> list[dict]:
        """Get features added this month."""
        query = text(f"""
            SELECT f.feature_name, f.feature_type, f.gene_name, f.dbxref_id,
                   fl.start_coord, fl.stop_coord, f.headline
            FROM {DB_SCHEMA}.feature f
            LEFT JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
                AND fl.is_loc_current = 'Y'
            WHERE f.organism_no = :organism_no
            AND TO_CHAR(f.date_created, 'YYYY-MM-DD') >= :previous_date
            AND f.feature_type NOT IN (
                'chromosome', 'contig', 'intron', 'CDS',
                'adjustment', 'noncoding_exon', 'gap'
            )
            ORDER BY f.date_created
        """)

        result = self.session.execute(query, {
            "organism_no": self.organism_no,
            "previous_date": self.previous_date,
        })

        return [
            {
                "feature_name": row[0],
                "feature_type": row[1],
                "gene_name": row[2] or "none",
                "dbxref_id": row[3] or "NA",
                "start": row[4] or "NA",
                "stop": row[5] or "NA",
                "headline": row[6] or "NA",
            }
            for row in result
        ]

    def get_newly_named_genes(self) -> list[dict]:
        """Get genes that were named this month."""
        query = text(f"""
            SELECT f.gene_name, f.feature_name, f.headline
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.update_log u ON f.feature_no = u.primary_key
            WHERE f.organism_no = :organism_no
            AND u.tab_name = 'FEATURE'
            AND u.col_name = 'GENE_NAME'
            AND u.old_value IS NULL
            AND u.new_value IS NOT NULL
            AND TO_CHAR(u.date_created, 'YYYY-MM-DD') >= :previous_date
            ORDER BY f.gene_name
        """)

        result = self.session.execute(query, {
            "organism_no": self.organism_no,
            "previous_date": self.previous_date,
        })

        return [
            {
                "gene_name": row[0],
                "feature_name": row[1],
                "headline": row[2] or "none",
            }
            for row in result
        ]

    def get_deleted_features(self) -> dict:
        """Get deleted features statistics."""
        # Total deleted
        query1 = text(f"""
            SELECT COUNT(DISTINCT f.feature_no)
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_property fp ON f.feature_no = fp.feature_no
            WHERE f.organism_no = :organism_no
            AND fp.property_type = 'feature_qualifier'
            AND fp.property_value LIKE 'Deleted%'
        """)

        total_result = self.session.execute(query1, {
            "organism_no": self.organism_no
        }).first()
        total = total_result[0] if total_result else 0

        # Recently deleted
        query2 = text(f"""
            SELECT f.feature_name, fp.property_value, f.dbxref_id, f.headline,
                   fl.start_coord, fl.stop_coord
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_property fp ON f.feature_no = fp.feature_no
            LEFT JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
                AND fl.is_loc_current = 'Y'
            WHERE f.organism_no = :organism_no
            AND fp.property_type = 'feature_qualifier'
            AND fp.property_value LIKE 'Deleted%'
            AND TO_CHAR(fp.date_created, 'YYYY-MM-DD') >= :previous_date
        """)

        result = self.session.execute(query2, {
            "organism_no": self.organism_no,
            "previous_date": self.previous_date,
        })

        recent = [
            {
                "feature_name": row[0],
                "qualifier": row[1],
                "dbxref_id": row[2] or "NA",
                "headline": row[3] or "NA",
                "start": row[4] or "NA",
                "stop": row[5] or "NA",
            }
            for row in result
        ]

        return {"total": total, "recent": recent}

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

    def generate_text_report(self) -> str:
        """Generate plain text report."""
        lines = []
        lines.append(f"{PROJECT_ACRONYM} State of the {self.strain_abbrev} Genome Report")
        lines.append(f"Period: {self.previous_date} to {self.current_date}")
        lines.append("")

        # Feature counts
        lines.append("CHROMOSOMAL FEATURES")
        lines.append("=" * 40)

        total = self.get_total_features()
        named = self.get_named_features()
        lines.append(f"Total number of features: {total}")
        lines.append(f"Total number of named features: {named}")
        lines.append("")

        counts = self.get_feature_counts()
        lines.append("Number of each feature type:")
        for feat_type, count in counts.items():
            lines.append(f"  {feat_type}: {count}")
        lines.append("")

        recent = self.get_recent_features()
        if recent:
            lines.append(f"Features added this month: {len(recent)}")
            for feat in recent[:10]:  # Limit output
                lines.append(
                    f"  {feat['feature_name']} ({feat['feature_type']})"
                )
        else:
            lines.append("No new features were added this month.")
        lines.append("")

        # Newly named genes
        lines.append("NEWLY NAMED FEATURES")
        lines.append("=" * 40)

        new_genes = self.get_newly_named_genes()
        if new_genes:
            lines.append(f"Features named this month: {len(new_genes)}")
            for gene in new_genes[:10]:
                lines.append(f"  {gene['gene_name']} ({gene['feature_name']})")
        else:
            lines.append("No features were given gene names this month.")
        lines.append("")

        # Deleted features
        lines.append("DELETED FEATURES")
        lines.append("=" * 40)

        deleted = self.get_deleted_features()
        lines.append(f"Total deleted features: {deleted['total']}")
        if deleted['recent']:
            lines.append(f"Deleted this month: {len(deleted['recent'])}")
            for feat in deleted['recent'][:10]:
                lines.append(f"  {feat['feature_name']}")
        else:
            lines.append("No features were deleted this month.")
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
                    f"({res['email']})"
                )
        else:
            lines.append("No expired gene name reservations.")

        return "\n".join(lines)

    def generate_html_report(self) -> str:
        """Generate HTML report."""
        html = []
        html.append("<!DOCTYPE html>")
        html.append("<html><head>")
        html.append(f"<title>{PROJECT_ACRONYM} Genome Report - {self.strain_abbrev}</title>")
        html.append("<style>")
        html.append("body { font-family: Arial, sans-serif; margin: 20px; }")
        html.append("h1 { color: #333; }")
        html.append("h2 { color: #666; border-bottom: 1px solid #ccc; }")
        html.append("table { border-collapse: collapse; margin: 10px 0; }")
        html.append("th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }")
        html.append("th { background-color: #f2f2f2; }")
        html.append("pre { background-color: #f5f5f5; padding: 10px; }")
        html.append("</style></head><body>")

        html.append(f"<h1>{PROJECT_ACRONYM} State of the {self.strain_abbrev} Genome Report</h1>")
        html.append(f"<p>Period: {self.previous_date} to {self.current_date}</p>")

        # Table of contents
        html.append("<h2>Contents</h2>")
        html.append("<ul>")
        html.append('<li><a href="#features">All Features</a></li>')
        html.append('<li><a href="#named">Newly Named Features</a></li>')
        html.append('<li><a href="#deleted">Deleted Features</a></li>')
        html.append('<li><a href="#expired">Expired Gene Reservations</a></li>')
        html.append("</ul>")

        # Feature section
        html.append('<h2 id="features">Chromosomal Feature Information</h2>')
        total = self.get_total_features()
        named = self.get_named_features()
        html.append(f"<pre>Total number of features: {total}\n")
        html.append(f"Total number of named features: {named}\n\n")
        html.append("Number of each feature type:\n")
        counts = self.get_feature_counts()
        for feat_type, count in counts.items():
            html.append(f"  {feat_type}: {count}\n")
        html.append("</pre>")

        recent = self.get_recent_features()
        if recent:
            html.append(f"<p><b>Features added this month: {len(recent)}</b></p>")
            html.append("<table>")
            html.append("<tr><th>Feature</th><th>Type</th><th>Gene</th><th>ID</th><th>Start</th><th>Stop</th><th>Description</th></tr>")
            for feat in recent:
                html.append(f"<tr><td>{feat['feature_name']}</td><td>{feat['feature_type']}</td>")
                html.append(f"<td>{feat['gene_name']}</td><td>{feat['dbxref_id']}</td>")
                html.append(f"<td>{feat['start']}</td><td>{feat['stop']}</td><td>{feat['headline']}</td></tr>")
            html.append("</table>")
        else:
            html.append("<p>No new features were added this month.</p>")

        # Newly named genes
        html.append('<h2 id="named">Newly Named Genes</h2>')
        new_genes = self.get_newly_named_genes()
        if new_genes:
            html.append(f"<p><b>Features named this month: {len(new_genes)}</b></p>")
            html.append("<table>")
            html.append("<tr><th>Gene</th><th>Feature</th><th>Description</th></tr>")
            for gene in new_genes:
                html.append(f"<tr><td>{gene['gene_name']}</td><td>{gene['feature_name']}</td><td>{gene['headline']}</td></tr>")
            html.append("</table>")
        else:
            html.append("<p>No features were given gene names this month.</p>")

        # Deleted features
        html.append('<h2 id="deleted">Deleted Features Information</h2>')
        deleted = self.get_deleted_features()
        html.append(f"<pre>Total number of deleted features: {deleted['total']}</pre>")
        if deleted['recent']:
            html.append(f"<p><b>Features deleted this month: {len(deleted['recent'])}</b></p>")
            html.append("<table>")
            html.append("<tr><th>Feature</th><th>Qualifier</th><th>ID</th><th>Start</th><th>Stop</th><th>Description</th></tr>")
            for feat in deleted['recent']:
                html.append(f"<tr><td>{feat['feature_name']}</td><td>{feat['qualifier']}</td>")
                html.append(f"<td>{feat['dbxref_id']}</td><td>{feat['start']}</td><td>{feat['stop']}</td>")
                html.append(f"<td>{feat['headline']}</td></tr>")
            html.append("</table>")
        else:
            html.append("<p>No features were deleted this month.</p>")

        # Expired reservations
        html.append('<h2 id="expired">Expired Gene Name Reservations</h2>')
        expired = self.get_expired_reservations()
        if expired:
            html.append(f"<p><b>Expired reservations: {len(expired)}</b></p>")
            html.append("<table>")
            html.append("<tr><th>Gene Name</th><th>Colleague</th><th>Email</th><th>Date</th></tr>")
            for res in expired:
                html.append(f"<tr><td>{res['gene_name']}</td>")
                html.append(f"<td>{res['first_name']} {res['last_name']}</td>")
                html.append(f"<td>{res['email']}</td><td>{res['expiration_date']}</td></tr>")
            html.append("</table>")
        else:
            html.append("<p>No expired gene name reservations.</p>")

        html.append("</body></html>")
        return "\n".join(html)


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


def generate_genome_report(strain_abbrev: str) -> bool:
    """
    Main function to generate genome report.

    Args:
        strain_abbrev: Strain abbreviation

    Returns:
        True on success, False on failure
    """
    try:
        with SessionLocal() as session:
            reporter = GenomeReporter(session, strain_abbrev)

            if not reporter.get_organism_info():
                logger.error(f"Strain {strain_abbrev} not found")
                return False

            reporter.get_seq_source()

            # Generate reports
            text_report = reporter.generate_text_report()
            html_report = reporter.generate_html_report()

            # Write HTML report
            report_dir = HTML_ROOT_DIR / "staff" / "reports"
            report_dir.mkdir(parents=True, exist_ok=True)

            html_file = report_dir / f"{strain_abbrev}_genomeReport{reporter.current_date}.html"
            with open(html_file, "w") as f:
                f.write(html_report)
            logger.info(f"HTML report written to {html_file}")

            # Append to index file
            index_file = report_dir / f"{strain_abbrev}_genomeReport.html"
            with open(index_file, "a") as f:
                f.write(f'<a href="{html_file.name}">{reporter.current_date} report</a>\n')

            # Send email
            if CURATOR_EMAIL:
                send_email(
                    subject=f"{strain_abbrev} genome report",
                    body=text_report,
                    to_email=CURATOR_EMAIL,
                )

            return True

    except Exception as e:
        logger.exception(f"Error generating genome report: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate genome status reports"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )

    args = parser.parse_args()

    success = generate_genome_report(args.strain)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
