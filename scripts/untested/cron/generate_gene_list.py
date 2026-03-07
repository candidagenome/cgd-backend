#!/usr/bin/env python3
"""
Generate gene list HTML page for search engine indexing.

This script generates an HTML table of all genes/ORFs in the database.
The page is designed to be crawled by search engines like Google to improve
discoverability of gene information.

Environment Variables:
    DATABASE_URL: Database connection URL
    HTML_DIR: Directory to write output HTML file
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
"""

import logging
import os
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
HTML_DIR = Path(os.getenv("HTML_DIR", "/var/www/html"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
OUTPUT_FILE = HTML_DIR / "genelist.shtml"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def escape_html(text: str | None) -> str:
    """Escape HTML special characters."""
    if text is None:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def generate_gene_list() -> bool:
    """
    Generate HTML page with all genes.

    Returns:
        True on success, False on failure
    """
    logger.info("Generating gene list HTML page")

    query = text("""
        SELECT f.feature_name, f.dbxref_id, f.gene_name, f.headline, o.organism_name
        FROM feature f
        JOIN organism o ON f.organism_no = o.organism_no
        WHERE f.feature_type = 'ORF'
        ORDER BY f.feature_name
    """)

    try:
        with SessionLocal() as session:
            result = session.execute(query)
            genes = result.fetchall()

        logger.info(f"Found {len(genes)} genes")

        # Ensure output directory exists
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

        with open(OUTPUT_FILE, "w") as f:
            # Write header with SSI include
            f.write(f"<!--#include virtual='/cgi-bin/htmlHeader.pl?tag={PROJECT_ACRONYM}'-->\n\n")
            f.write("<br>\n")
            f.write("<table border=1 id='paddedtbl'>\n")

            # Write gene rows
            for i, (feature_name, dbxref_id, gene_name, headline, organism_name) in enumerate(genes):
                # Write header row every 20 rows
                if i % 20 == 0:
                    f.write(
                        "<tr bgcolor='#dcdcdc'>"
                        "<th>Locus Id</th>"
                        "<th>Gene Name</th>"
                        "<th>Organism</th>"
                        "<th>Description</th>"
                        "</tr>\n"
                    )

                feature_name = escape_html(feature_name)
                gene_name = escape_html(gene_name) or ""
                headline = escape_html(headline) or ""
                organism_name = escape_html(organism_name) or ""

                f.write(
                    f"<tr>"
                    f"<td><a href='/locus/{feature_name}'>{feature_name}</a></td>"
                    f"<td><a href='/locus/{feature_name}'>{gene_name}</a></td>"
                    f"<td>{organism_name}</td>"
                    f"<td>{headline}</td>"
                    f"</tr>\n"
                )

            f.write("</table>\n")

        logger.info(f"Gene list written to {OUTPUT_FILE}")
        return True

    except Exception as e:
        logger.exception(f"Error generating gene list: {e}")
        return False


def main() -> int:
    """Main entry point."""
    success = generate_gene_list()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
