#!/usr/bin/env python3
"""
Check GO synonym transfer details.

This script checks the delete log for deleted GO terms and tracks which
synonyms were transferred when GO terms were merged/obsoleted.

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    LOG_DIR: Directory for log files (default: /tmp)
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
LOG_FILE = LOG_DIR / "load" / "go.synonymTransfer.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def check_synonym_transfers() -> bool:
    """
    Check for GO synonym transfers in today's delete log.

    Returns:
        True on success, False on failure
    """
    # Ensure log directory exists
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Checking GO synonym transfers for {today}")

    try:
        with SessionLocal() as session:
            # Get deleted GO rows from today
            delete_query = text(f"""
                SELECT deleted_row
                FROM {DB_SCHEMA}.delete_log
                WHERE tab_name = 'GO'
                AND TRUNC(date_created) = TO_DATE(:today, 'YYYY-MM-DD')
            """)

            deleted_rows = session.execute(delete_query, {"today": today}).fetchall()

            if not deleted_rows:
                logger.info("No GO deletions found for today")
                with open(LOG_FILE, "w") as f:
                    f.write(f"No GO deletions found for {today}\n")
                return True

            logger.info(f"Found {len(deleted_rows)} deleted GO rows")

            # Prepare query for finding synonym transfers
            gosyn_query = text(f"""
                SELECT g.goid, ggs.go_synonym_no, g.go_term
                FROM {DB_SCHEMA}.go_gosyn ggs
                JOIN {DB_SCHEMA}.go_synonym gs ON ggs.go_synonym_no = gs.go_synonym_no
                JOIN {DB_SCHEMA}.go g ON ggs.go_no = g.go_no
                WHERE gs.go_synonym = :go_term
            """)

            with open(LOG_FILE, "w") as f:
                f.write(f"GO Synonym Transfer Check - {today}\n")
                f.write("=" * 50 + "\n\n")

                for (deleted_row,) in deleted_rows:
                    if not deleted_row:
                        continue

                    # Parse the deleted row (tab-separated: goid, go_term, others...)
                    parts = deleted_row.split("\t")
                    if len(parts) < 2:
                        continue

                    secondary_goid = parts[0]
                    go_term = parts[1]

                    # Find where this term was transferred as a synonym
                    transfers = session.execute(
                        gosyn_query, {"go_term": go_term}
                    ).fetchall()

                    for primary_goid, gosyn_no, primary_term in transfers:
                        message = (
                            f"\nSynonymous goid {secondary_goid} has been deleted. "
                            f"Its go_term = '{go_term}' has been transferred to "
                            f"synonym (go_synonym_no = {gosyn_no}) for primary goid "
                            f"{primary_goid} with go_term = '{primary_term}'\n"
                        )
                        f.write(message)
                        logger.info(message.strip())

        logger.info(f"Log written to {LOG_FILE}")
        return True

    except Exception as e:
        logger.exception(f"Error checking synonym transfers: {e}")
        return False


def main() -> int:
    """Main entry point."""
    success = check_synonym_transfers()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
