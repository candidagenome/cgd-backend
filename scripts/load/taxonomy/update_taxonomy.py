#!/usr/bin/env python3
"""
Update TAXONOMY table from NCBI taxonomy dump.

This script downloads the NCBI taxonomy dump and updates the TAXONOMY table.
It handles:
- Downloading and extracting NCBI taxonomy files
- Checking for deleted or merged taxids
- Inserting new taxonomy entries
- Updating existing entries
- Deleting obsolete entries

Original Perl: updateTaxonomy (by Shuai Weng, June 2003)
Converted to Python: 2024

Usage:
    python update_taxonomy.py --created-by DBUSER [--dry-run]
"""

import argparse
import gzip
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from cgd.db.engine import SessionLocal

load_dotenv()

logger = logging.getLogger(__name__)

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")

# NCBI taxonomy FTP
TAX_FTP_URL = "https://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz"


def setup_logging(verbose: bool = False, log_file: Path = None) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def download_ncbi_taxonomy(data_dir: Path) -> bool:
    """
    Download and extract NCBI taxonomy files.

    Args:
        data_dir: Directory to store taxonomy files

    Returns:
        True on success
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    archive_path = data_dir / "taxdump.tar.gz"

    logger.info(f"Downloading NCBI taxonomy from {TAX_FTP_URL}")

    try:
        urllib.request.urlretrieve(TAX_FTP_URL, archive_path)
        logger.info(f"Downloaded to {archive_path}")

        # Extract
        with tarfile.open(archive_path, "r:gz") as tar:
            tar.extractall(path=data_dir)
        logger.info(f"Extracted to {data_dir}")

        return True

    except Exception as e:
        logger.error(f"Error downloading taxonomy: {e}")
        return False


def delete_unwanted_chars(text: str) -> str:
    """Clean up taxonomy term by removing unwanted characters."""
    if not text:
        return text
    # Remove non-printable characters and normalize whitespace
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
    text = ' '.join(text.split())
    return text.strip()


class TaxonomyUpdater:
    """Update taxonomy data from NCBI."""

    def __init__(self, session: Session, created_by: str):
        self.session = session
        self.created_by = created_by

        # Data from database
        self.term_for_dbid: dict[int, str] = {}
        self.rank_for_dbid: dict[int, str] = {}
        self.common_name_for_dbid: dict[int, str] = {}
        self.dbid_for_term: dict[str, int] = {}

        # Data from NCBI files
        self.term_for_ncbi_id: dict[int, str] = {}
        self.rank_for_ncbi_id: dict[int, str] = {}
        self.parent_for_ncbi_id: dict[int, int] = {}
        self.common_name_for_ncbi_id: dict[int, str] = {}
        self.ncbi_id_for_term: dict[str, int] = {}

        # Bad taxids (terms containing "samples")
        self.bad_tax_terms: set[int] = set()

        # Foreign key tracking
        self.fk_exists: set[int] = set()
        self.organism_taxids: set[int] = set()
        self.pdb_sequence_taxids: set[int] = set()
        self.blast_hit_taxids: set[int] = set()

        # Merged taxid tracking
        self.dbids_for_merged: dict[int, set[int]] = {}
        self.merged_id_for_dbid: dict[int, int] = {}

        # Deleted tracking
        self.dbid_deleted: set[int] = set()

        # Counters
        self.insert_count = 0
        self.update_count = 0
        self.delete_count = 0
        self.dup_term_count = 0

    def check_empty_tables(self) -> bool:
        """Check that TAX_SYNONYM and TAX_RELATIONSHIP are empty."""
        syn_count = self.session.execute(
            text(f"SELECT COUNT(*) FROM {DB_SCHEMA}.tax_synonym")
        ).scalar()

        rel_count = self.session.execute(
            text(f"SELECT COUNT(*) FROM {DB_SCHEMA}.tax_relationship")
        ).scalar()

        if syn_count > 0 or rel_count > 0:
            logger.error(
                f"Tables not empty: TAX_SYNONYM={syn_count}, TAX_RELATIONSHIP={rel_count}"
            )
            return False

        return True

    def retrieve_taxonomy_from_db(self) -> None:
        """Load existing taxonomy data from database."""
        # Get taxonomy entries
        result = self.session.execute(
            text(f"SELECT taxon_id, tax_term, common_name, rank FROM {DB_SCHEMA}.taxonomy")
        )
        for taxid, term, common_name, rank in result:
            self.term_for_dbid[taxid] = term
            self.dbid_for_term[term] = taxid
            if rank:
                self.rank_for_dbid[taxid] = rank
            if common_name:
                self.common_name_for_dbid[taxid] = common_name

        logger.info(f"Loaded {len(self.term_for_dbid)} taxonomy entries from database")

        # Get organism taxids (foreign keys)
        result = self.session.execute(
            text(f"SELECT DISTINCT taxon_id FROM {DB_SCHEMA}.organism WHERE taxon_id IS NOT NULL")
        )
        for (taxid,) in result:
            self.organism_taxids.add(taxid)
            self.fk_exists.add(taxid)

        # Get PDB sequence taxids
        result = self.session.execute(
            text(f"SELECT DISTINCT taxon_id FROM {DB_SCHEMA}.pdb_sequence WHERE taxon_id IS NOT NULL")
        )
        for (taxid,) in result:
            self.pdb_sequence_taxids.add(taxid)
            self.fk_exists.add(taxid)

        # Get BLAST hit taxids
        result = self.session.execute(
            text(f"SELECT DISTINCT taxon_id FROM {DB_SCHEMA}.blast_hit WHERE taxon_id IS NOT NULL")
        )
        for (taxid,) in result:
            self.blast_hit_taxids.add(taxid)
            self.fk_exists.add(taxid)

        logger.info(f"Found {len(self.fk_exists)} taxids with foreign key references")

    def read_ncbi_files(self, data_dir: Path) -> None:
        """Read NCBI taxonomy dump files."""
        nodes_file = data_dir / "nodes.dmp"
        names_file = data_dir / "names.dmp"
        deleted_file = data_dir / "delnodes.dmp"
        merged_file = data_dir / "merged.dmp"

        temp_term_for_taxid: dict[int, str] = {}

        # Read deleted nodes
        if deleted_file.exists():
            with open(deleted_file) as f:
                for line in f:
                    match = re.match(r'^(\d+)', line)
                    if match:
                        taxid = int(match.group(1))
                        self._check_deleted(taxid, self.organism_taxids, "ORGANISM")
                        self._check_deleted(taxid, self.blast_hit_taxids, "BLAST_HIT")
                        self._check_deleted(taxid, self.pdb_sequence_taxids, "PDB_SEQUENCE")

        # Read nodes (for rank and parent)
        with open(nodes_file) as f:
            for line in f:
                parts = line.split('\t|\t')
                if len(parts) >= 3:
                    taxid = int(parts[0])
                    parent_id = int(parts[1])
                    rank = parts[2].strip()

                    self.rank_for_ncbi_id[taxid] = rank
                    if taxid != parent_id:
                        self.parent_for_ncbi_id[taxid] = parent_id

        # Read names
        with open(names_file) as f:
            for line in f:
                parts = line.split('\t|\t')
                if len(parts) >= 4:
                    taxid = int(parts[0])
                    name_txt = parts[1].strip()
                    unique_name = parts[2].strip() if parts[2].strip() else None
                    name_class = parts[3].strip()

                    # Mark bad terms
                    if 'samples' in name_txt.lower():
                        self.bad_tax_terms.add(taxid)

                    # Use unique name if available (except for superkingdom)
                    rank = self.rank_for_ncbi_id.get(taxid, '')
                    if unique_name and 'superkingdom' not in rank.lower():
                        name_txt = unique_name

                    name_txt = delete_unwanted_chars(name_txt)

                    if 'scientific name' in name_class.lower():
                        temp_term_for_taxid[taxid] = name_txt
                    elif 'common name' in name_class.lower():
                        if taxid in self.common_name_for_ncbi_id:
                            self.common_name_for_ncbi_id[taxid] += "|" + name_txt
                        else:
                            self.common_name_for_ncbi_id[taxid] = name_txt

        # Find bad ancestors
        self._find_bad_ancestors()

        # Read merged nodes
        if merged_file.exists():
            with open(merged_file) as f:
                for line in f:
                    match = re.match(r'^(\d+)\s*\|\s*(\d+)', line)
                    if match:
                        old_id = int(match.group(1))
                        new_id = int(match.group(2))
                        self._check_merged(old_id, new_id, self.organism_taxids, "ORGANISM")
                        self._check_merged(old_id, new_id, self.blast_hit_taxids, "BLAST_HIT")
                        self._check_merged(old_id, new_id, self.pdb_sequence_taxids, "PDB_SEQUENCE")

        # Build final term mapping (excluding bad terms)
        for taxid, term in temp_term_for_taxid.items():
            if taxid in self.bad_tax_terms:
                continue

            if term in self.ncbi_id_for_term:
                logger.warning(f"Duplicate term '{term}' for taxids {self.ncbi_id_for_term[term]} and {taxid}")
                self.dup_term_count += 1
                continue

            self.ncbi_id_for_term[term] = taxid
            self.term_for_ncbi_id[taxid] = term

        logger.info(f"Read {len(self.term_for_ncbi_id)} valid taxonomy terms from NCBI")

        # Check foreign keys
        self._check_foreign_keys(self.organism_taxids, "ORGANISM")
        self._check_foreign_keys(self.blast_hit_taxids, "BLAST_HIT")
        self._check_foreign_keys(self.pdb_sequence_taxids, "PDB_SEQUENCE")

    def _check_deleted(self, taxid: int, table_taxids: set[int], table: str) -> None:
        """Check if deleted taxid is used in a table."""
        if taxid in table_taxids:
            raise ValueError(
                f"Taxid {taxid} was deleted from NCBI but is used in table {table}"
            )

    def _check_merged(self, old_id: int, new_id: int, table_taxids: set[int], table: str) -> None:
        """Track merged taxids."""
        if old_id in table_taxids:
            if new_id not in self.dbids_for_merged:
                self.dbids_for_merged[new_id] = set()
            self.dbids_for_merged[new_id].add(old_id)
            self.merged_id_for_dbid[old_id] = new_id
            logger.info(f"Taxid {old_id} in table {table} has been merged into {new_id}")

    def _check_foreign_keys(self, table_taxids: set[int], table: str) -> None:
        """Verify all foreign key taxids exist in NCBI taxonomy."""
        for taxid in table_taxids:
            if taxid not in self.term_for_ncbi_id and taxid not in self.merged_id_for_dbid:
                raise ValueError(
                    f"Taxid {taxid} used in table {table} not found in NCBI taxonomy"
                )

    def _find_bad_ancestors(self) -> None:
        """Mark taxids as bad if any ancestor is bad."""
        for taxid in list(self.parent_for_ncbi_id.keys()):
            if taxid in self.bad_tax_terms:
                continue

            child_id = taxid
            while child_id in self.parent_for_ncbi_id:
                parent_id = self.parent_for_ncbi_id[child_id]
                if parent_id in self.bad_tax_terms:
                    self.bad_tax_terms.add(taxid)
                    break
                child_id = parent_id

        # Check that bad taxids aren't used as foreign keys
        for taxid in self.bad_tax_terms:
            if taxid in self.fk_exists:
                raise ValueError(
                    f"Taxid {taxid} is marked as bad but is used as foreign key"
                )

    def delete_taxonomy(self) -> None:
        """Delete taxonomy entries that need updating."""
        for dbid in self.term_for_dbid:
            if dbid in self.fk_exists:
                continue

            should_delete = (
                dbid not in self.term_for_ncbi_id
                or self.term_for_ncbi_id.get(dbid) != self.term_for_dbid.get(dbid)
                or self.common_name_for_ncbi_id.get(dbid) != self.common_name_for_dbid.get(dbid)
                or self.rank_for_ncbi_id.get(dbid) != self.rank_for_dbid.get(dbid)
            )

            if should_delete:
                self._delete_taxid(dbid)

        self.session.commit()

    def _delete_taxid(self, taxid: int) -> None:
        """Delete a taxonomy entry."""
        try:
            self.session.execute(
                text(f"DELETE FROM {DB_SCHEMA}.taxonomy WHERE taxon_id = :taxid"),
                {"taxid": taxid}
            )
            self.dbid_deleted.add(taxid)
            self.delete_count += 1
            logger.debug(f"Deleted taxid {taxid}: {self.term_for_dbid.get(taxid)}")
        except Exception as e:
            logger.error(f"Error deleting taxid {taxid}: {e}")

    def _insert_taxid(self, taxid: int) -> None:
        """Insert a new taxonomy entry."""
        try:
            self.session.execute(
                text(f"""
                    INSERT INTO {DB_SCHEMA}.taxonomy
                    (taxon_id, tax_term, is_default_display, common_name, rank, created_by)
                    VALUES (:taxid, :term, 'N', :common_name, :rank, :created_by)
                """),
                {
                    "taxid": taxid,
                    "term": self.term_for_ncbi_id[taxid],
                    "common_name": self.common_name_for_ncbi_id.get(taxid),
                    "rank": self.rank_for_ncbi_id.get(taxid),
                    "created_by": self.created_by,
                }
            )
            self.insert_count += 1
            logger.debug(f"Inserted taxid {taxid}: {self.term_for_ncbi_id[taxid]}")
        except Exception as e:
            logger.error(f"Error inserting taxid {taxid}: {e}")

    def update_taxonomy(self) -> None:
        """Update taxonomy table."""
        count = 0

        for taxid in sorted(self.term_for_ncbi_id.keys()):
            inserted = taxid in self.term_for_dbid and taxid not in self.dbid_deleted

            # Update existing entries
            if inserted:
                updates = 0
                try:
                    if self.term_for_dbid.get(taxid) != self.term_for_ncbi_id.get(taxid):
                        new_term = self.term_for_ncbi_id[taxid]
                        alt_dbid = self.dbid_for_term.get(new_term)

                        if not alt_dbid or alt_dbid in self.dbid_deleted:
                            self.session.execute(
                                text(f"UPDATE {DB_SCHEMA}.taxonomy SET tax_term = :term WHERE taxon_id = :taxid"),
                                {"term": new_term, "taxid": taxid}
                            )
                            updates += 1
                        else:
                            logger.warning(f"Skipping update of {taxid}: term {new_term} used by {alt_dbid}")

                    if self.common_name_for_dbid.get(taxid) != self.common_name_for_ncbi_id.get(taxid):
                        self.session.execute(
                            text(f"UPDATE {DB_SCHEMA}.taxonomy SET common_name = :name WHERE taxon_id = :taxid"),
                            {"name": self.common_name_for_ncbi_id.get(taxid), "taxid": taxid}
                        )
                        updates += 1

                    if self.rank_for_dbid.get(taxid) != self.rank_for_ncbi_id.get(taxid):
                        self.session.execute(
                            text(f"UPDATE {DB_SCHEMA}.taxonomy SET rank = :rank WHERE taxon_id = :taxid"),
                            {"rank": self.rank_for_ncbi_id.get(taxid), "taxid": taxid}
                        )
                        updates += 1

                except Exception as e:
                    logger.error(f"Error updating taxid {taxid}: {e}")
                    continue

                if updates > 0:
                    self.update_count += 1

            # Handle merged taxids
            if taxid in self.dbids_for_merged:
                # Give defunct IDs temp terms to avoid unique constraint
                for old_id in self.dbids_for_merged[taxid]:
                    if old_id in self.term_for_dbid:
                        old_term = self.term_for_dbid[old_id]
                        if old_term in self.ncbi_id_for_term:
                            temp_term = f"TempTerm_{old_id}"
                            try:
                                self.session.execute(
                                    text(f"UPDATE {DB_SCHEMA}.taxonomy SET tax_term = :term WHERE taxon_id = :taxid"),
                                    {"term": temp_term, "taxid": old_id}
                                )
                            except Exception as e:
                                logger.error(f"Error updating temp term for {old_id}: {e}")

                # Insert new taxon before updating FKs
                if not inserted:
                    self._insert_taxid(taxid)
                    inserted = True

                # Update foreign keys and delete old taxids
                for old_id in self.dbids_for_merged[taxid]:
                    try:
                        if old_id in self.organism_taxids:
                            self.session.execute(
                                text(f"UPDATE {DB_SCHEMA}.organism SET taxon_id = :new WHERE taxon_id = :old"),
                                {"new": taxid, "old": old_id}
                            )
                        if old_id in self.pdb_sequence_taxids:
                            self.session.execute(
                                text(f"UPDATE {DB_SCHEMA}.pdb_sequence SET taxon_id = :new WHERE taxon_id = :old"),
                                {"new": taxid, "old": old_id}
                            )
                        if old_id in self.blast_hit_taxids:
                            self.session.execute(
                                text(f"UPDATE {DB_SCHEMA}.blast_hit SET taxon_id = :new WHERE taxon_id = :old"),
                                {"new": taxid, "old": old_id}
                            )
                        self._delete_taxid(old_id)
                    except Exception as e:
                        logger.error(f"Error updating merged taxid {old_id} -> {taxid}: {e}")

            # Insert new taxid
            if not inserted:
                self._insert_taxid(taxid)

            count += 1
            if count > 5000:
                self.session.commit()
                count = 0

        self.session.commit()

    def write_summary(self) -> str:
        """Generate summary report."""
        summary = f"""
Taxonomy Update Summary:
  Deleted: {self.delete_count}
  Inserted: {self.insert_count}
  Updated: {self.update_count}
  Bad terms skipped: {len(self.bad_tax_terms)}
  Duplicate terms skipped: {self.dup_term_count}
"""
        return summary


def update_taxonomy(
    created_by: str,
    data_dir: Path = None,
    dry_run: bool = False,
) -> bool:
    """
    Main function to update taxonomy.

    Args:
        created_by: Username for audit
        data_dir: Directory for taxonomy files
        dry_run: If True, don't commit changes

    Returns:
        True on success
    """
    if data_dir is None:
        data_dir = DATA_DIR / "taxonomy"

    # Download taxonomy files
    if not download_ncbi_taxonomy(data_dir):
        return False

    try:
        with SessionLocal() as session:
            updater = TaxonomyUpdater(session, created_by)

            # Check prerequisites
            if not updater.check_empty_tables():
                return False

            # Load data
            updater.retrieve_taxonomy_from_db()
            updater.read_ncbi_files(data_dir)

            # Update
            updater.delete_taxonomy()
            updater.update_taxonomy()

            # Summary
            summary = updater.write_summary()
            logger.info(summary)

            if dry_run:
                session.rollback()
                logger.info("Dry run - changes rolled back")
            else:
                session.commit()
                logger.info("Changes committed")

            return True

    except Exception as e:
        logger.exception(f"Error updating taxonomy: {e}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update TAXONOMY table from NCBI taxonomy dump"
    )
    parser.add_argument(
        "--created-by",
        required=True,
        help="Username for audit trail",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="Directory for taxonomy files",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Log file path",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't commit changes",
    )

    args = parser.parse_args()

    log_file = args.log_file
    if not log_file:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / "updateTaxonomy.log"

    setup_logging(args.verbose, log_file)

    logger.info(f"Started at {datetime.now()}")

    success = update_taxonomy(
        args.created_by,
        args.data_dir,
        args.dry_run,
    )

    logger.info(f"Completed at {datetime.now()}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
