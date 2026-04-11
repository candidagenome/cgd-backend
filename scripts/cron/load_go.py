#!/usr/bin/env python3
from __future__ import annotations

"""
Load/update GO (Gene Ontology) info from OBO file.

This script downloads the latest gene_ontology.obo file from the GO Consortium
and updates the GO table in the database. It handles:
- New GO entries (insertions)
- Updated GO entries (term, aspect, definition changes)
- Obsolete GO entries (deletions with validation)
- Secondary (alt_id) GO entries (merging with primary)
- GO synonyms (go_synonym and go_gosyn tables)

Safety Checks (before any database updates):
---------------------------------------------------------------------------
1. FILE SIZE CHECK
   - Minimum file size: 35 MB
   - Catches truncated or corrupted downloads
   - Current expected size: ~36.5 MB

2. FILE FORMAT VALIDATION
   - Verifies OBO format header (format-version:)
   - Confirms [Term] entries exist
   - Validates that terms have required fields (id:, namespace:)
   - Checks that 90%+ of sampled terms are well-formed

3. OPERATION THRESHOLDS (checked after processing, before commit)
   - Max deletions: 500 terms OR 5% of existing terms
   - Max insertions: 5,000 new terms
   - Max updates: 50% of existing terms
   - Prevents accidental bulk changes from corrupted data

4. ANNOTATION DEPENDENCY CHECKS
   - Obsolete GOIDs are checked for existing annotations
   - Secondary GOIDs are checked before merging
   - Prevents deletion of GOIDs still in use

If any check fails:
- Changes are rolled back (no database modifications)
- Error notification sent to Slack
- Script exits with non-zero status

On success:
- Summary sent to Slack with counts of new/updated/obsoleted terms
---------------------------------------------------------------------------

Based on loadGo.pl/updateGo by Gavin Sherlock (June 2000)
Rewritten by Shuai Weng (April 2004)

Usage:
    python load_go.py
    python load_go.py --obo /path/to/gene_ontology.obo

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for GO files
    LOG_DIR: Log directory
    ADMIN_USER: Admin username for database operations
    CURATOR_EMAIL: Email for notifications
    SLACK_WEBHOOK_URL: Slack webhook for notifications
"""

import argparse
import json
import logging
import os
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from sqlalchemy import text

# Safety thresholds
FILE_SIZE_MIN_BYTES = 35_000_000  # 35 MB minimum file size
MAX_DELETIONS = 500  # Max GO terms that can be deleted in one run
MAX_DELETIONS_PERCENT = 5.0  # Max percentage of existing terms that can be deleted
MAX_INSERTIONS = 5000  # Max GO terms that can be inserted in one run
MAX_UPDATES_PERCENT = 50.0  # Max percentage of existing terms that can be updated

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", str(PROJECT_ROOT / "data")))
LOG_DIR = Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")

# GO download URL
GO_DOWNLOAD_URL = "https://ontology-build.geneontology.org/go.obo"

# Slack configuration
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def delete_unwanted_char(text_value: str | None) -> str | None:
    """Remove unwanted characters from text."""
    if not text_value:
        return text_value
    # Remove backslash-escaped characters and normalize whitespace
    text_value = text_value.replace("\\n", " ")
    text_value = text_value.replace("\\", "")
    return text_value


def send_slack_message(message: str, is_error: bool = False) -> bool:
    """
    Send a message to Slack.

    Args:
        message: The message to send
        is_error: Whether this is an error message (affects emoji)

    Returns:
        True on success, False on failure
    """
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping Slack notification")
        return False

    env_label = "PROD" if ENV_STATE in ("prod", "production") else "DEV"
    emoji = ":x:" if is_error else ":white_check_mark:"

    slack_message = {
        "text": f"{emoji} *GO Ontology Load ({env_label})*\n{message}"
    }

    try:
        data = json.dumps(slack_message).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as response:
            return response.status == 200
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")
        return False


def validate_obo_file(obo_file: Path) -> tuple[bool, str]:
    """
    Validate the OBO file format and size.

    Args:
        obo_file: Path to the OBO file

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check file size
    file_size = obo_file.stat().st_size
    if file_size < FILE_SIZE_MIN_BYTES:
        size_mb = file_size / 1_000_000
        min_mb = FILE_SIZE_MIN_BYTES / 1_000_000
        return False, (
            f"File size too small: {size_mb:.1f} MB (minimum: {min_mb:.0f} MB). "
            f"The file may be truncated or corrupted."
        )

    # Read file content for format validation
    try:
        with open(obo_file, "r", encoding="utf-8") as f:
            content = f.read(100000)  # Read first 100KB for header check
    except Exception as e:
        return False, f"Failed to read OBO file: {e}"

    # Check for OBO format header
    if "format-version:" not in content[:1000]:
        return False, (
            "Invalid OBO file format: missing 'format-version:' header. "
            "The file format may have changed."
        )

    # Check for [Term] entries
    if "[Term]" not in content:
        return False, (
            "Invalid OBO file format: no [Term] entries found. "
            "The file format may have changed."
        )

    # Read more of the file to validate term structure
    with open(obo_file, "r", encoding="utf-8") as f:
        term_count = 0
        valid_terms = 0
        in_term = False
        has_id = False
        has_namespace = False

        for line in f:
            line = line.strip()

            if line == "[Term]":
                # Check previous term validity
                if in_term and has_id and has_namespace:
                    valid_terms += 1
                term_count += 1
                in_term = True
                has_id = False
                has_namespace = False
                continue

            if line == "[Typedef]":
                # Check last term
                if in_term and has_id and has_namespace:
                    valid_terms += 1
                break

            if in_term:
                if line.startswith("id:"):
                    has_id = True
                elif line.startswith("namespace:"):
                    has_namespace = True

            # Only check first 1000 terms for performance
            if term_count >= 1000:
                break

    if term_count == 0:
        return False, (
            "Invalid OBO file format: no [Term] entries could be parsed. "
            "The file format may have changed."
        )

    # Check that most terms have required fields
    validity_ratio = valid_terms / term_count if term_count > 0 else 0
    if validity_ratio < 0.9:
        return False, (
            f"Invalid OBO file format: only {valid_terms}/{term_count} terms "
            f"have required fields (id, namespace). The file format may have changed."
        )

    logger.info(
        f"OBO file validation passed: {file_size / 1_000_000:.1f} MB, "
        f"{valid_terms}/{term_count} sampled terms valid"
    )
    return True, ""


class GOLoader:
    """Load/update GO information from OBO file."""

    def __init__(self, session):
        self.session = session

        # Data caches from database
        self.go_info_db: dict[str, dict] = {}  # goid -> {go_no, term, aspect, definition}
        self.goid_for_term: dict[str, str] = {}  # term -> goid

        # Counters
        self.go_insert_count = 0
        self.go_update_count = 0
        self.go_delete_count = 0
        self.synonym_insert_count = 0
        self.synonym_delete_count = 0
        self.go_gosyn_insert_count = 0
        self.go_gosyn_delete_count = 0

        # Error tracking
        self.oracle_err = 0
        self.obsolete_err = 0
        self.secondary_err = 0
        self.check_term_err = 0

        # Error messages
        self.error_messages: list[str] = []

    def populate_hashes(self) -> None:
        """
        Retrieve data from GO and go_synonym tables.

        Populates go_info_db and goid_for_term dictionaries.
        """
        query = text(f"""
            SELECT go_no, goid, go_term, go_aspect, go_definition
            FROM {DB_SCHEMA}.go
        """)

        result = self.session.execute(query)

        for row in result:
            go_no, goid, go_term, go_aspect, go_def = row
            goid_str = str(goid)

            self.go_info_db[goid_str] = {
                "go_no": go_no,
                "term": go_term,
                "aspect": go_aspect,
                "definition": go_def,
            }

            if go_term:
                self.goid_for_term[go_term] = goid_str

        logger.info(f"Loaded {len(self.go_info_db)} GO entries from database")

    def process_obo_file(self, obo_file: Path) -> None:
        """
        Process the OBO ontology file.

        Args:
            obo_file: Path to gene_ontology.obo file
        """
        logger.info(f"Processing OBO file: {obo_file}")

        with open(obo_file, "r", encoding="utf-8") as f:
            entry = ""
            term_start = False

            for line in f:
                if line.strip() == "[Term]":
                    term_start = True
                    if entry:
                        self._process_entry(entry)
                    entry = ""
                    continue

                if line.strip() == "[Typedef]":
                    # End of terms section
                    if entry:
                        self._process_entry(entry)
                    break

                if term_start:
                    entry += line

            # Process last entry
            if entry:
                self._process_entry(entry)

    def _process_entry(self, entry: str) -> None:
        """Process a single OBO term entry."""
        goid = None
        term = None
        aspect = None
        definition = None
        synonyms: list[str] = []
        secondary_goids: list[str] = []
        is_obsolete = False

        for line in entry.split("\n"):
            line = line.strip()

            # Get GO ID
            match = re.match(r"^id:\s*GO:0*(\d+)$", line, re.IGNORECASE)
            if match:
                goid = match.group(1)
                continue

            # Get name/term
            match = re.match(r"^name:\s*(.+)$", line)
            if match and not term:
                term = match.group(1)
                term = term.replace("\\n", " ")
                term = term.replace("\\", "")
                continue

            # Get alt_id (secondary GOID)
            match = re.match(r"^alt_id:\s*GO:0*(\d+)$", line, re.IGNORECASE)
            if match:
                secondary_goids.append(match.group(1))
                continue

            # Get namespace/aspect
            match = re.match(
                r"^namespace:\s*(biological|molecular|cellular)[_ ]([pfc]).*$",
                line, re.IGNORECASE
            )
            if match:
                aspect = match.group(2).upper()
                continue

            # Get definition
            match = re.match(r'^def:\s*"(.+)"\s*\[.*\]$', line)
            if match:
                definition = match.group(1)
                definition = definition.replace("\\n", " ")
                definition = definition.replace("\\", "")
                continue

            # Get synonyms (various types)
            match = re.match(r'^[\w_]*synonym:\s*"(.+)"\s*\[.*\]$', line)
            if match:
                synonym = match.group(1)
                synonym = synonym.replace("\\n", " ")
                synonym = synonym.replace("\\", "")
                synonyms.append(synonym)
                continue

            # Check if obsolete
            if line.startswith("is_obsolete:") and "true" in line.lower():
                is_obsolete = True
                continue

        # Skip Gene_ontology root term
        if term and term.lower().startswith("gene_ontology"):
            return

        if not goid:
            return

        if not aspect:
            self.error_messages.append(
                f"There is no namespace (aspect) associated with goid = {goid}"
            )
            self.oracle_err += 1
            return

        if is_obsolete:
            self._check_and_delete_obsolete_goid(goid)
            return

        # Clean up text fields
        term = delete_unwanted_char(term)
        definition = delete_unwanted_char(definition)

        # Check if new entry
        if goid not in self.go_info_db or not self.go_info_db[goid].get("term"):
            go_no = self._insert_new_go_entry(goid, term, aspect, definition)

            if go_no:
                for synonym in synonyms:
                    self._insert_go_synonym(go_no, synonym)

            # Handle secondary GOIDs
            self._check_and_delete_secondary_goid(goid, secondary_goids)
            return

        # Check for updates
        db_entry = self.go_info_db[goid]
        if (db_entry.get("term") != term or
                db_entry.get("aspect") != aspect or
                db_entry.get("definition") != definition):
            self._update_go_entry(goid, term, aspect, definition)

        # Update synonyms
        self._update_synonyms(goid, synonyms)

        # Handle secondary GOIDs
        self._check_and_delete_secondary_goid(goid, secondary_goids)

    def _check_and_delete_obsolete_goid(self, goid: str) -> None:
        """Check and delete an obsolete GOID."""
        if goid not in self.go_info_db:
            return

        type_str = "Obsolete"
        before_err = self.obsolete_err

        # Check if obsolete goid is still used
        self._check_go_annotation(goid, type_str)
        self._check_go_set(goid, type_str)
        self._check_go_ref_support(goid, type_str)

        if self.obsolete_err > before_err:
            return

        # Delete the GO entry and associated data
        self._delete_go_entry(goid, type_str)

    def _check_and_delete_secondary_goid(
        self, primary_goid: str, secondary_goids: list[str]
    ) -> None:
        """Check and delete secondary GOIDs, transferring terms to synonyms."""
        type_str = "Synonymous"
        before_err = self.secondary_err

        for secondary_goid in secondary_goids:
            if secondary_goid not in self.go_info_db:
                continue

            # Check if secondary goid is still used
            self._check_go_annotation(secondary_goid, type_str)
            self._check_go_set(secondary_goid, type_str)
            self._check_go_ref_support(secondary_goid, type_str)

            if self.secondary_err > before_err:
                continue

            # Transfer term to synonym for primary GOID
            primary_go_no = self.go_info_db.get(primary_goid, {}).get("go_no")
            secondary_term = self.go_info_db.get(secondary_goid, {}).get("term")

            if primary_go_no and secondary_term:
                self._insert_go_synonym(primary_go_no, secondary_term)

            # Delete the secondary GO entry
            self._delete_go_entry(secondary_goid, type_str)

    def _delete_go_entry(self, goid: str, type_str: str) -> None:
        """Delete a GO entry and associated data."""
        go_no = self.go_info_db.get(goid, {}).get("go_no")
        if not go_no:
            return

        # Delete from go_path
        try:
            delete_path = text(f"""
                DELETE FROM {DB_SCHEMA}.go_path
                WHERE child_go_no = :go_no OR ancestor_go_no = :go_no
            """)
            self.session.execute(delete_path, {"go_no": go_no})
        except Exception as e:
            self.error_messages.append(
                f"Error deleting go_path for goid={goid} (go_no={go_no}): {e}"
            )
            self.oracle_err += 1
            return

        # Delete from go table
        try:
            delete_go = text(f"""
                DELETE FROM {DB_SCHEMA}.go WHERE go_no = :go_no
            """)
            self.session.execute(delete_go, {"go_no": go_no})
            self.go_delete_count += 1
            logger.info(f"Deletion: GOID {goid} from go table")
        except Exception as e:
            self.error_messages.append(
                f"Error deleting {type_str.lower()} GOID {goid} from go table: {e}"
            )
            self.oracle_err += 1
            return

        # Delete associated synonyms
        get_synonyms = text(f"""
            SELECT gs.go_synonym
            FROM {DB_SCHEMA}.go g
            JOIN {DB_SCHEMA}.go_gosyn gg ON g.go_no = gg.go_no
            JOIN {DB_SCHEMA}.go_synonym gs ON gg.go_synonym_no = gs.go_synonym_no
            WHERE g.goid = :goid
        """)

        result = self.session.execute(get_synonyms, {"goid": goid})
        for row in result:
            synonym = row[0]
            if synonym:
                self._delete_go_synonym(synonym)

        # Delete from dbxref table
        self._delete_goid_from_dbxref(goid, type_str)

    def _delete_goid_from_dbxref(self, goid: str, type_str: str) -> None:
        """Delete GOID from dbxref table."""
        try:
            delete_dbxref = text(f"""
                DELETE FROM {DB_SCHEMA}.dbxref
                WHERE source = 'GO Consortium'
                AND dbxref_type = 'GOID'
                AND dbxref_id = :goid
            """)
            self.session.execute(delete_dbxref, {"goid": goid})
            logger.info(f"Deletion: GOID {goid} from dbxref table")
        except Exception as e:
            self.error_messages.append(
                f"Error deleting {type_str} GOID {goid} from dbxref table: {e}"
            )
            self.oracle_err += 1

    def _insert_new_go_entry(
        self, goid: str, term: str, aspect: str, definition: str | None
    ) -> int | None:
        """Insert a new GO entry."""
        if self._is_term_in_use(goid, term, aspect):
            return None

        try:
            # Get next go_no
            get_max = text(f"SELECT MAX(go_no) FROM {DB_SCHEMA}.go")
            result = self.session.execute(get_max).scalar()
            new_go_no = (result or 0) + 1

            insert_go = text(f"""
                INSERT INTO {DB_SCHEMA}.go
                (go_no, goid, go_term, go_aspect, go_definition, created_by)
                VALUES (:go_no, :goid, :term, :aspect, :definition, :user)
            """)

            self.session.execute(insert_go, {
                "go_no": new_go_no,
                "goid": goid,
                "term": term,
                "aspect": aspect,
                "definition": definition,
                "user": ADMIN_USER,
            })

            self.go_insert_count += 1
            logger.info(
                f"Insertion: GOID {goid}, TERM: {term}, ASPECT: {aspect}"
            )

            # Update cache
            self.go_info_db[goid] = {
                "go_no": new_go_no,
                "term": term,
                "aspect": aspect,
                "definition": definition,
            }
            self.goid_for_term[term] = goid

            return new_go_no

        except Exception as e:
            self.error_messages.append(
                f"Error inserting GOID {goid}: {e}"
            )
            self.oracle_err += 1
            return None

    def _update_go_entry(
        self, goid: str, term: str, aspect: str, definition: str | None
    ) -> None:
        """Update an existing GO entry."""
        if self._is_term_in_use(goid, term, aspect):
            return

        db_entry = self.go_info_db[goid]
        updates = []
        params = {"goid": goid}
        messages = []

        if term != db_entry.get("term"):
            updates.append("go_term = :term")
            params["term"] = term
            messages.append(f'TERM: "{db_entry.get("term")}" to "{term}"')

        if aspect != db_entry.get("aspect"):
            updates.append("go_aspect = :aspect")
            params["aspect"] = aspect
            messages.append(f'ASPECT: "{db_entry.get("aspect")}" to "{aspect}"')

        if definition != db_entry.get("definition"):
            updates.append("go_definition = :definition")
            params["definition"] = definition
            messages.append(f'DEFINITION updated')

        if not updates:
            return

        try:
            update_sql = text(f"""
                UPDATE {DB_SCHEMA}.go
                SET {', '.join(updates)}
                WHERE goid = :goid
            """)

            self.session.execute(update_sql, params)
            self.go_update_count += 1

            message = f"Update GOID {goid}: " + ", ".join(messages)
            logger.info(message)

            # Update cache
            if "term" in params:
                self.go_info_db[goid]["term"] = params["term"]
                self.goid_for_term[params["term"]] = goid
            if "aspect" in params:
                self.go_info_db[goid]["aspect"] = params["aspect"]
            if "definition" in params:
                self.go_info_db[goid]["definition"] = params["definition"]

        except Exception as e:
            self.error_messages.append(
                f"Error updating GOID {goid}: {e}"
            )
            self.oracle_err += 1

    def _update_synonyms(self, goid: str, new_synonyms: list[str]) -> None:
        """Update synonyms for a GO entry."""
        go_no = self.go_info_db.get(goid, {}).get("go_no")
        if not go_no:
            return

        # Get existing synonyms from database
        get_synonyms = text(f"""
            SELECT gs.go_synonym
            FROM {DB_SCHEMA}.go g
            JOIN {DB_SCHEMA}.go_gosyn gg ON g.go_no = gg.go_no
            JOIN {DB_SCHEMA}.go_synonym gs ON gg.go_synonym_no = gs.go_synonym_no
            WHERE g.goid = :goid
        """)

        result = self.session.execute(get_synonyms, {"goid": goid})
        db_synonyms = {row[0] for row in result if row[0]}

        new_synonym_set = {s for s in new_synonyms if s}

        # Insert new synonyms
        for synonym in new_synonym_set:
            if synonym not in db_synonyms:
                self._insert_go_synonym(go_no, synonym)

        # Delete old synonyms
        for synonym in db_synonyms:
            if synonym not in new_synonym_set:
                self._delete_go_synonym(synonym, go_no)

    def _insert_go_synonym(self, go_no: int, synonym: str) -> None:
        """Insert a GO synonym."""
        if not synonym:
            return

        synonym_no = self._get_go_synonym_no(synonym)

        if not synonym_no:
            # Insert new synonym
            try:
                get_max = text(f"SELECT MAX(go_synonym_no) FROM {DB_SCHEMA}.go_synonym")
                result = self.session.execute(get_max).scalar()
                synonym_no = (result or 0) + 1

                insert_syn = text(f"""
                    INSERT INTO {DB_SCHEMA}.go_synonym
                    (go_synonym_no, go_synonym, created_by)
                    VALUES (:syn_no, :synonym, :user)
                """)

                self.session.execute(insert_syn, {
                    "syn_no": synonym_no,
                    "synonym": synonym,
                    "user": ADMIN_USER,
                })

                self.synonym_insert_count += 1
                logger.info(f'Insertion: "{synonym}" into go_synonym table')

            except Exception as e:
                self.error_messages.append(
                    f"Error inserting synonym ({synonym}): {e}"
                )
                self.oracle_err += 1
                return

        # Insert into go_gosyn table
        self._insert_go_gosyn(go_no, synonym_no)

    def _delete_go_synonym(self, synonym: str, go_no: int | None = None) -> None:
        """Delete a GO synonym."""
        synonym_no = self._get_go_synonym_no(synonym)
        if not synonym_no:
            return

        if go_no:
            self._delete_go_gosyn(go_no, synonym_no)

        # Check if synonym is still in use
        if self._is_synonym_in_use(synonym_no):
            return

        try:
            delete_syn = text(f"""
                DELETE FROM {DB_SCHEMA}.go_synonym
                WHERE go_synonym = :synonym
            """)
            self.session.execute(delete_syn, {"synonym": synonym})
            self.synonym_delete_count += 1
            logger.info(f'Deletion: go_synonym ({synonym}) from go_synonym table')

        except Exception as e:
            self.error_messages.append(
                f"Error deleting go_synonym ({synonym}): {e}"
            )
            self.oracle_err += 1

    def _insert_go_gosyn(self, go_no: int, synonym_no: int) -> None:
        """Insert an entry into go_gosyn table."""
        if go_no <= 0 or synonym_no <= 0:
            return

        # Check if already exists
        check_sql = text(f"""
            SELECT go_no FROM {DB_SCHEMA}.go_gosyn
            WHERE go_no = :go_no AND go_synonym_no = :syn_no
        """)
        result = self.session.execute(
            check_sql, {"go_no": go_no, "syn_no": synonym_no}
        ).first()

        if result:
            return

        try:
            insert_sql = text(f"""
                INSERT INTO {DB_SCHEMA}.go_gosyn (go_no, go_synonym_no)
                VALUES (:go_no, :syn_no)
            """)
            self.session.execute(insert_sql, {"go_no": go_no, "syn_no": synonym_no})
            self.go_gosyn_insert_count += 1
            logger.info(
                f"Insertion: go_no={go_no}, go_synonym_no={synonym_no} into go_gosyn"
            )

        except Exception as e:
            self.error_messages.append(
                f"Error inserting go_gosyn (go_no={go_no}, syn_no={synonym_no}): {e}"
            )
            self.oracle_err += 1

    def _delete_go_gosyn(self, go_no: int, synonym_no: int) -> None:
        """Delete an entry from go_gosyn table."""
        try:
            delete_sql = text(f"""
                DELETE FROM {DB_SCHEMA}.go_gosyn
                WHERE go_no = :go_no AND go_synonym_no = :syn_no
            """)
            self.session.execute(delete_sql, {"go_no": go_no, "syn_no": synonym_no})
            self.go_gosyn_delete_count += 1
            logger.info(
                f"Deletion: go_no={go_no}, go_synonym_no={synonym_no} from go_gosyn"
            )

        except Exception as e:
            self.error_messages.append(
                f"Error deleting go_gosyn (go_no={go_no}, syn_no={synonym_no}): {e}"
            )
            self.oracle_err += 1

    def _get_go_synonym_no(self, synonym: str) -> int | None:
        """Get go_synonym_no for a synonym."""
        query = text(f"""
            SELECT go_synonym_no FROM {DB_SCHEMA}.go_synonym
            WHERE go_synonym = :synonym
        """)
        result = self.session.execute(query, {"synonym": synonym}).first()
        return result[0] if result else None

    def _check_go_annotation(self, goid: str, type_str: str) -> None:
        """Check if GOID is still used in go_annotation."""
        go_no = self.go_info_db.get(goid, {}).get("go_no")
        if not go_no:
            return

        query = text(f"""
            SELECT f.feature_no, f.feature_name, f.gene_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.go_annotation ga ON ga.feature_no = f.feature_no
            WHERE ga.go_no = :go_no
        """)

        result = self.session.execute(query, {"go_no": go_no})

        for row in result:
            feat_no, feat_name, gene_name = row
            self.error_messages.append(
                f"{type_str} GOID ({goid}) is still associated with "
                f"feature_no={feat_no} (feature_name='{feat_name}', "
                f"gene_name='{gene_name}') in go_annotation table."
            )
            if type_str == "Obsolete":
                self.obsolete_err += 1
            else:
                self.secondary_err += 1

    def _check_go_set(self, goid: str, type_str: str) -> None:
        """Check if GOID is still used in go_set."""
        go_no = self.go_info_db.get(goid, {}).get("go_no")
        if not go_no:
            return

        query = text(f"""
            SELECT go_set_name FROM {DB_SCHEMA}.go_set
            WHERE go_no = :go_no
        """)

        result = self.session.execute(query, {"go_no": go_no})

        for row in result:
            go_set_name = row[0]
            self.error_messages.append(
                f"{type_str} GOID ({goid}) is still associated with "
                f"go_set_name='{go_set_name}' in go_set table."
            )
            if type_str == "Obsolete":
                self.obsolete_err += 1
            else:
                self.secondary_err += 1

    def _check_go_ref_support(self, goid: str, type_str: str) -> None:
        """Check if GOID is still used in go_ref support evidence."""
        query = text(f"""
            SELECT DISTINCT f.feature_no, f.feature_name, f.gene_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.go_annotation ga ON ga.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.go_ref gr ON gr.go_annotation_no = ga.go_annotation_no
            JOIN {DB_SCHEMA}.goref_dbxref gd ON gd.go_ref_no = gr.go_ref_no
            JOIN {DB_SCHEMA}.dbxref d ON d.dbxref_no = gd.dbxref_no
            WHERE d.dbxref_id = :goid
            AND d.dbxref_type = 'GOID'
        """)

        result = self.session.execute(query, {"goid": goid})

        for row in result:
            feat_no, feat_name, gene_name = row
            self.error_messages.append(
                f"{type_str} GOID ({goid}) is still associated with go_ref "
                f"support evidence for feature_no={feat_no} "
                f"(feature_name='{feat_name}', gene_name='{gene_name}')."
            )
            if type_str == "Obsolete":
                self.obsolete_err += 1
            else:
                self.secondary_err += 1

    def _is_term_in_use(self, goid: str, term: str, aspect: str) -> bool:
        """Check if term is used by another GOID in the same aspect."""
        existing_goid = self.goid_for_term.get(term)

        if existing_goid and existing_goid != goid:
            existing_aspect = self.go_info_db.get(existing_goid, {}).get("aspect")
            if existing_aspect == aspect:
                self.error_messages.append(
                    f"Another GOID ({existing_goid}) exists for TERM: {term}, "
                    f"ASPECT: {aspect}, for which you are trying to assign GOID {goid}"
                )
                self.check_term_err += 1
                return True

        return False

    def _is_synonym_in_use(self, synonym_no: int) -> bool:
        """Check if synonym is still used by any GO entry."""
        query = text(f"""
            SELECT go_no FROM {DB_SCHEMA}.go_gosyn
            WHERE go_synonym_no = :syn_no
        """)
        result = self.session.execute(query, {"syn_no": synonym_no}).first()
        return result is not None

    def get_summary(self) -> dict:
        """Get summary of operations."""
        return {
            "go_inserts": self.go_insert_count,
            "go_updates": self.go_update_count,
            "go_deletes": self.go_delete_count,
            "synonym_inserts": self.synonym_insert_count,
            "synonym_deletes": self.synonym_delete_count,
            "go_gosyn_inserts": self.go_gosyn_insert_count,
            "go_gosyn_deletes": self.go_gosyn_delete_count,
            "errors": len(self.error_messages),
            "oracle_errors": self.oracle_err,
            "obsolete_errors": self.obsolete_err,
            "secondary_errors": self.secondary_err,
            "term_errors": self.check_term_err,
            "existing_terms": len(self.go_info_db),
        }

    def check_operation_thresholds(self) -> tuple[bool, str]:
        """
        Check if operations exceed safety thresholds.

        Returns:
            Tuple of (is_safe, error_message)
        """
        existing_count = len(self.go_info_db)
        if existing_count == 0:
            # First load, skip threshold checks
            return True, ""

        issues = []

        # Check deletion thresholds
        if self.go_delete_count > MAX_DELETIONS:
            issues.append(
                f"Too many deletions: {self.go_delete_count} "
                f"(max allowed: {MAX_DELETIONS})"
            )

        delete_percent = (self.go_delete_count / existing_count) * 100
        if delete_percent > MAX_DELETIONS_PERCENT:
            issues.append(
                f"Deletion percentage too high: {delete_percent:.1f}% "
                f"(max allowed: {MAX_DELETIONS_PERCENT}%)"
            )

        # Check insertion threshold
        if self.go_insert_count > MAX_INSERTIONS:
            issues.append(
                f"Too many insertions: {self.go_insert_count} "
                f"(max allowed: {MAX_INSERTIONS})"
            )

        # Check update threshold
        update_percent = (self.go_update_count / existing_count) * 100
        if update_percent > MAX_UPDATES_PERCENT:
            issues.append(
                f"Update percentage too high: {update_percent:.1f}% "
                f"(max allowed: {MAX_UPDATES_PERCENT}%)"
            )

        if issues:
            return False, " | ".join(issues)

        return True, ""


def download_go_file(output_path: Path) -> bool:
    """
    Download the latest GO ontology file.

    Args:
        output_path: Path to save the downloaded file

    Returns:
        True on success, False on failure
    """
    logger.info(f"Downloading {GO_DOWNLOAD_URL} to {output_path}")

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        response = requests.get(GO_DOWNLOAD_URL, timeout=300)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            f.write(response.content)

        logger.info(f"Downloaded GO ontology file ({len(response.content)} bytes)")
        return True

    except Exception as e:
        logger.error(f"Error downloading GO file: {e}")
        return False


def load_go(obo_file: Path | None = None, download: bool = True) -> bool:
    """
    Main function to load/update GO information.

    Args:
        obo_file: Path to OBO file (default: DATA_DIR/GO/gene_ontology.obo)
        download: Whether to download the latest file

    Returns:
        True on success, False on failure
    """
    if obo_file is None:
        obo_file = DATA_DIR / "GO" / "gene_ontology.obo"

    # Set up logging to file
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "load" / "go.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    error_log = LOG_DIR / "load" / "go_error.log"
    error_handler = logging.FileHandler(error_log, mode="w")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(error_handler)

    logger.info("*" * 40)
    logger.info(f"Started at {datetime.now()}")

    # Download latest file if requested
    if download:
        if not download_go_file(obo_file):
            error_msg = "Failed to download GO file"
            logger.error(error_msg)
            send_slack_message(error_msg, is_error=True)
            return False

    if not obo_file.exists():
        error_msg = f"OBO file not found: {obo_file}"
        logger.error(error_msg)
        send_slack_message(error_msg, is_error=True)
        return False

    # Validate OBO file format and size
    is_valid, validation_error = validate_obo_file(obo_file)
    if not is_valid:
        logger.error(f"OBO file validation failed: {validation_error}")
        send_slack_message(
            f"*OBO file validation failed*\n{validation_error}\n\n"
            "The GO load was aborted. Please check the OBO file manually.",
            is_error=True
        )
        return False

    try:
        with SessionLocal() as session:
            loader = GOLoader(session)

            # Load existing data from database
            loader.populate_hashes()

            # Process OBO file
            loader.process_obo_file(obo_file)

            # Check for database errors
            if loader.oracle_err > 0:
                logger.error("Oracle errors occurred, rolling back")
                session.rollback()

                # Log error messages
                for msg in loader.error_messages:
                    logger.error(msg)

                error_summary = (
                    f"*Database errors occurred - changes rolled back*\n"
                    f"• Oracle errors: {loader.oracle_err}\n"
                    f"• First error: {loader.error_messages[0] if loader.error_messages else 'Unknown'}"
                )
                send_slack_message(error_summary, is_error=True)
                return False

            # Check operation thresholds before committing
            is_safe, threshold_error = loader.check_operation_thresholds()
            if not is_safe:
                logger.error(f"Operation thresholds exceeded: {threshold_error}")
                session.rollback()

                summary = loader.get_summary()
                error_summary = (
                    f"*Operation thresholds exceeded - changes rolled back*\n"
                    f"{threshold_error}\n\n"
                    f"*Attempted operations:*\n"
                    f"• New GO terms: {summary['go_inserts']}\n"
                    f"• Updated GO terms: {summary['go_updates']}\n"
                    f"• Obsoleted GO terms: {summary['go_deletes']}\n"
                    f"• Existing terms in DB: {summary['existing_terms']}\n\n"
                    "Please review the OBO file and database manually."
                )
                send_slack_message(error_summary, is_error=True)
                return False

            # Commit changes
            session.commit()

            # Log summary
            summary = loader.get_summary()
            logger.info("\n" + "=" * 40)
            logger.info("Summary:")
            logger.info(f"  Existing GO terms in database: {summary['existing_terms']}")
            if summary["go_inserts"] > 0:
                logger.info(f"  {summary['go_inserts']} GO entries inserted (new)")
            if summary["go_updates"] > 0:
                logger.info(f"  {summary['go_updates']} GO entries updated")
            if summary["go_deletes"] > 0:
                logger.info(f"  {summary['go_deletes']} GO entries deleted (obsoleted)")
            if summary["synonym_inserts"] > 0:
                logger.info(f"  {summary['synonym_inserts']} synonyms inserted")
            if summary["synonym_deletes"] > 0:
                logger.info(f"  {summary['synonym_deletes']} synonyms deleted")
            if summary["go_gosyn_inserts"] > 0:
                logger.info(f"  {summary['go_gosyn_inserts']} go_gosyn entries inserted")
            if summary["go_gosyn_deletes"] > 0:
                logger.info(f"  {summary['go_gosyn_deletes']} go_gosyn entries deleted")

            # Log annotation errors for curator review
            if loader.error_messages and loader.oracle_err == 0:
                logger.warning("\nAnnotation errors for curator review:")
                for msg in loader.error_messages:
                    logger.warning(f"  {msg}")

            logger.info(f"Ended at {datetime.now()}")

            # Send success summary to Slack
            slack_summary = (
                f"*GO Ontology load completed successfully*\n\n"
                f"*Summary:*\n"
                f"• New GO terms added: {summary['go_inserts']}\n"
                f"• GO terms updated: {summary['go_updates']}\n"
                f"• GO terms obsoleted: {summary['go_deletes']}\n"
                f"• Existing terms in DB: {summary['existing_terms']}"
            )
            if loader.error_messages:
                slack_summary += (
                    f"\n\n_Note: {len(loader.error_messages)} annotation "
                    f"warning(s) for curator review (see logs)_"
                )
            send_slack_message(slack_summary, is_error=False)

            return True

    except Exception as e:
        logger.exception(f"Error loading GO: {e}")
        send_slack_message(
            f"*Unexpected error during GO load*\n```{str(e)[:500]}```",
            is_error=True
        )
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load/update GO info from OBO ontology file"
    )
    parser.add_argument(
        "--obo",
        type=Path,
        help="Path to gene_ontology.obo file",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Skip downloading the latest OBO file",
    )

    args = parser.parse_args()

    success = load_go(args.obo, download=not args.no_download)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
