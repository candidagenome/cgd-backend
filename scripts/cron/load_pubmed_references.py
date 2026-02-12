#!/usr/bin/env python3
"""
Load PubMed references from NCBI.

This script:
1. Downloads the NCBI journal file (J_Medline.txt) for journal lookups
2. Retrieves all PubMed IDs from REF_BAD that should never be loaded
3. Retrieves all PubMed IDs from REF_UNLINK that should not be linked to features
4. Retrieves all existing PubMed IDs from the database
5. Retrieves all query terms (gene names, aliases) for features
6. Queries NCBI PubMed for new references by gene names
7. Compares local and NCBI PubMed IDs to find new references
8. Retrieves and parses Medline format for new PubMed IDs
9. Loads new references into database tables (journal, author, reference, abstract)
10. Loads literature guide curation status

Based on LoadPubMedReferences.pl by Gail Binkley (2000-08-24)

Usage:
    python load_pubmed_references.py --species-query "Candida AND albicans" \
        --species-abbrev C_albicans --link-genes Y

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
    NCBI_EMAIL: Email for NCBI E-utilities
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
    CURATOR_EMAIL: Email for curator reports
"""

import argparse
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import TextIO
from urllib.parse import quote_plus

import requests
from Bio import Entrez, Medline
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
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "admin@candidagenome.org")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")

# NCBI URLs
NCBI_JOURNAL_URL = "ftp://ftp.ncbi.nih.gov/pubmed/J_Medline.txt"

# PDF status constants
PDF_STATUS_N = "N"
PDF_STATUS_NAP = "NAP"

# Configure Entrez
Entrez.email = NCBI_EMAIL

# Gene names to avoid using in query (common words that produce false positives)
IGNORE_WORDS = {
    "CGD": {
        "beta", "alpha", "gamma", "kappa", "chi", "pi", "zeta",
        "mu", "MU", "ACT", "RED", "MET"
    },
    "AspGD": {
        "abpA", "sidE", "asp", "areA", "his", "pro", "man"
    }
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class MedlineParser:
    """Parse Medline format records."""

    @staticmethod
    def parse_title(record: dict) -> str | None:
        """Parse title from Medline record."""
        return record.get("TI")

    @staticmethod
    def parse_authors(record: dict) -> list[str]:
        """Parse authors from Medline record."""
        return record.get("AU", [])

    @staticmethod
    def parse_abstract(record: dict) -> str | None:
        """Parse abstract from Medline record."""
        return record.get("AB")

    @staticmethod
    def parse_journal(record: dict) -> str | None:
        """Parse journal abbreviation from Medline record."""
        return record.get("TA")

    @staticmethod
    def parse_date_published(record: dict) -> str | None:
        """Parse publication date from Medline record."""
        return record.get("DP")

    @staticmethod
    def parse_volume(record: dict) -> str | None:
        """Parse volume from Medline record."""
        return record.get("VI")

    @staticmethod
    def parse_issue(record: dict) -> str | None:
        """Parse issue from Medline record."""
        return record.get("IP")

    @staticmethod
    def parse_pages(record: dict) -> str | None:
        """Parse pages from Medline record."""
        return record.get("PG")

    @staticmethod
    def parse_pubtypes(record: dict) -> list[str]:
        """Parse publication types from Medline record."""
        return record.get("PT", [])

    @staticmethod
    def parse_pst(record: dict) -> str | None:
        """Parse publication status from Medline record."""
        return record.get("PST")

    @staticmethod
    def parse_date_revised(record: dict) -> str | None:
        """Parse last revision date from Medline record."""
        return record.get("LR")

    @staticmethod
    def parse_medline_id(record: dict) -> str | None:
        """Parse Medline ID from Medline record."""
        return record.get("PMID")

    @staticmethod
    def parse_url(record: dict) -> str | None:
        """Parse URL from Medline record (AID field with doi)."""
        aids = record.get("AID", [])
        for aid in aids:
            if "[doi]" in aid:
                doi = aid.replace(" [doi]", "")
                return f"https://doi.org/{doi}"
        return None

    @staticmethod
    def create_citation(
        year: str | None,
        title: str | None,
        journal: str | None,
        volume: str | None,
        issue: str | None,
        pages: str | None,
        authors: list[str]
    ) -> str | None:
        """Create a citation string from parsed Medline data."""
        if not title or not authors:
            return None

        # Format authors
        if len(authors) == 1:
            author_str = authors[0]
        elif len(authors) == 2:
            author_str = f"{authors[0]} and {authors[1]}"
        else:
            author_str = f"{authors[0]} et al."

        # Build citation
        parts = [f"{author_str} ({year or 'n.d.'})"]

        if title:
            parts.append(title)

        if journal:
            journal_part = journal
            if volume:
                journal_part += f" {volume}"
            if issue:
                journal_part += f"({issue})"
            if pages:
                journal_part += f":{pages}"
            parts.append(journal_part)

        citation = " ".join(parts)

        # Truncate if too long
        if len(citation) > 480:
            citation = citation[:477] + "..."

        return citation


class JournalFileParser:
    """Parse NCBI J_Medline.txt journal file."""

    def __init__(self, journal_file: Path):
        self.journal_file = journal_file
        self.issn_by_abbrev: dict[str, str] = {}
        self.fullname_by_abbrev: dict[str, str] = {}

    def download(self) -> bool:
        """Download NCBI journal file."""
        try:
            logger.info(f"Downloading NCBI journal file from {NCBI_JOURNAL_URL}")
            response = requests.get(NCBI_JOURNAL_URL, timeout=300)
            response.raise_for_status()

            self.journal_file.parent.mkdir(parents=True, exist_ok=True)
            self.journal_file.write_text(response.text)

            logger.info(f"Successfully downloaded journal file to {self.journal_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to download journal file: {e}")
            return False

    def parse(self) -> None:
        """Parse the journal file."""
        if not self.journal_file.exists():
            logger.warning(f"Journal file not found: {self.journal_file}")
            return

        content = self.journal_file.read_text()

        # Split into records by JrId:
        records = content.split("JrId:")

        for record in records:
            if not record.strip():
                continue

            abbrev = None
            issn = None
            title = None

            for line in record.split("\n"):
                line = line.strip()
                if line.startswith("JournalTitle:"):
                    title = line.replace("JournalTitle:", "").strip()
                elif line.startswith("MedAbbr:"):
                    abbrev = line.replace("MedAbbr:", "").strip()
                elif line.startswith("ISSN:"):
                    issn = line.replace("ISSN:", "").strip()

            if abbrev:
                if issn:
                    self.issn_by_abbrev[abbrev] = issn
                if title:
                    self.fullname_by_abbrev[abbrev] = title


class PubMedLoader:
    """Load PubMed references from NCBI."""

    def __init__(
        self,
        session,
        species_query: str,
        species_abbrev: str,
        link_genes: bool,
        log_file: TextIO,
        error_file: TextIO
    ):
        self.session = session
        self.species_query = species_query
        self.species_abbrev = species_abbrev
        self.link_genes = link_genes
        self.log_file = log_file
        self.error_file = error_file

        # Get ignore words for this project
        self.ignore_words = IGNORE_WORDS.get(PROJECT_ACRONYM, set())

        # Counters
        self.bad_ref_count = 0
        self.bad_title_count = 0
        self.bad_date_count = 0
        self.load_ref_count = 0
        self.bad_gi_count = 0
        self.load_gi_count = 0
        self.no_citation_count = 0

        # Data hashes
        self.curated_pmids: set[int] = set()
        self.bad_pmids: set[int] = set()
        self.temp_pmids: set[int] = set()
        self.unlink_pmids: dict[int, set[str]] = {}
        self.db_pmids: set[int] = set()
        self.local_pmids_by_feat: dict[str, set[int]] = {}
        self.query_terms_by_feat: dict[str, set[str]] = {}
        self.feature_no_by_name: dict[str, int] = {}
        self.ncbi_pmids_by_feat: dict[str, set[int]] = {}
        self.new_ncbi_obj_pmids: dict[str, set[int]] = {}
        self.new_ncbi_pmids: set[int] = set()
        self.not_loaded_pmids: set[int] = set()
        self.ncbi_ref_types: dict[str, int] = {}

        # Journal data
        self.issn_by_abbrev: dict[str, str] = {}
        self.fullname_by_abbrev: dict[str, str] = {}

        # Get strain info
        self.seq_source = self._get_seq_source()
        self.gene_prefix = self._get_gene_prefix()

    def log(self, message: str) -> None:
        """Write to log file."""
        self.log_file.write(f"{message}\n")
        self.log_file.flush()

    def error(self, message: str) -> None:
        """Write to error file."""
        self.error_file.write(f"{message}\n")
        self.error_file.flush()

    def _get_seq_source(self) -> str | None:
        """Get sequence source for species."""
        query = text(f"""
            SELECT DISTINCT fl.seq_source
            FROM {DB_SCHEMA}.feat_location fl
            JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_abbrev = :species_abbrev
            AND fl.is_loc_current = 'Y'
        """)

        result = self.session.execute(
            query, {"species_abbrev": self.species_abbrev}
        ).first()

        return result[0] if result else None

    def _get_gene_prefix(self) -> str:
        """Get gene prefix for species (e.g., 'Ca' for C. albicans)."""
        # Extract first letter of each word in species name
        parts = self.species_abbrev.split("_")
        if len(parts) >= 2:
            return parts[0][0] + parts[1][0].lower()
        return ""

    def get_curated_pmids(self) -> None:
        """Get PubMed IDs that have already been fully curated."""
        query = text(f"""
            SELECT r.pubmed
            FROM {DB_SCHEMA}.reference r
            JOIN {DB_SCHEMA}.ref_property rp ON r.reference_no = rp.reference_no
            WHERE rp.property_type = 'curation_status'
            AND rp.property_value IN (
                'Basic, lit guide, GO, Pheno curation done',
                'Done:Abstract curated, full text not curated'
            )
            AND r.pubmed IS NOT NULL
        """)

        result = self.session.execute(query)
        for row in result:
            self.curated_pmids.add(row[0])

        self.log(f"Retrieved {len(self.curated_pmids)} curated PubMed IDs from REF_PROPERTY")

    def get_bad_pmids(self) -> None:
        """Get PubMed IDs that should never be loaded."""
        query = text(f"""
            SELECT pubmed
            FROM {DB_SCHEMA}.ref_bad
        """)

        result = self.session.execute(query)
        for row in result:
            self.bad_pmids.add(row[0])

        self.log(f"Retrieved {len(self.bad_pmids)} bad PubMed IDs from REF_BAD")

    def get_temp_pmids(self) -> None:
        """Get PubMed IDs in temporary reference table."""
        query = text(f"""
            SELECT pubmed
            FROM {DB_SCHEMA}.ref_temp
        """)

        result = self.session.execute(query)
        for row in result:
            self.temp_pmids.add(row[0])

        self.log(f"Retrieved {len(self.temp_pmids)} temp PubMed IDs from REF_TEMP")

    def get_unlink_pmids(self) -> None:
        """Get PubMed IDs that should not be linked to certain features."""
        query = text(f"""
            SELECT r.pubmed, f.feature_name
            FROM {DB_SCHEMA}.ref_unlink r
            JOIN {DB_SCHEMA}.feature f ON r.primary_key = f.feature_no
            WHERE r.tab_name = 'FEATURE'
        """)

        result = self.session.execute(query)
        for row in result:
            pmid, feature_name = row
            if pmid not in self.unlink_pmids:
                self.unlink_pmids[pmid] = set()
            self.unlink_pmids[pmid].add(feature_name)

        self.log(f"Retrieved {len(self.unlink_pmids)} unlink PubMed IDs from REF_UNLINK")

    def get_db_pmids(self) -> None:
        """Get all existing PubMed IDs in database."""
        query = text(f"""
            SELECT pubmed
            FROM {DB_SCHEMA}.reference
            WHERE pubmed IS NOT NULL
        """)

        result = self.session.execute(query)
        for row in result:
            self.db_pmids.add(row[0])

        self.log(f"Retrieved {len(self.db_pmids)} existing PubMed IDs")

    def get_feature_pmids(self) -> None:
        """Get all PubMed IDs associated with features via literature guide."""
        query = text(f"""
            SELECT f.feature_name, r.pubmed
            FROM {DB_SCHEMA}.refprop_feat rpf
            JOIN {DB_SCHEMA}.feature f ON rpf.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
                AND fl.is_loc_current = 'Y'
            JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
                AND s.is_seq_current = 'Y'
                AND s.source = :seq_source
            JOIN {DB_SCHEMA}.genome_version gv ON s.genome_version_no = gv.genome_version_no
                AND gv.is_ver_current = 'Y'
            JOIN {DB_SCHEMA}.ref_property rp ON rpf.ref_property_no = rp.ref_property_no
            JOIN {DB_SCHEMA}.reference r ON rp.reference_no = r.reference_no
            WHERE r.pubmed IS NOT NULL
        """)

        result = self.session.execute(query, {"seq_source": self.seq_source})
        for row in result:
            feature_name, pmid = row
            if feature_name not in self.local_pmids_by_feat:
                self.local_pmids_by_feat[feature_name] = set()
            self.local_pmids_by_feat[feature_name].add(pmid)

        self.log(f"Retrieved PubMed IDs for {len(self.local_pmids_by_feat)} features")

    def get_query_terms(self) -> None:
        """Get gene names and aliases for querying PubMed."""
        query = text(f"""
            SELECT f.feature_name, f.feature_no, f.gene_name, a.alias_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
                AND fl.is_loc_current = 'Y'
            JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
                AND s.source = :seq_source
                AND s.is_seq_current = 'Y'
            JOIN {DB_SCHEMA}.genome_version gv ON s.genome_version_no = gv.genome_version_no
                AND gv.is_ver_current = 'Y'
            LEFT OUTER JOIN {DB_SCHEMA}.feat_alias fa ON f.feature_no = fa.feature_no
            LEFT OUTER JOIN {DB_SCHEMA}.alias a ON fa.alias_no = a.alias_no
            WHERE f.feature_type IN (
                SELECT col_value
                FROM {DB_SCHEMA}.web_metadata
                WHERE application_name = 'Locus Page'
                AND tab_name = 'FEATURE'
                AND col_name = 'FEATURE_TYPE'
            )
        """)

        result = self.session.execute(query, {"seq_source": self.seq_source})

        for row in result:
            feature_name, feature_no, gene_name, alias_name = row

            # Skip if feature name is in ignore list
            if feature_name in self.ignore_words:
                continue

            if feature_name not in self.query_terms_by_feat:
                self.query_terms_by_feat[feature_name] = set()

            self.feature_no_by_name[feature_name] = feature_no

            # Add gene name
            if gene_name and gene_name not in self.ignore_words:
                self.query_terms_by_feat[feature_name].add(gene_name)
                # Add prefixed version
                self.query_terms_by_feat[feature_name].add(
                    f"{self.gene_prefix}{gene_name}"
                )

            # Add alias
            if alias_name and alias_name not in self.ignore_words:
                self.query_terms_by_feat[feature_name].add(alias_name)
                # Add prefixed version if in standard format
                if re.match(r"^[A-Za-z]{3}\d+$", alias_name):
                    self.query_terms_by_feat[feature_name].add(
                        f"{self.gene_prefix}{alias_name}"
                    )

        self.log(f"Retrieved query terms for {len(self.query_terms_by_feat)} features")

    def get_ref_types(self) -> None:
        """Get NCBI reference types from database."""
        query = text(f"""
            SELECT ref_type, ref_type_no
            FROM {DB_SCHEMA}.ref_type
            WHERE source = 'NCBI'
        """)

        result = self.session.execute(query)
        for row in result:
            self.ncbi_ref_types[row[0]] = row[1]

        self.log(f"Retrieved {len(self.ncbi_ref_types)} NCBI reference types")

    def get_ncbi_pmids(self) -> None:
        """Query NCBI PubMed for references by gene names."""
        self.log("Querying NCBI PubMed for references...")

        for feature_name, terms in self.query_terms_by_feat.items():
            if not terms:
                continue

            for term in terms:
                # Build query: term AND species
                query = f'"{term}"[TW] AND ({self.species_query})'

                try:
                    # Search PubMed
                    handle = Entrez.esearch(
                        db="pubmed",
                        term=query,
                        retmax=1000,
                        usehistory="n"
                    )
                    record = Entrez.read(handle)
                    handle.close()

                    pmids = [int(pmid) for pmid in record.get("IdList", [])]

                    # Filter PMIDs
                    for pmid in pmids:
                        # Skip curated, bad, temp, or unlinked PMIDs
                        if pmid in self.curated_pmids:
                            continue
                        if pmid in self.bad_pmids:
                            continue
                        if pmid in self.temp_pmids:
                            continue
                        if pmid in self.unlink_pmids and feature_name in self.unlink_pmids[pmid]:
                            continue

                        if feature_name not in self.ncbi_pmids_by_feat:
                            self.ncbi_pmids_by_feat[feature_name] = set()
                        self.ncbi_pmids_by_feat[feature_name].add(pmid)

                except Exception as e:
                    self.log(f"Error querying PubMed for {term}: {e}")
                    continue

        total_pmids = sum(len(pmids) for pmids in self.ncbi_pmids_by_feat.values())
        self.log(f"Retrieved {total_pmids} PMIDs for {len(self.ncbi_pmids_by_feat)} features")

    def compare_pmids(self) -> None:
        """Compare local and NCBI PubMed IDs to find new references."""
        self.log("Comparing local and NCBI PubMed IDs...")

        all_new_pmids: set[int] = set()

        for feature_name, ncbi_pmids in self.ncbi_pmids_by_feat.items():
            local_pmids = self.local_pmids_by_feat.get(feature_name, set())

            for pmid in ncbi_pmids:
                all_new_pmids.add(pmid)

                # Check if this feature/pmid combination is new
                if pmid not in local_pmids:
                    if feature_name not in self.new_ncbi_obj_pmids:
                        self.new_ncbi_obj_pmids[feature_name] = set()
                    self.new_ncbi_obj_pmids[feature_name].add(pmid)

        # Find PMIDs that are completely new to the database
        for pmid in all_new_pmids:
            if pmid not in self.db_pmids:
                self.new_ncbi_pmids.add(pmid)

        self.log(f"Found {len(self.new_ncbi_pmids)} new PubMed IDs to load")
        self.log(f"Found {sum(len(p) for p in self.new_ncbi_obj_pmids.values())} new feature-PMID associations")

    def get_medline_record(self, pmid: int) -> dict | None:
        """Fetch Medline record for a PubMed ID."""
        try:
            handle = Entrez.efetch(
                db="pubmed",
                id=str(pmid),
                rettype="medline",
                retmode="text"
            )
            records = list(Medline.parse(handle))
            handle.close()

            if records:
                return records[0]
            return None
        except Exception as e:
            self.log(f"Error fetching Medline for {pmid}: {e}")
            return None

    def clean_text(self, text: str | None) -> str:
        """Clean text by removing unwanted characters."""
        if not text:
            return ""
        # Remove control characters
        text = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", text)
        return text.strip()

    def get_author_no(self, author_name: str) -> int | None:
        """Get or create author and return author_no."""
        # Check if author exists
        query = text(f"""
            SELECT author_no
            FROM {DB_SCHEMA}.author
            WHERE author_name = :name
        """)
        result = self.session.execute(query, {"name": author_name}).first()

        if result:
            return result[0]

        # Insert new author
        insert = text(f"""
            INSERT INTO {DB_SCHEMA}.author (author_name, created_by)
            VALUES (:name, :user)
        """)
        self.session.execute(insert, {"name": author_name, "user": ADMIN_USER})
        self.session.commit()

        # Get the new author_no
        result = self.session.execute(query, {"name": author_name}).first()
        if result:
            self.log(f"Inserted author: '{author_name}'")
            return result[0]

        return None

    def get_journal_no(self, journal_abbrev: str) -> int | None:
        """Get or create journal and return journal_no."""
        # Check if journal exists
        query = text(f"""
            SELECT journal_no
            FROM {DB_SCHEMA}.journal
            WHERE abbreviation = :abbrev
        """)
        result = self.session.execute(query, {"abbrev": journal_abbrev}).first()

        if result:
            return result[0]

        # Insert new journal
        issn = self.issn_by_abbrev.get(journal_abbrev)
        full_name = self.fullname_by_abbrev.get(journal_abbrev)

        insert = text(f"""
            INSERT INTO {DB_SCHEMA}.journal (full_name, abbreviation, issn, created_by)
            VALUES (:full_name, :abbrev, :issn, :user)
        """)
        self.session.execute(insert, {
            "full_name": full_name,
            "abbrev": journal_abbrev,
            "issn": issn,
            "user": ADMIN_USER
        })
        self.session.commit()

        # Get the new journal_no
        result = self.session.execute(query, {"abbrev": journal_abbrev}).first()
        if result:
            self.log(f"Inserted journal: '{journal_abbrev}'")
            return result[0]

        return None

    def insert_ref_type(self, ref_type: str) -> int | None:
        """Insert reference type into ref_type table."""
        # Capitalize words
        ref_type = " ".join(word.capitalize() for word in ref_type.split())

        if ref_type in self.ncbi_ref_types:
            return self.ncbi_ref_types[ref_type]

        # Insert new ref_type
        insert = text(f"""
            INSERT INTO {DB_SCHEMA}.ref_type (source, ref_type, created_by)
            VALUES ('NCBI', :ref_type, :user)
        """)
        self.session.execute(insert, {"ref_type": ref_type, "user": ADMIN_USER})
        self.session.commit()

        # Get the new ref_type_no
        query = text(f"""
            SELECT ref_type_no
            FROM {DB_SCHEMA}.ref_type
            WHERE ref_type = :ref_type AND source = 'NCBI'
        """)
        result = self.session.execute(query, {"ref_type": ref_type}).first()

        if result:
            self.ncbi_ref_types[ref_type] = result[0]
            self.log(f"Inserted ref_type: '{ref_type}'")
            return result[0]

        return None

    def load_reference(self, pmid: int, record: dict) -> str | None:
        """Load a single reference into the database.

        Returns:
            None on success, error code string on failure
        """
        parser = MedlineParser()

        # Parse record
        title = self.clean_text(parser.parse_title(record))
        if not title or "[In Process Citation]" in title:
            return "Title"

        pst = parser.parse_pst(record) or ""
        ref_status = "Epub ahead of print" if "aheadofprint" in pst else "Published"

        date_published = self.clean_text(parser.parse_date_published(record))
        date_revised = self.clean_text(parser.parse_date_revised(record))

        # Get year from date_published
        year = None
        if date_published:
            year_match = re.match(r"(\d{4})", date_published)
            if year_match:
                year = year_match.group(1)

        # Handle year spanning two years (e.g., "2023-2024")
        if year and "-" in year and len(year) > 4:
            year = year.split("-")[0]

        journal = self.clean_text(parser.parse_journal(record))
        volume = self.clean_text(parser.parse_volume(record))
        issue = self.clean_text(parser.parse_issue(record))
        pages = self.clean_text(parser.parse_pages(record))
        abstract = self.clean_text(parser.parse_abstract(record))
        url = parser.parse_url(record)

        authors = parser.parse_authors(record)
        authors = [self.clean_text(a) for a in authors]

        pub_types = parser.parse_pubtypes(record)

        # Create citation
        citation = parser.create_citation(
            year, title, journal, volume, issue, pages, authors
        )
        if not citation:
            return "Citation"

        # Validate required fields
        if not year or not re.match(r"^\d{4}$", year):
            return f"Required fields missing: year={year}"
        if not title or len(title) > 400:
            return f"Required fields missing: title length={len(title) if title else 0}"
        if len(citation) > 480:
            return f"Required fields missing: citation length={len(citation)}"

        # Get or create authors
        author_nos = []
        for author in authors:
            if author:
                author_no = self.get_author_no(author)
                if author_no:
                    author_nos.append(author_no)

        # Get or create journal
        journal_no = None
        if journal:
            journal_no = self.get_journal_no(journal)

        # Determine PDF status
        pdf_status = PDF_STATUS_N if ref_status == "Published" else PDF_STATUS_NAP

        # Insert reference
        insert_ref = text(f"""
            INSERT INTO {DB_SCHEMA}.reference
            (source, status, citation, year, pubmed, date_published, date_revised,
             issue, page, volume, title, journal_no, pdf_status, created_by)
            VALUES ('PubMed script', :status, :citation, :year, :pubmed, :date_pub,
                    :date_rev, :issue, :page, :volume, :title, :journal_no,
                    :pdf_status, :user)
        """)
        self.session.execute(insert_ref, {
            "status": ref_status,
            "citation": citation,
            "year": year,
            "pubmed": pmid,
            "date_pub": date_published,
            "date_rev": date_revised,
            "issue": issue,
            "page": pages,
            "volume": volume,
            "title": title,
            "journal_no": journal_no,
            "pdf_status": pdf_status,
            "user": ADMIN_USER
        })

        # Get reference_no
        query = text(f"""
            SELECT reference_no
            FROM {DB_SCHEMA}.reference
            WHERE pubmed = :pmid
        """)
        result = self.session.execute(query, {"pmid": pmid}).first()
        if not result:
            return "Reference insert failed"
        reference_no = result[0]

        # Insert abstract
        if abstract:
            if len(abstract) > 4000:
                abstract = abstract[:3950] + "...ABSTRACT TRUNCATED AT 3950 CHARACTERS."

            insert_abs = text(f"""
                INSERT INTO {DB_SCHEMA}.abstract (reference_no, abstract)
                VALUES (:ref_no, :abstract)
            """)
            self.session.execute(insert_abs, {
                "ref_no": reference_no,
                "abstract": abstract
            })

        # Insert URL
        if url:
            # Check if URL exists
            query_url = text(f"""
                SELECT url_no FROM {DB_SCHEMA}.url
                WHERE url = :url AND url_type = 'Reference full text'
            """)
            result = self.session.execute(query_url, {"url": url}).first()

            if not result:
                insert_url = text(f"""
                    INSERT INTO {DB_SCHEMA}.url (source, url_type, url, created_by)
                    VALUES ('Publisher', 'Reference full text', :url, :user)
                """)
                self.session.execute(insert_url, {"url": url, "user": ADMIN_USER})

            result = self.session.execute(query_url, {"url": url}).first()
            if result:
                url_no = result[0]
                insert_ref_url = text(f"""
                    INSERT INTO {DB_SCHEMA}.ref_url (reference_no, url_no)
                    VALUES (:ref_no, :url_no)
                """)
                self.session.execute(insert_ref_url, {
                    "ref_no": reference_no,
                    "url_no": url_no
                })

        # Insert publication types
        for pub_type in pub_types:
            pub_type = self.clean_text(pub_type)
            if not pub_type:
                continue

            ref_type_no = self.insert_ref_type(pub_type)
            if ref_type_no:
                insert_reftype = text(f"""
                    INSERT INTO {DB_SCHEMA}.ref_reftype (reference_no, ref_type_no)
                    VALUES (:ref_no, :type_no)
                """)
                self.session.execute(insert_reftype, {
                    "ref_no": reference_no,
                    "type_no": ref_type_no
                })

        # Insert author_editor records
        for i, author_no in enumerate(author_nos, 1):
            insert_ae = text(f"""
                INSERT INTO {DB_SCHEMA}.author_editor
                (author_no, reference_no, author_type, author_order)
                VALUES (:author_no, :ref_no, 'Author', :order)
            """)
            self.session.execute(insert_ae, {
                "author_no": author_no,
                "ref_no": reference_no,
                "order": i
            })

        self.session.commit()
        self.log(f"Loaded reference for PubMed ID: {pmid}")
        return None

    def rollback_reference(self, pmid: int) -> None:
        """Remove partially loaded reference."""
        query = text(f"""
            SELECT reference_no FROM {DB_SCHEMA}.reference
            WHERE pubmed = :pmid
        """)
        result = self.session.execute(query, {"pmid": pmid}).first()

        if result:
            reference_no = result[0]

            # Delete from related tables
            for table in ["abstract", "ref_url", "ref_reftype", "author_editor"]:
                delete = text(f"""
                    DELETE FROM {DB_SCHEMA}.{table}
                    WHERE reference_no = :ref_no
                """)
                self.session.execute(delete, {"ref_no": reference_no})

            # Delete reference
            delete_ref = text(f"""
                DELETE FROM {DB_SCHEMA}.reference
                WHERE reference_no = :ref_no
            """)
            self.session.execute(delete_ref, {"ref_no": reference_no})
            self.session.commit()

            self.log(f"Rolled back reference for PubMed ID: {pmid}")

    def get_medline_and_load(self) -> None:
        """Fetch Medline records and load references."""
        self.log(f"Loading {len(self.new_ncbi_pmids)} new references...")

        for pmid in self.new_ncbi_pmids:
            record = self.get_medline_record(pmid)
            if not record:
                self.not_loaded_pmids.add(pmid)
                self.bad_ref_count += 1
                continue

            try:
                result = self.load_reference(pmid, record)

                if result == "Title":
                    self.bad_title_count += 1
                    self.not_loaded_pmids.add(pmid)
                    self.rollback_reference(pmid)
                elif result == "Citation":
                    self.no_citation_count += 1
                    self.not_loaded_pmids.add(pmid)
                elif result and result.startswith("Required"):
                    self.log(f"PubMed ID {pmid} not loaded: {result}")
                    self.not_loaded_pmids.add(pmid)
                elif result:
                    self.bad_ref_count += 1
                    self.not_loaded_pmids.add(pmid)
                    self.error(f"Error loading PubMed ID {pmid}: {result}")
                else:
                    self.load_ref_count += 1

            except Exception as e:
                self.bad_ref_count += 1
                self.not_loaded_pmids.add(pmid)
                self.error(f"Exception loading PubMed ID {pmid}: {e}")
                self.rollback_reference(pmid)

    def load_curation_status(self, curation_status: str = "High Priority") -> None:
        """Load literature guide curation status for new feature-PMID associations."""
        self.log(f"Loading curation status '{curation_status}' for new associations...")

        # Get PMID to reference_no mapping
        query = text(f"""
            SELECT pubmed, reference_no
            FROM {DB_SCHEMA}.reference
            WHERE pubmed IS NOT NULL
        """)
        result = self.session.execute(query)
        ref_no_by_pmid = {row[0]: row[1] for row in result}

        for feature_name, pmids in self.new_ncbi_obj_pmids.items():
            feature_no = self.feature_no_by_name.get(feature_name)
            if not feature_no:
                continue

            for pmid in pmids:
                # Skip PMIDs that weren't loaded
                if pmid in self.not_loaded_pmids:
                    continue

                reference_no = ref_no_by_pmid.get(pmid)
                if not reference_no:
                    continue

                try:
                    # Insert ref_property
                    insert_prop = text(f"""
                        INSERT INTO {DB_SCHEMA}.ref_property
                        (reference_no, property_type, property_value, created_by)
                        VALUES (:ref_no, 'curation_status', :status, :user)
                    """)
                    self.session.execute(insert_prop, {
                        "ref_no": reference_no,
                        "status": curation_status,
                        "user": ADMIN_USER
                    })

                    # Get ref_property_no
                    query_prop = text(f"""
                        SELECT ref_property_no
                        FROM {DB_SCHEMA}.ref_property
                        WHERE reference_no = :ref_no
                        AND property_type = 'curation_status'
                        AND property_value = :status
                    """)
                    result = self.session.execute(query_prop, {
                        "ref_no": reference_no,
                        "status": curation_status
                    }).first()

                    if result and self.link_genes:
                        ref_property_no = result[0]

                        # Insert refprop_feat
                        insert_feat = text(f"""
                            INSERT INTO {DB_SCHEMA}.refprop_feat
                            (ref_property_no, feature_no, created_by)
                            VALUES (:prop_no, :feat_no, :user)
                        """)
                        self.session.execute(insert_feat, {
                            "prop_no": ref_property_no,
                            "feat_no": feature_no,
                            "user": ADMIN_USER
                        })

                    self.session.commit()
                    self.load_gi_count += 1
                    self.log(f"Loaded curation status for {feature_name} - PMID {pmid}")

                except Exception as e:
                    self.bad_gi_count += 1
                    self.error(f"Error loading curation status for {feature_name} - PMID {pmid}: {e}")
                    self.session.rollback()

    def run(self, journal_parser: JournalFileParser) -> None:
        """Run the full PubMed loading process."""
        self.issn_by_abbrev = journal_parser.issn_by_abbrev
        self.fullname_by_abbrev = journal_parser.fullname_by_abbrev

        # Get exclusion lists
        self.get_curated_pmids()
        self.get_bad_pmids()
        self.get_temp_pmids()
        self.get_unlink_pmids()

        # Get existing PMIDs
        self.get_db_pmids()

        # Get feature PMIDs and query terms
        self.get_feature_pmids()
        self.get_query_terms()

        # Get reference types
        self.get_ref_types()

        # Query NCBI
        self.get_ncbi_pmids()

        # Compare PMIDs
        self.compare_pmids()

        # Load new references
        self.get_medline_and_load()

        # Load curation status
        self.load_curation_status()

        # Print summary
        self.log(f"\n{self.bad_ref_count} ERROR(s) occurred while loading PubMed references.")
        self.log(f"{self.bad_gi_count} ERROR(s) occurred while loading Gene Infos.")
        self.log(f"{self.bad_title_count} PubMed reference(s) not loaded (citation in process).")
        self.log(f"{self.no_citation_count} PubMed reference(s) not loaded (no citation available).")
        self.log(f"\n{self.load_ref_count} PubMed reference(s) were loaded.")
        self.log(f"{self.load_gi_count} Gene Info(s) were loaded.")


def load_pubmed_references(
    species_query: str,
    species_abbrev: str,
    link_genes: bool
) -> bool:
    """
    Main function to load PubMed references.

    Args:
        species_query: Species query string for PubMed (e.g., "Candida AND albicans")
        species_abbrev: Species abbreviation for gene names (e.g., C_albicans)
        link_genes: Whether to link papers to genes

    Returns:
        True on success, False on failure
    """
    # Create directories
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Set up log files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"{species_abbrev}_PubMed_{timestamp}.log"
    error_file = LOG_DIR / f"{species_abbrev}_PubMed_error_{timestamp}.log"

    try:
        with open(log_file, "w") as log_fh, open(error_file, "w") as error_fh:
            log_fh.write(f"Started: {datetime.now()}\n\n")
            log_fh.write(f"Executing: {species_query} {species_abbrev} {'Y' if link_genes else 'N'}\n\n")

            # Download and parse journal file
            journal_file = DATA_DIR / "J_Medline.txt"
            journal_parser = JournalFileParser(journal_file)
            journal_parser.download()
            journal_parser.parse()

            # Run PubMed loader
            with SessionLocal() as session:
                loader = PubMedLoader(
                    session=session,
                    species_query=species_query,
                    species_abbrev=species_abbrev,
                    link_genes=link_genes,
                    log_file=log_fh,
                    error_file=error_fh
                )
                loader.run(journal_parser)

            log_fh.write(f"\nFinished: {datetime.now()}\n")

        logger.info(f"PubMed loading complete. See {log_file} for details.")
        return True

    except Exception as e:
        logger.exception(f"Error loading PubMed references: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load PubMed references from NCBI"
    )
    parser.add_argument(
        "--species-query",
        required=True,
        help="Species query string for PubMed (e.g., 'Candida AND albicans')",
    )
    parser.add_argument(
        "--species-abbrev",
        required=True,
        help="Species abbreviation for gene names (e.g., C_albicans)",
    )
    parser.add_argument(
        "--link-genes",
        choices=["Y", "N"],
        default="Y",
        help="Link papers to genes (Y/N)",
    )

    args = parser.parse_args()

    link_genes = args.link_genes == "Y"

    success = load_pubmed_references(
        args.species_query,
        args.species_abbrev,
        link_genes
    )
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
