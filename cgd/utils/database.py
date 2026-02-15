"""
Common database query utilities.

This module provides reusable database query functions for
common lookups used across CGD scripts.
"""

import os
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

# Get schema from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")


def get_organism_no(
    session: Session,
    strain_abbrev: str,
    schema: str = None,
) -> Optional[int]:
    """
    Get organism_no for a strain abbreviation.

    Args:
        session: Database session
        strain_abbrev: Organism abbreviation (e.g., "C_albicans_SC5314")
        schema: Database schema (default: from environment)

    Returns:
        organism_no or None if not found
    """
    schema = schema or DB_SCHEMA
    query = text(f"""
        SELECT organism_no
        FROM {schema}.organism
        WHERE organism_abbrev = :abbrev
    """)
    result = session.execute(query, {"abbrev": strain_abbrev}).first()
    return result[0] if result else None


def get_seq_source(
    session: Session,
    strain_abbrev: str,
    schema: str = None,
) -> Optional[str]:
    """
    Get sequence source for a strain.

    Args:
        session: Database session
        strain_abbrev: Organism abbreviation
        schema: Database schema

    Returns:
        Sequence source string or None
    """
    schema = schema or DB_SCHEMA
    query = text(f"""
        SELECT DISTINCT s.source
        FROM {schema}.seq s
        JOIN {schema}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {schema}.feature f ON fl.feature_no = f.feature_no
        JOIN {schema}.organism o ON f.organism_no = o.organism_no
        WHERE s.is_seq_current = 'Y'
        AND o.organism_abbrev = :strain_abbrev
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    return result[0] if result else None


def get_strain_config(
    session: Session,
    strain_abbrev: str,
    schema: str = None,
) -> Optional[dict]:
    """
    Get strain configuration from database.

    Args:
        session: Database session
        strain_abbrev: Organism abbreviation
        schema: Database schema

    Returns:
        Dictionary with organism_no, organism_abbrev, taxon_id, or None
    """
    schema = schema or DB_SCHEMA
    query = text(f"""
        SELECT o.organism_no, o.organism_abbrev, o.taxon_id
        FROM {schema}.organism o
        WHERE o.organism_abbrev = :strain_abbrev
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    if not result:
        return None
    return {
        "organism_no": result[0],
        "organism_abbrev": result[1],
        "taxon_id": result[2],
    }


def get_chromosome_lengths(
    session: Session,
    organism_no: Optional[int] = None,
    schema: str = None,
) -> dict[str, int]:
    """
    Get chromosome lengths.

    Args:
        session: Database session
        organism_no: Optional organism filter
        schema: Database schema

    Returns:
        Dictionary mapping chromosome names to lengths
    """
    schema = schema or DB_SCHEMA

    if organism_no:
        query = text(f"""
            SELECT f.feature_name, fl.stop_coord
            FROM {schema}.feature f
            JOIN {schema}.feat_location fl ON f.feature_no = fl.feature_no
            WHERE f.feature_type = 'chromosome'
            AND f.organism_no = :org_no
        """)
        result = session.execute(query, {"org_no": organism_no})
    else:
        query = text(f"""
            SELECT f.feature_name, fl.stop_coord
            FROM {schema}.feature f
            JOIN {schema}.feat_location fl ON f.feature_no = fl.feature_no
            WHERE f.feature_type = 'chromosome'
        """)
        result = session.execute(query)

    return {row[0]: row[1] for row in result.fetchall()}


def get_chromosome_roman_map(
    session: Session,
    schema: str = None,
) -> dict[int, str]:
    """
    Get mapping of chromosome numbers to Roman numerals.

    Args:
        session: Database session
        schema: Database schema

    Returns:
        Dictionary mapping chromosome numbers to Roman numerals
    """
    # Standard mapping (used if not in database)
    default_map = {
        1: "I", 2: "II", 3: "III", 4: "IV", 5: "V",
        6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X",
        11: "XI", 12: "XII", 13: "XIII", 14: "XIV", 15: "XV",
        16: "XVI", 17: "Mito",
    }
    return default_map


class CachedLookup:
    """
    Cached database lookups for efficient repeated queries.

    This class pre-loads common lookup tables into memory
    to avoid repeated database queries.
    """

    def __init__(self, session: Session, organism_no: Optional[int] = None):
        """
        Initialize cached lookups.

        Args:
            session: Database session
            organism_no: Optional organism filter
        """
        self.session = session
        self.organism_no = organism_no
        self.schema = DB_SCHEMA

        # Caches
        self._feature_no_by_dbid: dict[str, int] = {}
        self._feature_no_by_name: dict[str, int] = {}
        self._dbid_by_feature_name: dict[str, str] = {}
        self._go_no_by_goid: dict[str, int] = {}
        self._reference_no_by_pubmed: dict[str, int] = {}

        self._loaded = False

    def load_caches(self) -> None:
        """Load all caches from database."""
        if self._loaded:
            return

        self._load_feature_cache()
        self._load_go_cache()
        self._load_reference_cache()
        self._loaded = True

    def _load_feature_cache(self) -> None:
        """Load feature lookup cache."""
        if self.organism_no:
            query = text(f"""
                SELECT feature_no, feature_name, dbxref_id
                FROM {self.schema}.feature
                WHERE organism_no = :org_no
            """)
            result = self.session.execute(query, {"org_no": self.organism_no})
        else:
            query = text(f"""
                SELECT feature_no, feature_name, dbxref_id
                FROM {self.schema}.feature
            """)
            result = self.session.execute(query)

        for feat_no, feat_name, dbid in result:
            if dbid:
                self._feature_no_by_dbid[dbid.upper()] = feat_no
            if feat_name:
                self._feature_no_by_name[feat_name.upper()] = feat_no
                if dbid:
                    self._dbid_by_feature_name[feat_name.upper()] = dbid

    def _load_go_cache(self) -> None:
        """Load GO lookup cache."""
        query = text(f"SELECT go_no, goid FROM {self.schema}.go")
        result = self.session.execute(query)
        for go_no, goid in result:
            self._go_no_by_goid[str(goid)] = go_no

    def _load_reference_cache(self) -> None:
        """Load reference lookup cache."""
        query = text(f"""
            SELECT reference_no, pubmed
            FROM {self.schema}.reference
            WHERE pubmed IS NOT NULL
        """)
        result = self.session.execute(query)
        for ref_no, pubmed in result:
            self._reference_no_by_pubmed[str(pubmed)] = ref_no

    def get_feature_no_by_dbid(self, dbid: str) -> Optional[int]:
        """Get feature_no by database ID (SGDID/CGDID)."""
        self.load_caches()
        return self._feature_no_by_dbid.get(dbid.upper())

    def get_feature_no_by_name(self, name: str) -> Optional[int]:
        """Get feature_no by feature name."""
        self.load_caches()
        return self._feature_no_by_name.get(name.upper())

    def get_dbid_by_feature_name(self, name: str) -> Optional[str]:
        """Get database ID by feature name."""
        self.load_caches()
        return self._dbid_by_feature_name.get(name.upper())

    def get_go_no_by_goid(self, goid: str | int) -> Optional[int]:
        """Get go_no by GO ID."""
        self.load_caches()
        goid_str = str(goid).replace("GO:", "")
        return self._go_no_by_goid.get(goid_str)

    def get_reference_no_by_pubmed(self, pubmed: str | int) -> Optional[int]:
        """Get reference_no by PubMed ID."""
        self.load_caches()
        return self._reference_no_by_pubmed.get(str(pubmed))


def execute_query(
    session: Session,
    query_text: str,
    params: dict = None,
) -> list[tuple]:
    """
    Execute a SQL query and return all results.

    Args:
        session: Database session
        query_text: SQL query string
        params: Query parameters

    Returns:
        List of result tuples
    """
    query = text(query_text)
    if params:
        result = session.execute(query, params)
    else:
        result = session.execute(query)
    return result.fetchall()


def execute_scalar(
    session: Session,
    query_text: str,
    params: dict = None,
) -> Any:
    """
    Execute a SQL query and return single scalar result.

    Args:
        session: Database session
        query_text: SQL query string
        params: Query parameters

    Returns:
        Single value or None
    """
    query = text(query_text)
    if params:
        result = session.execute(query, params).first()
    else:
        result = session.execute(query).first()
    return result[0] if result else None
