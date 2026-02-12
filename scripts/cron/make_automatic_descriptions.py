#!/usr/bin/env python3
"""
Generate automatic description lines for uncharacterized genes.

This script generates automatic descriptions for features without headlines
based on:
1. Orthology-based GO annotations
2. Domain-based GO annotations (if no orthology GO)
3. Ortholog gene names (if no GO annotations)

Based on makeAutomaticDescriptions.pl by Prachi Shah (Oct 2011)

Usage:
    python make_automatic_descriptions.py --strain C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    LOG_DIR: Log directory
    ADMIN_USER: Admin username
"""

import argparse
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
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
PROJECT = os.getenv("PROJECT", "CGD")

# Reference numbers for different projects (used for ref_link)
REFERENCE_NOS = {
    "CGD": {
        "auto_description": 56824,  # Reference for auto-generated headlines
        "ortho_go_transfer": 49605,  # Reference for orthology-based GO transfer
        "domain_go_transfer": 58947,  # Reference for domain-based GO transfer
    },
    "AspGD": {
        "auto_description": 3444,
        "ortho_go_transfer": 3,
        "domain_go_transfer": 11154,
    },
}

# Maximum length for descriptions
MAX_DESCRIPTION_LENGTH = 240

# GO aspect descriptions
ASPECT_DESCRIPTIONS = {
    "F": " activity",  # MF - molecular function
    "P": "role in ",   # BP - biological process
    "C": " localization",  # CC - cellular component
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class AutomaticDescriptionGenerator:
    """Generate automatic descriptions for features without headlines."""

    def __init__(self, session, strain_abbrev: str, load_to_db: bool = True):
        self.session = session
        self.strain_abbrev = strain_abbrev
        self.load_to_db = load_to_db

        # Get organism_no
        self.organism_no = self._get_organism_no()
        if not self.organism_no:
            raise ValueError(f"No organism found for: {strain_abbrev}")

        # Get reference numbers
        refs = REFERENCE_NOS.get(PROJECT, REFERENCE_NOS["CGD"])
        self.auto_desc_ref_no = refs["auto_description"]
        self.ortho_go_ref_no = refs["ortho_go_transfer"]
        self.domain_go_ref_no = refs["domain_go_transfer"]

        # Counters
        self.description_count = 0

    def _get_organism_no(self) -> int | None:
        """Get organism_no for the strain."""
        query = text(f"""
            SELECT organism_no FROM {DB_SCHEMA}.organism
            WHERE organism_abbrev = :abbrev
        """)
        result = self.session.execute(query, {"abbrev": self.strain_abbrev}).first()
        return result[0] if result else None

    def get_candidate_features(self) -> list[tuple[int, str]]:
        """Get features without headlines that are candidates for auto descriptions."""
        query = text(f"""
            SELECT DISTINCT f.feature_no, f.feature_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_property fp ON f.feature_no = fp.feature_no
            WHERE f.headline IS NULL
            AND f.feature_type = 'ORF'
            AND f.organism_no = :org_no
            AND fp.property_value NOT LIKE 'Deleted%'
        """)

        result = self.session.execute(query, {"org_no": self.organism_no})
        features = [(row[0], row[1]) for row in result]
        logger.info(f"Found {len(features)} candidate features")
        return features

    def get_go_annotations(self, feature_no: int, reference_no: int) -> dict[str, list[str]]:
        """
        Get GO annotations for a feature from a specific reference.

        Args:
            feature_no: Feature number
            reference_no: Reference number (ortho or domain transfer)

        Returns:
            Dict of aspect -> list of GO terms
        """
        query = text(f"""
            SELECT DISTINCT g.goid, g.go_term, g.go_aspect
            FROM {DB_SCHEMA}.go_annotation ga
            JOIN {DB_SCHEMA}.go g ON ga.go_no = g.go_no
            JOIN {DB_SCHEMA}.go_ref gr ON ga.go_annotation_no = gr.go_annotation_no
            WHERE ga.feature_no = :feat_no
            AND gr.reference_no = :ref_no
            AND ga.go_evidence = 'IEA'
            ORDER BY g.go_aspect, g.go_term
        """)

        result = self.session.execute(query, {
            "feat_no": feature_no,
            "ref_no": reference_no,
        })

        go_terms: dict[str, list[str]] = {}
        for row in result:
            _, go_term, aspect = row
            if aspect not in go_terms:
                go_terms[aspect] = []
            go_terms[aspect].append(go_term)

        return go_terms

    def get_orthologs(self, feature_no: int) -> dict[str, list[str]]:
        """
        Get orthologs for a feature grouped by species.

        Args:
            feature_no: Feature number

        Returns:
            Dict of species -> list of ortholog names
        """
        # Get SGD orthologs
        query1 = text(f"""
            SELECT d.dbxref_id, d.description, 'S. cerevisiae' AS species
            FROM {DB_SCHEMA}.dbxref d
            JOIN {DB_SCHEMA}.dbxref_feat df ON d.dbxref_no = df.dbxref_no
            WHERE df.feature_no = :feat_no
            AND d.source = 'SGD'
            AND d.dbxref_type = 'Gene ID'
        """)

        # Get internal homology orthologs
        query2 = text(f"""
            SELECT f2.dbxref_id, f2.feature_name || '/' || COALESCE(f2.gene_name, ''),
                   o2.organism_abbrev AS species
            FROM {DB_SCHEMA}.feature f2
            JOIN {DB_SCHEMA}.organism o2 ON f2.organism_no = o2.organism_no
            JOIN {DB_SCHEMA}.feat_homology fh2 ON f2.feature_no = fh2.feature_no
            JOIN {DB_SCHEMA}.homology_group hg ON fh2.homology_group_no = hg.homology_group_no
            JOIN {DB_SCHEMA}.feat_homology fh1 ON fh1.homology_group_no = hg.homology_group_no
            WHERE fh1.feature_no = :feat_no
            AND fh1.feature_no != fh2.feature_no
            AND hg.homology_group_type = 'ortholog'
        """)

        # Get external homology orthologs
        query3 = text(f"""
            SELECT '', d.dbxref_id, d.description AS species
            FROM {DB_SCHEMA}.dbxref d
            JOIN {DB_SCHEMA}.dbxref_homology dh ON d.dbxref_no = dh.dbxref_no
            JOIN {DB_SCHEMA}.homology_group hg ON dh.homology_group_no = hg.homology_group_no
            JOIN {DB_SCHEMA}.feat_homology fh ON fh.homology_group_no = hg.homology_group_no
            WHERE fh.feature_no = :feat_no
            AND (d.source LIKE 'Orthologous genes %' OR d.source LIKE 'Orthologs in %')
            AND d.dbxref_type = 'Gene ID'
            AND hg.homology_group_type = 'ortholog'
        """)

        orthologs: dict[str, list[str]] = {}

        for query in [query1, query2, query3]:
            result = self.session.execute(query, {"feat_no": feature_no})
            for row in result:
                _, ortho_name, species = row
                if ortho_name:
                    ortho_name = ortho_name.rstrip("/")
                    if species not in orthologs:
                        orthologs[species] = []
                    orthologs[species].append(ortho_name)

        return orthologs

    def create_desc_from_go(
        self, go_terms: dict[str, list[str]], desc_start: str
    ) -> str | None:
        """
        Create description from GO annotations.

        Args:
            go_terms: Dict of aspect -> list of GO terms
            desc_start: Description prefix

        Returns:
            Description string or None
        """
        if not go_terms:
            return None

        desc_parts = []

        for aspect in ["F", "P", "C"]:
            if aspect not in go_terms:
                continue

            terms_str = ", ".join(go_terms[aspect])

            if aspect == "P":
                prefix = ASPECT_DESCRIPTIONS[aspect]
                if not terms_str.startswith(prefix):
                    terms_str = prefix + terms_str
            else:
                suffix = ASPECT_DESCRIPTIONS[aspect]
                if not terms_str.endswith(suffix):
                    terms_str += suffix

            desc_parts.append(terms_str)

        return self._join_and_check_length("GO", desc_start, desc_parts)

    def create_desc_from_orthologs(
        self, orthologs: dict[str, list[str]], desc_start: str
    ) -> str | None:
        """
        Create description from ortholog list.

        Args:
            orthologs: Dict of species -> list of ortholog names
            desc_start: Description prefix

        Returns:
            Description string or None
        """
        if not orthologs:
            return None

        # Sort species (S. cerevisiae first)
        sorted_species = []
        if "S. cerevisiae" in orthologs:
            sorted_species.append("S. cerevisiae")

        for species in sorted(orthologs.keys()):
            if species not in sorted_species:
                sorted_species.append(species)

        desc_parts = []
        for species in sorted_species:
            if species not in orthologs:
                continue

            species_display = self._get_species_display_name(species)
            ortho_list = ", ".join(orthologs[species])
            desc_parts.append(f"<i>{species_display}</i> : {ortho_list}")

        return self._join_and_check_length("Ortho", desc_start, desc_parts)

    def _get_species_display_name(self, species: str) -> str:
        """Get display name for a species."""
        if species == "S. cerevisiae":
            return species

        # Try to get common name from organism table
        query = text(f"""
            SELECT common_name FROM {DB_SCHEMA}.organism
            WHERE organism_abbrev = :abbrev
        """)
        result = self.session.execute(query, {"abbrev": species}).first()
        return result[0] if result else species

    def _join_and_check_length(
        self, desc_source: str, desc_start: str, desc_parts: list[str]
    ) -> str | None:
        """Join description parts and check length."""
        num_parts = len(desc_parts)

        desc = desc_start + self._join_parts(desc_parts, num_parts)

        while num_parts > 0 and len(desc) >= MAX_DESCRIPTION_LENGTH:
            num_parts -= 1
            desc = desc_start + self._join_parts(desc_parts, num_parts)

        # If still too long with GO, try truncating
        if len(desc) >= MAX_DESCRIPTION_LENGTH and desc_source == "GO":
            if desc_parts:
                terms = desc_parts[0].split(", ")
                desc = self._truncate_go_list(desc_start, terms)

        if len(desc) >= MAX_DESCRIPTION_LENGTH:
            return None

        return desc

    def _join_parts(self, parts: list[str], num_parts: int) -> str:
        """Join description parts with proper separators."""
        if num_parts <= 0:
            return ""

        parts_to_join = parts[:num_parts]

        if num_parts > 2:
            return ", ".join(parts_to_join[:-1]) + " and " + parts_to_join[-1]
        elif num_parts == 2:
            return " and ".join(parts_to_join)
        else:
            return parts_to_join[0]

    def _truncate_go_list(self, desc_start: str, terms: list[str]) -> str | None:
        """Truncate GO terms list to fit length limit."""
        more_link = ', <a href="#GOsection">more</a>'
        num_parts = len(terms)

        desc = desc_start + self._join_parts(terms, num_parts)

        while num_parts > 0 and len(desc) >= MAX_DESCRIPTION_LENGTH:
            num_parts -= 1
            desc = desc_start + self._join_parts(terms, num_parts) + more_link

        return desc if len(desc) < MAX_DESCRIPTION_LENGTH else None

    def delete_previous_headlines(self) -> None:
        """Delete previously generated automatic headlines."""
        # Reset headlines
        update_sql = text(f"""
            UPDATE {DB_SCHEMA}.feature
            SET headline = NULL
            WHERE feature_no IN (
                SELECT primary_key FROM {DB_SCHEMA}.ref_link
                WHERE reference_no = :ref_no
            )
            AND organism_no = :org_no
        """)

        self.session.execute(update_sql, {
            "ref_no": self.auto_desc_ref_no,
            "org_no": self.organism_no,
        })

        # Delete ref_links
        delete_sql = text(f"""
            DELETE FROM {DB_SCHEMA}.ref_link
            WHERE reference_no = :ref_no
            AND tab_name = 'FEATURE'
            AND primary_key IN (
                SELECT feature_no FROM {DB_SCHEMA}.feature
                WHERE organism_no = :org_no
            )
        """)

        self.session.execute(delete_sql, {
            "ref_no": self.auto_desc_ref_no,
            "org_no": self.organism_no,
        })

        self.session.commit()
        logger.info(f"Deleted previous automatic descriptions for {self.strain_abbrev}")

    def load_description(self, feature_no: int, description: str) -> bool:
        """Load description for a feature."""
        # Check if headline already exists
        check_sql = text(f"""
            SELECT headline FROM {DB_SCHEMA}.feature
            WHERE feature_no = :feat_no
        """)
        result = self.session.execute(check_sql, {"feat_no": feature_no}).first()
        if result and result[0]:
            logger.warning(f"Headline already exists for feature_no={feature_no}, skipping")
            return False

        # Update headline
        update_sql = text(f"""
            UPDATE {DB_SCHEMA}.feature
            SET headline = :headline
            WHERE feature_no = :feat_no
        """)

        self.session.execute(update_sql, {
            "headline": description,
            "feat_no": feature_no,
        })

        # Insert ref_link
        insert_sql = text(f"""
            INSERT INTO {DB_SCHEMA}.ref_link
            (reference_no, tab_name, col_name, primary_key, created_by)
            VALUES (:ref_no, 'FEATURE', 'HEADLINE', :pk, :user)
        """)

        try:
            self.session.execute(insert_sql, {
                "ref_no": self.auto_desc_ref_no,
                "pk": feature_no,
                "user": ADMIN_USER,
            })
        except Exception:
            pass  # Ref link may already exist

        self.session.commit()
        return True

    def generate_description(self, feature_no: int, feature_name: str) -> str | None:
        """
        Generate description for a single feature.

        Args:
            feature_no: Feature number
            feature_name: Feature name

        Returns:
            Description string or None
        """
        description = None

        # Try orthology-based GO annotations
        go_terms = self.get_go_annotations(feature_no, self.ortho_go_ref_no)
        if go_terms:
            description = self.create_desc_from_go(go_terms, "Ortholog(s) have ")

        # Try domain-based GO annotations
        if not description:
            go_terms = self.get_go_annotations(feature_no, self.domain_go_ref_no)
            if go_terms:
                description = self.create_desc_from_go(
                    go_terms, "Has domain(s) with predicted "
                )

        # Try ortholog names
        if not description:
            orthologs = self.get_orthologs(feature_no)
            if orthologs:
                description = self.create_desc_from_orthologs(orthologs, "Ortholog of ")

        # Default description
        if not description:
            description = "Protein of unknown function"

        return description

    def run(self) -> dict:
        """Run the automatic description generation."""
        # Delete previous headlines
        if self.load_to_db:
            self.delete_previous_headlines()

        # Get candidate features
        features = self.get_candidate_features()

        descriptions: list[tuple[str, str]] = []

        for feature_no, feature_name in features:
            description = self.generate_description(feature_no, feature_name)

            if description:
                descriptions.append((feature_name, description))

                if self.load_to_db:
                    self.load_description(feature_no, description)
                    self.description_count += 1

        return {
            "total_features": len(features),
            "descriptions_generated": len(descriptions),
            "descriptions_loaded": self.description_count,
            "descriptions": descriptions,
        }


def make_automatic_descriptions(
    strain_abbrev: str, load_to_db: bool = True
) -> bool:
    """
    Main function to generate automatic descriptions.

    Args:
        strain_abbrev: Strain abbreviation
        load_to_db: Whether to load descriptions to database

    Returns:
        True on success, False on failure
    """
    # Set up logging
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "load" / f"AutoDescriptions_{strain_abbrev}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting automatic description generation for {strain_abbrev}")
    logger.info(f"Started at {datetime.now()}")

    try:
        with SessionLocal() as session:
            generator = AutomaticDescriptionGenerator(
                session, strain_abbrev, load_to_db=load_to_db
            )
            results = generator.run()

            # Log descriptions
            for feat_name, desc in results["descriptions"]:
                logger.info(f"{feat_name}\t{desc}")

            logger.info(f"\n{results['descriptions_loaded']} headlines updated successfully")
            logger.info(f"Finished at {datetime.now()}")

            return True

    except Exception as e:
        logger.exception(f"Error generating automatic descriptions: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate automatic descriptions for uncharacterized genes"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate descriptions but don't load to database",
    )

    args = parser.parse_args()

    success = make_automatic_descriptions(args.strain, load_to_db=not args.dry_run)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
