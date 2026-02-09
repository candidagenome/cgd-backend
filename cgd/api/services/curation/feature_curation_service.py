"""
Feature Curation Service - Business logic for creating new features.

Handles creation of new features (ORFs, genes, etc.) with coordinates,
relationships, and references.
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import func, or_, text
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    FeatLocation,
    FeatRelationship,
    GenomeVersion,
    Organism,
    RefLink,
    Reference,
    Seq,
)

logger = logging.getLogger(__name__)


class FeatureCurationError(Exception):
    """Custom exception for feature curation errors."""

    pass


class FeatureCurationService:
    """Service for creating and managing new features."""

    # Valid feature types (from database CODE table or legacy system)
    FEATURE_TYPES = [
        "ORF",
        "blocked_reading_frame",
        "pseudogene",
        "transposable_element_gene",
        "not in systematic sequence",
        "not physically mapped",
        "tRNA gene",
        "ncRNA gene",
        "snoRNA gene",
        "snRNA gene",
        "rRNA gene",
        "telomere",
        "centromere",
        "ARS",
        "ARS consensus sequence",
        "long_terminal_repeat",
        "LTR_retrotransposon",
        "origin_of_replication",
        "matrix_attachment_site",
        "intein_encoding_region",
        "allele",
    ]

    # Valid feature qualifiers
    FEATURE_QUALIFIERS = [
        "Verified",
        "Uncharacterized",
        "Dubious",
        "Silenced",
        "not in systematic sequence",
        "not physically mapped",
    ]

    # Strand values
    STRANDS = ["W", "C"]  # Watson (forward), Crick (reverse)

    # Source for CGD
    SOURCE = "CGD"

    def __init__(self, db: Session):
        self.db = db

    def get_organisms(self) -> list[dict]:
        """Get list of organisms for dropdown."""
        organisms = (
            self.db.query(Organism)
            .order_by(Organism.organism_order)
            .all()
        )

        return [
            {
                "organism_no": org.organism_no,
                "organism_name": org.organism_name,
                "organism_abbrev": org.organism_abbrev,
            }
            for org in organisms
        ]

    def get_chromosomes(self, organism_abbrev: str) -> list[dict]:
        """Get chromosomes for an organism."""
        # Get organism
        organism = (
            self.db.query(Organism)
            .filter(func.upper(Organism.organism_abbrev) == organism_abbrev.upper())
            .first()
        )

        if not organism:
            return []

        # Get chromosome features for this organism
        chromosomes = (
            self.db.query(Feature)
            .filter(
                Feature.organism_no == organism.organism_no,
                or_(
                    Feature.feature_type == "chromosome",
                    Feature.feature_type.like("Chr%"),
                ),
            )
            .order_by(Feature.feature_name)
            .all()
        )

        return [
            {
                "feature_no": chr.feature_no,
                "feature_name": chr.feature_name,
            }
            for chr in chromosomes
        ]

    def check_feature_exists(self, feature_name: str) -> Optional[dict]:
        """
        Check if a feature already exists in the database.

        Returns feature info if found, None if not found.
        """
        feature = (
            self.db.query(Feature)
            .filter(
                or_(
                    func.upper(Feature.feature_name) == feature_name.upper(),
                    func.upper(Feature.gene_name) == feature_name.upper(),
                )
            )
            .first()
        )

        if feature:
            return {
                "feature_no": feature.feature_no,
                "feature_name": feature.feature_name,
                "gene_name": feature.gene_name,
                "feature_type": feature.feature_type,
            }

        return None

    def create_feature(
        self,
        feature_name: str,
        feature_type: str,
        organism_abbrev: str,
        curator_userid: str,
        chromosome_name: Optional[str] = None,
        start_coord: Optional[int] = None,
        stop_coord: Optional[int] = None,
        strand: Optional[str] = None,
        qualifiers: Optional[list[str]] = None,
        reference_no: Optional[int] = None,
    ) -> int:
        """
        Create a new feature with optional coordinates and references.

        Returns the feature_no of the created feature.
        """
        # Validate feature doesn't already exist
        existing = self.check_feature_exists(feature_name)
        if existing:
            raise FeatureCurationError(
                f"Feature '{feature_name}' already exists as '{existing['feature_name']}'"
            )

        # Validate feature type
        if feature_type not in self.FEATURE_TYPES:
            raise FeatureCurationError(
                f"Invalid feature type: {feature_type}. "
                f"Valid types: {', '.join(self.FEATURE_TYPES[:5])}..."
            )

        # Get organism
        organism = (
            self.db.query(Organism)
            .filter(func.upper(Organism.organism_abbrev) == organism_abbrev.upper())
            .first()
        )

        if not organism:
            raise FeatureCurationError(f"Organism '{organism_abbrev}' not found")

        # Validate coordinates if provided
        if chromosome_name:
            if not start_coord or not stop_coord:
                raise FeatureCurationError(
                    "Start and stop coordinates are required when chromosome is specified"
                )

            # Validate strand for certain feature types
            requires_strand = feature_type not in [
                "not in systematic sequence",
                "not physically mapped",
                "ARS",
                "ARS consensus sequence",
                "telomere",
            ]
            if requires_strand and not strand:
                raise FeatureCurationError(
                    f"Strand is required for feature type '{feature_type}'"
                )

            if strand and strand not in self.STRANDS:
                raise FeatureCurationError(
                    f"Invalid strand: {strand}. Use 'W' (Watson) or 'C' (Crick)"
                )

            # Validate coordinate/strand consistency
            if strand == "C" and start_coord < stop_coord:
                raise FeatureCurationError(
                    "For Crick strand, start_coord should be > stop_coord"
                )
            if strand == "W" and start_coord > stop_coord:
                raise FeatureCurationError(
                    "For Watson strand, start_coord should be < stop_coord"
                )

        # For "not in systematic" types, gene_name = feature_name
        gene_name = None
        if feature_type in ["not physically mapped", "not in systematic sequence"]:
            gene_name = feature_name

        # Generate dbxref_id (typically same as feature_name for CGD)
        dbxref_id = feature_name

        # Create feature
        feature = Feature(
            organism_no=organism.organism_no,
            feature_name=feature_name,
            dbxref_id=dbxref_id,
            feature_type=feature_type,
            source=self.SOURCE,
            gene_name=gene_name,
            created_by=curator_userid,
        )
        self.db.add(feature)
        self.db.flush()  # Get feature_no

        logger.info(
            f"Created feature {feature.feature_no}: {feature_name} ({feature_type})"
        )

        # Add coordinates if provided
        if chromosome_name and start_coord and stop_coord:
            self._add_feature_location(
                feature_no=feature.feature_no,
                chromosome_name=chromosome_name,
                organism_abbrev=organism_abbrev,
                start_coord=start_coord,
                stop_coord=stop_coord,
                strand=strand,
                curator_userid=curator_userid,
            )

        # Add reference if provided
        if reference_no:
            self._add_feature_reference(
                feature_no=feature.feature_no,
                reference_no=reference_no,
                col_name="FEATURE_NO",
                curator_userid=curator_userid,
            )

        self.db.commit()

        return feature.feature_no

    def _add_feature_location(
        self,
        feature_no: int,
        chromosome_name: str,
        organism_abbrev: str,
        start_coord: int,
        stop_coord: int,
        strand: Optional[str],
        curator_userid: str,
    ) -> int:
        """Add location/coordinates for a feature."""
        # Get chromosome feature
        chromosome = (
            self.db.query(Feature)
            .filter(func.upper(Feature.feature_name) == chromosome_name.upper())
            .first()
        )

        if not chromosome:
            raise FeatureCurationError(f"Chromosome '{chromosome_name}' not found")

        # Get chromosome's root sequence
        chr_seq = (
            self.db.query(Seq)
            .filter(
                Seq.feature_no == chromosome.feature_no,
                Seq.seq_type == "genomic",
                Seq.is_seq_current == "Y",
            )
            .first()
        )

        if not chr_seq:
            raise FeatureCurationError(
                f"No current genomic sequence found for chromosome '{chromosome_name}'"
            )

        # Get current genome version
        organism = (
            self.db.query(Organism)
            .filter(func.upper(Organism.organism_abbrev) == organism_abbrev.upper())
            .first()
        )

        genome_version = (
            self.db.query(GenomeVersion)
            .filter(
                GenomeVersion.organism_no == organism.organism_no,
                GenomeVersion.is_ver_current == "Y",
            )
            .first()
        )

        if not genome_version:
            raise FeatureCurationError(
                f"No current genome version found for organism '{organism_abbrev}'"
            )

        # Extract sequence for this feature
        if strand == "C":
            # Crick strand - reverse complement
            seq_start = min(start_coord, stop_coord)
            seq_end = max(start_coord, stop_coord)
        else:
            seq_start = min(start_coord, stop_coord)
            seq_end = max(start_coord, stop_coord)

        # Get sequence from chromosome (simplified - actual implementation would
        # extract subsequence and possibly reverse complement)
        seq_length = abs(stop_coord - start_coord) + 1

        # Create SEQ entry for this feature
        feature_seq = Seq(
            feature_no=feature_no,
            genome_version_no=genome_version.genome_version_no,
            seq_version=datetime.now(),
            seq_type="genomic",
            source=chr_seq.source,
            is_seq_current="Y",
            seq_length=seq_length,
            residues="N" * seq_length,  # Placeholder - actual sequence TBD
            created_by=curator_userid,
        )
        self.db.add(feature_seq)
        self.db.flush()

        # Create FEAT_LOCATION entry
        feat_location = FeatLocation(
            feature_no=feature_no,
            root_seq_no=chr_seq.seq_no,
            seq_no=feature_seq.seq_no,
            coord_version=datetime.now(),
            start_coord=start_coord,
            stop_coord=stop_coord,
            strand=strand or "W",
            is_loc_current="Y",
            created_by=curator_userid,
        )
        self.db.add(feat_location)
        self.db.flush()

        # Create FEAT_RELATIONSHIP (child of chromosome)
        feat_rel = FeatRelationship(
            parent_feature_no=chromosome.feature_no,
            child_feature_no=feature_no,
            relationship_type="part of",
            rank=1,
            created_by=curator_userid,
        )
        self.db.add(feat_rel)

        logger.info(
            f"Added location for feature {feature_no}: "
            f"{chromosome_name}:{start_coord}-{stop_coord} ({strand})"
        )

        return feat_location.feat_location_no

    def _add_feature_reference(
        self,
        feature_no: int,
        reference_no: int,
        col_name: str,
        curator_userid: str,
    ) -> int:
        """Add a reference link to a feature."""
        # Validate reference exists
        reference = (
            self.db.query(Reference)
            .filter(Reference.reference_no == reference_no)
            .first()
        )

        if not reference:
            raise FeatureCurationError(f"Reference {reference_no} not found")

        # Check if link already exists
        existing = (
            self.db.query(RefLink)
            .filter(
                RefLink.tab_name == "FEATURE",
                RefLink.primary_key == feature_no,
                RefLink.reference_no == reference_no,
                RefLink.col_name == col_name,
            )
            .first()
        )

        if existing:
            return existing.ref_link_no

        # Create ref link
        ref_link = RefLink(
            reference_no=reference_no,
            tab_name="FEATURE",
            primary_key=feature_no,
            col_name=col_name,
            created_by=curator_userid,
        )
        self.db.add(ref_link)
        self.db.flush()

        logger.info(f"Added reference {reference_no} to feature {feature_no}")

        return ref_link.ref_link_no

    def delete_feature(self, feature_no: int, curator_userid: str) -> None:
        """
        Delete a feature.

        Warning: This will fail if the feature has linked annotations.
        """
        feature = (
            self.db.query(Feature)
            .filter(Feature.feature_no == feature_no)
            .first()
        )

        if not feature:
            raise FeatureCurationError(f"Feature {feature_no} not found")

        # Check for linked annotations (GO, phenotype, etc.)
        # This would be expanded based on actual constraints

        try:
            # Delete related records first
            self.db.query(FeatLocation).filter(
                FeatLocation.feature_no == feature_no
            ).delete()
            self.db.query(FeatRelationship).filter(
                FeatRelationship.child_feature_no == feature_no
            ).delete()
            self.db.query(RefLink).filter(
                RefLink.tab_name == "FEATURE",
                RefLink.primary_key == feature_no,
            ).delete()
            self.db.query(Seq).filter(Seq.feature_no == feature_no).delete()

            # Delete the feature
            self.db.delete(feature)
            self.db.commit()

            logger.info(f"Deleted feature {feature_no} by {curator_userid}")

        except Exception as e:
            self.db.rollback()
            raise FeatureCurationError(
                f"Cannot delete feature: {str(e)}. "
                "It may have linked annotations that must be removed first."
            )
