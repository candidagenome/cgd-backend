"""
Sequence Curation Service - Update chromosome/contig sequences.

Mirrors functionality from legacy UpdateRootSequence.pm for curators to
insert, delete, or substitute nucleotides in root sequences.
"""

import logging
from typing import Optional
from datetime import datetime

from sqlalchemy import func, text, and_, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    FeatLocation,
    Seq,
    Note,
    RefLink,
    Reference,
    Dbxref,
)

logger = logging.getLogger(__name__)

# Constants
NOTE_TYPE = "Sequence change"
SOURCE = "CGD"


class SequenceCurationService:
    """Service for chromosome/contig sequence curation."""

    def __init__(self, db: Session):
        self.db = db

    def get_root_sequences(self) -> list[dict]:
        """
        Get all root sequences (chromosomes/contigs) grouped by assembly.

        Returns:
            List of root sequences with assembly grouping
        """
        # Get root features (chromosomes, contigs, etc.) with their sequences
        results = (
            self.db.query(
                Feature.feature_no,
                Feature.feature_name,
                Feature.feature_type,
                Seq.seq_no,
                Seq.seq_length,
                Seq.source.label("seq_source"),
            )
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                Feature.feature_type.in_(["chromosome", "contig", "plasmid"]),
                func.upper(Seq.seq_type) == "GENOMIC",
                Seq.is_seq_current == "Y",
            )
            .order_by(Seq.source, Feature.feature_name)
            .all()
        )

        # Group by assembly/source
        grouped = {}
        for row in results:
            source = row.seq_source or "Unknown"
            if source not in grouped:
                grouped[source] = []
            grouped[source].append({
                "feature_no": row.feature_no,
                "feature_name": row.feature_name,
                "feature_type": row.feature_type,
                "seq_no": row.seq_no,
                "seq_length": row.seq_length,
            })

        return [
            {"assembly": assembly, "sequences": seqs}
            for assembly, seqs in grouped.items()
        ]

    def get_sequence_segment(
        self,
        feature_name: str,
        start: int,
        length: int = 100,
    ) -> dict:
        """
        Get a segment of sequence around a coordinate.

        Args:
            feature_name: Chromosome/contig name
            start: Starting coordinate (1-based)
            length: Number of nucleotides to return

        Returns:
            Sequence segment with metadata
        """
        # Get the current sequence for this feature
        result = (
            self.db.query(Feature, Seq)
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                func.upper(Feature.feature_name) == feature_name.upper(),
                func.upper(Seq.seq_type) == "GENOMIC",
                Seq.is_seq_current == "Y",
            )
            .first()
        )

        if not result:
            return None

        feature, seq = result

        # Validate coordinates
        if start < 1:
            start = 1
        if start > seq.seq_length:
            return {
                "feature_name": feature.feature_name,
                "seq_length": seq.seq_length,
                "start": start,
                "end": start,
                "sequence": "",
                "error": f"Start coordinate exceeds sequence length ({seq.seq_length})",
            }

        # Calculate actual end position
        end = min(start + length - 1, seq.seq_length)

        # Extract sequence segment (1-based coordinates)
        segment = seq.residues[start - 1:end]

        return {
            "feature_name": feature.feature_name,
            "feature_no": feature.feature_no,
            "seq_no": seq.seq_no,
            "seq_length": seq.seq_length,
            "start": start,
            "end": end,
            "sequence": segment,
        }

    def preview_changes(
        self,
        feature_name: str,
        changes: list[dict],
    ) -> dict:
        """
        Preview the effect of sequence changes without committing.

        Args:
            feature_name: Chromosome/contig name
            changes: List of changes, each with type (insertion/deletion/substitution)
                     and relevant coordinates/sequences

        Returns:
            Preview of old vs new sequence and affected features
        """
        # Get current sequence
        result = (
            self.db.query(Feature, Seq)
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                func.upper(Feature.feature_name) == feature_name.upper(),
                func.upper(Seq.seq_type) == "GENOMIC",
                Seq.is_seq_current == "Y",
            )
            .first()
        )

        if not result:
            return {"error": f"Feature {feature_name} not found"}

        feature, seq = result

        # Process changes to compute new sequence
        new_sequence = seq.residues
        net_change = 0  # Track cumulative position shift

        sorted_changes = sorted(changes, key=lambda c: c.get("position", 0))

        change_details = []

        for change in sorted_changes:
            change_type = change.get("type")
            position = change.get("position", 0)

            if change_type == "insertion":
                # Insert after the given position
                insert_seq = change.get("sequence", "").upper()
                adjusted_pos = position + net_change

                old_context = self._get_context(new_sequence, adjusted_pos, 20)
                new_sequence = (
                    new_sequence[:adjusted_pos] +
                    insert_seq +
                    new_sequence[adjusted_pos:]
                )
                new_context = self._get_context(new_sequence, adjusted_pos, 20 + len(insert_seq))

                net_change += len(insert_seq)
                change_details.append({
                    "type": "insertion",
                    "position": position,
                    "sequence": insert_seq,
                    "length": len(insert_seq),
                    "old_context": old_context,
                    "new_context": new_context,
                })

            elif change_type == "deletion":
                # Delete from start to end (1-based, inclusive)
                start = change.get("start", 0)
                end = change.get("end", start)
                adjusted_start = start + net_change - 1  # Convert to 0-based
                adjusted_end = end + net_change

                deleted_seq = new_sequence[adjusted_start:adjusted_end]
                old_context = self._get_context(new_sequence, adjusted_start, 20 + len(deleted_seq))

                new_sequence = new_sequence[:adjusted_start] + new_sequence[adjusted_end:]
                new_context = self._get_context(new_sequence, adjusted_start, 20)

                deletion_length = end - start + 1
                net_change -= deletion_length
                change_details.append({
                    "type": "deletion",
                    "start": start,
                    "end": end,
                    "deleted_sequence": deleted_seq,
                    "length": deletion_length,
                    "old_context": old_context,
                    "new_context": new_context,
                })

            elif change_type == "substitution":
                # Replace from start to end with new sequence
                start = change.get("start", 0)
                end = change.get("end", start)
                new_seq = change.get("sequence", "").upper()
                adjusted_start = start + net_change - 1  # Convert to 0-based
                adjusted_end = end + net_change

                old_seq = new_sequence[adjusted_start:adjusted_end]
                old_context = self._get_context(new_sequence, adjusted_start, 20 + len(old_seq))

                new_sequence = (
                    new_sequence[:adjusted_start] +
                    new_seq +
                    new_sequence[adjusted_end:]
                )
                new_context = self._get_context(new_sequence, adjusted_start, 20 + len(new_seq))

                length_diff = len(new_seq) - (end - start + 1)
                net_change += length_diff
                change_details.append({
                    "type": "substitution",
                    "start": start,
                    "end": end,
                    "old_sequence": old_seq,
                    "new_sequence": new_seq,
                    "length_change": length_diff,
                    "old_context": old_context,
                    "new_context": new_context,
                })

        # Get affected features
        affected_features = self._get_affected_features(
            seq.seq_no, changes, net_change
        )

        return {
            "feature_name": feature.feature_name,
            "feature_no": feature.feature_no,
            "seq_no": seq.seq_no,
            "old_length": seq.seq_length,
            "new_length": len(new_sequence),
            "net_change": net_change,
            "changes": change_details,
            "affected_features": affected_features,
        }

    def _get_context(self, sequence: str, position: int, context_size: int) -> str:
        """Get sequence context around a position."""
        start = max(0, position - 10)
        end = min(len(sequence), position + context_size + 10)
        return sequence[start:end]

    def _get_affected_features(
        self,
        root_seq_no: int,
        changes: list[dict],
        net_change: int,
    ) -> list[dict]:
        """
        Get features that would be affected by the sequence changes.

        A feature is affected if its coordinates overlap with any change position.
        """
        # Get min and max positions of all changes
        positions = []
        for change in changes:
            if change.get("type") == "insertion":
                positions.append(change.get("position", 0))
            else:
                positions.append(change.get("start", 0))
                positions.append(change.get("end", 0))

        if not positions:
            return []

        min_pos = min(positions)
        max_pos = max(positions)

        # Find features that overlap with the change region
        results = (
            self.db.query(Feature, FeatLocation)
            .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
            .filter(
                FeatLocation.root_seq_no == root_seq_no,
                FeatLocation.is_loc_current == "Y",
                or_(
                    # Feature overlaps the change region
                    and_(
                        FeatLocation.start_coord <= max_pos,
                        FeatLocation.stop_coord >= min_pos,
                    ),
                    # Feature is downstream (will have coordinates shifted)
                    FeatLocation.start_coord > max_pos,
                ),
            )
            .order_by(FeatLocation.start_coord)
            .limit(100)
            .all()
        )

        affected = []
        for feature, loc in results:
            is_overlapping = (loc.start_coord <= max_pos and loc.stop_coord >= min_pos)

            affected.append({
                "feature_no": feature.feature_no,
                "feature_name": feature.feature_name,
                "gene_name": feature.gene_name,
                "feature_type": feature.feature_type,
                "start_coord": loc.start_coord,
                "stop_coord": loc.stop_coord,
                "strand": loc.strand,
                "is_overlapping": is_overlapping,
                "is_downstream": loc.start_coord > max_pos,
                "new_start": loc.start_coord if is_overlapping else loc.start_coord + net_change,
                "new_stop": loc.stop_coord if is_overlapping else loc.stop_coord + net_change,
            })

        return affected

    def get_nearby_features(
        self,
        feature_name: str,
        position: int,
        range_size: int = 5000,
    ) -> list[dict]:
        """
        Get features near a given coordinate.

        Args:
            feature_name: Chromosome/contig name
            position: Coordinate to search around
            range_size: Size of range to search (default 5000bp)

        Returns:
            List of features near the position
        """
        # Get the root sequence
        result = (
            self.db.query(Feature, Seq)
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                func.upper(Feature.feature_name) == feature_name.upper(),
                func.upper(Seq.seq_type) == "GENOMIC",
                Seq.is_seq_current == "Y",
            )
            .first()
        )

        if not result:
            return []

        feature, seq = result

        # Find features in range
        min_pos = max(1, position - range_size)
        max_pos = min(seq.seq_length, position + range_size)

        results = (
            self.db.query(Feature, FeatLocation)
            .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
            .filter(
                FeatLocation.root_seq_no == seq.seq_no,
                FeatLocation.is_loc_current == "Y",
                FeatLocation.start_coord <= max_pos,
                FeatLocation.stop_coord >= min_pos,
            )
            .order_by(FeatLocation.start_coord)
            .all()
        )

        return [
            {
                "feature_no": f.feature_no,
                "feature_name": f.feature_name,
                "gene_name": f.gene_name,
                "feature_type": f.feature_type,
                "start_coord": loc.start_coord,
                "stop_coord": loc.stop_coord,
                "strand": loc.strand,
            }
            for f, loc in results
        ]
