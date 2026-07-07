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
    NoteLink,
    RefLink,
    Reference,
    Dbxref,
    Organism,
    SeqChangeArchive,
)

logger = logging.getLogger(__name__)

# Constants
NOTE_TYPE = "Sequence change"
SOURCE = "CGD"

# Feature types whose protein sequence is regenerated after a root-sequence edit.
CODING_FEATURE_TYPES = {"ORF", "pseudogene"}

# Organisms that use translation table 12 (CTG -> Ser). Mirrors
# locus_service.TRANSLATION_TABLE_12_ORGANISMS; kept local to avoid importing
# the heavy locus_service module here.
TRANSLATION_TABLE_12_ORGANISMS = {
    "Candida albicans",
    "Candida albicans SC5314",
    "Candida dubliniensis",
    "Candida dubliniensis CD36",
    "Candida tropicalis",
    "Candida tropicalis MYA-3404",
    "Candida parapsilosis",
    "Candida parapsilosis CDC317",
    "Lodderomyces elongisporus",
    "Lodderomyces elongisporus NRRL YB-4239",
    "Candida auris",
}

# Standard genetic code (kept local; the shared cgd.utils.sequence module is not
# imported here because its package __init__ is not Python 3.9 compatible).
CODON_TABLE = {
    "TTT": "F", "TTC": "F", "TTA": "L", "TTG": "L",
    "TCT": "S", "TCC": "S", "TCA": "S", "TCG": "S",
    "TAT": "Y", "TAC": "Y", "TAA": "*", "TAG": "*",
    "TGT": "C", "TGC": "C", "TGA": "*", "TGG": "W",
    "CTT": "L", "CTC": "L", "CTA": "L", "CTG": "L",
    "CCT": "P", "CCC": "P", "CCA": "P", "CCG": "P",
    "CAT": "H", "CAC": "H", "CAA": "Q", "CAG": "Q",
    "CGT": "R", "CGC": "R", "CGA": "R", "CGG": "R",
    "ATT": "I", "ATC": "I", "ATA": "I", "ATG": "M",
    "ACT": "T", "ACC": "T", "ACA": "T", "ACG": "T",
    "AAT": "N", "AAC": "N", "AAA": "K", "AAG": "K",
    "AGT": "S", "AGC": "S", "AGA": "R", "AGG": "R",
    "GTT": "V", "GTC": "V", "GTA": "V", "GTG": "V",
    "GCT": "A", "GCC": "A", "GCA": "A", "GCG": "A",
    "GAT": "D", "GAC": "D", "GAA": "E", "GAG": "E",
    "GGT": "G", "GGC": "G", "GGA": "G", "GGG": "G",
}

# Standard code with the table-12 CTG -> Ser reassignment.
CODON_TABLE_12 = {**CODON_TABLE, "CTG": "S"}

_COMPLEMENT = {
    "A": "T", "T": "A", "G": "C", "C": "G", "N": "N",
    "R": "Y", "Y": "R", "M": "K", "K": "M", "S": "S", "W": "W",
    "B": "V", "V": "B", "D": "H", "H": "D",
}


def reverse_complement(seq: str) -> str:
    return "".join(_COMPLEMENT.get(b, b) for b in reversed(seq.upper()))


def extract_subsequence(seq: str, start: int, stop: int, strand: str) -> str:
    """Extract 1-based inclusive coords; reverse-complement for Crick strand."""
    lo, hi = (start, stop) if start <= stop else (stop, start)
    subseq = seq[lo - 1:hi]
    if str(strand).upper() in ("C", "-"):
        subseq = reverse_complement(subseq)
    return subseq


def translate_dna(dna_seq: str, codon_table: dict) -> str:
    dna_seq = dna_seq.upper()
    return "".join(
        codon_table.get(dna_seq[i:i + 3], "X")
        for i in range(0, len(dna_seq) - 2, 3)
    )


class SequenceCurationError(Exception):
    """Raised when a sequence-curation apply operation is invalid or fails."""


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

    # ------------------------------------------------------------------
    # Commit path (ported from legacy UpdateRootSequence.pm / RootSequence.pm)
    #
    # v1 scope: equal-length SUBSTITUTIONS only. Because no base is added or
    # removed, every feature keeps its coordinates and only needs its
    # feat_location re-pointed to the new root-sequence version; the coordinate
    # propagation machinery of the legacy tool (for insertions/deletions) is
    # intentionally not reproduced here.
    # ------------------------------------------------------------------
    def apply_changes(
        self,
        feature_name: str,
        changes: list[dict],
        note_text: str,
        curator_userid: str,
        reference_nos: Optional[list[int]] = None,
        dry_run: bool = True,
    ) -> dict:
        """
        Apply equal-length substitution edits to a root (chromosome/contig)
        sequence, following the legacy UpdateRootSequence semantics.

        Steps (single transaction):
          1. Validate every change is an equal-length substitution.
          2. Verify the current residues at each window match what the caller
             believes is there (guards against a stale preview).
          3. Insert a new current SEQ version with corrected residues and
             deprecate the old one.
          4. Re-point every current feat_location on the old root_seq to the
             new SEQ (and the root feature's own seq_no).
          5. Regenerate genomic (and protein, for coding features) sequences
             for any feature whose residues actually changed.
          6. Record seq_change_archive rows + a note (+ optional references).

        With dry_run=True the transaction is rolled back and a plan is returned.
        """
        reference_nos = reference_nos or []
        creator = (curator_userid or "")[:12]

        # --- 1. validate scope ---
        if not changes:
            raise SequenceCurationError("No changes supplied.")
        subs = []
        for c in changes:
            if c.get("type") != "substitution":
                raise SequenceCurationError(
                    "This tool currently supports substitution changes only "
                    "(equal length). Insertions/deletions are not yet enabled."
                )
            start = int(c["start"])
            end = int(c["end"])
            new_frag = (c.get("sequence") or "").upper()
            if end < start:
                raise SequenceCurationError(
                    f"Substitution end ({end}) is before start ({start})."
                )
            if len(new_frag) != (end - start + 1):
                raise SequenceCurationError(
                    f"Substitution at {start}-{end} changes length "
                    f"({end - start + 1} -> {len(new_frag)}); only equal-length "
                    "substitutions are supported."
                )
            subs.append({"start": start, "end": end, "new": new_frag,
                         "expected_old": (c.get("old_sequence") or "").upper() or None})
        if not note_text or not note_text.strip():
            raise SequenceCurationError("A note describing the change is required.")

        # --- 2. load current root sequence ---
        row = (
            self.db.query(Feature, Seq)
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                func.upper(Feature.feature_name) == feature_name.upper(),
                func.upper(Seq.seq_type) == "GENOMIC",
                Seq.is_seq_current == "Y",
            )
            .first()
        )
        if not row:
            raise SequenceCurationError(
                f"No current genomic sequence found for '{feature_name}'."
            )
        chr_feature, old_seq = row
        old_root_seq_no = old_seq.seq_no
        old_residues = old_seq.residues
        seq_len = len(old_residues)

        # organism -> translation table selection for protein regeneration
        organism = (
            self.db.query(Organism)
            .filter(Organism.organism_no == chr_feature.organism_no)
            .first()
        )
        use_table_12 = bool(organism and organism.organism_name in TRANSLATION_TABLE_12_ORGANISMS)

        # --- verify + compute new residues ---
        new_residues = old_residues
        archive_specs = []
        for s in subs:
            cur_old = old_residues[s["start"] - 1:s["end"]].upper()
            if s["expected_old"] and s["expected_old"] != cur_old:
                raise SequenceCurationError(
                    f"Current residues at {s['start']}-{s['end']} are '{cur_old}', "
                    f"but the preview expected '{s['expected_old']}'. The reference "
                    "may have changed; re-run the preview."
                )
            new_residues = new_residues[:s["start"] - 1] + s["new"] + new_residues[s["end"]:]
            archive_specs.append({"start": s["start"], "end": s["end"],
                                  "old": cur_old, "new": s["new"]})
        if len(new_residues) != seq_len:
            raise SequenceCurationError(
                "Internal error: corrected sequence length changed; aborting."
            )
        if new_residues == old_residues:
            raise SequenceCurationError("The edits produce no change to the sequence.")

        min_pos = min(s["start"] for s in subs)
        max_pos = max(s["end"] for s in subs)

        # --- features overlapping the edited window (residues may change) ---
        overlap_rows = (
            self.db.query(Feature, FeatLocation)
            .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
            .filter(
                FeatLocation.root_seq_no == old_root_seq_no,
                FeatLocation.is_loc_current == "Y",
                func.least(FeatLocation.start_coord, FeatLocation.stop_coord) <= max_pos,
                func.greatest(FeatLocation.start_coord, FeatLocation.stop_coord) >= min_pos,
                Feature.feature_no != chr_feature.feature_no,
            )
            .all()
        )

        # ================= writes =================
        now = datetime.now()

        # 3. insert new current root SEQ, deprecate old
        new_root_seq = Seq(
            feature_no=chr_feature.feature_no,
            seq_version=now,
            seq_type="genomic",
            genome_version_no=old_seq.genome_version_no,
            is_seq_current="Y",
            seq_length=seq_len,
            residues=new_residues,
            source=old_seq.source,
            created_by=creator,
        )
        self.db.add(new_root_seq)
        self.db.flush()
        new_root_seq_no = new_root_seq.seq_no

        old_seq.is_seq_current = "N"

        # 4. re-point all current feat_locations on this root to the new SEQ
        repointed = (
            self.db.query(FeatLocation)
            .filter(
                FeatLocation.root_seq_no == old_root_seq_no,
                FeatLocation.is_loc_current == "Y",
            )
            .update({FeatLocation.root_seq_no: new_root_seq_no},
                    synchronize_session=False)
        )
        # the root feature's own feat_location.seq_no also points at the root SEQ
        self.db.query(FeatLocation).filter(
            FeatLocation.feature_no == chr_feature.feature_no,
            FeatLocation.is_loc_current == "Y",
        ).update({FeatLocation.seq_no: new_root_seq_no}, synchronize_session=False)

        # 5. regenerate genomic/protein for features whose residues changed
        regenerated = self._regenerate_feature_sequences(
            overlap_rows, old_residues, new_residues, now, creator, use_table_12
        )

        # 6. seq_change_archive + note (+ references)
        # NOTE_UK is UNIQUE(note_type, note): the same curatorial note text may
        # already exist (e.g. the identical note applied to the other haplotype).
        # Reuse it instead of inserting a duplicate, which would violate NOTE_UK.
        note_text_clean = note_text.strip()
        note = (
            self.db.query(Note)
            .filter(Note.note_type == NOTE_TYPE, Note.note == note_text_clean)
            .first()
        )
        if note is None:
            note = Note(note=note_text_clean, note_type=NOTE_TYPE, created_by=creator)
            self.db.add(note)
            self.db.flush()
        for spec in archive_specs:
            arch = SeqChangeArchive(
                seq_no=old_root_seq_no,
                seq_change_type="Substitution",
                change_start_coord=spec["start"],
                change_stop_coord=spec["end"],
                old_seq=spec["old"],
                new_seq=spec["new"],
                created_by=creator,
            )
            self.db.add(arch)
            self.db.flush()
            self.db.add(NoteLink(
                note_no=note.note_no,
                tab_name="SEQ_CHANGE_ARCHIVE",
                primary_key=arch.seq_change_archive_no,
                created_by=creator,
            ))
        for ref_no in reference_nos:
            # REF_LINK_UK is UNIQUE(tab_name, primary_key, reference_no, col_name):
            # when the note is reused, its ref links may already exist, so only
            # add ones that are missing.
            existing_ref_link = (
                self.db.query(RefLink)
                .filter(
                    RefLink.reference_no == ref_no,
                    RefLink.tab_name == "NOTE",
                    RefLink.col_name == "NOTE_NO",
                    RefLink.primary_key == note.note_no,
                )
                .first()
            )
            if existing_ref_link is None:
                self.db.add(RefLink(
                    reference_no=ref_no,
                    tab_name="NOTE",
                    col_name="NOTE_NO",
                    primary_key=note.note_no,
                    created_by=creator,
                ))

        plan = {
            "feature_name": chr_feature.feature_name,
            "feature_no": chr_feature.feature_no,
            "old_root_seq_no": old_root_seq_no,
            "new_root_seq_no": new_root_seq_no,
            "seq_length": seq_len,
            "substitutions": archive_specs,
            "feat_locations_repointed": repointed,
            "features_regenerated": regenerated,
            "note_no": note.note_no,
            "reference_nos": reference_nos,
            "dry_run": dry_run,
        }

        if dry_run:
            self.db.rollback()
            logger.info("apply_changes dry-run rolled back for %s", feature_name)
        else:
            self.db.commit()
            logger.info(
                "apply_changes committed for %s: new_root_seq_no=%s, %d feat_locations re-pointed, %d features regenerated",
                feature_name, new_root_seq_no, repointed, len(regenerated),
            )
        return plan

    def _regenerate_feature_sequences(
        self, overlap_rows, old_residues, new_residues, now, creator, use_table_12,
    ) -> list[dict]:
        """Regenerate genomic (+protein) SEQ for features whose residues changed."""
        regenerated = []
        for feature, loc in overlap_rows:
            new_genomic = extract_subsequence(
                new_residues, loc.start_coord, loc.stop_coord, loc.strand
            )
            cur_genomic_seq = (
                self.db.query(Seq)
                .filter(
                    Seq.feature_no == feature.feature_no,
                    func.upper(Seq.seq_type) == "GENOMIC",
                    Seq.is_seq_current == "Y",
                )
                .first()
            )
            # No residue change for this feature -> nothing to regenerate.
            if cur_genomic_seq and cur_genomic_seq.residues == new_genomic:
                continue

            # new genomic version
            if cur_genomic_seq:
                cur_genomic_seq.is_seq_current = "N"
            new_gseq = Seq(
                feature_no=feature.feature_no,
                seq_version=now,
                seq_type="genomic",
                genome_version_no=(cur_genomic_seq.genome_version_no
                                   if cur_genomic_seq else None),
                is_seq_current="Y",
                seq_length=len(new_genomic),
                residues=new_genomic,
                source=(cur_genomic_seq.source if cur_genomic_seq else None),
                created_by=creator,
            )
            self.db.add(new_gseq)
            self.db.flush()
            # point this feature's current feat_location at the new genomic seq
            self.db.query(FeatLocation).filter(
                FeatLocation.feature_no == feature.feature_no,
                FeatLocation.is_loc_current == "Y",
            ).update({FeatLocation.seq_no: new_gseq.seq_no}, synchronize_session=False)

            entry = {
                "feature_no": feature.feature_no,
                "feature_name": feature.feature_name,
                "feature_type": feature.feature_type,
                "new_genomic_seq_no": new_gseq.seq_no,
                "genomic_length": len(new_genomic),
            }

            # protein for coding features
            if feature.feature_type in CODING_FEATURE_TYPES:
                protein = translate_dna(
                    new_genomic, codon_table=(CODON_TABLE_12 if use_table_12 else CODON_TABLE)
                )
                if protein.endswith("*"):
                    protein = protein[:-1]
                cur_prot = (
                    self.db.query(Seq)
                    .filter(
                        Seq.feature_no == feature.feature_no,
                        func.upper(Seq.seq_type) == "PROTEIN",
                        Seq.is_seq_current == "Y",
                    )
                    .first()
                )
                if cur_prot:
                    cur_prot.is_seq_current = "N"
                new_pseq = Seq(
                    feature_no=feature.feature_no,
                    seq_version=now,
                    seq_type="protein",
                    genome_version_no=(cur_prot.genome_version_no if cur_prot
                                       else new_gseq.genome_version_no),
                    is_seq_current="Y",
                    seq_length=len(protein),
                    residues=protein,
                    source=(cur_prot.source if cur_prot else new_gseq.source),
                    created_by=creator,
                )
                self.db.add(new_pseq)
                self.db.flush()
                entry["new_protein_seq_no"] = new_pseq.seq_no
                entry["protein_length"] = len(protein)

            regenerated.append(entry)
        return regenerated
