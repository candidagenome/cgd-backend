"""
Sequence Curation Service - Update chromosome/contig sequences.

Mirrors functionality from legacy UpdateRootSequence.pm for curators to
insert, delete, or substitute nucleotides in root sequences.
"""

import logging
from dataclasses import dataclass
from typing import Optional
from datetime import datetime

from sqlalchemy import func, text
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


# IUPAC nucleotide codes accepted in inserted/substituted fragments (as in the
# legacy RootSequence.pm _check_sequence_fragment).
SEQ_FRAGMENT_CHARS = set("ACGTRYKMSWBDHVN")

ARCHIVE_CHANGE_TYPES = {
    "insertion": "Insertion",
    "deletion": "Deletion",
    "substitution": "Substitution",
}


@dataclass
class SequenceEdit:
    """
    One curator edit, expressed (as in the legacy RootSequence.pm) as the
    replacement of the original window [start, end] by ``new``. All
    coordinates are 1-based on the sequence as it was before any edit, so
    edits never depend on the order they were entered in. An insertion after
    position p is the empty window [p + 1, p].
    """

    type: str
    start: int
    end: int
    new: str
    old: str
    position: Optional[int] = None    # insertions only: "insert after"

    @property
    def delta(self) -> int:
        return len(self.new) - (self.end - self.start + 1)


def normalize_changes(changes: list[dict], residues: Optional[str] = None) -> list[SequenceEdit]:
    """
    Validate curator changes and return them as SequenceEdits sorted by
    position. With ``residues`` the coordinates are range-checked, the old
    fragments are filled in, and any ``old_sequence`` the caller sends (what
    its preview showed) must still match the current residues.
    """
    seq_len = len(residues) if residues is not None else None
    edits = []
    for c in changes:
        change_type = c.get("type")
        frag = (c.get("sequence") or "").upper()
        if change_type == "insertion":
            if c.get("position") is None or not frag:
                raise SequenceCurationError("An insertion needs a position and a sequence.")
            position = int(c["position"])
            if position < 0 or (seq_len is not None and position > seq_len):
                raise SequenceCurationError(
                    f"Insertion position {position} is outside the sequence (0-{seq_len})."
                )
            edit = SequenceEdit("insertion", position + 1, position, frag, "", position)
        elif change_type in ("deletion", "substitution"):
            if c.get("start") is None or c.get("end") is None:
                raise SequenceCurationError(f"A {change_type} needs start and end coordinates.")
            start, end = int(c["start"]), int(c["end"])
            if change_type == "deletion":
                frag = ""
            elif not frag:
                raise SequenceCurationError("A substitution needs a replacement sequence.")
            if end < start:
                raise SequenceCurationError(
                    f"{change_type.capitalize()} end ({end}) is before start ({start})."
                )
            if start < 1 or (seq_len is not None and end > seq_len):
                raise SequenceCurationError(
                    f"{change_type.capitalize()} {start}-{end} is outside the sequence (1-{seq_len})."
                )
            edit = SequenceEdit(change_type, start, end, frag, "")
        else:
            raise SequenceCurationError(f"Invalid change type: {change_type}")
        bad = set(frag) - SEQ_FRAGMENT_CHARS
        if bad:
            raise SequenceCurationError(
                f"Sequence fragments may only contain IUPAC nucleotide codes; found {''.join(sorted(bad))}."
            )
        if residues is not None:
            edit.old = residues[edit.start - 1:edit.end].upper()
            expected = (c.get("old_sequence") or "").upper()
            if expected and expected != edit.old:
                raise SequenceCurationError(
                    f"Current residues at {edit.start}-{edit.end} are '{edit.old}', but the "
                    f"preview expected '{expected}'. The sequence may have changed; re-run the preview."
                )
        edits.append(edit)

    edits.sort(key=lambda e: (e.start, e.end))
    for prev, cur in zip(edits, edits[1:]):
        if cur.start <= prev.end or (
                cur.type == prev.type == "insertion" and cur.position == prev.position):
            raise SequenceCurationError(
                f"Changes at {prev.start}-{prev.end} and {cur.start}-{cur.end} overlap; "
                "combine them into one change."
            )
    return edits


def apply_edits(residues: str, edits: list[SequenceEdit]) -> str:
    """Apply sorted, non-overlapping edits (original coordinates) to residues."""
    pieces = []
    cursor = 0
    for e in edits:
        pieces.append(residues[cursor:e.start - 1])
        pieces.append(e.new)
        cursor = e.end
    pieces.append(residues[cursor:])
    return "".join(pieces)


def adjust_coordinate(coord: int, edits: list[SequenceEdit]) -> int:
    """
    Map an original coordinate onto the edited sequence: it moves by the
    length change of every edit that ends before it (legacy _adjust_coordinate).
    """
    return coord + sum(e.delta for e in edits if e.end < coord)


def edit_containing(coord: int, edits: list[SequenceEdit]) -> Optional[SequenceEdit]:
    """
    The length-changing edit whose window contains ``coord``, if any. A
    feature boundary there has no well-defined new position.
    """
    for e in edits:
        if e.delta != 0 and e.start <= coord <= e.end:
            return e
    return None


def overlaps_edit(start: int, stop: int, edits: list[SequenceEdit]) -> bool:
    """Whether a feature spanning start..stop contains or touches an edit."""
    lo, hi = min(start, stop), max(start, stop)
    return any(lo <= e.end and hi >= e.start for e in edits)


def resolve_reference_nos(db: Session, values: list[int]) -> tuple[list[int], list[int]]:
    """
    Map curator-entered reference identifiers to CGD reference_nos.

    Each value is taken as a reference_no if one exists, otherwise as the PMID
    of a reference already in CGD. Returns (reference_nos, unknown_values).
    """
    values = list(dict.fromkeys(values))
    if not values:
        return [], []
    by_ref_no = {
        r for (r,) in db.query(Reference.reference_no).filter(Reference.reference_no.in_(values)).all()
    }
    by_pubmed = dict(
        db.query(Reference.pubmed, Reference.reference_no)
        .filter(Reference.pubmed.in_([v for v in values if v not in by_ref_no]))
        .all()
    ) if len(by_ref_no) < len(values) else {}
    resolved, unknown = [], []
    for v in values:
        if v in by_ref_no:
            resolved.append(v)
        elif v in by_pubmed:
            resolved.append(by_pubmed[v])
        else:
            unknown.append(v)
    return list(dict.fromkeys(resolved)), unknown


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
        # Get root features (chromosomes, contigs, etc.) with their sequences.
        # seq_type is compared as stored ('genomic') so Oracle can use the
        # SEQ_TYPE_CURRENT_I index; upper() forced a full scan of SEQ, which
        # took minutes on a cold cache.
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
                Seq.seq_type == "genomic",
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
                Seq.seq_type == "genomic",
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
                Seq.seq_type == "genomic",
                Seq.is_seq_current == "Y",
            )
            .first()
        )

        if not result:
            return {"error": f"Feature {feature_name} not found"}

        feature, seq = result

        # Every edit is in original coordinates, so the outcome does not depend
        # on the order the changes were entered in.
        old_sequence = seq.residues
        edits = normalize_changes(changes, old_sequence)
        new_sequence = apply_edits(old_sequence, edits)
        net_change = len(new_sequence) - len(old_sequence)

        change_details = []
        for e in edits:
            # 0-based offsets of the window in the old and the new sequence
            old_at = e.start - 1
            new_at = adjust_coordinate(e.start, edits) - 1
            detail = {
                "type": e.type,
                "old_context": self._get_context(old_sequence, old_at, 20 + len(e.old)),
                "new_context": self._get_context(new_sequence, new_at, 20 + len(e.new)),
            }
            if e.type == "insertion":
                detail.update(position=e.position, sequence=e.new, length=len(e.new))
            elif e.type == "deletion":
                detail.update(start=e.start, end=e.end, deleted_sequence=e.old, length=len(e.old))
            else:
                detail.update(start=e.start, end=e.end, old_sequence=e.old,
                              new_sequence=e.new, length_change=e.delta)
            change_details.append(detail)

        affected_features = self._get_affected_features(
            seq.seq_no, changes, net_change, edits=edits
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
        edits: Optional[list[SequenceEdit]] = None,
    ) -> list[dict]:
        """
        Get features that would be affected by the sequence changes: those
        overlapping an edit (their sequence changes) and the first of those
        downstream (their coordinates shift). ``boundary_in_edit`` marks a
        feature with an end inside a length-changing edit, which the commit
        refuses.
        """
        if edits is None:
            if not changes:
                return []
            edits = normalize_changes(changes)
        if not edits:
            return []

        min_pos = min(e.start for e in edits) - 1   # an insertion's window is empty
        max_pos = max(e.end for e in edits)
        lo = func.least(FeatLocation.start_coord, FeatLocation.stop_coord)
        hi = func.greatest(FeatLocation.start_coord, FeatLocation.stop_coord)

        results = (
            self.db.query(Feature, FeatLocation)
            .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
            .filter(
                FeatLocation.root_seq_no == root_seq_no,
                FeatLocation.is_loc_current == "Y",
                Feature.feature_type.notin_(["chromosome", "contig", "plasmid"]),
                # overlapping the edited span, or downstream of it
                hi >= min_pos,
            )
            .order_by(lo)
            .limit(100)
            .all()
        )

        affected = []
        for feature, loc in results:
            is_overlapping = overlaps_edit(loc.start_coord, loc.stop_coord, edits)
            affected.append({
                "feature_no": feature.feature_no,
                "feature_name": feature.feature_name,
                "gene_name": feature.gene_name,
                "feature_type": feature.feature_type,
                "start_coord": loc.start_coord,
                "stop_coord": loc.stop_coord,
                "strand": loc.strand,
                "is_overlapping": is_overlapping,
                "is_downstream": min(loc.start_coord, loc.stop_coord) > max_pos,
                "boundary_in_edit": bool(
                    edit_containing(loc.start_coord, edits)
                    or edit_containing(loc.stop_coord, edits)
                ),
                "new_start": adjust_coordinate(loc.start_coord, edits),
                "new_stop": adjust_coordinate(loc.stop_coord, edits),
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
                Seq.seq_type == "genomic",
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
    # Insertions, deletions and substitutions of any length. As in the legacy
    # tool, every edit is the replacement of an original window, and a
    # coordinate moves by the length change of every edit ending before it.
    # A feature whose coordinates move gets a new current feat_location
    # (coord_version = the edit time) and its old one is kept as history; a
    # feature whose coordinates do not move is re-pointed to the new root
    # sequence in place.
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
        Apply insertion/deletion/substitution edits to a root (chromosome/
        contig) sequence, following the legacy UpdateRootSequence semantics.

        Steps (single transaction):
          1. Validate the edits (coordinates, IUPAC fragments, no overlaps) and
             verify the current residues at each window match what the caller's
             preview showed (guards against a stale preview).
          2. Refuse if any feature has an end inside a length-changing edit:
             its new boundary is ambiguous and needs manual curation.
          3. Insert a new current SEQ version with the edited residues and
             deprecate the old one.
          4. Give every feature whose coordinates move a new current
             feat_location; re-point the rest (and the root feature's own
             seq_no) to the new SEQ.
          5. Regenerate genomic (and protein, for coding features) sequences
             for any feature whose residues actually changed.
          6. Record seq_change_archive rows + a note (+ optional references).

        With dry_run=True the transaction is rolled back and a plan is returned.
        """
        reference_nos = reference_nos or []
        creator = (curator_userid or "")[:12]

        if not changes:
            raise SequenceCurationError("No changes supplied.")
        if not note_text or not note_text.strip():
            raise SequenceCurationError("A note describing the change is required.")
        reference_nos, unknown_refs = resolve_reference_nos(self.db, reference_nos)
        if unknown_refs:
            raise SequenceCurationError(
                "Reference(s) not found in CGD: " + ", ".join(map(str, unknown_refs))
                + ". Enter a CGD reference_no, or the PMID of a paper already in CGD."
            )

        # --- 2. load current root sequence ---
        row = (
            self.db.query(Feature, Seq)
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                func.upper(Feature.feature_name) == feature_name.upper(),
                Seq.seq_type == "genomic",
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
        old_len = len(old_residues)

        # organism -> translation table selection for protein regeneration
        organism = (
            self.db.query(Organism)
            .filter(Organism.organism_no == chr_feature.organism_no)
            .first()
        )
        use_table_12 = bool(organism and organism.organism_name in TRANSLATION_TABLE_12_ORGANISMS)

        # --- 1. validate + compute new residues ---
        edits = normalize_changes(changes, old_residues)
        new_residues = apply_edits(old_residues, edits)
        if new_residues == old_residues:
            raise SequenceCurationError("The edits produce no change to the sequence.")
        new_len = len(new_residues)

        # --- 2. plan every current feature location on this root ---
        loc_rows = (
            self.db.query(Feature, FeatLocation)
            .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
            .filter(
                FeatLocation.root_seq_no == old_root_seq_no,
                FeatLocation.is_loc_current == "Y",
                Feature.feature_no != chr_feature.feature_no,
            )
            .all()
        )
        moved = []         # (feature, old loc, new start, new stop)
        overlapping = []   # (feature, new start, new stop, strand): residues may change
        conflicts = []
        for feature, loc in loc_rows:
            if edit_containing(loc.start_coord, edits) or edit_containing(loc.stop_coord, edits):
                conflicts.append(
                    f"{feature.feature_name} ({feature.feature_type}, {loc.start_coord}-{loc.stop_coord})"
                )
                continue
            new_start = adjust_coordinate(loc.start_coord, edits)
            new_stop = adjust_coordinate(loc.stop_coord, edits)
            if (new_start, new_stop) != (loc.start_coord, loc.stop_coord):
                moved.append((feature, loc, new_start, new_stop))
            if overlaps_edit(loc.start_coord, loc.stop_coord, edits):
                overlapping.append((feature, new_start, new_stop, loc.strand))
        if conflicts:
            shown = "; ".join(conflicts[:10])
            more = f" and {len(conflicts) - 10} more" if len(conflicts) > 10 else ""
            raise SequenceCurationError(
                "These features have an end inside an edit that changes the sequence length, so "
                f"their new boundaries are ambiguous: {shown}{more}. Adjust the edit so it does not "
                "cross a feature boundary, or curate those features first."
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
            seq_length=new_len,
            residues=new_residues,
            source=old_seq.source,
            created_by=creator,
        )
        self.db.add(new_root_seq)
        self.db.flush()
        new_root_seq_no = new_root_seq.seq_no

        old_seq.is_seq_current = "N"

        # 4a. moved features: archive the current location, insert the new one
        for feature, loc, new_start, new_stop in moved:
            loc.is_loc_current = "N"
            self.db.add(FeatLocation(
                feature_no=feature.feature_no,
                seq_no=loc.seq_no,
                root_seq_no=new_root_seq_no,
                coord_version=now,
                start_coord=new_start,
                stop_coord=new_stop,
                strand=loc.strand,
                is_loc_current="Y",
                created_by=creator,
            ))
        self.db.flush()

        # 4b. the root feature's own location spans the whole sequence
        root_locs = (
            self.db.query(FeatLocation)
            .filter(
                FeatLocation.feature_no == chr_feature.feature_no,
                FeatLocation.is_loc_current == "Y",
            )
            .all()
        )
        for loc in root_locs:
            if new_len != old_len and max(loc.start_coord, loc.stop_coord) == old_len:
                loc.is_loc_current = "N"
                grow = new_len - old_len
                self.db.add(FeatLocation(
                    feature_no=chr_feature.feature_no,
                    seq_no=new_root_seq_no,
                    root_seq_no=(new_root_seq_no if loc.root_seq_no == old_root_seq_no
                                 else loc.root_seq_no),
                    coord_version=now,
                    start_coord=loc.start_coord + (grow if loc.start_coord > loc.stop_coord else 0),
                    stop_coord=loc.stop_coord + (grow if loc.stop_coord >= loc.start_coord else 0),
                    strand=loc.strand,
                    is_loc_current="Y",
                    created_by=creator,
                ))
            else:
                loc.seq_no = new_root_seq_no
        self.db.flush()

        # 4c. everything else keeps its coordinates: re-point in place
        repointed = (
            self.db.query(FeatLocation)
            .filter(
                FeatLocation.root_seq_no == old_root_seq_no,
                FeatLocation.is_loc_current == "Y",
            )
            .update({FeatLocation.root_seq_no: new_root_seq_no},
                    synchronize_session=False)
        )

        # 5. regenerate genomic/protein for features whose residues changed
        regenerated = self._regenerate_feature_sequences(
            overlapping, new_residues, now, creator, use_table_12
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
        archive_specs = []
        for e in edits:
            # legacy archive convention: an insertion is recorded at its
            # "insert after" position, with no old sequence
            start, end = (e.position, e.position) if e.type == "insertion" else (e.start, e.end)
            archive_specs.append({"type": e.type, "start": start, "end": end,
                                  "old": e.old, "new": e.new, "length_change": e.delta})
            arch = SeqChangeArchive(
                seq_no=old_root_seq_no,
                seq_change_type=ARCHIVE_CHANGE_TYPES[e.type],
                change_start_coord=start,
                change_stop_coord=end,
                old_seq=e.old or None,
                new_seq=e.new or None,
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
            "old_length": old_len,
            "seq_length": new_len,
            "net_change": new_len - old_len,
            "changes": archive_specs,
            "feat_locations_repointed": repointed,
            "feat_locations_moved": len(moved),
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
                "apply_changes committed for %s: new_root_seq_no=%s, net change %+d, "
                "%d feat_locations moved, %d re-pointed, %d features regenerated",
                feature_name, new_root_seq_no, new_len - old_len, len(moved), repointed,
                len(regenerated),
            )
        return plan

    def _regenerate_feature_sequences(
        self, overlapping, new_residues, now, creator, use_table_12,
    ) -> list[dict]:
        """
        Regenerate genomic (+protein) SEQ for features whose residues changed.
        ``overlapping`` holds (feature, start, stop, strand) with the feature's
        coordinates on the edited sequence.
        """
        regenerated = []
        for feature, start, stop, strand in overlapping:
            new_genomic = extract_subsequence(new_residues, start, stop, strand)
            cur_genomic_seq = (
                self.db.query(Seq)
                .filter(
                    Seq.feature_no == feature.feature_no,
                    Seq.seq_type == "genomic",
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

            # Protein for coding features, and for any feature that already has
            # one: C. albicans _B alleles are feature_type 'allele' but carry a
            # protein (as the merge tool also assumes).
            cur_prot = (
                self.db.query(Seq)
                .filter(
                    Seq.feature_no == feature.feature_no,
                    func.upper(Seq.seq_type) == "PROTEIN",
                    Seq.is_seq_current == "Y",
                )
                .first()
            )
            if feature.feature_type in CODING_FEATURE_TYPES or cur_prot is not None:
                protein = translate_dna(
                    new_genomic, codon_table=(CODON_TABLE_12 if use_table_12 else CODON_TABLE)
                )
                if protein.endswith("*"):
                    protein = protein[:-1]
                # a curator fixing a frameshift wants to see whether it worked
                entry["internal_stops"] = protein.count("*")
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
