"""
Feature Merge Service - Merge two ORF features into one.

Used when a sequencing-error correction removes a spurious stop/frameshift so
that two adjacent ORF fragments are actually a single gene. The merge:

  1. Extends the *survivor* ORF (and its co-terminal single-exon CDS child) to a
     new boundary and regenerates its genomic + protein sequences.
  2. Transfers the *retired* feature's annotations onto the survivor, skipping
     any that would duplicate an annotation the survivor already has
     (transfer-and-dedup).
  3. Preserves the retired feature's identifiers (systematic name + CGDID) as
     "Retired name" aliases / dbxref links on the survivor so old URLs and
     searches still resolve.
  4. Retires the redundant ORF and its CDS child by deleting them. The database
     AFTER-DELETE row triggers archive every deleted row to DELETE_LOG
     automatically, so no manual archival is performed here.
  5. Records a note (and optional references) documenting the merge.

The whole thing runs in one transaction; dry_run=True rolls it back and returns
the plan (used by the preview).

v1 scope: the survivor must be a single-exon gene (one CDS child whose location
is co-terminal with the ORF). More complex intron/exon structures are refused
with a clear message rather than silently mis-handled.
"""

import logging
from typing import Optional
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    FeatLocation,
    Seq,
    Note,
    NoteLink,
    RefLink,
    Alias,
    FeatAlias,
    Dbxref,
    DbxrefFeat,
    GoAnnotation,
    PhenoAnnotation,
    FeatHomology,
    FeatInteract,
    FeatUrl,
    RefpropFeat,
    FeatRelationship,
    Organism,
)
from cgd.api.services.curation.sequence_curation_service import (
    reverse_complement,
    extract_subsequence,
    translate_dna,
    CODON_TABLE,
    CODON_TABLE_12,
    TRANSLATION_TABLE_12_ORGANISMS,
)

logger = logging.getLogger(__name__)

NOTE_TYPE = "Sequence change"
# The retired feature's systematic name is preserved as an ordinary synonym on
# the survivor. ("Retired name" is reserved for deprecated *gene symbols*.)
SYNONYM_ALIAS_TYPE = "Non-uniform"
PART_OF = "part of"

# Annotation tables transferred from the retired feature to the survivor.
# Each entry: (model, pk_attr, identity_attrs) where identity_attrs are the
# non-feature_no columns of the table's unique key. A retired row whose identity
# tuple already exists on the survivor is redundant and left to be deleted with
# the feature; otherwise its feature_no is re-pointed to the survivor.
TRANSFER_SPECS = [
    (GoAnnotation, "go_annotation_no",
     ("go_no", "go_evidence", "annotation_type", "source")),
    (PhenoAnnotation, "pheno_annotation_no", ("phenotype_no", "experiment_no")),
    (FeatHomology, "feat_homology_no", ("homology_group_no",)),
    (FeatInteract, "feat_interact_no", ("interaction_no", "action")),
    (DbxrefFeat, "dbxref_feat_no", ("dbxref_no",)),
    (FeatUrl, "feat_url_no", ("url_no",)),
    (RefpropFeat, "refprop_feat_no", ("ref_property_no",)),
    (FeatAlias, "feat_alias_no", ("alias_no",)),
]

# Feature types whose protein sequence is regenerated.
CODING_FEATURE_TYPES = {"ORF", "pseudogene"}


class FeatureMergeError(Exception):
    """Raised when a feature-merge operation is invalid or cannot proceed."""


class FeatureMergeService:
    """Service for merging two ORF features into one."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Read helpers (used to populate the UI / preview)
    # ------------------------------------------------------------------
    def _load_feature(self, feature_name: str) -> Feature:
        feat = (
            self.db.query(Feature)
            .filter(func.upper(Feature.feature_name) == feature_name.upper())
            .first()
        )
        if not feat:
            raise FeatureMergeError(f"Feature '{feature_name}' not found.")
        return feat

    def _current_location(self, feature_no: int) -> Optional[FeatLocation]:
        return (
            self.db.query(FeatLocation)
            .filter(
                FeatLocation.feature_no == feature_no,
                FeatLocation.is_loc_current == "Y",
            )
            .first()
        )

    def _current_seq(self, feature_no: int, seq_type: str) -> Optional[Seq]:
        return (
            self.db.query(Seq)
            .filter(
                Seq.feature_no == feature_no,
                func.upper(Seq.seq_type) == seq_type.upper(),
                Seq.is_seq_current == "Y",
            )
            .first()
        )

    def _part_of_children(self, parent_feature_no: int):
        """Return (Feature, FeatLocation) for current 'part of' children."""
        rows = (
            self.db.query(Feature, FeatLocation)
            .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
            .join(
                FeatRelationship,
                FeatRelationship.child_feature_no == Feature.feature_no,
            )
            .filter(
                FeatRelationship.parent_feature_no == parent_feature_no,
                FeatRelationship.relationship_type == PART_OF,
                FeatLocation.is_loc_current == "Y",
            )
            .all()
        )
        return rows

    def get_feature_summary(self, feature_name: str) -> dict:
        """Lightweight summary of a feature for the merge form."""
        feat = self._load_feature(feature_name)
        loc = self._current_location(feat.feature_no)
        children = self._part_of_children(feat.feature_no)
        counts = {}
        for model, _pk, _identity in TRANSFER_SPECS:
            counts[model.__tablename__] = (
                self.db.query(func.count())
                .select_from(model)
                .filter(model.feature_no == feat.feature_no)
                .scalar()
            )
        return {
            "feature_no": feat.feature_no,
            "feature_name": feat.feature_name,
            "gene_name": feat.gene_name,
            "feature_type": feat.feature_type,
            "dbxref_id": feat.dbxref_id,
            "location": None if not loc else {
                "start_coord": loc.start_coord,
                "stop_coord": loc.stop_coord,
                "strand": loc.strand,
                "root_seq_no": loc.root_seq_no,
            },
            "cds_children": [
                {
                    "feature_no": c.feature_no,
                    "feature_name": c.feature_name,
                    "feature_type": c.feature_type,
                    "start_coord": cl.start_coord,
                    "stop_coord": cl.stop_coord,
                    "strand": cl.strand,
                }
                for c, cl in children
            ],
            "annotation_counts": counts,
        }

    # ------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------
    def merge_features(
        self,
        survivor_name: str,
        retire_name: str,
        new_stop_coord: int,
        note_text: str,
        curator_userid: str,
        reference_nos: Optional[list[int]] = None,
        dry_run: bool = True,
    ) -> dict:
        """
        Merge ``retire_name`` into ``survivor_name``, extending the survivor's
        boundary (Crick strand: its lower coordinate) to ``new_stop_coord``.

        Returns a plan describing every change. With dry_run=True all writes are
        rolled back.
        """
        reference_nos = reference_nos or []
        creator = (curator_userid or "")[:12]
        note_text_clean = (note_text or "").strip()
        # A note is required to commit, but a dry-run preview may omit it.
        if not dry_run and not note_text_clean:
            raise FeatureMergeError("A note describing the merge is required.")

        survivor = self._load_feature(survivor_name)
        retire = self._load_feature(retire_name)
        if survivor.feature_no == retire.feature_no:
            raise FeatureMergeError("Survivor and retired feature are the same.")
        if survivor.organism_no != retire.organism_no:
            raise FeatureMergeError(
                "Survivor and retired feature belong to different organisms."
            )

        surv_loc = self._current_location(survivor.feature_no)
        retire_loc = self._current_location(retire.feature_no)
        if not surv_loc or not retire_loc:
            raise FeatureMergeError(
                "Both features must have a current location."
            )
        if surv_loc.root_seq_no != retire_loc.root_seq_no:
            raise FeatureMergeError(
                "Features are on different root sequences; cannot merge."
            )
        if surv_loc.strand != retire_loc.strand:
            raise FeatureMergeError(
                "Features are on opposite strands; cannot merge."
            )

        strand = surv_loc.strand
        old_start = surv_loc.start_coord
        old_stop = surv_loc.stop_coord

        # Determine the moved boundary. For Crick strand start_coord > stop_coord
        # and we extend the stop (lower) coordinate downward; for Watson the
        # survivor's higher coordinate moves up. In both cases the *non-moving*
        # end stays at old_start.
        new_start = old_start
        new_stop = int(new_stop_coord)
        if strand == "C":
            if new_stop >= old_stop:
                raise FeatureMergeError(
                    f"For a Crick-strand extension the new stop ({new_stop}) must "
                    f"be below the current stop ({old_stop})."
                )
        else:  # Watson
            if new_stop <= old_stop:
                raise FeatureMergeError(
                    f"For a Watson-strand extension the new stop ({new_stop}) must "
                    f"be above the current stop ({old_stop})."
                )

        # v1: survivor must be single-exon (exactly one co-terminal CDS child).
        surv_children = self._part_of_children(survivor.feature_no)
        coterminal = [
            (c, cl) for c, cl in surv_children
            if cl.start_coord == old_start and cl.stop_coord == old_stop
        ]
        non_coterminal = [
            (c, cl) for c, cl in surv_children
            if not (cl.start_coord == old_start and cl.stop_coord == old_stop)
        ]
        if non_coterminal:
            names = ", ".join(c.feature_name for c, _ in non_coterminal)
            raise FeatureMergeError(
                "Survivor has subfeature(s) whose coordinates are not co-terminal "
                f"with the ORF ({names}); this multi-exon structure needs manual "
                "curation and is not supported by the merge tool."
            )

        # chromosome residues for sequence regeneration
        root_seq = self.db.query(Seq).filter(Seq.seq_no == surv_loc.root_seq_no).first()
        if not root_seq:
            raise FeatureMergeError("Root (chromosome) sequence not found.")
        chr_residues = root_seq.residues

        organism = (
            self.db.query(Organism)
            .filter(Organism.organism_no == survivor.organism_no)
            .first()
        )
        use_table_12 = bool(
            organism and organism.organism_name in TRANSLATION_TABLE_12_ORGANISMS
        )

        now = datetime.now()

        # ================= writes =================
        # 1. Extend survivor ORF: regenerate genomic + protein, version the loc.
        extended = []
        new_genomic = extract_subsequence(chr_residues, new_start, new_stop, strand)
        orf_entry = self._reversion_feature(
            survivor, surv_loc, new_start, new_stop, strand,
            new_genomic, now, creator, use_table_12,
        )
        extended.append(orf_entry)

        # 1b. Extend the co-terminal CDS child(ren) in lockstep.
        for child, child_loc in coterminal:
            child_genomic = extract_subsequence(chr_residues, new_start, new_stop, strand)
            child_entry = self._reversion_feature(
                child, child_loc, new_start, new_stop, strand,
                child_genomic, now, creator, use_table_12,
            )
            extended.append(child_entry)

        # 2. Transfer annotations retire -> survivor (dedup).
        transfer_summary = self._transfer_annotations(
            retire.feature_no, survivor.feature_no
        )

        # 3. Preserve retired identifiers as "Retired name" aliases on survivor.
        preserved = self._preserve_identifiers(retire, survivor, creator)

        # 4. Soft-retire the redundant ORF and its CDS children. CGD forbids
        #    deleting archival (is_current='N') rows, so the feature row and its
        #    history are kept; we remove only the *current* location + seq so it
        #    drops off the current assembly, and drop the now-redundant
        #    annotations left on it after the transfer. Every deleted row is
        #    archived to DELETE_LOG by the AFTER-DELETE triggers.
        retire_children = [c for c, _ in self._part_of_children(retire.feature_no)]
        retired_summary = []
        for feat in [retire] + retire_children:
            retired_summary.append(self._soft_retire_feature(feat))
        dropped_annotations = self._drop_remaining_annotations(retire.feature_no)

        # 5. Note (+ references), linked to the survivor feature. Skipped when a
        #    dry-run preview supplies no note text.
        note_no = None
        if note_text_clean:
            note = (
                self.db.query(Note)
                .filter(Note.note_type == NOTE_TYPE, Note.note == note_text_clean)
                .first()
            )
            if note is None:
                note = Note(note=note_text_clean, note_type=NOTE_TYPE, created_by=creator)
                self.db.add(note)
                self.db.flush()
            self._ensure_note_link(note.note_no, "FEATURE", survivor.feature_no, creator)
            for ref_no in reference_nos:
                self._ensure_ref_link(ref_no, note.note_no, creator)
            note_no = note.note_no

        plan = {
            "survivor": {
                "feature_no": survivor.feature_no,
                "feature_name": survivor.feature_name,
                "gene_name": survivor.gene_name,
                "old_coords": [old_start, old_stop],
                "new_coords": [new_start, new_stop],
                "strand": strand,
            },
            "retired": {
                "feature_no": retire.feature_no,
                "feature_name": retire.feature_name,
                "dbxref_id": retire.dbxref_id,
            },
            "extended_features": extended,
            "annotation_transfer": transfer_summary,
            "identifiers_preserved": preserved,
            "features_retired": retired_summary,
            "redundant_annotations_dropped": dropped_annotations,
            "note_no": note_no,
            "reference_nos": reference_nos,
            "dry_run": dry_run,
        }

        if dry_run:
            self.db.rollback()
            logger.info(
                "merge_features dry-run rolled back: %s <- %s",
                survivor_name, retire_name,
            )
        else:
            self.db.commit()
            logger.info(
                "merge_features committed: %s <- %s (retired feature_no %s)",
                survivor_name, retire_name, retire.feature_no,
            )
        return plan

    # ------------------------------------------------------------------
    # Internal write helpers
    # ------------------------------------------------------------------
    def _reversion_feature(
        self, feature, loc, new_start, new_stop, strand,
        new_genomic, now, creator, use_table_12,
    ) -> dict:
        """Version a feature's genomic (+protein) seq and its feat_location.

        Protein is regenerated whenever the feature already has a current protein
        sequence (true for both ORF and allele features, false for CDS children),
        rather than keying off feature_type."""
        entry = {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "feature_type": feature.feature_type,
            "old_coords": [loc.start_coord, loc.stop_coord],
            "new_coords": [new_start, new_stop],
        }

        # new genomic seq version
        cur_genomic = self._current_seq(feature.feature_no, "genomic")
        if cur_genomic:
            cur_genomic.is_seq_current = "N"
        new_gseq = Seq(
            feature_no=feature.feature_no,
            seq_version=now,
            seq_type="genomic",
            genome_version_no=(cur_genomic.genome_version_no if cur_genomic else None),
            is_seq_current="Y",
            seq_length=len(new_genomic),
            residues=new_genomic,
            source=(cur_genomic.source if cur_genomic else None),
            created_by=creator,
        )
        self.db.add(new_gseq)
        self.db.flush()
        entry["new_genomic_seq_no"] = new_gseq.seq_no
        entry["genomic_length"] = len(new_genomic)

        # version the feat_location: deprecate old, insert new (feature_no is
        # immutable per FEATLOCATION_BIUDR, so we insert a fresh current row).
        loc.is_loc_current = "N"
        new_loc = FeatLocation(
            feature_no=feature.feature_no,
            seq_no=new_gseq.seq_no,
            root_seq_no=loc.root_seq_no,
            coord_version=now,
            start_coord=new_start,
            stop_coord=new_stop,
            strand=strand,
            is_loc_current="Y",
            created_by=creator,
        )
        self.db.add(new_loc)
        self.db.flush()
        entry["new_feat_location_no"] = new_loc.feat_location_no

        cur_prot = self._current_seq(feature.feature_no, "protein")
        if cur_prot is not None:
            protein = translate_dna(
                new_genomic, codon_table=(CODON_TABLE_12 if use_table_12 else CODON_TABLE)
            )
            entry["internal_stops"] = protein.rstrip("*").count("*")
            if protein.endswith("*"):
                protein = protein[:-1]
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
        return entry

    def _transfer_annotations(self, retire_no: int, survivor_no: int) -> list[dict]:
        """Re-point retire's annotations to the survivor, skipping duplicates."""
        summary = []
        for model, _pk, identity_attrs in TRANSFER_SPECS:
            survivor_keys = set()
            for row in self.db.query(model).filter(model.feature_no == survivor_no):
                survivor_keys.add(tuple(getattr(row, a) for a in identity_attrs))
            transferred = 0
            redundant = 0
            for row in self.db.query(model).filter(model.feature_no == retire_no):
                key = tuple(getattr(row, a) for a in identity_attrs)
                if key in survivor_keys:
                    redundant += 1  # left on retiree; deleted with the feature
                    continue
                row.feature_no = survivor_no
                survivor_keys.add(key)
                transferred += 1
            self.db.flush()
            summary.append({
                "table": model.__tablename__,
                "transferred": transferred,
                "redundant_dropped": redundant,
            })
        return summary

    def _preserve_identifiers(self, retire, survivor, creator) -> list[dict]:
        """Preserve the retired feature's systematic name as a synonym of the
        survivor so it still resolves. (Its other systematic aliases and its
        CGDID dbxref are already carried over by the annotation transfer.)"""
        preserved = []
        name = retire.feature_name
        if not name:
            return preserved
        alias = (
            self.db.query(Alias)
            .filter(Alias.alias_name == name,
                    Alias.alias_type == SYNONYM_ALIAS_TYPE)
            .first()
        )
        if alias is None:
            alias = Alias(alias_name=name, alias_type=SYNONYM_ALIAS_TYPE,
                          created_by=creator)
            self.db.add(alias)
            self.db.flush()
        link = (
            self.db.query(FeatAlias)
            .filter(FeatAlias.feature_no == survivor.feature_no,
                    FeatAlias.alias_no == alias.alias_no)
            .first()
        )
        if link is None:
            self.db.add(FeatAlias(feature_no=survivor.feature_no,
                                  alias_no=alias.alias_no))
            self.db.flush()
            preserved.append({"alias_name": name, "alias_type": SYNONYM_ALIAS_TYPE})
        return preserved

    def _soft_retire_feature(self, feature) -> dict:
        """Deprecate a feature's *current* location + sequences (set
        is_current='N') so it drops off the current assembly, while keeping the
        feature row and all its history. Deprecation (not deletion) is used
        because CGD forbids deleting archival rows and those archival rows still
        reference the current seq (FL_SEQ_FK); versioning the current rows to 'N'
        achieves the same effect without any deletion."""
        loc_n = (
            self.db.query(FeatLocation)
            .filter(FeatLocation.feature_no == feature.feature_no,
                    FeatLocation.is_loc_current == "Y")
            .update({FeatLocation.is_loc_current: "N"}, synchronize_session=False)
        )
        seq_n = (
            self.db.query(Seq)
            .filter(Seq.feature_no == feature.feature_no,
                    Seq.is_seq_current == "Y")
            .update({Seq.is_seq_current: "N"}, synchronize_session=False)
        )
        self.db.flush()
        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "feature_type": feature.feature_type,
            "current_locations_deprecated": loc_n,
            "current_seqs_deprecated": seq_n,
            "archival_history_kept": True,
        }

    def _drop_remaining_annotations(self, retire_no: int) -> list[dict]:
        """Delete the redundant annotation rows left on the retired feature after
        the transfer (their unique values now live on the survivor)."""
        dropped = []
        for model, _pk, _identity in TRANSFER_SPECS:
            n = (
                self.db.query(model)
                .filter(model.feature_no == retire_no)
                .delete(synchronize_session=False)
            )
            self.db.flush()
            if n:
                dropped.append({"table": model.__tablename__, "deleted": n})
        return dropped

    def _ensure_note_link(self, note_no, tab_name, primary_key, creator):
        exists = (
            self.db.query(NoteLink)
            .filter(NoteLink.note_no == note_no,
                    NoteLink.tab_name == tab_name,
                    NoteLink.primary_key == primary_key)
            .first()
        )
        if exists is None:
            self.db.add(NoteLink(note_no=note_no, tab_name=tab_name,
                                 primary_key=primary_key, created_by=creator))
            self.db.flush()

    def _ensure_ref_link(self, reference_no, note_no, creator):
        exists = (
            self.db.query(RefLink)
            .filter(RefLink.reference_no == reference_no,
                    RefLink.tab_name == "NOTE",
                    RefLink.col_name == "NOTE_NO",
                    RefLink.primary_key == note_no)
            .first()
        )
        if exists is None:
            self.db.add(RefLink(reference_no=reference_no, tab_name="NOTE",
                                col_name="NOTE_NO", primary_key=note_no,
                                created_by=creator))
            self.db.flush()
