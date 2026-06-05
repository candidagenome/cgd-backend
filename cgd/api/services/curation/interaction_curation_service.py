"""
Interaction Curation Service.

CRUD for curator-entered physical and genetic interactions. Curator-entered
interactions are tagged source='CGD'; imported BioGRID interactions
(source='BioGRID') are read-only and never modified here.

Data model (schema MULTI):
  INTERACTION  (experiment_type, source, description)
    <- FEAT_INTERACT (feature_no, interaction_no, action: 'Bait'/'Hit')
    <- REF_LINK      (tab_name='INTERACTION', col_name='INTERACTION_NO',
                      primary_key=interaction_no, reference_no)

Physical vs genetic is derived from experiment_type (GENETIC_TYPES below),
matching the BioGRID loader and the frontend InteractionDetails component.
"""
import logging
from typing import Optional

from sqlalchemy import and_
from sqlalchemy.orm import Session

from cgd.models.models import Feature, Interaction, FeatInteract, RefLink
from cgd.api.services.curation.phenotype_curation_service import (
    PhenotypeCurationService,
)

logger = logging.getLogger(__name__)

SOURCE = "CGD"

# Genetic interaction experiment types; everything else is physical.
GENETIC_TYPES = [
    "Dosage Growth Defect",
    "Dosage Lethality",
    "Dosage Rescue",
    "Negative Genetic",
    "Phenotypic Enhancement",
    "Phenotypic Suppression",
    "Positive Genetic",
    "Synthetic Growth Defect",
    "Synthetic Haploinsufficiency",
    "Synthetic Lethality",
    "Synthetic Rescue",
]

# Physical interaction experiment types offered to curators (BioGRID vocabulary).
PHYSICAL_TYPES = [
    "Affinity Capture-Luminescence",
    "Affinity Capture-MS",
    "Affinity Capture-RNA",
    "Affinity Capture-Western",
    "Biochemical Activity",
    "Co-crystal Structure",
    "Co-fractionation",
    "Co-localization",
    "Co-purification",
    "Far Western",
    "FRET",
    "PCA",
    "Protein-peptide",
    "Proximity Label-MS",
    "Reconstituted Complex",
    "Two-hybrid",
]

_GENETIC_SET = {t.lower() for t in GENETIC_TYPES}
_ALL_TYPES = {t.lower(): t for t in (GENETIC_TYPES + PHYSICAL_TYPES)}

REF_TAB = "INTERACTION"
REF_COL = "INTERACTION_NO"


class InteractionCurationError(Exception):
    """Raised for curator-facing interaction curation errors."""


class InteractionCurationService:
    def __init__(self, db: Session):
        self.db = db
        # Reuse the tested feature / reference resolution helpers.
        self._helper = PhenotypeCurationService(db)

    # ------------------------------------------------------------------
    # Vocabulary
    # ------------------------------------------------------------------
    def classify(self, experiment_type: Optional[str]) -> str:
        """Return 'genetic' or 'physical' for an experiment type."""
        return "genetic" if (experiment_type or "").lower() in _GENETIC_SET else "physical"

    def get_experiment_types(self) -> dict:
        return {"physical": PHYSICAL_TYPES, "genetic": GENETIC_TYPES}

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------
    def get_interactions(self, feature_name: str, organism_abbrev: Optional[str] = None) -> dict:
        """Return a gene's interactions split into physical / genetic lists."""
        feature = self._helper.get_feature_by_name(feature_name, organism_abbrev)
        if not feature:
            raise InteractionCurationError(
                f"Gene '{feature_name}' not found"
                + (f" in {organism_abbrev}" if organism_abbrev else "")
            )

        physical: list[dict] = []
        genetic: list[dict] = []
        seen: set[int] = set()

        for fi in feature.feat_interact:
            interaction = fi.interaction
            if interaction is None or interaction.interaction_no in seen:
                continue
            seen.add(interaction.interaction_no)

            # Partner genes (everything in this interaction other than the query)
            partners = []
            for other in interaction.feat_interact:
                of = other.feature
                if of and of.feature_no != feature.feature_no:
                    partners.append({
                        "feature_name": of.feature_name,
                        "gene_name": of.gene_name,
                        "action": other.action,
                    })
            # Self-interaction: only the query gene is linked
            if not partners:
                partners.append({
                    "feature_name": feature.feature_name,
                    "gene_name": feature.gene_name,
                    "action": fi.action,
                })

            references = []
            ref_links = (
                self.db.query(RefLink)
                .filter(
                    RefLink.tab_name == REF_TAB,
                    RefLink.primary_key == interaction.interaction_no,
                )
                .all()
            )
            for rl in ref_links:
                ref = rl.reference
                if ref:
                    references.append({"pubmed": ref.pubmed, "citation": ref.citation})

            row = {
                "interaction_no": interaction.interaction_no,
                "experiment_type": interaction.experiment_type,
                "source": interaction.source,
                "description": interaction.description,
                "partners": partners,
                "references": references,
                # Only CGD-curated interactions are editable/deletable.
                "editable": (interaction.source or "").upper() == SOURCE,
            }
            if self.classify(interaction.experiment_type) == "genetic":
                genetic.append(row)
            else:
                physical.append(row)

        return {
            "feature_name": feature.feature_name,
            "gene_name": feature.gene_name,
            "organism": feature.organism.organism_name if feature.organism else None,
            "physical": physical,
            "genetic": genetic,
        }

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------
    def create_interaction(
        self,
        feature_name: str,
        organism_abbrev: Optional[str],
        partner_name: str,
        experiment_type: str,
        pubmed: Optional[int],
        description: Optional[str],
        curator_userid: str,
    ) -> int:
        """Create a curator (source='CGD') interaction. Queried gene = Bait."""
        canonical = _ALL_TYPES.get((experiment_type or "").lower())
        if not canonical:
            raise InteractionCurationError(f"Unknown experiment type: '{experiment_type}'")

        bait = self._helper.get_feature_by_name(feature_name, organism_abbrev)
        if not bait:
            raise InteractionCurationError(
                f"Gene '{feature_name}' not found"
                + (f" in {organism_abbrev}" if organism_abbrev else "")
            )
        hit = self._helper.get_feature_by_name(partner_name, organism_abbrev)
        if not hit:
            raise InteractionCurationError(
                f"Interacting gene '{partner_name}' not found"
                + (f" in {organism_abbrev}" if organism_abbrev else "")
            )

        if pubmed is None:
            raise InteractionCurationError("A PubMed ID is required.")
        reference_no = self._helper.get_reference_no_by_pubmed(pubmed)
        if not reference_no:
            raise InteractionCurationError(
                f"PubMed {pubmed} is not in the CGD reference database. "
                "Add the reference first, then curate the interaction."
            )

        existing = self._find_existing_cgd_interaction(
            canonical, bait.feature_no, hit.feature_no, reference_no
        )
        if existing:
            raise InteractionCurationError(
                f"An identical CGD interaction already exists (interaction {existing})."
            )

        interaction = Interaction(
            experiment_type=canonical,
            source=SOURCE,
            description=(description or None),
            created_by=curator_userid[:12],
        )
        self.db.add(interaction)
        self.db.flush()  # populate interaction_no (Oracle trigger)
        interaction_no = interaction.interaction_no

        self.db.add(RefLink(
            reference_no=reference_no,
            tab_name=REF_TAB,
            col_name=REF_COL,
            primary_key=interaction_no,
            created_by=curator_userid[:12],
        ))

        # Queried gene = Bait
        self.db.add(FeatInteract(
            feature_no=bait.feature_no,
            interaction_no=interaction_no,
            action="Bait",
            created_by=curator_userid[:12],
        ))
        # Partner = Hit (omit for a self-interaction)
        if hit.feature_no != bait.feature_no:
            self.db.add(FeatInteract(
                feature_no=hit.feature_no,
                interaction_no=interaction_no,
                action="Hit",
                created_by=curator_userid[:12],
            ))

        self.db.commit()
        logger.info(
            "Created CGD interaction %s: %s (%s) %s-%s by %s",
            interaction_no, canonical, self.classify(canonical),
            bait.feature_name, hit.feature_name, curator_userid,
        )
        return interaction_no

    def _find_existing_cgd_interaction(
        self, experiment_type: str, bait_no: int, hit_no: int, reference_no: int
    ) -> Optional[int]:
        """Return an existing matching CGD interaction_no, or None."""
        candidates = (
            self.db.query(Interaction.interaction_no)
            .join(FeatInteract, FeatInteract.interaction_no == Interaction.interaction_no)
            .join(RefLink, and_(
                RefLink.primary_key == Interaction.interaction_no,
                RefLink.tab_name == REF_TAB,
            ))
            .filter(
                Interaction.source == SOURCE,
                Interaction.experiment_type == experiment_type,
                FeatInteract.feature_no == bait_no,
                RefLink.reference_no == reference_no,
            )
            .all()
        )
        cand_nos = {c[0] for c in candidates}
        if not cand_nos:
            return None
        if bait_no == hit_no:
            return next(iter(cand_nos))
        hit_match = (
            self.db.query(FeatInteract.interaction_no)
            .filter(
                FeatInteract.interaction_no.in_(cand_nos),
                FeatInteract.feature_no == hit_no,
            )
            .first()
        )
        return hit_match[0] if hit_match else None

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------
    def delete_interaction(self, interaction_no: int, curator_userid: str) -> bool:
        """Delete a CGD-curated interaction. BioGRID interactions cannot be deleted."""
        interaction = (
            self.db.query(Interaction)
            .filter(Interaction.interaction_no == interaction_no)
            .first()
        )
        if not interaction:
            raise InteractionCurationError(f"Interaction {interaction_no} not found.")
        if (interaction.source or "").upper() != SOURCE:
            raise InteractionCurationError(
                f"Only CGD-curated interactions can be deleted "
                f"(interaction {interaction_no} is from {interaction.source})."
            )

        # Reference links are in a generic table (no FK cascade) -> delete explicitly.
        self.db.query(RefLink).filter(
            RefLink.tab_name == REF_TAB,
            RefLink.primary_key == interaction_no,
        ).delete(synchronize_session=False)
        self.db.query(FeatInteract).filter(
            FeatInteract.interaction_no == interaction_no,
        ).delete(synchronize_session=False)
        self.db.delete(interaction)
        self.db.commit()

        logger.info("Deleted CGD interaction %s by %s", interaction_no, curator_userid)
        return True
