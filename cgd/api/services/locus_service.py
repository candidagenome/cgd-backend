from sqlalchemy.orm import Session, joinedload

from cgd.api.crud.locus_crud import get_features_for_locus_name
from cgd.schemas.locus_schema import LocusByOrganismResponse, FeatureOut
from cgd.schemas.go_schema import (
    GODetailsResponse,
    GODetailsForOrganism,
    GOAnnotationOut,
    GOTerm,
    GOEvidence,
)
from cgd.schemas.phenotype_schema import (
    PhenotypeDetailsResponse,
    PhenotypeDetailsForOrganism,
    PhenotypeAnnotationOut,
    PhenotypeTerm,
    ReferenceStub,
)
from cgd.schemas.interaction_schema import (
    InteractionDetailsResponse,
    InteractionDetailsForOrganism,
    InteractionOut,
    InteractorOut,
)
from cgd.schemas.protein_schema import (
    ProteinDetailsResponse,
    ProteinDetailsForOrganism,
    ProteinInfoOut,
)
from cgd.schemas.homology_schema import (
    HomologyDetailsResponse,
    HomologyDetailsForOrganism,
    HomologyGroupOut,
    HomologOut,
)


def _get_organism_name(f) -> str:
    """Extract organism name from a feature, with fallback."""
    org = f.organism
    organism_name = None
    if org is not None:
        organism_name = (
            getattr(org, "organism_name", None)
            or getattr(org, "display_name", None)
            or getattr(org, "name", None)
        )
    if not organism_name:
        organism_name = str(f.organism_no)
    return organism_name


def get_locus_by_organism(db: Session, name: str) -> LocusByOrganismResponse:
    features = get_features_for_locus_name(db, name)

    out: dict[str, FeatureOut] = {}

    for f in features:
        organism_name = _get_organism_name(f)
        out[organism_name] = FeatureOut.model_validate(f)

    return LocusByOrganismResponse(results=out)


def get_locus_go_details(db: Session, name: str) -> GODetailsResponse:
    """
    Query GO annotations for each feature matching the locus name,
    grouped by organism.
    """
    from cgd.models.locus_model import Feature
    from sqlalchemy import func, or_

    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.go_annotation).joinedload("go"),
            joinedload(Feature.go_annotation).joinedload("go_ref").joinedload("reference"),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
            )
        )
        .all()
    )

    out: dict[str, GODetailsForOrganism] = {}

    for f in features:
        organism_name = _get_organism_name(f)
        locus_display_name = f.gene_name or f.feature_name

        annotations = []
        for ga in f.go_annotation:
            go = ga.go
            if go is None:
                continue

            # Format GOID as GO:XXXXXXX
            goid_str = f"GO:{go.goid:07d}" if isinstance(go.goid, int) else str(go.goid)

            term = GOTerm(
                goid=goid_str,
                display_name=go.go_term,
                aspect=go.go_aspect,
                link=f"/go/{goid_str}",
            )

            evidence = GOEvidence(
                code=ga.go_evidence,
                with_from=None,  # Could be extended if with_from data exists
            )

            # Get references from go_ref relationship
            references = []
            for gr in ga.go_ref:
                ref = gr.reference
                if ref and ref.pubmed:
                    references.append(f"PMID:{ref.pubmed}")
                elif ref:
                    references.append(ref.dbxref_id)

            annotations.append(GOAnnotationOut(
                term=term,
                evidence=evidence,
                references=references,
            ))

        out[organism_name] = GODetailsForOrganism(
            locus_display_name=locus_display_name,
            annotations=annotations,
        )

    return GODetailsResponse(results=out)


def get_locus_phenotype_details(db: Session, name: str) -> PhenotypeDetailsResponse:
    """
    Query phenotype annotations for each feature matching the locus name,
    grouped by organism.
    """
    from cgd.models.locus_model import Feature
    from sqlalchemy import func, or_

    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.pheno_annotation).joinedload("phenotype"),
            joinedload(Feature.pheno_annotation).joinedload("experiment"),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
            )
        )
        .all()
    )

    out: dict[str, PhenotypeDetailsForOrganism] = {}

    for f in features:
        organism_name = _get_organism_name(f)
        locus_display_name = f.gene_name or f.feature_name

        annotations = []
        for pa in f.pheno_annotation:
            phenotype = pa.phenotype
            if phenotype is None:
                continue

            pheno_term = PhenotypeTerm(
                display_name=phenotype.observable,
                link=f"/phenotype/{phenotype.phenotype_no}",
            )

            experiment = pa.experiment
            experiment_comment = None
            strain = None
            if experiment:
                experiment_comment = getattr(experiment, "experiment_comment", None)
                strain = getattr(experiment, "strain_background", None)

            annotations.append(PhenotypeAnnotationOut(
                phenotype=pheno_term,
                qualifier=phenotype.qualifier,
                experiment=phenotype.experiment_type,
                strain=strain,
                references=[],  # References could be added via ref_link if needed
            ))

        out[organism_name] = PhenotypeDetailsForOrganism(
            locus_display_name=locus_display_name,
            annotations=annotations,
        )

    return PhenotypeDetailsResponse(results=out)


def get_locus_interaction_details(db: Session, name: str) -> InteractionDetailsResponse:
    """
    Query interaction data for each feature matching the locus name,
    grouped by organism. Excludes genetic interactions (those in interact_pheno).
    """
    from cgd.models.locus_model import Feature
    from cgd.models.models import RefLink
    from sqlalchemy import func, or_

    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.feat_interact).joinedload("interaction"),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
            )
        )
        .all()
    )

    out: dict[str, InteractionDetailsForOrganism] = {}

    for f in features:
        organism_name = _get_organism_name(f)
        locus_display_name = f.gene_name or f.feature_name

        interactions = []
        seen_interactions = set()

        for fi in f.feat_interact:
            interaction = fi.interaction
            if interaction is None:
                continue

            # Skip if already processed (dedup)
            if interaction.interaction_no in seen_interactions:
                continue
            seen_interactions.add(interaction.interaction_no)

            # Skip genetic interactions (those linked to phenotypes)
            if interaction.interact_pheno:
                continue

            # Get all interactors for this interaction
            interactors = []
            for other_fi in interaction.feat_interact:
                other_feat = other_fi.feature
                if other_feat and other_feat.feature_no != f.feature_no:
                    interactors.append(InteractorOut(
                        feature_name=other_feat.feature_name,
                        gene_name=other_feat.gene_name,
                        action=other_fi.action,
                    ))

            # Get references via ref_link table
            references = []
            ref_links = (
                db.query(RefLink)
                .filter(
                    RefLink.tab_name == "INTERACTION",
                    RefLink.primary_key == interaction.interaction_no,
                )
                .all()
            )
            for rl in ref_links:
                ref = rl.reference
                if ref and ref.pubmed:
                    references.append(f"PMID:{ref.pubmed}")
                elif ref:
                    references.append(ref.dbxref_id)

            interactions.append(InteractionOut(
                interaction_no=interaction.interaction_no,
                experiment_type=interaction.experiment_type,
                description=interaction.description,
                source=interaction.source,
                interactors=interactors,
                references=references,
            ))

        out[organism_name] = InteractionDetailsForOrganism(
            locus_display_name=locus_display_name,
            interactions=interactions,
        )

    return InteractionDetailsResponse(results=out)


def get_locus_protein_details(db: Session, name: str) -> ProteinDetailsResponse:
    """
    Query protein information for each feature matching the locus name,
    grouped by organism.
    """
    from cgd.models.locus_model import Feature
    from sqlalchemy import func, or_

    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.protein_info),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
            )
        )
        .all()
    )

    out: dict[str, ProteinDetailsForOrganism] = {}

    for f in features:
        organism_name = _get_organism_name(f)
        locus_display_name = f.gene_name or f.feature_name

        protein_info = None
        # Typically one protein_info per feature, but handle list
        if f.protein_info:
            pi = f.protein_info[0]

            # Build amino acid composition dictionary
            amino_acids = {
                "ala": pi.ala,
                "arg": pi.arg,
                "asn": pi.asn,
                "asp": pi.asp,
                "cys": pi.cys,
                "gln": pi.gln,
                "glu": pi.glu,
                "gly": pi.gly,
                "his": pi.his,
                "ile": pi.ile,
                "leu": pi.leu,
                "lys": pi.lys,
                "met": pi.met,
                "phe": pi.phe,
                "pro": pi.pro,
                "ser": pi.ser,
                "thr": pi.thr,
                "trp": pi.trp,
                "tyr": pi.tyr,
                "val": pi.val,
            }
            # Filter out None values
            amino_acids = {k: v for k, v in amino_acids.items() if v is not None}

            protein_info = ProteinInfoOut(
                protein_length=pi.protein_length,
                molecular_weight=pi.molecular_weight,
                pi=float(pi.pi) if pi.pi is not None else None,
                cai=float(pi.cai) if pi.cai is not None else None,
                codon_bias=float(pi.codon_bias) if pi.codon_bias is not None else None,
                fop_score=float(pi.fop_score) if pi.fop_score is not None else None,
                n_term_seq=pi.n_term_seq,
                c_term_seq=pi.c_term_seq,
                gravy_score=float(pi.gravy_score) if pi.gravy_score is not None else None,
                aromaticity_score=float(pi.aromaticity_score) if pi.aromaticity_score is not None else None,
                amino_acids=amino_acids if amino_acids else None,
            )

        out[organism_name] = ProteinDetailsForOrganism(
            locus_display_name=locus_display_name,
            protein_info=protein_info,
        )

    return ProteinDetailsResponse(results=out)


def get_locus_homology_details(db: Session, name: str) -> HomologyDetailsResponse:
    """
    Query homology group data for each feature matching the locus name,
    grouped by organism.
    """
    from cgd.models.locus_model import Feature
    from sqlalchemy import func, or_

    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.feat_homology).joinedload("homology_group"),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
            )
        )
        .all()
    )

    out: dict[str, HomologyDetailsForOrganism] = {}

    for f in features:
        organism_name = _get_organism_name(f)
        locus_display_name = f.gene_name or f.feature_name

        homology_groups = []
        seen_groups = set()

        for fh in f.feat_homology:
            hg = fh.homology_group
            if hg is None:
                continue

            # Skip if already processed
            if hg.homology_group_no in seen_groups:
                continue
            seen_groups.add(hg.homology_group_no)

            # Get all members in this homology group
            members = []

            # Get internal (CGD) members via feat_homology
            for other_fh in hg.feat_homology:
                other_feat = other_fh.feature
                if other_feat and other_feat.feature_no != f.feature_no:
                    other_org_name = _get_organism_name(other_feat)
                    members.append(HomologOut(
                        feature_name=other_feat.feature_name,
                        gene_name=other_feat.gene_name,
                        organism_name=other_org_name,
                        dbxref_id=other_feat.dbxref_id,
                    ))

            # Get external members via dbxref_homology
            for dh in hg.dbxref_homology:
                dbxref = dh.dbxref
                ext_org = "External"
                if dbxref:
                    ext_org = dbxref.source if hasattr(dbxref, "source") else "External"
                members.append(HomologOut(
                    feature_name=dh.name,
                    gene_name=None,
                    organism_name=ext_org,
                    dbxref_id=dbxref.dbxref_id if dbxref else dh.name,
                ))

            homology_groups.append(HomologyGroupOut(
                homology_group_type=hg.homology_group_type,
                method=hg.method,
                members=members,
            ))

        out[organism_name] = HomologyDetailsForOrganism(
            locus_display_name=locus_display_name,
            homology_groups=homology_groups,
        )

    return HomologyDetailsResponse(results=out)
