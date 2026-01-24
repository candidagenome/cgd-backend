from sqlalchemy.orm import Session, joinedload
from fastapi import HTTPException

from cgd.schemas.reference_schema import (
    ReferenceResponse,
    ReferenceOut,
    AuthorOut,
    ReferenceLocusResponse,
    LocusForReference,
    ReferenceGOResponse,
    GOAnnotationForReference,
    ReferencePhenotypeResponse,
    PhenotypeForReference,
    ReferenceInteractionResponse,
    InteractionForReference,
    InteractorForReference,
)
from cgd.models.models import (
    Reference,
    Abstract,
    RefLink,
    RefProperty,
    RefUrl,
    AuthorEditor,
    Interaction,
    FeatInteract,
    GoRef,
    GoAnnotation,
    RefpropFeat,
    PhenoAnnotation,
    Feature,
)


def _get_organism_name(feature) -> str:
    """Extract organism name from a feature, with fallback."""
    org = feature.organism
    organism_name = None
    if org is not None:
        organism_name = (
            getattr(org, "organism_name", None)
            or getattr(org, "display_name", None)
            or getattr(org, "name", None)
        )
    if not organism_name:
        organism_name = str(feature.organism_no)
    return organism_name


def _get_reference_by_pubmed(db: Session, pubmed_id: int) -> Reference:
    """Get a reference by PubMed ID, raise 404 if not found."""
    ref = (
        db.query(Reference)
        .options(
            joinedload(Reference.journal),
            joinedload(Reference.book),
            joinedload(Reference.author_editor).joinedload(AuthorEditor.author),
            joinedload(Reference.ref_url).joinedload(RefUrl.url),
            joinedload(Reference.ref_property).joinedload(RefProperty.refprop_feat)
            .joinedload(RefpropFeat.feature).joinedload(Feature.organism),
        )
        .filter(Reference.pubmed == pubmed_id)
        .first()
    )
    if ref is None:
        raise HTTPException(status_code=404, detail=f"Reference with PubMed ID {pubmed_id} not found")
    return ref


def get_reference(db: Session, pubmed_id: int) -> ReferenceResponse:
    """
    Get basic reference info by PubMed ID.
    """
    ref = _get_reference_by_pubmed(db, pubmed_id)

    # Get authors ordered by author_order
    authors = []
    for ae in sorted(ref.author_editor, key=lambda x: x.author_order):
        author = ae.author
        if author:
            authors.append(AuthorOut(
                author_name=author.author_name,
                author_type=ae.author_type,
                author_order=ae.author_order,
            ))

    # Get journal info
    journal_name = None
    journal_abbrev = None
    if ref.journal:
        journal_name = ref.journal.full_name
        journal_abbrev = ref.journal.abbreviation

    # Get abstract (Abstract is a subclass of Reference with same primary key)
    abstract_text = None
    abstract_obj = db.query(Abstract).filter(Abstract.reference_no == ref.reference_no).first()
    if abstract_obj:
        abstract_text = abstract_obj.abstract

    # Get URLs from ref_url relationship
    urls = []
    for ref_url in ref.ref_url:
        url_obj = ref_url.url
        if url_obj:
            urls.append(url_obj.url)

    result = ReferenceOut(
        reference_no=ref.reference_no,
        dbxref_id=ref.dbxref_id,
        pubmed=ref.pubmed,
        citation=ref.citation,
        title=ref.title,
        year=ref.year,
        status=ref.status,
        source=ref.source,
        journal_name=journal_name,
        journal_abbrev=journal_abbrev,
        volume=ref.volume,
        issue=ref.issue,
        page=ref.page,
        authors=authors,
        abstract=abstract_text,
        urls=urls,
    )

    return ReferenceResponse(result=result)


def get_reference_locus_details(db: Session, pubmed_id: int) -> ReferenceLocusResponse:
    """
    Get loci/genes addressed in this paper via ref_property -> refprop_feat -> feature.
    """
    ref = _get_reference_by_pubmed(db, pubmed_id)

    # Query RefpropFeat linked to this reference via RefProperty
    loci = []
    seen_features = set()

    for ref_prop in ref.ref_property:
        for rpf in ref_prop.refprop_feat:
            feature = rpf.feature
            if feature and feature.feature_no not in seen_features:
                seen_features.add(feature.feature_no)
                organism_name = _get_organism_name(feature)
                loci.append(LocusForReference(
                    feature_no=feature.feature_no,
                    feature_name=feature.feature_name,
                    gene_name=feature.gene_name,
                    organism_name=organism_name,
                    headline=feature.headline,
                ))

    return ReferenceLocusResponse(
        reference_no=ref.reference_no,
        loci=loci,
    )


def get_reference_go_details(db: Session, pubmed_id: int) -> ReferenceGOResponse:
    """
    Get GO annotations citing this reference via go_ref -> go_annotation -> feature + go.
    """
    ref = _get_reference_by_pubmed(db, pubmed_id)

    # Query GoRef records for this reference
    go_refs = (
        db.query(GoRef)
        .options(
            joinedload(GoRef.go_annotation).joinedload(GoAnnotation.feature)
            .joinedload(Feature.organism),
            joinedload(GoRef.go_annotation).joinedload(GoAnnotation.go),
        )
        .filter(GoRef.reference_no == ref.reference_no)
        .all()
    )

    annotations = []
    for gr in go_refs:
        ga = gr.go_annotation
        if ga is None:
            continue

        feature = ga.feature
        go = ga.go
        if feature is None or go is None:
            continue

        organism_name = _get_organism_name(feature)
        goid_str = f"GO:{go.goid:07d}" if isinstance(go.goid, int) else str(go.goid)

        annotations.append(GOAnnotationForReference(
            feature_name=feature.feature_name,
            gene_name=feature.gene_name,
            organism_name=organism_name,
            goid=goid_str,
            go_term=go.go_term,
            go_aspect=go.go_aspect,
            evidence=ga.go_evidence,
        ))

    return ReferenceGOResponse(
        reference_no=ref.reference_no,
        annotations=annotations,
    )


def get_reference_phenotype_details(db: Session, pubmed_id: int) -> ReferencePhenotypeResponse:
    """
    Get phenotype annotations citing this reference via
    ref_link (tab_name='PHENO_ANNOTATION') -> pheno_annotation -> phenotype + feature.
    """
    ref = _get_reference_by_pubmed(db, pubmed_id)

    # Get ref_links for PHENO_ANNOTATION
    ref_links = (
        db.query(RefLink)
        .filter(
            RefLink.reference_no == ref.reference_no,
            RefLink.tab_name == "PHENO_ANNOTATION",
        )
        .all()
    )

    annotations = []
    pheno_annotation_nos = [rl.primary_key for rl in ref_links]

    if pheno_annotation_nos:
        pheno_anns = (
            db.query(PhenoAnnotation)
            .options(
                joinedload(PhenoAnnotation.feature).joinedload(Feature.organism),
                joinedload(PhenoAnnotation.phenotype),
            )
            .filter(PhenoAnnotation.pheno_annotation_no.in_(pheno_annotation_nos))
            .all()
        )

        for pa in pheno_anns:
            feature = pa.feature
            phenotype = pa.phenotype
            if feature is None or phenotype is None:
                continue

            organism_name = _get_organism_name(feature)

            annotations.append(PhenotypeForReference(
                feature_name=feature.feature_name,
                gene_name=feature.gene_name,
                organism_name=organism_name,
                observable=phenotype.observable,
                qualifier=phenotype.qualifier,
                experiment_type=phenotype.experiment_type,
                mutant_type=phenotype.mutant_type,
            ))

    return ReferencePhenotypeResponse(
        reference_no=ref.reference_no,
        annotations=annotations,
    )


def get_reference_interaction_details(db: Session, pubmed_id: int) -> ReferenceInteractionResponse:
    """
    Get interactions citing this reference via
    ref_link (tab_name='INTERACTION') -> interaction -> feat_interact -> feature.
    """
    ref = _get_reference_by_pubmed(db, pubmed_id)

    # Get ref_links for INTERACTION
    ref_links = (
        db.query(RefLink)
        .filter(
            RefLink.reference_no == ref.reference_no,
            RefLink.tab_name == "INTERACTION",
        )
        .all()
    )

    interactions = []
    interaction_nos = [rl.primary_key for rl in ref_links]

    if interaction_nos:
        interaction_objs = (
            db.query(Interaction)
            .options(
                joinedload(Interaction.feat_interact).joinedload(FeatInteract.feature),
            )
            .filter(Interaction.interaction_no.in_(interaction_nos))
            .all()
        )

        for interaction in interaction_objs:
            interactors = []
            for fi in interaction.feat_interact:
                feature = fi.feature
                if feature:
                    interactors.append(InteractorForReference(
                        feature_name=feature.feature_name,
                        gene_name=feature.gene_name,
                        action=fi.action,
                    ))

            interactions.append(InteractionForReference(
                interaction_no=interaction.interaction_no,
                experiment_type=interaction.experiment_type,
                description=interaction.description,
                interactors=interactors,
            ))

    return ReferenceInteractionResponse(
        reference_no=ref.reference_no,
        interactions=interactions,
    )
