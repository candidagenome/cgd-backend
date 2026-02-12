from datetime import datetime, timedelta

from sqlalchemy.orm import Session, joinedload
from fastapi import HTTPException

from cgd.schemas.reference_schema import (
    ReferenceResponse,
    ReferenceOut,
    AuthorOut,
    CitationLink,
    ReferenceLocusResponse,
    LocusForReference,
    ReferenceGOResponse,
    GOAnnotationForReference,
    ReferencePhenotypeResponse,
    PhenotypeForReference,
    ReferenceInteractionResponse,
    InteractionForReference,
    InteractorForReference,
    ReferenceLiteratureTopicsResponse,
    LiteratureTopic,
    FeatureForTopic,
    AuthorSearchResponse,
    ReferenceSearchResult,
    NewPapersThisWeekResponse,
    NewPaperItem,
    GenomeWideAnalysisPapersResponse,
    GenomeWideAnalysisPaper,
    GeneForPaper,
)
from cgd.models.models import (
    Reference,
    Abstract,
    RefLink,
    RefProperty,
    RefUrl,
    Url,
    Author,
    AuthorEditor,
    Interaction,
    FeatInteract,
    GoRef,
    GoAnnotation,
    RefpropFeat,
    PhenoAnnotation,
    Feature,
    Organism,
    Cv,
    CvTerm,
)


def _get_organism_info(feature) -> tuple[str, int]:
    """Extract organism name and taxon_id from a feature, with fallback."""
    org = feature.organism
    organism_name = None
    taxon_id = 0
    if org is not None:
        organism_name = (
            getattr(org, "organism_name", None)
            or getattr(org, "display_name", None)
            or getattr(org, "name", None)
        )
        taxon_id = getattr(org, "taxon_id", 0) or 0
    if not organism_name:
        organism_name = str(feature.organism_no)
    return organism_name, taxon_id


def _build_citation_links(ref, ref_urls) -> list[CitationLink]:
    """
    Build citation links for a reference.

    Generates links for:
    - CGD Paper (internal link to reference page)
    - PubMed (external link to NCBI PubMed)
    - Access Full Text (if url_type contains "linkout", e.g., "Reference LINKOUT")
    - Download Datasets (if url_type contains "reference data", "download", or "dataset")
    - Reference Supplement (if url_type contains "supplement", e.g., "Reference supplement")

    Args:
        ref: Reference object with pubmed and dbxref_id
        ref_urls: List of RefUrl objects linked to this reference

    Returns:
        List of CitationLink objects
    """
    links = []

    # CGD Paper link (always present) - always use dbxref_id (CGDID)
    links.append(CitationLink(
        name="CGD Paper",
        url=f"/reference/{ref.dbxref_id}",
        link_type="internal"
    ))

    # PubMed link (if pubmed ID exists)
    if ref.pubmed:
        links.append(CitationLink(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{ref.pubmed}",
            link_type="external"
        ))

    # Process URLs from ref_url table
    # Match Perl behavior: show all URLs except 'Reference supplement' and 'Reference Data'
    for ref_url in ref_urls:
        url_obj = ref_url.url
        if url_obj and url_obj.url:
            url_type = (url_obj.url_type or "").lower()

            # Skip Reference supplement (displayed separately)
            if "supplement" in url_type:
                links.append(CitationLink(
                    name="Reference Supplement",
                    url=url_obj.url,
                    link_type="external"
                ))
            # Skip Reference Data (not shown as full text)
            elif "reference data" in url_type:
                continue
            # Download Datasets
            elif any(kw in url_type for kw in ["download", "dataset"]):
                links.append(CitationLink(
                    name="Download Datasets",
                    url=url_obj.url,
                    link_type="external"
                ))
            # All other URL types are shown as Full Text (matching Perl default behavior)
            else:
                links.append(CitationLink(
                    name="Full Text",
                    url=url_obj.url,
                    link_type="external"
                ))

    return links


def _get_reference_by_identifier(db: Session, identifier: str) -> Reference:
    """
    Get a reference by reference_no, PubMed ID, or DBXREF_ID, raise 404 if not found.

    Args:
        db: Database session
        identifier: Either a reference_no/PubMed ID (numeric string) or a DBXREF_ID (e.g., 'CGD_REF:xxx')
    """
    base_query = (
        db.query(Reference)
        .options(
            joinedload(Reference.journal),
            joinedload(Reference.book),
            joinedload(Reference.author_editor).joinedload(AuthorEditor.author),
            joinedload(Reference.ref_url).joinedload(RefUrl.url),
            joinedload(Reference.ref_property).joinedload(RefProperty.refprop_feat)
            .joinedload(RefpropFeat.feature).joinedload(Feature.organism),
        )
    )

    ref = None

    # Try to parse as integer (could be reference_no or PubMed ID)
    try:
        num_id = int(identifier)
        # Try reference_no first (primary key)
        ref = base_query.filter(Reference.reference_no == num_id).first()
        # If not found, try PubMed ID
        if ref is None:
            ref = base_query.filter(Reference.pubmed == num_id).first()
    except ValueError:
        # Not an integer, treat as DBXREF_ID
        pass

    # If not found by numeric ID, try DBXREF_ID
    if ref is None:
        ref = base_query.filter(Reference.dbxref_id == identifier).first()

    if ref is None:
        raise HTTPException(
            status_code=404,
            detail=f"Reference with identifier '{identifier}' not found"
        )
    return ref


def get_reference(db: Session, identifier: str) -> ReferenceResponse:
    """
    Get basic reference info by PubMed ID or DBXREF_ID.
    """
    ref = _get_reference_by_identifier(db, identifier)

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

    # Query URLs explicitly from ref_url and url tables
    ref_url_records = (
        db.query(RefUrl, Url)
        .join(Url, RefUrl.url_no == Url.url_no)
        .filter(RefUrl.reference_no == ref.reference_no)
        .all()
    )

    # Extract URLs and specific URL types
    urls = []
    full_text_url = None
    supplement_url = None
    ref_url_list = []  # For building citation links
    for ref_url_obj, url_obj in ref_url_records:
        if url_obj and url_obj.url:
            urls.append(url_obj.url)
            url_type = (url_obj.url_type or "").lower()
            # Extract full text URL (url_type = "Reference LINKOUT")
            if "linkout" in url_type:
                full_text_url = url_obj.url
            # Extract supplement URL (url_type = "Reference supplement")
            elif "supplement" in url_type:
                supplement_url = url_obj.url
            # Build a simple object for citation links
            ref_url_obj.url = url_obj
            ref_url_list.append(ref_url_obj)

    # Build citation links (CGD Paper, PubMed, Full Text, etc.)
    citation_links = _build_citation_links(ref, ref_url_list)

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
        links=citation_links,
        full_text_url=full_text_url,
        supplement_url=supplement_url,
    )

    return ReferenceResponse(result=result)


def get_reference_locus_details(db: Session, identifier: str) -> ReferenceLocusResponse:
    """
    Get loci/genes addressed in this paper via ref_property -> refprop_feat -> feature.
    """
    ref = _get_reference_by_identifier(db, identifier)

    # Query RefpropFeat linked to this reference via RefProperty
    loci = []
    seen_features = set()

    for ref_prop in ref.ref_property:
        for rpf in ref_prop.refprop_feat:
            feature = rpf.feature
            if feature and feature.feature_no not in seen_features:
                seen_features.add(feature.feature_no)
                organism_name, taxon_id = _get_organism_info(feature)
                loci.append(LocusForReference(
                    feature_no=feature.feature_no,
                    feature_name=feature.feature_name,
                    gene_name=feature.gene_name,
                    organism_name=organism_name,
                    taxon_id=taxon_id,
                    headline=feature.headline,
                ))

    return ReferenceLocusResponse(
        reference_no=ref.reference_no,
        loci=loci,
    )


def get_reference_go_details(db: Session, identifier: str) -> ReferenceGOResponse:
    """
    Get GO annotations citing this reference via go_ref -> go_annotation -> feature + go.
    """
    ref = _get_reference_by_identifier(db, identifier)

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
    seen_annotation_nos = set()  # Track go_annotation_no to deduplicate

    for gr in go_refs:
        ga = gr.go_annotation
        if ga is None:
            continue

        # Deduplicate by go_annotation_no (primary key)
        if ga.go_annotation_no in seen_annotation_nos:
            continue
        seen_annotation_nos.add(ga.go_annotation_no)

        feature = ga.feature
        go = ga.go
        if feature is None or go is None:
            continue

        organism_name, taxon_id = _get_organism_info(feature)
        goid_str = f"GO:{go.goid:07d}" if isinstance(go.goid, int) else str(go.goid)

        annotations.append(GOAnnotationForReference(
            feature_name=feature.feature_name,
            gene_name=feature.gene_name,
            organism_name=organism_name,
            taxon_id=taxon_id,
            goid=goid_str,
            go_term=go.go_term,
            go_aspect=go.go_aspect,
            evidence=ga.go_evidence,
        ))

    return ReferenceGOResponse(
        reference_no=ref.reference_no,
        annotations=annotations,
    )


def get_reference_phenotype_details(db: Session, identifier: str) -> ReferencePhenotypeResponse:
    """
    Get phenotype annotations citing this reference via
    ref_link (tab_name='PHENO_ANNOTATION') -> pheno_annotation -> phenotype + feature.
    """
    ref = _get_reference_by_identifier(db, identifier)

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

            organism_name, taxon_id = _get_organism_info(feature)

            annotations.append(PhenotypeForReference(
                feature_name=feature.feature_name,
                gene_name=feature.gene_name,
                organism_name=organism_name,
                taxon_id=taxon_id,
                observable=phenotype.observable,
                qualifier=phenotype.qualifier,
                experiment_type=phenotype.experiment_type,
                mutant_type=phenotype.mutant_type,
            ))

    return ReferencePhenotypeResponse(
        reference_no=ref.reference_no,
        annotations=annotations,
    )


def get_reference_interaction_details(db: Session, identifier: str) -> ReferenceInteractionResponse:
    """
    Get interactions citing this reference via
    ref_link (tab_name='INTERACTION') -> interaction -> feat_interact -> feature.
    """
    ref = _get_reference_by_identifier(db, identifier)

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


def get_reference_literature_topics(db: Session, identifier: str) -> ReferenceLiteratureTopicsResponse:
    """
    Get literature topics (curation topics) for this reference.

    Literature topics are stored in ref_property table. Only topics that exist in
    cv_term table with cv_name='literature_topic' are included (filtering out
    internal curation states like "Basic, lit guide, GO, Pheno curation done").

    Returns topics grouped by topic name, with lists of features for each topic,
    plus a list of all unique features for building the topic matrix.
    """
    ref = _get_reference_by_identifier(db, identifier)

    # Get valid literature topics from cv_term table
    # This filters out curation status values like "Basic, lit guide, GO, Pheno curation done"
    valid_topics_query = (
        db.query(CvTerm.term_name)
        .join(Cv, CvTerm.cv_no == Cv.cv_no)
        .filter(Cv.cv_name == 'literature_topic')
        .all()
    )
    valid_topics = {row[0] for row in valid_topics_query}

    # Query RefProperty for this reference
    ref_properties = (
        db.query(RefProperty)
        .options(
            joinedload(RefProperty.refprop_feat)
            .joinedload(RefpropFeat.feature)
            .joinedload(Feature.organism),
        )
        .filter(RefProperty.reference_no == ref.reference_no)
        .all()
    )

    # Build a mapping of topic -> features
    topic_features: dict[str, list[FeatureForTopic]] = {}
    all_features_dict: dict[int, FeatureForTopic] = {}  # feature_no -> FeatureForTopic

    for ref_prop in ref_properties:
        topic = ref_prop.property_value
        if not topic:
            continue

        # Only include topics that are valid literature topics (not curation states)
        if topic not in valid_topics:
            continue

        if topic not in topic_features:
            topic_features[topic] = []

        # Get features linked to this topic via refprop_feat
        for rpf in ref_prop.refprop_feat:
            feature = rpf.feature
            if feature:
                organism_name, taxon_id = _get_organism_info(feature)
                feat_obj = FeatureForTopic(
                    feature_no=feature.feature_no,
                    feature_name=feature.feature_name,
                    gene_name=feature.gene_name,
                    organism_name=organism_name,
                    taxon_id=taxon_id,
                )
                # Add to topic's features if not already present
                if not any(f.feature_no == feature.feature_no for f in topic_features[topic]):
                    topic_features[topic].append(feat_obj)

                # Add to all_features dict
                if feature.feature_no not in all_features_dict:
                    all_features_dict[feature.feature_no] = feat_obj

    # Build the response
    topics = []
    for topic_name in sorted(topic_features.keys()):
        features = sorted(topic_features[topic_name], key=lambda f: f.gene_name or f.feature_name)
        topics.append(LiteratureTopic(
            topic=topic_name,
            features=features,
        ))

    # Sort all_features by gene_name/feature_name
    all_features = sorted(
        all_features_dict.values(),
        key=lambda f: f.gene_name or f.feature_name
    )

    return ReferenceLiteratureTopicsResponse(
        reference_no=ref.reference_no,
        topics=topics,
        all_features=all_features,
    )


def search_references_by_author(db: Session, author_name: str) -> AuthorSearchResponse:
    """
    Search for references by author name.

    Searches the author table for authors matching the given name pattern
    and returns all references associated with those authors.

    Args:
        db: Database session
        author_name: Author name to search for (case-insensitive, supports wildcards)

    Returns:
        AuthorSearchResponse with matching references
    """
    # Normalize the search pattern - uppercase and add wildcard if not present
    search_pattern = author_name.upper()
    if '*' in search_pattern:
        search_pattern = search_pattern.replace('*', '%')
    if not search_pattern.endswith('%'):
        search_pattern = search_pattern + '%'

    # Query for matching authors and their references
    # This mirrors the Perl query: author -> author_editor -> reference
    results = (
        db.query(
            Reference.reference_no,
            Reference.citation,
            Reference.year,
            Reference.pubmed,
            Reference.dbxref_id,
        )
        .distinct()
        .join(AuthorEditor, Reference.reference_no == AuthorEditor.reference_no)
        .join(Author, AuthorEditor.author_no == Author.author_no)
        .filter(Author.author_name.ilike(search_pattern))
        .order_by(Reference.year.desc())
        .all()
    )

    # Count unique authors matching the pattern
    author_count = (
        db.query(Author.author_no)
        .filter(Author.author_name.ilike(search_pattern))
        .distinct()
        .count()
    )

    # Build response with full reference info
    references = []
    for row in results:
        ref_no, citation, year, pubmed, dbxref_id = row

        # Get author list for this reference
        author_editors = (
            db.query(Author.author_name)
            .join(AuthorEditor, Author.author_no == AuthorEditor.author_no)
            .filter(AuthorEditor.reference_no == ref_no)
            .order_by(AuthorEditor.author_order)
            .all()
        )
        author_list = ', '.join([ae[0] for ae in author_editors])

        # Build citation links
        links = []
        if dbxref_id:
            links.append(CitationLink(
                name='CGD Paper',
                url=f'/reference/{dbxref_id}',
                link_type='internal',
            ))
        if pubmed:
            links.append(CitationLink(
                name='PubMed',
                url=f'https://pubmed.ncbi.nlm.nih.gov/{pubmed}',
                link_type='external',
            ))

        references.append(ReferenceSearchResult(
            reference_no=ref_no,
            dbxref_id=dbxref_id or '',
            pubmed=pubmed,
            citation=citation or '',
            year=year or 0,
            author_list=author_list,
            links=links,
        ))

    return AuthorSearchResponse(
        author_query=author_name,
        author_count=author_count,
        reference_count=len(references),
        references=references,
    )


def get_new_papers_this_week(db: Session, days: int = 7) -> NewPapersThisWeekResponse:
    """
    Get references added to CGD within the last N days (default 7).

    Args:
        db: Database session
        days: Number of days to look back (default 7)

    Returns:
        NewPapersThisWeekResponse with list of new papers
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    # Query references created within the date range
    refs = (
        db.query(Reference)
        .filter(Reference.date_created >= start_date)
        .filter(Reference.date_created <= end_date)
        .order_by(Reference.date_created.desc())
        .all()
    )

    # Build response
    references = []
    for ref in refs:
        # Get URLs for citation links
        ref_url_records = (
            db.query(RefUrl, Url)
            .join(Url, RefUrl.url_no == Url.url_no)
            .filter(RefUrl.reference_no == ref.reference_no)
            .all()
        )

        # Build ref_url list for citation links
        ref_url_list = []
        for ref_url_obj, url_obj in ref_url_records:
            if url_obj and url_obj.url:
                ref_url_obj.url = url_obj
                ref_url_list.append(ref_url_obj)

        # Build citation links
        links = _build_citation_links(ref, ref_url_list)

        references.append(NewPaperItem(
            reference_no=ref.reference_no,
            dbxref_id=ref.dbxref_id,
            pubmed=ref.pubmed,
            citation=ref.citation,
            title=ref.title,
            year=ref.year,
            date_created=ref.date_created.isoformat() if ref.date_created else "",
            links=links,
        ))

    return NewPapersThisWeekResponse(
        start_date=start_date.date().isoformat(),
        end_date=end_date.date().isoformat(),
        total_count=len(references),
        references=references,
    )


GENOME_WIDE_TOPICS = [
    "Genome-wide Analysis",
    "Proteome-wide Analysis",
    "Comparative genomic hybridization",
    "Computational analysis",
    "Genomic co-immunoprecipitation study",
    "Genomic expression study",
    "Large-scale genetic interaction",
    "Large-scale phenotype analysis",
    "Other genomic analysis",
    "Large-scale protein detection",
    "Large-scale protein interaction",
    "Large-scale protein localization",
    "Large-scale protein modification",
    "Other large-scale proteomic analysis",
]


def get_genome_wide_analysis_papers(
    db: Session,
    topic: str = None,
    page: int = 1,
    page_size: int = 50,
) -> GenomeWideAnalysisPapersResponse:
    """
    Get references tagged with genome-wide analysis literature topics.

    Args:
        db: Database session
        topic: Optional specific topic to filter by
        page: Page number (1-indexed)
        page_size: Number of results per page

    Returns:
        GenomeWideAnalysisPapersResponse with list of genome-wide analysis papers
    """
    # Determine which topics to filter by
    if topic and topic in GENOME_WIDE_TOPICS:
        filter_topics = [topic]
    else:
        filter_topics = GENOME_WIDE_TOPICS
        topic = None  # Reset to None if invalid

    # Get total count first
    total_count = (
        db.query(Reference.reference_no)
        .join(RefProperty, Reference.reference_no == RefProperty.reference_no)
        .filter(RefProperty.property_value.in_(filter_topics))
        .distinct()
        .count()
    )

    # Calculate pagination
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    offset = (page - 1) * page_size

    # Query references with pagination
    ref_nos = (
        db.query(Reference.reference_no)
        .join(RefProperty, Reference.reference_no == RefProperty.reference_no)
        .filter(RefProperty.property_value.in_(filter_topics))
        .distinct()
        .order_by(Reference.reference_no.desc())
        .offset(offset)
        .limit(page_size)
        .all()
    )
    ref_no_list = [r[0] for r in ref_nos]

    # Fetch full reference data
    refs = (
        db.query(Reference)
        .filter(Reference.reference_no.in_(ref_no_list))
        .order_by(Reference.year.desc(), Reference.citation)
        .all()
    )

    # Build response
    references = []
    for ref in refs:
        # Get literature topics for this reference (only genome-wide ones)
        ref_topics = (
            db.query(RefProperty.property_value)
            .filter(
                RefProperty.reference_no == ref.reference_no,
                RefProperty.property_value.in_(GENOME_WIDE_TOPICS),
            )
            .distinct()
            .all()
        )
        paper_topics = [t[0] for t in ref_topics]

        # Get species from organisms of features linked to this reference
        species_query = (
            db.query(Organism.organism_name)
            .join(Feature, Organism.organism_no == Feature.organism_no)
            .join(RefpropFeat, Feature.feature_no == RefpropFeat.feature_no)
            .join(RefProperty, RefpropFeat.ref_property_no == RefProperty.ref_property_no)
            .filter(RefProperty.reference_no == ref.reference_no)
            .distinct()
            .all()
        )
        species = [s[0] for s in species_query]

        # Get genes addressed via refprop_feat
        genes_query = (
            db.query(Feature.feature_name, Feature.gene_name)
            .join(RefpropFeat, Feature.feature_no == RefpropFeat.feature_no)
            .join(RefProperty, RefpropFeat.ref_property_no == RefProperty.ref_property_no)
            .filter(RefProperty.reference_no == ref.reference_no)
            .distinct()
            .limit(10)  # Limit to avoid huge lists
            .all()
        )
        genes = [
            GeneForPaper(feature_name=fn, gene_name=gn)
            for fn, gn in genes_query
        ]

        # Get URLs for citation links
        ref_url_records = (
            db.query(RefUrl, Url)
            .join(Url, RefUrl.url_no == Url.url_no)
            .filter(RefUrl.reference_no == ref.reference_no)
            .all()
        )

        ref_url_list = []
        for ref_url_obj, url_obj in ref_url_records:
            if url_obj and url_obj.url:
                ref_url_obj.url = url_obj
                ref_url_list.append(ref_url_obj)

        links = _build_citation_links(ref, ref_url_list)

        references.append(GenomeWideAnalysisPaper(
            reference_no=ref.reference_no,
            dbxref_id=ref.dbxref_id,
            pubmed=ref.pubmed,
            citation=ref.citation,
            year=ref.year,
            topics=paper_topics,
            species=species,
            genes=genes,
            links=links,
        ))

    return GenomeWideAnalysisPapersResponse(
        available_topics=GENOME_WIDE_TOPICS,
        selected_topic=topic,
        total_count=total_count,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        references=references,
    )
