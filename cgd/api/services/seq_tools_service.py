"""
Sequence Tools Service - handles gene/sequence lookup and tool link generation.
"""
from __future__ import annotations

from typing import Optional
from urllib.parse import quote, urlencode
from sqlalchemy.orm import Session
from sqlalchemy import func

from cgd.models.models import Feature, Seq, FeatLocation, Organism, GenomeVersion
from cgd.schemas.seq_tools_schema import (
    InputType,
    SeqType,
    ToolLink,
    ToolCategory,
    FeatureInfo,
    SeqToolsResponse,
    AssemblyInfo,
    AssembliesResponse,
    ChromosomeInfo,
    ChromosomesResponse,
)


# JBrowse configuration
JBROWSE_BASE_URL = "https://www.candidagenome.org/jbrowse"
DEFAULT_JBROWSE_TRACKS = "DNA,Genes"


def resolve_gene_query(
    db: Session,
    query: str,
    seq_source: Optional[str] = None,
) -> Optional[FeatureInfo]:
    """
    Resolve a gene query to feature information.

    Args:
        db: Database session
        query: Gene name, ORF name, feature name, or CGDID
        seq_source: Optional assembly/sequence source

    Returns:
        FeatureInfo with resolved feature data, or None if not found
    """
    query_upper = query.strip().upper()

    # Find feature by gene_name, feature_name, or dbxref_id
    feature = (
        db.query(Feature)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(func.upper(Feature.gene_name) == query_upper)
        .first()
    )

    if not feature:
        feature = (
            db.query(Feature)
            .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
            .filter(func.upper(Feature.feature_name) == query_upper)
            .first()
        )

    if not feature:
        feature = (
            db.query(Feature)
            .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
            .filter(func.upper(Feature.dbxref_id) == query_upper)
            .first()
        )

    if not feature:
        return None

    # Get location info
    location = (
        db.query(FeatLocation)
        .filter(
            FeatLocation.feature_no == feature.feature_no,
            FeatLocation.is_loc_current == "Y"
        )
        .first()
    )

    chromosome = None
    start_coord = None
    end_coord = None
    strand = None

    if location:
        # Get chromosome name from root sequence
        root_seq = (
            db.query(Seq)
            .join(Feature, Seq.feature_no == Feature.feature_no)
            .filter(Seq.seq_no == location.root_seq_no)
            .first()
        )
        if root_seq and root_seq.feature:
            chromosome = root_seq.feature.feature_name

        start_coord = location.start_coord
        end_coord = location.stop_coord
        strand = location.strand

    organism_name = feature.organism.organism_name if feature.organism else "Unknown"

    return FeatureInfo(
        feature_name=feature.feature_name,
        gene_name=feature.gene_name,
        dbxref_id=feature.dbxref_id,
        organism=organism_name,
        chromosome=chromosome,
        start=start_coord,
        end=end_coord,
        strand=strand,
    )


def _build_jbrowse_link(
    chromosome: Optional[str],
    start: Optional[int],
    end: Optional[int],
    flank_left: int = 0,
    flank_right: int = 0,
) -> Optional[str]:
    """Build JBrowse URL for a genomic location."""
    if not chromosome or not start or not end:
        return None

    # Add flanking regions
    view_start = max(1, start - flank_left)
    view_end = end + flank_right

    params = {
        "loc": f"{chromosome}:{view_start}..{view_end}",
        "tracks": DEFAULT_JBROWSE_TRACKS,
    }
    return f"{JBROWSE_BASE_URL}?{urlencode(params)}"


def _build_blast_link(sequence: str) -> str:
    """Build BLAST link for a sequence."""
    params = {"seq": sequence[:5000]}  # Limit sequence length for URL
    return f"/blast?{urlencode(params)}"


def _build_blast_link_for_locus(locus: str) -> str:
    """Build BLAST link for a locus name."""
    params = {"locus": locus, "qtype": "locus"}
    return f"/blast?{urlencode(params)}"


def _build_pattern_match_link(sequence: str) -> str:
    """Build pattern match link for a short sequence."""
    params = {"pattern": sequence[:100]}
    return f"/patmatch?{urlencode(params)}"


def _build_restriction_map_link(sequence: str) -> str:
    """Build restriction map link for DNA sequence."""
    params = {"seq": sequence[:5000], "type": "sequence"}
    return f"/restriction-mapper?{urlencode(params)}"


def _build_restriction_map_link_for_locus(locus: str) -> str:
    """Build restriction map link for a locus."""
    params = {"locus": locus, "type": "locus"}
    return f"/restriction-mapper?{urlencode(params)}"


def _build_primer_design_link(sequence: str) -> str:
    """Build primer design link for DNA sequence."""
    return f"/webprimer?seq={quote(sequence)}"


def _build_sequence_retrieval_links(
    feature_name: str,
    flank_left: int = 0,
    flank_right: int = 0,
) -> list[ToolLink]:
    """Build sequence retrieval links for a feature."""
    links = []

    # DNA/Genomic sequence
    dna_params = {
        "locus": feature_name,
        "seqtype": "genomic",
        "format": "fasta",
    }
    if flank_left > 0:
        dna_params["flankl"] = flank_left
    if flank_right > 0:
        dna_params["flankr"] = flank_right

    links.append(ToolLink(
        name="DNA Sequence (FASTA)",
        url=f"/api/sequence?{urlencode(dna_params)}",
        description="Download genomic DNA sequence in FASTA format",
    ))

    # Coding sequence (CDS)
    coding_params = {
        "locus": feature_name,
        "seqtype": "coding",
        "format": "fasta",
    }
    links.append(ToolLink(
        name="Coding Sequence (FASTA)",
        url=f"/api/sequence?{urlencode(coding_params)}",
        description="Download coding sequence (exons only) in FASTA format",
    ))

    # Protein sequence
    protein_params = {
        "locus": feature_name,
        "seqtype": "protein",
        "format": "fasta",
    }
    links.append(ToolLink(
        name="Protein Sequence (FASTA)",
        url=f"/api/sequence?{urlencode(protein_params)}",
        description="Download protein sequence in FASTA format",
    ))

    return links


def _build_coordinate_sequence_link(
    chromosome: str,
    start: int,
    end: int,
    flank_left: int = 0,
    flank_right: int = 0,
) -> str:
    """Build sequence retrieval link for coordinates."""
    params = {
        "chr": chromosome,
        "start": max(1, start - flank_left),
        "end": end + flank_right,
        "format": "fasta",
    }
    return f"/api/sequence/region?{urlencode(params)}"


def get_tools_for_gene(
    db: Session,
    feature: FeatureInfo,
    flank_left: int = 0,
    flank_right: int = 0,
    reverse_complement: bool = False,
) -> list[ToolCategory]:
    """
    Get available tools for a gene/feature.

    Args:
        db: Database session
        feature: Resolved feature information
        flank_left: Left flanking bp
        flank_right: Right flanking bp
        reverse_complement: Whether to show reverse complement option

    Returns:
        List of tool categories with links
    """
    categories = []

    # Biology/Literature category
    bio_tools = [
        ToolLink(
            name="Locus Information",
            url=f"/locus/{feature.feature_name}",
            description="View detailed locus information page",
        ),
        ToolLink(
            name="Literature Summaries",
            url=f"/locus/{feature.feature_name}#references",
            description="View literature references for this gene",
        ),
    ]
    categories.append(ToolCategory(name="Biology/Literature", tools=bio_tools))

    # Maps/Tables category
    maps_tools = []
    jbrowse_url = _build_jbrowse_link(
        feature.chromosome,
        feature.start,
        feature.end,
        flank_left,
        flank_right,
    )
    if jbrowse_url:
        maps_tools.append(ToolLink(
            name="JBrowse Genome Browser",
            url=jbrowse_url,
            description="View in JBrowse genome browser",
            external=True,
        ))

    if feature.chromosome and feature.start and feature.end:
        batch_params = {
            "chrFeatTable": "1",
            "chr": feature.chromosome,
            "beg": max(1, feature.start - flank_left),
            "end": feature.end + flank_right,
        }
        maps_tools.append(ToolLink(
            name="Batch Download Features",
            url=f"/batch-download?{urlencode(batch_params)}",
            description="Download features in this region",
        ))

    if maps_tools:
        categories.append(ToolCategory(name="Maps/Tables", tools=maps_tools))

    # Sequence Analysis category
    analysis_tools = []

    # Get the sequence for analysis tools
    seq_record = (
        db.query(Seq)
        .filter(
            Seq.feature_no == (
                db.query(Feature.feature_no)
                .filter(Feature.feature_name == feature.feature_name)
                .scalar_subquery()
            ),
            Seq.seq_type == "genomic",
            Seq.is_seq_current == "Y"
        )
        .first()
    )

    if seq_record and seq_record.residues:
        seq_len = len(seq_record.residues)

        # BLAST (for sequences > 15 bp)
        if seq_len > 15:
            analysis_tools.append(ToolLink(
                name="BLAST",
                url=_build_blast_link_for_locus(feature.feature_name),
                description="Search for similar sequences using BLAST",
            ))

        # Pattern Match (for short sequences <= 20 bp)
        if seq_len <= 20:
            analysis_tools.append(ToolLink(
                name="Pattern Match",
                url=f"/patmatch?dnaPat={quote(seq_record.residues[:100])}",
                description="Search for this pattern in the genome",
            ))

        # Restriction Map
        analysis_tools.append(ToolLink(
            name="Restriction Map",
            url=_build_restriction_map_link_for_locus(feature.feature_name),
            description="Show restriction enzyme cut sites",
        ))

        # Primer Design (for sequences > 15 bp)
        if seq_len > 15:
            analysis_tools.append(ToolLink(
                name="Design Primers",
                url=f"/webprimer?locus={feature.feature_name}",
                description="Design PCR primers for this sequence",
            ))

    if analysis_tools:
        categories.append(ToolCategory(name="Sequence Analysis", tools=analysis_tools))

    # Sequence Retrieval category
    retrieval_tools = _build_sequence_retrieval_links(
        feature.feature_name,
        flank_left,
        flank_right,
    )
    categories.append(ToolCategory(name="Sequence Retrieval", tools=retrieval_tools))

    return categories


def get_tools_for_coordinates(
    chromosome: str,
    start: int,
    end: int,
    flank_left: int = 0,
    flank_right: int = 0,
    reverse_complement: bool = False,
) -> list[ToolCategory]:
    """
    Get available tools for chromosomal coordinates.

    Args:
        chromosome: Chromosome name
        start: Start coordinate
        end: End coordinate
        flank_left: Left flanking bp
        flank_right: Right flanking bp
        reverse_complement: Whether to show reverse complement option

    Returns:
        List of tool categories with links
    """
    categories = []

    # Calculate actual region with flanking
    view_start = max(1, start - flank_left)
    view_end = end + flank_right

    # Maps/Tables category
    maps_tools = []

    jbrowse_url = _build_jbrowse_link(chromosome, start, end, flank_left, flank_right)
    if jbrowse_url:
        maps_tools.append(ToolLink(
            name="JBrowse Genome Browser",
            url=jbrowse_url,
            description="View in JBrowse genome browser",
            external=True,
        ))

    batch_params = {
        "chrFeatTable": "1",
        "chr": chromosome,
        "beg": view_start,
        "end": view_end,
    }
    maps_tools.append(ToolLink(
        name="Batch Download Features",
        url=f"/batch-download?{urlencode(batch_params)}",
        description="Download features in this region",
    ))

    categories.append(ToolCategory(name="Maps/Tables", tools=maps_tools))

    # Sequence Retrieval category
    retrieval_tools = [
        ToolLink(
            name="DNA Sequence (FASTA)",
            url=_build_coordinate_sequence_link(
                chromosome, start, end, flank_left, flank_right
            ),
            description="Download genomic DNA sequence for this region",
        ),
    ]
    categories.append(ToolCategory(name="Sequence Retrieval", tools=retrieval_tools))

    return categories


def get_tools_for_sequence(
    sequence: str,
    seq_type: SeqType,
) -> list[ToolCategory]:
    """
    Get available tools for a raw sequence.

    Args:
        sequence: Raw DNA or protein sequence
        seq_type: Type of sequence (DNA or protein)

    Returns:
        List of tool categories with links
    """
    categories = []
    analysis_tools = []

    # Clean sequence (remove whitespace and numbers)
    clean_seq = "".join(c for c in sequence if c.isalpha())
    seq_len = len(clean_seq)

    if seq_len == 0:
        return categories

    # Sequence to use in URLs (limit to reasonable size)
    url_seq = clean_seq[:5000]

    # BLAST (for sequences > 15 residues)
    if seq_len > 15:
        analysis_tools.append(ToolLink(
            name="BLAST",
            url=_build_blast_link(url_seq),
            description="Search for similar sequences using BLAST",
        ))

    if seq_type == SeqType.DNA:
        # Pattern Match (for short DNA sequences <= 20 bp)
        if seq_len <= 20:
            analysis_tools.append(ToolLink(
                name="Pattern Match",
                url=_build_pattern_match_link(url_seq),
                description="Search for this pattern in the genome",
            ))

        # Restriction Map
        if seq_len >= 10:
            analysis_tools.append(ToolLink(
                name="Restriction Map",
                url=_build_restriction_map_link(url_seq),
                description="Show restriction enzyme cut sites",
            ))

        # Primer Design (for sequences > 15 bp)
        if seq_len > 15:
            analysis_tools.append(ToolLink(
                name="Design Primers",
                url=_build_primer_design_link(url_seq),
                description="Design PCR primers for this sequence",
            ))

    if analysis_tools:
        categories.append(ToolCategory(name="Sequence Analysis", tools=analysis_tools))

    return categories


def get_available_assemblies(db: Session) -> AssembliesResponse:
    """
    Get list of available assemblies/genome versions.

    Args:
        db: Database session

    Returns:
        AssembliesResponse with available assemblies
    """
    assemblies = []

    # Query genome versions with organism info
    genome_versions = (
        db.query(GenomeVersion, Organism)
        .join(Organism, GenomeVersion.organism_no == Organism.organism_no)
        .filter(GenomeVersion.is_ver_current == "Y")
        .order_by(Organism.organism_order, GenomeVersion.genome_version)
        .all()
    )

    for gv, org in genome_versions:
        assemblies.append(AssemblyInfo(
            name=gv.genome_version,
            display_name=f"{org.organism_name} ({gv.genome_version})",
            organism=org.organism_name,
            is_default=(gv.is_ver_current == "Y"),
        ))

    # If no versions found, add a default
    if not assemblies:
        assemblies.append(AssemblyInfo(
            name="default",
            display_name="C. albicans SC5314 Assembly 22",
            organism="Candida albicans SC5314",
            is_default=True,
        ))

    return AssembliesResponse(assemblies=assemblies)


def get_chromosomes(
    db: Session,
    seq_source: Optional[str] = None,
) -> ChromosomesResponse:
    """
    Get list of chromosomes for an assembly.

    Args:
        db: Database session
        seq_source: Assembly/genome version name

    Returns:
        ChromosomesResponse with available chromosomes
    """
    chromosomes = []

    # Query chromosome features
    query = (
        db.query(Feature, Seq)
        .join(Seq, Feature.feature_no == Seq.feature_no)
        .filter(
            Feature.feature_type == "chromosome",
            Seq.seq_type == "genomic",
            Seq.is_seq_current == "Y",
        )
        .order_by(Feature.feature_name)
    )

    results = query.all()

    for feature, seq in results:
        chromosomes.append(ChromosomeInfo(
            name=feature.feature_name,
            display_name=feature.feature_name,
            length=seq.seq_length if seq else None,
        ))

    return ChromosomesResponse(chromosomes=chromosomes)


def resolve_and_get_tools(
    db: Session,
    query: Optional[str] = None,
    seq_source: Optional[str] = None,
    chromosome: Optional[str] = None,
    start: Optional[int] = None,
    end: Optional[int] = None,
    sequence: Optional[str] = None,
    seq_type: Optional[SeqType] = None,
    flank_left: int = 0,
    flank_right: int = 0,
    reverse_complement: bool = False,
) -> Optional[SeqToolsResponse]:
    """
    Main entry point: resolve input and return appropriate tools.

    Args:
        db: Database session
        query: Gene name, ORF, or CGDID for gene lookup
        seq_source: Assembly selector
        chromosome: Chromosome name for coordinate lookup
        start: Start coordinate
        end: End coordinate
        sequence: Raw sequence input
        seq_type: Type of raw sequence (DNA or protein)
        flank_left: Left flanking bp
        flank_right: Right flanking bp
        reverse_complement: Whether to use reverse complement

    Returns:
        SeqToolsResponse with resolved info and tools, or None if invalid input
    """
    # Determine input type and process accordingly
    if query:
        # Gene/ORF query
        feature = resolve_gene_query(db, query, seq_source)
        if not feature:
            return None

        categories = get_tools_for_gene(
            db, feature, flank_left, flank_right, reverse_complement
        )

        return SeqToolsResponse(
            input_type=InputType.GENE,
            feature=feature,
            categories=categories,
        )

    elif chromosome and start is not None and end is not None:
        # Coordinate query
        if start > end:
            return None

        categories = get_tools_for_coordinates(
            chromosome, start, end, flank_left, flank_right, reverse_complement
        )

        return SeqToolsResponse(
            input_type=InputType.COORDINATES,
            feature=None,
            categories=categories,
        )

    elif sequence:
        # Raw sequence input
        effective_seq_type = seq_type or SeqType.DNA
        clean_seq = "".join(c for c in sequence if c.isalpha())

        if not clean_seq:
            return None

        categories = get_tools_for_sequence(clean_seq, effective_seq_type)

        return SeqToolsResponse(
            input_type=InputType.SEQUENCE,
            feature=None,
            categories=categories,
            sequence_length=len(clean_seq),
        )

    return None
