import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import locus_service, synteny_service, es_search_service, expression_service
from cgd.core.elasticsearch import get_es_client
from cgd.schemas.locus_schema import (
    LocusByOrganismResponse,
    SequenceDetailsResponse,
    LocusReferencesResponse,
    LocusSummaryNotesResponse,
    LocusHistoryResponse,
    OrthologOrganismsResponse,
    OrthologOrganismOut,
)
from cgd.schemas.phenotype_schema import PhenotypeDetailsResponse
from cgd.schemas.go_schema import GODetailsResponse
from cgd.schemas.protein_schema import ProteinDetailsResponse, ProteinPropertiesResponse, ProteinDomainResponse
from cgd.schemas.homology_schema import HomologyDetailsResponse
from cgd.schemas.synteny_schema import SyntenyResponse
from cgd.schemas.expression_schema import ExpressionDetailsResponse
from cgd.schemas.interaction_schema import (
    InteractionDetailsResponse,
    InteractionNetworkResponse,
    StringEnrichmentResponse,
    NetworkEnrichmentResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/locus", tags=["locus"])


@router.get("/{name}", response_model=LocusByOrganismResponse)
def locus(name: str, db: Session = Depends(get_db)):
    """
    Get basic locus info by name, grouped by organism.

    Returns feature info including aliases and external links.
    """
    return locus_service.get_locus_by_organism(db, name)


@router.get("/{name}/go_details", response_model=GODetailsResponse)
def go_details(name: str, db: Session = Depends(get_db)):
    """
    Get GO annotations for this locus, grouped by organism.
    """
    try:
        return locus_service.get_locus_go_details(db, name)
    except Exception as e:
        logger.error(f"Error in go_details for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/phenotype_details", response_model=PhenotypeDetailsResponse)
def phenotype_details(name: str, db: Session = Depends(get_db)):
    """
    Get phenotype annotations for this locus, grouped by organism.
    """
    try:
        return locus_service.get_locus_phenotype_details(db, name)
    except Exception as e:
        logger.error(f"Error in phenotype_details for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/protein_details", response_model=ProteinDetailsResponse)
def protein_details(name: str, db: Session = Depends(get_db)):
    """
    Get protein information for this locus, grouped by organism.

    Returns data matching the Perl protein page format:
    - Stanford Name (gene_name)
    - Systematic Name (feature_name)
    - Alias Names
    - Description (headline)
    - Experimental Observations
    - Structural Information
    - Conserved Domains
    - Sequence Detail
    - Homologs
    - External Sequence Database
    - References Cited on This Page
    """
    try:
        return locus_service.get_locus_protein_details(db, name)
    except Exception as e:
        logger.error(f"Error in protein_details for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/homology_details", response_model=HomologyDetailsResponse)
def homology_details(name: str, db: Session = Depends(get_db)):
    """
    Get homology/ortholog information for this locus, grouped by organism.
    """
    return locus_service.get_locus_homology_details(db, name)


@router.get("/{name}/sequence_details", response_model=SequenceDetailsResponse)
def sequence_details(name: str, db: Session = Depends(get_db)):
    """
    Get sequence and location information for this locus, grouped by organism.

    Returns chromosomal coordinates and DNA/protein sequences.
    """
    return locus_service.get_locus_sequence_details(db, name)


@router.get("/{name}/interaction_details", response_model=InteractionDetailsResponse)
def interaction_details(name: str, db: Session = Depends(get_db)):
    """
    Get physical interaction data for this locus, grouped by organism.

    Returns protein-protein interactions from BioGRID and other sources.
    """
    try:
        return locus_service.get_locus_interaction_details(db, name)
    except Exception as e:
        logger.error(f"Error in interaction_details for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/interaction_network", response_model=InteractionNetworkResponse)
def interaction_network(
    name: str,
    depth: int = 2,
    include_string: bool = True,
    string_score: int = 400,
    db: Session = Depends(get_db)
):
    """
    Get interaction network graph for this locus, grouped by organism.

    Returns nodes (genes) and edges (interactions) for network visualization.
    Includes interactions between interactors when depth > 1.

    Args:
        name: Locus name (gene_name, feature_name, or dbxref_id)
        depth: Network depth (1 = direct interactions only, 2 = include interactor-interactor)
        include_string: Whether to include STRING database interactions (default True)
        string_score: Minimum STRING confidence score 0-1000 (default 400 = medium)
    """
    try:
        return locus_service.get_locus_interaction_network(
            db, name, max_depth=depth,
            include_string=include_string, string_score=string_score
        )
    except Exception as e:
        logger.error(f"Error in interaction_network for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/string_enrichment", response_model=StringEnrichmentResponse)
def string_enrichment(
    name: str,
    string_score: int = 400,
    db: Session = Depends(get_db)
):
    """
    Get STRING functional enrichment of this gene's interaction network,
    grouped by organism.

    Reports GO/KEGG/Reactome terms over-represented among the gene's STRING
    network partners. Only returns data for STRING-supported organisms.

    Args:
        name: Locus name (gene_name, feature_name, or dbxref_id)
        string_score: Minimum STRING confidence score 0-1000 (default 400)
    """
    try:
        return locus_service.get_locus_string_enrichment(db, name, string_score=string_score)
    except Exception as e:
        logger.error(f"Error in string_enrichment for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/network_enrichment", response_model=NetworkEnrichmentResponse)
def network_enrichment(
    name: str,
    include_string: bool = False,
    p_value_cutoff: float = 0.01,
    db: Session = Depends(get_db)
):
    """
    CGD-native GO and phenotype enrichment of this gene's interaction network,
    grouped by organism.

    Runs CGD's GO Term Finder and Phenotype Enrichment engines (hypergeometric
    test against CGD's own annotations) over the gene's interaction partners.

    Args:
        name: Locus name (gene_name, feature_name, or dbxref_id)
        include_string: also include STRING-predicted partners in the gene set
            (default False = curated BioGRID interactors only)
        p_value_cutoff: significance cutoff (default 0.01)
    """
    try:
        return locus_service.get_locus_network_enrichment(
            db, name, include_string=include_string, p_value_cutoff=p_value_cutoff
        )
    except Exception as e:
        logger.error(f"Error in network_enrichment for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/references", response_model=LocusReferencesResponse)
def references(name: str, db: Session = Depends(get_db)):
    """
    Get references citing this locus, grouped by organism.
    """
    return locus_service.get_locus_references(db, name)


@router.get("/{name}/summary_notes", response_model=LocusSummaryNotesResponse)
def summary_notes(name: str, db: Session = Depends(get_db)):
    """
    Get summary paragraphs for this locus, grouped by organism.
    """
    return locus_service.get_locus_summary_notes(db, name)


@router.get("/{name}/history", response_model=LocusHistoryResponse)
def history(name: str, db: Session = Depends(get_db)):
    """
    Get change history for this locus, grouped by organism.
    """
    return locus_service.get_locus_history(db, name)


@router.get("/{name}/expression_details", response_model=ExpressionDetailsResponse)
def expression_details(name: str, db: Session = Depends(get_db)):
    """
    Get RNA-seq expression data for this locus, grouped by organism.

    Returns fold change values across multiple studies and conditions.
    """
    try:
        return expression_service.get_expression_details_by_organism(db, name)
    except Exception as e:
        logger.error(f"Error in expression_details for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/protein_properties", response_model=ProteinPropertiesResponse)
def protein_properties(name: str, db: Session = Depends(get_db)):
    """
    Get physico-chemical properties for this protein, grouped by organism.

    Returns:
    - Amino acid composition
    - Bulk protein properties (pI, GRAVY, aromaticity, aliphatic index, instability index)
    - Extinction coefficients
    - Codon usage statistics
    - Atomic composition
    """
    try:
        return locus_service.get_locus_protein_properties(db, name)
    except Exception as e:
        logger.error(f"Error in protein_properties for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/domain_details", response_model=ProteinDomainResponse)
def domain_details(name: str, db: Session = Depends(get_db)):
    """
    Get domain/motif information for this protein, grouped by organism.

    Returns:
    - Conserved domains (grouped by InterPro ID)
    - Transmembrane domains (TMHMM predictions)
    - Signal peptides (SignalP predictions)
    - External links to domain databases
    """
    try:
        return locus_service.get_locus_domain_details(db, name)
    except Exception as e:
        logger.error(f"Error in domain_details for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/synteny", response_model=SyntenyResponse)
def synteny(name: str, flanking_count: int = 10, db: Session = Depends(get_db)):
    """
    Get synteny data for this locus across CGD species.

    Returns flanking genes and ortholog connections for visualization
    in the synteny viewer.

    Args:
        name: Locus name (gene_name, feature_name, or dbxref_id)
        flanking_count: Number of genes upstream/downstream to include (default: 10)
    """
    try:
        return synteny_service.get_synteny_data(db, name, flanking_count)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error in synteny for {name}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/ortholog_organisms", response_model=OrthologOrganismsResponse)
def ortholog_organisms(name: str):
    """
    Get list of organisms that have orthologs for this locus.

    Uses Elasticsearch for fast lookup. Returns organism names and
    feature names for navigation to ortholog locus pages.

    Args:
        name: Locus name (gene_name, feature_name, or dbxref_id)
    """
    try:
        es = get_es_client()
        if not es:
            return OrthologOrganismsResponse(organisms=[])

        results = es_search_service.get_ortholog_organisms(es, name)
        organisms = [
            OrthologOrganismOut(organism=r["organism"], feature_name=r["feature_name"])
            for r in results
        ]
        return OrthologOrganismsResponse(organisms=organisms)
    except Exception as e:
        logger.error(f"Error in ortholog_organisms for {name}: {e}")
        logger.error(traceback.format_exc())
        return OrthologOrganismsResponse(organisms=[])


