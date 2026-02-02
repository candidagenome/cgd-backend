"""
Elasticsearch indexing service.

Provides functions to populate Elasticsearch from the database.
"""
from __future__ import annotations

import logging
from typing import Generator

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from sqlalchemy.orm import Session, joinedload

from cgd.core.elasticsearch import INDEX_NAME, INDEX_MAPPING
from cgd.models.models import Feature, Go, Phenotype, Reference, FeatAlias

logger = logging.getLogger(__name__)


def create_index(es: Elasticsearch) -> None:
    """Create the Elasticsearch index with mappings."""
    if es.indices.exists(index=INDEX_NAME):
        logger.info(f"Index '{INDEX_NAME}' already exists, skipping creation")
        return

    es.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)
    logger.info(f"Created index '{INDEX_NAME}'")


def delete_index(es: Elasticsearch) -> None:
    """Delete the Elasticsearch index if it exists."""
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        logger.info(f"Deleted index '{INDEX_NAME}'")
    else:
        logger.info(f"Index '{INDEX_NAME}' does not exist, nothing to delete")


def _format_goid(goid: int) -> str:
    """Format GOID as GO:XXXXXXX (7-digit padded)."""
    return f"GO:{goid:07d}"


def _generate_gene_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for genes/features."""
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.feat_alias).joinedload(FeatAlias.alias)
        )
        .all()
    )

    for feat in features:
        # Collect aliases
        aliases = []
        for fa in feat.feat_alias:
            if fa.alias and fa.alias.alias_name:
                aliases.append(fa.alias.alias_name)

        display_name = feat.gene_name or feat.feature_name
        organism_name = feat.organism.organism_name if feat.organism else None

        doc = {
            "_index": INDEX_NAME,
            "_id": f"gene_{feat.feature_no}",
            "_source": {
                "type": "gene",
                "id": feat.dbxref_id,
                "name": display_name,
                "gene_name": feat.gene_name,
                "feature_name": feat.feature_name,
                "dbxref_id": feat.dbxref_id,
                "headline": feat.headline,
                "aliases": " ".join(aliases) if aliases else None,
                "organism": organism_name,
                "link": f"/locus/{feat.feature_name}",
            }
        }
        yield doc


def index_genes(db: Session, es: Elasticsearch) -> int:
    """Index all genes/features with their aliases."""
    success, _ = bulk(es, _generate_gene_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} genes")
    return success


def _generate_go_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for GO terms."""
    go_terms = db.query(Go).all()

    for go in go_terms:
        formatted_goid = _format_goid(go.goid)
        doc = {
            "_index": INDEX_NAME,
            "_id": f"go_{go.go_no}",
            "_source": {
                "type": "go_term",
                "id": formatted_goid,
                "name": go.go_term,
                "goid": formatted_goid,
                "go_term": go.go_term,
                "go_aspect": go.go_aspect,
                "go_definition": go.go_definition,
                "link": f"/go/{formatted_goid}",
            }
        }
        yield doc


def index_go_terms(db: Session, es: Elasticsearch) -> int:
    """Index all GO terms."""
    success, _ = bulk(es, _generate_go_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} GO terms")
    return success


def _generate_phenotype_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for distinct phenotype observables."""
    # Get distinct observables
    observables = (
        db.query(Phenotype.observable)
        .distinct()
        .all()
    )

    for idx, (observable,) in enumerate(observables):
        doc = {
            "_index": INDEX_NAME,
            "_id": f"phenotype_{idx}",
            "_source": {
                "type": "phenotype",
                "id": observable,
                "name": observable,
                "observable": observable,
                "link": f"/phenotype/search?observable={observable}",
            }
        }
        yield doc


def index_phenotypes(db: Session, es: Elasticsearch) -> int:
    """Index distinct phenotype observables."""
    success, _ = bulk(es, _generate_phenotype_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} phenotypes")
    return success


def _generate_reference_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for references."""
    references = db.query(Reference).all()

    for ref in references:
        display_name = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id
        doc = {
            "_index": INDEX_NAME,
            "_id": f"reference_{ref.reference_no}",
            "_source": {
                "type": "reference",
                "id": ref.dbxref_id,
                "name": display_name,
                "pubmed": ref.pubmed,
                "citation": ref.citation,
                "year": ref.year,
                "link": f"/reference/{ref.dbxref_id}",
            }
        }
        yield doc


def index_references(db: Session, es: Elasticsearch) -> int:
    """Index all references."""
    success, _ = bulk(es, _generate_reference_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} references")
    return success


def rebuild_index(db: Session, es: Elasticsearch) -> dict:
    """
    Full reindex: delete existing index, create new one, and populate all data.

    Returns a summary of indexed documents.
    """
    logger.info("Starting full index rebuild...")

    # Delete existing index
    delete_index(es)

    # Create new index with mappings
    create_index(es)

    # Index all entity types
    genes_count = index_genes(db, es)
    go_count = index_go_terms(db, es)
    phenotypes_count = index_phenotypes(db, es)
    references_count = index_references(db, es)

    # Refresh index to make documents searchable immediately
    es.indices.refresh(index=INDEX_NAME)

    summary = {
        "genes": genes_count,
        "go_terms": go_count,
        "phenotypes": phenotypes_count,
        "references": references_count,
        "total": genes_count + go_count + phenotypes_count + references_count,
    }

    logger.info(f"Index rebuild complete: {summary}")
    return summary
