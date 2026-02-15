"""
CGD Pydantic Schemas

This package contains Pydantic models for request/response validation
in the FastAPI application.

Schemas:
- locus_schema: Locus/gene data schemas
- reference_schema: Reference/publication schemas
- go_schema: Gene Ontology annotation schemas
- phenotype_schema: Phenotype data schemas
- interaction_schema: Interaction data schemas
- protein_schema: Protein information schemas
- search_schema: Search request/response schemas
- sequence_schema: Sequence data schemas
- seq_tools_schema: Sequence tools schemas
- batch_download_schema: Batch download schemas
- restriction_mapper_schema: Restriction mapper schemas
- feature_search_schema: Feature search schemas
- genome_version_schema: Genome version schemas
- colleague_schema: Colleague data schemas
- gene_registry_schema: Gene registry schemas
- chromosome_schema: Chromosome data schemas
- webprimer_schema: WebPrimer tool schemas
- go_term_finder_schema: GO Term Finder schemas
- go_slim_mapper_schema: GO Slim Mapper schemas
- blast_schema: BLAST search schemas
- patmatch_schema: Pattern match schemas
- homology_schema: Homology/ortholog schemas

Subpackages:
- tables: Database table schemas
"""

from .tables import *  # noqa: F401, F403
