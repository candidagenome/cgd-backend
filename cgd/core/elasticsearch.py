"""
Elasticsearch client configuration and index settings.
"""
from elasticsearch import Elasticsearch

from cgd.core.settings import settings


def get_es_client() -> Elasticsearch:
    """Create and return an Elasticsearch client instance."""
    return Elasticsearch(hosts=[settings.elasticsearch_url])


INDEX_NAME = settings.elasticsearch_index

# Index mapping for the unified CGD index
INDEX_MAPPING = {
    "mappings": {
        "properties": {
            # Common fields
            "type": {"type": "keyword"},
            "id": {"type": "keyword"},
            "name": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword"}}
            },
            "description": {"type": "text"},
            "link": {"type": "keyword", "index": False},
            "organism": {"type": "keyword"},

            # Gene/Feature fields
            "gene_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },
            "feature_name": {"type": "keyword"},
            "dbxref_id": {"type": "keyword"},
            "aliases": {"type": "text"},
            "headline": {"type": "text"},
            "name_description": {"type": "text"},
            "feature_no": {"type": "integer"},

            # GO term fields
            "goid": {"type": "keyword"},
            "go_term": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },
            "go_aspect": {"type": "keyword"},
            "go_definition": {"type": "text"},
            "go_synonyms": {"type": "text"},

            # Phenotype fields
            "observable": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },

            # Reference fields
            "pubmed": {"type": "integer"},
            "citation": {"type": "text"},
            "title": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },
            "year": {"type": "integer"},
            "reference_no": {"type": "integer"},

            # Abstract fields
            "abstract": {"type": "text"},

            # Paragraph fields (locus summaries)
            "paragraph_text": {"type": "text"},

            # Author fields
            "author_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },

            # Colleague fields
            "last_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },
            "other_last_name": {"type": "text"},
            "first_name": {"type": "text"},
            "institution": {"type": "text"},

            # Pathway fields
            "pathway_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },
            "pathway_id": {"type": "keyword"},

            # Note fields
            "note_text": {"type": "text"},
            "note_type": {"type": "keyword"},

            # External ID fields
            "external_id": {"type": "keyword"},
            "source": {"type": "keyword"},

            # Ortholog fields
            "homology_group_no": {"type": "integer"},
            "ortholog_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },
            "ortholog_source": {"type": "keyword"},
            "ortholog_organism": {"type": "keyword"},
            "ortholog_display": {"type": "text"},
            "ortholog_type": {"type": "keyword"},
            "ortholog_feature_name": {"type": "keyword"},
            "cgd_gene_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },
            "cgd_feature_name": {"type": "keyword"},
            "cgd_gene_id": {"type": "keyword"},
            "related_genes": {"type": "text"},

            # Literature topic fields
            "literature_topic": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "default": {
                    "type": "standard"
                }
            }
        }
    }
}
