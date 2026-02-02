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

            # GO term fields
            "goid": {"type": "keyword"},
            "go_term": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },
            "go_aspect": {"type": "keyword"},
            "go_definition": {"type": "text"},

            # Phenotype fields
            "observable": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}}
            },

            # Reference fields
            "pubmed": {"type": "integer"},
            "citation": {"type": "text"},
            "title": {"type": "text"},
            "year": {"type": "integer"},
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
