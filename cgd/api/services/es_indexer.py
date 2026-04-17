"""
Elasticsearch indexing service.

Provides functions to populate Elasticsearch from the database.
"""
from __future__ import annotations

import logging
from typing import Generator, Optional

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, or_

from cgd.core.elasticsearch import INDEX_NAME, INDEX_MAPPING
from cgd.models.models import (
    Feature, Go, Phenotype, Reference, FeatAlias, Alias, Organism,
    Abstract, Paragraph, FeatPara, Author, AuthorEditor, Colleague,
    Dbxref, DbxrefFeat, Note, NoteLink, HomologyGroup, FeatHomology,
    DbxrefHomology, RefProperty, GoSynonym, GoGosyn, FeatRelationship,
    PhenoAnnotation, GoAnnotation, RefpropFeat, ExptProperty, ExptExptprop,
    RefLink,
)
from cgd.schemas.virulence_schema import (
    VIRULENCE_CATEGORIES,
    PHENOTYPE_EVIDENCE_TIERS,
    HOUSEKEEPING_GO_TERMS,
    EVIDENCE_WEIGHTS,
    get_confidence_tier,
    extract_evidence_types,
    generate_inclusion_reason,
    split_evidence,
)

logger = logging.getLogger(__name__)

# Valid feature types for gene indexing
GENE_FEATURE_TYPES = [
    'ORF', 'blocked_reading_frame', 'pseudogene',
    'transposable_element_gene', 'gene_group', 'ncRNA_gene',
    'rRNA_gene', 'snoRNA_gene', 'snRNA_gene', 'tRNA_gene',
]

# Sources to exclude from external_ids (they have their own categories)
EXTERNAL_ID_EXCLUDE_SOURCES = ['CalbiCyc', 'SGD', 'POMBASE', 'AspGD', 'CGD']

# Ortholog sources for external DB orthologs
ORTHOLOG_SOURCES = ['SGD', 'POMBASE', 'AspGD', 'CGD']


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


def _get_a21_exclusion_set(db: Session) -> set[int]:
    """
    Get set of feature_nos to exclude (Assembly 21 features with A22 equivalents).
    """
    # Direct Assembly 21 features
    direct_a21 = (
        db.query(FeatRelationship.child_feature_no)
        .filter(
            FeatRelationship.relationship_type == 'Assembly 21 Primary Allele',
            FeatRelationship.rank == 3,
        )
        .all()
    )
    exclude_set = {r[0] for r in direct_a21}

    # Alleles of Assembly 21 features - batch to avoid Oracle 1000-item IN clause limit
    exclude_list = list(exclude_set)
    batch_size = 900  # Stay under Oracle's 1000 limit
    for i in range(0, len(exclude_list), batch_size):
        batch = exclude_list[i:i + batch_size]
        alleles_of_a21 = (
            db.query(FeatRelationship.child_feature_no)
            .filter(
                FeatRelationship.relationship_type == 'allele',
                FeatRelationship.rank == 3,
                FeatRelationship.parent_feature_no.in_(batch)
            )
            .all()
        )
        exclude_set.update(r[0] for r in alleles_of_a21)

    return exclude_set


def _generate_gene_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for genes/features."""
    a21_exclude = _get_a21_exclusion_set(db)

    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.feat_alias).joinedload(FeatAlias.alias)
        )
        .filter(Feature.feature_type.in_(GENE_FEATURE_TYPES))
        .all()
    )

    for feat in features:
        # Skip Assembly 21 features
        if feat.feature_no in a21_exclude:
            continue

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
                "feature_no": feat.feature_no,
                "dbxref_id": feat.dbxref_id,
                "headline": feat.headline,
                "name_description": feat.name_description,
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
    """Generate Elasticsearch documents for GO terms with synonyms."""
    # Get all GO terms with their synonyms
    go_terms = db.query(Go).all()

    # Build synonym mapping
    synonym_map: dict[int, list[str]] = {}
    go_gosyns = (
        db.query(GoGosyn.go_no, GoSynonym.go_synonym)
        .join(GoSynonym, GoGosyn.go_synonym_no == GoSynonym.go_synonym_no)
        .all()
    )
    for go_no, synonym in go_gosyns:
        if go_no not in synonym_map:
            synonym_map[go_no] = []
        synonym_map[go_no].append(synonym)

    for go in go_terms:
        formatted_goid = _format_goid(go.goid)
        synonyms = synonym_map.get(go.go_no, [])

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
                "go_synonyms": " ".join(synonyms) if synonyms else None,
                "link": f"/go/{formatted_goid}",
            }
        }
        yield doc


def index_go_terms(db: Session, es: Elasticsearch) -> int:
    """Index all GO terms with synonyms."""
    success, _ = bulk(es, _generate_go_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} GO terms")
    return success


def _generate_phenotype_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for distinct phenotype observables."""
    observables = (
        db.query(Phenotype.observable)
        .distinct()
        .all()
    )

    for idx, (observable,) in enumerate(observables):
        if not observable:
            continue
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
    """Generate Elasticsearch documents for references with abstracts."""
    # Get references with abstracts
    references = (
        db.query(Reference, Abstract.abstract)
        .outerjoin(Abstract, Reference.reference_no == Abstract.reference_no)
        .all()
    )

    for ref, abstract_text in references:
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
                "title": ref.title,
                "abstract": abstract_text,
                "year": ref.year,
                "reference_no": ref.reference_no,
                "link": f"/reference/{ref.dbxref_id}",
            }
        }
        yield doc


def index_references(db: Session, es: Elasticsearch) -> int:
    """Index all references with abstracts."""
    success, _ = bulk(es, _generate_reference_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} references")
    return success


def _generate_paragraph_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for paragraphs (locus summaries)."""
    a21_exclude = _get_a21_exclusion_set(db)

    # Get paragraphs with their associated features
    paragraphs = (
        db.query(Paragraph, Feature)
        .join(FeatPara, Paragraph.paragraph_no == FeatPara.paragraph_no)
        .join(Feature, FeatPara.feature_no == Feature.feature_no)
        .options(joinedload(Feature.organism))
        .all()
    )

    for para, feat in paragraphs:
        # Skip Assembly 21 features
        if feat.feature_no in a21_exclude:
            continue

        display_name = feat.gene_name or feat.feature_name
        organism_name = feat.organism.organism_name if feat.organism else None

        doc = {
            "_index": INDEX_NAME,
            "_id": f"paragraph_{para.paragraph_no}_{feat.feature_no}",
            "_source": {
                "type": "paragraph",
                "id": str(para.paragraph_no),
                "name": display_name,
                "paragraph_text": para.paragraph_text,
                "gene_name": feat.gene_name,
                "feature_name": feat.feature_name,
                "feature_no": feat.feature_no,
                "dbxref_id": feat.dbxref_id,
                "organism": organism_name,
                "link": f"/locus/{feat.feature_name}",
            }
        }
        yield doc


def index_paragraphs(db: Session, es: Elasticsearch) -> int:
    """Index all paragraphs (locus summaries)."""
    success, _ = bulk(es, _generate_paragraph_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} paragraphs")
    return success


def _generate_author_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for authors."""
    # Get authors with their references
    authors_refs = (
        db.query(Author, Reference)
        .join(AuthorEditor, Author.author_no == AuthorEditor.author_no)
        .join(Reference, AuthorEditor.reference_no == Reference.reference_no)
        .all()
    )

    # Group by author to avoid duplicates
    author_data: dict[int, dict] = {}
    for author, ref in authors_refs:
        if author.author_no not in author_data:
            author_data[author.author_no] = {
                "author": author,
                "references": []
            }
        author_data[author.author_no]["references"].append(ref)

    for author_no, data in author_data.items():
        author = data["author"]
        refs = data["references"]

        # Create one doc per author-reference pair for better search
        for ref in refs:
            doc = {
                "_index": INDEX_NAME,
                "_id": f"author_{author_no}_{ref.reference_no}",
                "_source": {
                    "type": "author",
                    "id": str(author_no),
                    "name": author.author_name,
                    "author_name": author.author_name,
                    "pubmed": ref.pubmed,
                    "citation": ref.citation,
                    "reference_no": ref.reference_no,
                    "link": f"/reference/{ref.dbxref_id}",
                }
            }
            yield doc


def index_authors(db: Session, es: Elasticsearch) -> int:
    """Index all authors."""
    success, _ = bulk(es, _generate_author_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} author-reference pairs")
    return success


def _generate_colleague_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for colleagues."""
    colleagues = db.query(Colleague).all()

    for coll in colleagues:
        # Build description from location info
        location_parts = []
        if coll.institution:
            location_parts.append(coll.institution)
        if coll.city:
            location_parts.append(coll.city)
        if coll.country:
            location_parts.append(coll.country)
        description = ", ".join(location_parts) if location_parts else None

        # Full name for display
        name_parts = []
        if coll.first_name:
            name_parts.append(coll.first_name)
        if coll.last_name:
            name_parts.append(coll.last_name)
        if coll.suffix:
            name_parts.append(coll.suffix)
        full_name = " ".join(name_parts) if name_parts else coll.last_name

        doc = {
            "_index": INDEX_NAME,
            "_id": f"colleague_{coll.colleague_no}",
            "_source": {
                "type": "colleague",
                "id": str(coll.colleague_no),
                "name": full_name,
                "last_name": coll.last_name,
                "other_last_name": coll.other_last_name,
                "first_name": coll.first_name,
                "institution": coll.institution,
                "description": description,
                "link": f"/colleague/{coll.colleague_no}",
            }
        }
        yield doc


def index_colleagues(db: Session, es: Elasticsearch) -> int:
    """Index all colleagues."""
    success, _ = bulk(es, _generate_colleague_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} colleagues")
    return success


def _generate_pathway_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for pathways (CalbiCyc)."""
    # Get pathways with associated features
    pathways = (
        db.query(Dbxref, Feature)
        .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
        .join(Feature, DbxrefFeat.feature_no == Feature.feature_no)
        .filter(Dbxref.source == 'CalbiCyc')
        .options(joinedload(Feature.organism))
        .all()
    )

    # Group by pathway
    pathway_data: dict[int, dict] = {}
    for dbx, feat in pathways:
        if dbx.dbxref_no not in pathway_data:
            pathway_data[dbx.dbxref_no] = {
                "dbxref": dbx,
                "genes": []
            }
        pathway_data[dbx.dbxref_no]["genes"].append(feat.gene_name or feat.feature_name)

    for dbxref_no, data in pathway_data.items():
        dbx = data["dbxref"]
        genes = data["genes"]

        doc = {
            "_index": INDEX_NAME,
            "_id": f"pathway_{dbxref_no}",
            "_source": {
                "type": "pathway",
                "id": dbx.dbxref_id,
                "name": dbx.description or dbx.dbxref_id,
                "pathway_name": dbx.description,
                "pathway_id": dbx.dbxref_id,
                "related_genes": " ".join(genes) if genes else None,
                "link": f"https://pathway.candidagenome.org/CALBI/NEW-IMAGE?type=PATHWAY&object={dbx.dbxref_id}",
            }
        }
        yield doc


def index_pathways(db: Session, es: Elasticsearch) -> int:
    """Index all pathways."""
    success, _ = bulk(es, _generate_pathway_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} pathways")
    return success


def _generate_note_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for notes."""
    a21_exclude = _get_a21_exclusion_set(db)

    # Get notes linked to features
    feature_notes = (
        db.query(Note, Feature)
        .join(NoteLink, Note.note_no == NoteLink.note_no)
        .join(Feature, NoteLink.primary_key == Feature.feature_no)
        .filter(NoteLink.tab_name == 'FEATURE')
        .options(joinedload(Feature.organism))
        .all()
    )

    for note, feat in feature_notes:
        # Skip Assembly 21 features
        if feat.feature_no in a21_exclude:
            continue

        display_name = feat.gene_name or feat.feature_name
        organism_name = feat.organism.organism_name if feat.organism else None

        doc = {
            "_index": INDEX_NAME,
            "_id": f"note_feat_{note.note_no}_{feat.feature_no}",
            "_source": {
                "type": "note",
                "id": str(note.note_no),
                "name": display_name,
                "note_text": note.note,
                "note_type": note.note_type,
                "gene_name": feat.gene_name,
                "feature_name": feat.feature_name,
                "organism": organism_name,
                "link": f"/locus/{feat.feature_name}",
            }
        }
        yield doc

    # Get notes linked to references
    ref_notes = (
        db.query(Note, Reference)
        .join(NoteLink, Note.note_no == NoteLink.note_no)
        .join(Reference, NoteLink.primary_key == Reference.reference_no)
        .filter(NoteLink.tab_name == 'REFERENCE')
        .all()
    )

    for note, ref in ref_notes:
        display_name = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id

        doc = {
            "_index": INDEX_NAME,
            "_id": f"note_ref_{note.note_no}_{ref.reference_no}",
            "_source": {
                "type": "note",
                "id": str(note.note_no),
                "name": display_name,
                "note_text": note.note,
                "note_type": note.note_type,
                "reference_no": ref.reference_no,
                "link": f"/reference/{ref.dbxref_id}",
            }
        }
        yield doc


def index_notes(db: Session, es: Elasticsearch) -> int:
    """Index all notes."""
    success, _ = bulk(es, _generate_note_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} notes")
    return success


def _generate_external_id_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for external IDs."""
    a21_exclude = _get_a21_exclusion_set(db)

    # Get external IDs (excluding pathways and ortholog sources)
    external_ids = (
        db.query(Dbxref, Feature)
        .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
        .join(Feature, DbxrefFeat.feature_no == Feature.feature_no)
        .filter(~Dbxref.source.in_(EXTERNAL_ID_EXCLUDE_SOURCES))
        .options(joinedload(Feature.organism))
        .all()
    )

    for dbx, feat in external_ids:
        # Skip Assembly 21 features
        if feat.feature_no in a21_exclude:
            continue

        display_name = feat.gene_name or feat.feature_name
        organism_name = feat.organism.organism_name if feat.organism else None

        doc = {
            "_index": INDEX_NAME,
            "_id": f"external_id_{dbx.dbxref_no}_{feat.feature_no}",
            "_source": {
                "type": "external_id",
                "id": dbx.dbxref_id,
                "name": display_name,
                "external_id": dbx.dbxref_id,
                "source": dbx.source,
                "description": dbx.description,
                "gene_name": feat.gene_name,
                "feature_name": feat.feature_name,
                "organism": organism_name,
                "link": f"/locus/{feat.feature_name}",
            }
        }
        yield doc


def index_external_ids(db: Session, es: Elasticsearch) -> int:
    """Index all external IDs."""
    success, _ = bulk(es, _generate_external_id_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} external IDs")
    return success


def _generate_ortholog_docs(db: Session) -> Generator[dict, None, None]:
    """
    Generate Elasticsearch documents for ortholog relationships.

    Each document represents ONE ortholog relationship:
    - A CGD gene
    - One of its orthologs (from another CGD organism OR external DB like SGD)

    For a gene with 4 CGD orthologs + 1 SGD ortholog = 5 documents.
    """
    a21_exclude = _get_a21_exclusion_set(db)

    # Get CGOB homology groups
    homology_groups = (
        db.query(HomologyGroup)
        .filter(
            HomologyGroup.method == 'CGOB',
            HomologyGroup.homology_group_type == 'ortholog'
        )
        .all()
    )

    for hg in homology_groups:
        # Get all CGD features in this homology group
        feat_homologies = (
            db.query(FeatHomology, Feature)
            .join(Feature, FeatHomology.feature_no == Feature.feature_no)
            .filter(FeatHomology.homology_group_no == hg.homology_group_no)
            .options(joinedload(Feature.organism))
            .all()
        )

        # Filter out A21 features
        valid_features = [
            (fh, feat) for fh, feat in feat_homologies
            if feat.feature_no not in a21_exclude
        ]

        # Collect all ortholog names for searchability
        all_ortholog_names = []
        for fh, feat in valid_features:
            if feat.gene_name:
                all_ortholog_names.append(feat.gene_name)

        # For each CGD gene, create docs for its orthologs
        for fh, cgd_gene in valid_features:
            cgd_name = cgd_gene.gene_name or cgd_gene.feature_name
            cgd_organism = cgd_gene.organism.organism_name if cgd_gene.organism else None

            # CGD-to-CGD orthologs (other CGD organisms)
            for fh2, ortholog in valid_features:
                if ortholog.feature_no == cgd_gene.feature_no:
                    continue  # Skip self

                orth_name = ortholog.gene_name or ortholog.feature_name
                orth_organism = ortholog.organism.organism_name if ortholog.organism else None
                short_organism = _get_short_organism_name(orth_organism)

                doc = {
                    "_index": INDEX_NAME,
                    "_id": f"ortholog_{hg.homology_group_no}_{cgd_gene.feature_no}_{ortholog.feature_no}",
                    "_source": {
                        "type": "ortholog",
                        "id": cgd_gene.dbxref_id,
                        # CGD gene (the gene we're showing orthologs FOR)
                        "cgd_gene_name": cgd_name,
                        "cgd_feature_name": cgd_gene.feature_name,
                        "cgd_gene_id": cgd_gene.dbxref_id,
                        "organism": cgd_organism,
                        # The ortholog
                        "ortholog_name": orth_name,
                        "ortholog_feature_name": ortholog.feature_name,
                        "ortholog_organism": orth_organism,
                        "ortholog_display": f"{short_organism} {ortholog.feature_name}/{orth_name}",
                        "ortholog_type": "Ortholog",
                        "ortholog_source": "CGOB",
                        # For searching - search by ortholog name to find genes
                        "name": orth_name,
                        "gene_name": orth_name,
                        "feature_name": ortholog.feature_name,
                        "homology_group_no": hg.homology_group_no,
                        "related_genes": " ".join(all_ortholog_names),
                        "link": f"/locus/{cgd_gene.feature_name}",
                    }
                }
                yield doc

            # External DB orthologs (SGD) - stored in DbxrefFeat, not DbxrefHomology
            # Query DbxrefFeat for this gene to find external orthologs
            external_orthologs = (
                db.query(Dbxref)
                .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
                .filter(
                    DbxrefFeat.feature_no == cgd_gene.feature_no,
                    Dbxref.source == 'SGD'  # Only SGD for now
                )
                .all()
            )

            for dbx in external_orthologs:
                orth_name = dbx.description or dbx.dbxref_id
                source_organism = _get_organism_for_source(dbx.source)

                doc = {
                    "_index": INDEX_NAME,
                    "_id": f"ortholog_ext_{cgd_gene.feature_no}_{dbx.dbxref_no}",
                    "_source": {
                        "type": "ortholog",
                        "id": cgd_gene.dbxref_id,
                        # CGD gene
                        "cgd_gene_name": cgd_name,
                        "cgd_feature_name": cgd_gene.feature_name,
                        "cgd_gene_id": cgd_gene.dbxref_id,
                        "organism": cgd_organism,
                        # The ortholog (external)
                        "ortholog_name": orth_name,
                        "ortholog_organism": source_organism,
                        "ortholog_display": f"{source_organism} {orth_name}",
                        "ortholog_type": "Ortholog",
                        "ortholog_source": dbx.source,
                        "external_id": dbx.dbxref_id,
                        # For searching - search by ortholog name
                        "name": orth_name,
                        "gene_name": orth_name,
                        "feature_name": cgd_gene.feature_name,
                        "homology_group_no": hg.homology_group_no,
                        "related_genes": " ".join(all_ortholog_names),
                        "link": f"/locus/{cgd_gene.feature_name}",
                    }
                }
                yield doc


def _get_short_organism_name(organism_name: str | None) -> str:
    """Get short organism name for display (e.g., 'C. albicans')."""
    if not organism_name:
        return ""
    mapping = {
        "Candida albicans SC5314": "C. albicans",
        "Candida glabrata CBS138": "C. glabrata",
        "Candida auris B8441": "C. auris",
        "Candida dubliniensis CD36": "C. dubliniensis",
        "Candida parapsilosis CDC317": "C. parapsilosis",
    }
    return mapping.get(organism_name, organism_name.split()[0][:2] + ". " + organism_name.split()[-1] if organism_name else "")


def _get_organism_for_source(source: str) -> str:
    """Map external DB source to organism name."""
    mapping = {
        "SGD": "S. cerevisiae",
        "POMBASE": "S. pombe",
        "AspGD": "A. nidulans",
    }
    return mapping.get(source, source)


def index_orthologs(db: Session, es: Elasticsearch) -> int:
    """Index all orthologs (CGOB and external DB)."""
    success, _ = bulk(es, _generate_ortholog_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} orthologs")
    return success


def _generate_literature_topic_docs(db: Session) -> Generator[dict, None, None]:
    """Generate Elasticsearch documents for literature topics."""
    # Get literature topics with references
    lit_topics = (
        db.query(RefProperty, Reference)
        .join(Reference, RefProperty.reference_no == Reference.reference_no)
        .filter(RefProperty.property_type == 'literature_topic')
        .all()
    )

    for rp, ref in lit_topics:
        display_name = f"PMID:{ref.pubmed}" if ref.pubmed else ref.dbxref_id

        doc = {
            "_index": INDEX_NAME,
            "_id": f"lit_topic_{rp.ref_property_no}",
            "_source": {
                "type": "literature_topic",
                "id": str(rp.ref_property_no),
                "name": display_name,
                "literature_topic": rp.property_value,
                "citation": ref.citation,
                "pubmed": ref.pubmed,
                "year": ref.year,
                "reference_no": ref.reference_no,
                "link": f"/reference/{ref.dbxref_id}",
            }
        }
        yield doc


def index_literature_topics(db: Session, es: Elasticsearch) -> int:
    """Index all literature topics."""
    success, _ = bulk(es, _generate_literature_topic_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} literature topics")
    return success


def _convert_goid_to_int(goid_str: str) -> Optional[int]:
    """Convert GO:XXXXXXX format to integer."""
    if goid_str.startswith("GO:"):
        try:
            return int(goid_str[3:])
        except ValueError:
            return None
    return None


def _get_virulence_category_matches(
    db: Session,
    feature: Feature,
    a21_exclude: set[int],
) -> dict[str, list[str]]:
    """
    Determine which virulence categories a feature matches and why.

    Returns dict of category_key -> list of match reasons.
    """
    from sqlalchemy import func, or_

    if feature.feature_no in a21_exclude:
        return {}

    matches: dict[str, list[str]] = {}

    for cat_key, cat_config in VIRULENCE_CATEGORIES.items():
        rules = cat_config.get("rules", {})
        reasons: list[str] = []

        # Check gene patterns
        if "gene_patterns" in rules and feature.gene_name:
            for pattern in rules["gene_patterns"]:
                sql_pattern = pattern.replace("%", "")
                if feature.gene_name.upper().startswith(sql_pattern.upper()):
                    reasons.append(f"gene pattern: {feature.gene_name}")
                    break

        # Check phenotype observables
        if "phenotype_observables" in rules:
            pheno_matches = (
                db.query(Phenotype.observable)
                .join(PhenoAnnotation, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
                .filter(PhenoAnnotation.feature_no == feature.feature_no)
                .distinct()
                .all()
            )
            for (observable,) in pheno_matches:
                if observable:
                    for obs_pattern in rules["phenotype_observables"]:
                        import re
                        sql_pattern = obs_pattern.replace("%", ".*")
                        if re.search(sql_pattern, observable, re.IGNORECASE):
                            reasons.append(f"phenotype: {observable}")
                            break

        # Check GO terms
        if "go_terms" in rules:
            goids = [_convert_goid_to_int(g) for g in rules["go_terms"]]
            goids = [g for g in goids if g is not None]
            if goids:
                go_matches = (
                    db.query(Go.goid, Go.go_term)
                    .join(GoAnnotation, GoAnnotation.go_no == Go.go_no)
                    .filter(GoAnnotation.feature_no == feature.feature_no)
                    .filter(Go.goid.in_(goids))
                    .distinct()
                    .all()
                )
                for goid, go_term in go_matches:
                    reasons.append(f"GO: {go_term} (GO:{goid:07d})")

        # Check headlines
        if "headlines" in rules and feature.headline:
            for pattern in rules["headlines"]:
                import re
                sql_pattern = pattern.replace("%", ".*")
                if re.search(sql_pattern, feature.headline, re.IGNORECASE):
                    reasons.append(f"headline: {feature.headline[:50]}")
                    break

        # Check virulence model - search in both phenotype observables and experiment properties
        if rules.get("phenotype_has_virulence_model"):
            virulence_patterns = ["%virulence%", "%mouse%", "%galleria%"]
            found_virulence = False

            # Search in phenotype observables
            for pattern in virulence_patterns:
                vir_matches = (
                    db.query(Phenotype.observable)
                    .join(PhenoAnnotation, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
                    .filter(PhenoAnnotation.feature_no == feature.feature_no)
                    .filter(func.upper(Phenotype.observable).like(func.upper(pattern)))
                    .first()
                )
                if vir_matches:
                    reasons.append(f"virulence model: {vir_matches[0]}")
                    found_virulence = True
                    break

            # Also search in experiment properties (like Oracle does)
            if not found_virulence:
                expt_match = (
                    db.query(ExptProperty.property_value)
                    .join(ExptExptprop, ExptExptprop.expt_property_no == ExptProperty.expt_property_no)
                    .join(PhenoAnnotation, PhenoAnnotation.experiment_no == ExptExptprop.experiment_no)
                    .filter(PhenoAnnotation.feature_no == feature.feature_no)
                    .filter(
                        or_(
                            func.upper(ExptProperty.property_value).like('%VIRULENCE%'),
                            func.upper(ExptProperty.property_value).like('%MOUSE%'),
                            func.upper(ExptProperty.property_value).like('%GALLERIA%'),
                        )
                    )
                    .first()
                )
                if expt_match:
                    reasons.append(f"virulence model: {expt_match[0][:50]}")

        # Check literature topics
        if "literature_topics" in rules:
            topic_matches = (
                db.query(RefProperty.property_value)
                .join(RefpropFeat, RefpropFeat.ref_property_no == RefProperty.ref_property_no)
                .filter(RefpropFeat.feature_no == feature.feature_no)
                .filter(func.upper(RefProperty.property_type) == 'TOPIC')
                .filter(func.upper(RefProperty.property_value).in_([t.upper() for t in rules["literature_topics"]]))
                .distinct()
                .all()
            )
            for (topic,) in topic_matches:
                reasons.append(f"literature topic: {topic}")

        if reasons:
            matches[cat_key] = reasons

    return matches


def _classify_phenotype_tier_es(observable: str) -> tuple[int, str]:
    """
    Classify phenotype observable into evidence tiers (1=best, 4=weakest).

    Args:
        observable: The phenotype observable string

    Returns:
        Tuple of (tier_number, tier_name)
    """
    import re
    observable_lower = observable.lower()

    for tier_num in sorted(PHENOTYPE_EVIDENCE_TIERS.keys()):
        tier_config = PHENOTYPE_EVIDENCE_TIERS[tier_num]
        for pattern in tier_config["patterns"]:
            # Convert SQL LIKE pattern to regex
            regex_pattern = pattern.replace("%", ".*")
            if re.search(regex_pattern, observable_lower):
                return tier_num, tier_config["name"]

    return 4, "Indirect"


def _get_best_evidence_tier_es(match_reasons: list[str]) -> tuple[int, str]:
    """
    Determine the best (lowest number) evidence tier from match reasons.

    Args:
        match_reasons: List of match reason strings

    Returns:
        Tuple of (best_tier_number, tier_name)
    """
    best_tier = 4
    best_tier_name = "Indirect"

    for reason in match_reasons:
        if reason.startswith("phenotype:"):
            observable = reason.replace("phenotype: ", "")
            tier, tier_name = _classify_phenotype_tier_es(observable)
            if tier < best_tier:
                best_tier = tier
                best_tier_name = tier_name
        elif reason.startswith("virulence model:"):
            # Virulence model evidence is tier 1
            if 1 < best_tier:
                best_tier = 1
                best_tier_name = "Direct Virulence"

    return best_tier, best_tier_name


def _is_housekeeping_gene_es(db: Session, feature: Feature) -> tuple[bool, Optional[str]]:
    """
    Detect if gene is likely housekeeping/essential.

    Methods:
    1. GO annotation to housekeeping terms (translation, DNA replication, etc.)
    2. Conserved across all 5 Candida species (ortholog groups)

    Args:
        db: Database session
        feature: The feature to check

    Returns:
        Tuple of (is_housekeeping, reason_string)
    """
    from sqlalchemy import func, distinct

    # Method 1: Check GO annotations for housekeeping terms
    housekeeping_goids = [_convert_goid_to_int(g) for g in HOUSEKEEPING_GO_TERMS]
    housekeeping_goids = [g for g in housekeeping_goids if g is not None]

    if housekeeping_goids:
        go_match = (
            db.query(Go.go_term)
            .join(GoAnnotation, GoAnnotation.go_no == Go.go_no)
            .filter(GoAnnotation.feature_no == feature.feature_no)
            .filter(Go.goid.in_(housekeeping_goids))
            .first()
        )

        if go_match:
            return True, f"GO: {go_match[0]}"

    # Method 2: Check ortholog conservation across Candida species
    ortholog_count = _get_ortholog_count_es(db, feature)
    if ortholog_count >= 5:
        return True, f"Conserved in {ortholog_count} Candida species"

    return False, None


def _get_ortholog_count_es(db: Session, feature: Feature) -> int:
    """
    Count how many Candida species have orthologs of this gene.

    Args:
        db: Database session
        feature: The feature to check

    Returns:
        Number of distinct Candida species with orthologs
    """
    from sqlalchemy import func, distinct

    # Find homology groups this feature belongs to
    homology_group_nos = (
        db.query(FeatHomology.homology_group_no)
        .filter(FeatHomology.feature_no == feature.feature_no)
        .all()
    )

    if not homology_group_nos:
        return 0

    hg_nos = [h[0] for h in homology_group_nos]

    # Count distinct organisms in these homology groups
    organism_count = (
        db.query(func.count(distinct(Feature.organism_no)))
        .join(FeatHomology, FeatHomology.feature_no == Feature.feature_no)
        .join(HomologyGroup, FeatHomology.homology_group_no == HomologyGroup.homology_group_no)
        .filter(FeatHomology.homology_group_no.in_(hg_nos))
        .filter(HomologyGroup.method == 'CGOB')
        .filter(HomologyGroup.homology_group_type == 'ortholog')
        .scalar()
    )

    return organism_count or 0


def _get_paper_count_and_pmids_es(
    db: Session,
    feature: Feature,
    max_pmids: int = 10,
) -> tuple[int, list[int]]:
    """
    Get paper count and PMID list for a feature.

    Args:
        db: Database session
        feature: The feature to get papers for
        max_pmids: Maximum number of PMIDs to return (default 10)

    Returns:
        Tuple of (paper_count, pmid_list)
    """
    # Query references via RefLink
    refs = (
        db.query(Reference.pubmed)
        .join(RefLink, RefLink.reference_no == Reference.reference_no)
        .filter(
            RefLink.tab_name == "FEATURE",
            RefLink.primary_key == feature.feature_no,
            Reference.pubmed.isnot(None),
        )
        .distinct()
        .all()
    )

    pmids = [r[0] for r in refs if r[0] is not None]
    paper_count = len(pmids)

    # Sort by PMID descending (most recent first) and limit
    pmids_sorted = sorted(pmids, reverse=True)[:max_pmids]

    return paper_count, pmids_sorted


def _calculate_confidence_score_es(
    match_reasons: list[str],
    evidence_tier: int,
    is_housekeeping: bool,
) -> int:
    """
    Calculate confidence score (0-20 range) based on evidence quality.

    Args:
        match_reasons: List of match reason strings
        evidence_tier: The best evidence tier (1-4)
        is_housekeeping: Whether the gene is a housekeeping gene

    Returns:
        Confidence score (0-20)
    """
    score = 0

    for reason in match_reasons:
        reason_lower = reason.lower()

        if "virulence model:" in reason_lower:
            score += EVIDENCE_WEIGHTS["virulence_model"]
        elif "phenotype:" in reason_lower:
            if evidence_tier == 1:
                score += EVIDENCE_WEIGHTS["tier1_phenotype"]
            elif evidence_tier == 2:
                score += EVIDENCE_WEIGHTS["tier2_phenotype"]
            # Tier 3 and 4 phenotypes don't add points
        elif "go:" in reason_lower:
            # Check for virulence-related GO terms
            if any(t in reason_lower for t in ["pathogenesis", "host", "virulence"]):
                score += EVIDENCE_WEIGHTS["virulence_go"]
        elif "literature topic: disease" in reason_lower:
            score += EVIDENCE_WEIGHTS["disease_literature"]
        elif "gene pattern:" in reason_lower:
            score += EVIDENCE_WEIGHTS["gene_pattern"]
        elif "headline:" in reason_lower:
            score += EVIDENCE_WEIGHTS["keyword_match"]

    if is_housekeeping:
        score += EVIDENCE_WEIGHTS["housekeeping_penalty"]

    return max(0, min(20, score))  # Clamp to 0-20 range


def _generate_virulence_docs(db: Session) -> Generator[dict, None, None]:
    """
    Generate Elasticsearch documents for virulence factors.

    Pre-computes category assignments and evidence quality scores for each gene
    so searches are fast.
    """
    from sqlalchemy import func

    a21_exclude = _get_a21_exclusion_set(db)

    # Get all ORF features
    features = (
        db.query(Feature)
        .options(joinedload(Feature.organism))
        .filter(func.lower(Feature.feature_type) == 'orf')
        .all()
    )

    count = 0
    for feat in features:
        if feat.feature_no in a21_exclude:
            continue

        # Get category matches for this feature
        category_matches = _get_virulence_category_matches(db, feat, a21_exclude)

        if not category_matches:
            continue  # Not a virulence factor

        # Flatten categories and reasons
        categories = list(category_matches.keys())
        category_names = [VIRULENCE_CATEGORIES[c]["name"] for c in categories]
        all_reasons = []
        for reasons in category_matches.values():
            all_reasons.extend(reasons)

        # Determine match types for filtering
        match_types = set()
        for reason in all_reasons:
            if reason.startswith("GO:"):
                match_types.add("go_term")
            elif reason.startswith("phenotype:"):
                match_types.add("phenotype")
            elif reason.startswith("gene pattern:"):
                match_types.add("gene_pattern")
            elif reason.startswith("headline:"):
                match_types.add("headline")
            elif reason.startswith("literature topic:"):
                match_types.add("literature")
            elif reason.startswith("virulence model:"):
                match_types.add("virulence_model")

        # Calculate evidence quality fields
        evidence_tier, evidence_tier_name = _get_best_evidence_tier_es(all_reasons)
        is_housekeeping, housekeeping_reason = _is_housekeeping_gene_es(db, feat)
        ortholog_count = _get_ortholog_count_es(db, feat)
        confidence_score = _calculate_confidence_score_es(
            all_reasons, evidence_tier, is_housekeeping
        )

        # Compute quick win fields
        confidence_tier = get_confidence_tier(confidence_score)
        evidence_types = extract_evidence_types(all_reasons)
        inclusion_reason = generate_inclusion_reason(all_reasons, category_names)

        # Get paper count and PMIDs
        paper_count, pmids = _get_paper_count_and_pmids_es(db, feat)

        # Split evidence into direct and indirect
        direct_evidence, indirect_evidence = split_evidence(all_reasons)

        display_name = feat.gene_name or feat.feature_name
        organism_name = feat.organism.organism_name if feat.organism else None
        organism_abbrev = feat.organism.organism_abbrev if feat.organism else None

        doc = {
            "_index": INDEX_NAME,
            "_id": f"virulence_{feat.feature_no}",
            "_source": {
                "type": "virulence_factor",
                "id": feat.dbxref_id,
                "name": display_name,
                "gene_name": feat.gene_name,
                "feature_name": feat.feature_name,
                "feature_no": feat.feature_no,
                "dbxref_id": feat.dbxref_id,
                "headline": feat.headline,
                "organism": organism_name,
                "organism_abbrev": organism_abbrev,
                # Virulence-specific fields
                "categories": categories,  # ["adhesins", "biofilm"]
                "category_names": category_names,  # ["Adhesins", "Biofilm Formation"]
                "match_reasons": all_reasons,  # ["gene pattern: ALS1", "GO: cell adhesion"]
                "match_types": list(match_types),  # ["gene_pattern", "go_term"]
                # Evidence quality fields
                "evidence_tier": evidence_tier,
                "evidence_tier_name": evidence_tier_name,
                "confidence_score": confidence_score,
                "confidence_tier": confidence_tier,
                "is_housekeeping": is_housekeeping,
                "housekeeping_reason": housekeeping_reason,
                "ortholog_count": ortholog_count,
                # Quick win fields
                "inclusion_reason": inclusion_reason,
                "evidence_types": evidence_types,
                # Paper/reference fields
                "paper_count": paper_count,
                "pmids": pmids,
                # Split evidence fields
                "direct_evidence": direct_evidence,
                "indirect_evidence": indirect_evidence,
                # Searchable text
                "searchable_text": f"{display_name} {feat.feature_name} {feat.headline or ''} {' '.join(all_reasons)}",
                "link": f"/locus/{feat.feature_name}",
            }
        }
        yield doc
        count += 1

        # Log progress periodically
        if count % 500 == 0:
            logger.info(f"Generated {count} virulence factor documents...")


def index_virulence_factors(db: Session, es: Elasticsearch) -> int:
    """Index all virulence factors with pre-computed category assignments."""
    success, _ = bulk(es, _generate_virulence_docs(db), raise_on_error=False)
    logger.info(f"Indexed {success} virulence factors")
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
    paragraphs_count = index_paragraphs(db, es)
    authors_count = index_authors(db, es)
    colleagues_count = index_colleagues(db, es)
    pathways_count = index_pathways(db, es)
    notes_count = index_notes(db, es)
    external_ids_count = index_external_ids(db, es)
    orthologs_count = index_orthologs(db, es)
    lit_topics_count = index_literature_topics(db, es)
    virulence_count = index_virulence_factors(db, es)

    # Refresh index to make documents searchable immediately
    es.indices.refresh(index=INDEX_NAME)

    summary = {
        "genes": genes_count,
        "go_terms": go_count,
        "phenotypes": phenotypes_count,
        "references": references_count,
        "paragraphs": paragraphs_count,
        "authors": authors_count,
        "colleagues": colleagues_count,
        "pathways": pathways_count,
        "notes": notes_count,
        "external_ids": external_ids_count,
        "orthologs": orthologs_count,
        "literature_topics": lit_topics_count,
        "virulence_factors": virulence_count,
        "total": (
            genes_count + go_count + phenotypes_count + references_count +
            paragraphs_count + authors_count + colleagues_count + pathways_count +
            notes_count + external_ids_count + orthologs_count + lit_topics_count +
            virulence_count
        ),
    }

    logger.info(f"Index rebuild complete: {summary}")
    return summary
