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
from sqlalchemy import and_

from cgd.core.elasticsearch import INDEX_NAME, INDEX_MAPPING
from cgd.models.models import (
    Feature, Go, Phenotype, Reference, FeatAlias, Alias, Organism,
    Abstract, Paragraph, FeatPara, Author, AuthorEditor, Colleague,
    Dbxref, DbxrefFeat, Note, NoteLink, HomologyGroup, FeatHomology,
    DbxrefHomology, RefProperty, GoSynonym, GoGosyn, FeatRelationship,
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
    Generate Elasticsearch documents for ortholog RELATIONSHIPS.

    Each document represents a relationship between:
    - A CGD gene (the gene in the target organism)
    - An ortholog (from another CGD organism or external DB like SGD)

    This supports the table view: "Ortholog → CGD Gene"
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

        # Get external orthologs (SGD, etc.) in this group
        dbxref_homologies = (
            db.query(DbxrefHomology, Dbxref)
            .join(Dbxref, DbxrefHomology.dbxref_no == Dbxref.dbxref_no)
            .filter(DbxrefHomology.homology_group_no == hg.homology_group_no)
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
        for dh, dbx in dbxref_homologies:
            if dh.name:
                all_ortholog_names.append(dh.name)
            elif dbx.description:
                all_ortholog_names.append(dbx.description)

        # Create ortholog relationship docs:
        # For each CGD gene, create docs for all its orthologs (CGD + external)
        for fh, cgd_gene in valid_features:
            cgd_name = cgd_gene.gene_name or cgd_gene.feature_name
            cgd_organism = cgd_gene.organism.organism_name if cgd_gene.organism else None

            # CGD-to-CGD ortholog relationships (other organisms)
            for fh2, ortholog in valid_features:
                if ortholog.feature_no == cgd_gene.feature_no:
                    continue  # Skip self

                orth_name = ortholog.gene_name or ortholog.feature_name
                orth_organism = ortholog.organism.organism_name if ortholog.organism else None

                # Short organism name for display
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
                        "organism": cgd_organism,  # Organism of the CGD gene
                        # The ortholog (from another organism)
                        "ortholog_name": orth_name,
                        "ortholog_feature_name": ortholog.feature_name,
                        "ortholog_organism": orth_organism,
                        "ortholog_display": f"{short_organism} {ortholog.feature_name}/{orth_name}",
                        "ortholog_type": "Ortholog",
                        "ortholog_source": "CGOB",
                        # For searching
                        "name": cgd_name,
                        "gene_name": cgd_name,
                        "feature_name": cgd_gene.feature_name,
                        "homology_group_no": hg.homology_group_no,
                        "related_genes": " ".join(all_ortholog_names),
                        "link": f"/locus/{cgd_gene.feature_name}",
                    }
                }
                yield doc

            # External DB orthologs (SGD, POMBASE, AspGD)
            for dh, dbx in dbxref_homologies:
                orth_name = dh.name or dbx.description or dbx.dbxref_id
                source_organism = _get_organism_for_source(dbx.source)

                doc = {
                    "_index": INDEX_NAME,
                    "_id": f"ortholog_ext_{hg.homology_group_no}_{cgd_gene.feature_no}_{dbx.dbxref_no}",
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
                        # For searching
                        "name": cgd_name,
                        "gene_name": cgd_name,
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
        "total": (
            genes_count + go_count + phenotypes_count + references_count +
            paragraphs_count + authors_count + colleagues_count + pathways_count +
            notes_count + external_ids_count + orthologs_count + lit_topics_count
        ),
    }

    logger.info(f"Index rebuild complete: {summary}")
    return summary
