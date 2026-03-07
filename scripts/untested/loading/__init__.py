"""
CGD Data Loading Scripts

This package contains scripts for loading various types of data
into the database.

Scripts (15 converted):

Reference/Literature:
- load_pdf_status.py: Update PDF status column in reference table
- load_ref_temp.py: Load recent PubMed references into ref_temp table
- load_headlines.py: Load headlines and name descriptions for features
- load_pubmed_references.py: Search PubMed and load references

External Links/URLs:
- load_external_links.py: Load external links (DBXREF, URL, WEB_DISPLAY)
- new_dbxref_url.py: Load DBXREF URLs for external database cross-references
- new_feat_url.py: Load feature URLs for locus page links
- fulltext_url_update.py: Update fulltext URLs for references

Protein/Expression Data:
- load_molecules_per_cell.py: Load protein abundance (molecules/cell) data
- load_dataset.py: Load expression datasets from CDT files
- load_best_hits.py: Load BLAST best hits into homolog tables

Phenotype/Interaction Data:
- load_phenotype.py: Bulk load phenotype data
- load_grid.py: Load BioGRID interaction data

Database Cross-Reference Updates:
- update_dbxref_from_uniprot.py: Update UniProt/Swiss-Prot/TrEMBL dbxrefs
- update_ncbi_dbxref.py: Update NCBI Gene and RefSeq dbxrefs

One-time scripts (not converted):
- copyLitGuideNotes.pl: One-time migration script
- copyLitGuideToRefProperty.pl: One-time migration script
- fixSSAs.pl: One-time fix script

Usage Examples:

Load PDF status:
    python load_pdf_status.py pdf_list.txt --log-file status.log

Load headlines:
    python load_headlines.py headlines.tsv --created-by CURATOR

Load reference temp:
    python load_ref_temp.py --search-terms "Candida" "albicans" --reldate 7

Load external links:
    python load_external_links.py links.tsv --url "http://example.com/DBXREF" \\
        --source "ExampleDB" --label "Example Link"

Load DBXREF URLs:
    python new_dbxref_url.py data.tsv --url "http://example.com/ID" \\
        --url-source NCBI --url-type "query by ID" \\
        --dbxref-source NCBI --dbxref-type "Gene ID" --created-by USER

Load feature URLs:
    python new_feat_url.py --url "http://example.com/FEATURE" \\
        --url-source ExampleDB --url-type "query by ORF" \\
        --feature-type "ORF" --created-by USER

Load molecules per cell:
    python load_molecules_per_cell.py abundance.tsv --created-by USER

Load dataset:
    python load_dataset.py --param-file params.txt --data-file expression.cdt \\
        --graphed --created-by USER

Load best hits:
    python load_best_hits.py besthits.txt --mod CGD --score-type e-value

Load phenotypes:
    python load_phenotype.py phenotypes.tsv --source CGD --created-by USER

Load BioGRID interactions:
    python load_grid.py BIOGRID-ORGANISM-*.tab3.txt --source BioGRID --created-by USER

Load PubMed references:
    python load_pubmed_references.py --search-terms "Candida" "albicans" --reldate 30

Update UniProt dbxrefs:
    python update_dbxref_from_uniprot.py --taxon-id 5476 --created-by USER

Update NCBI dbxrefs:
    python update_ncbi_dbxref.py --taxon-id 5476 --email user@example.com

Update fulltext URLs:
    python fulltext_url_update.py --email user@example.com

Original Perl Scripts:
    The scripts in this directory are conversions of Perl scripts from
    ~/cgd/bin/loading/.
"""
