"""
CGD Load Scripts

This package contains scripts for loading data into the CGD database.
These scripts are converted from the original Perl scripts in bin/load/.

Scripts:
- bulk_annotation.py: Load bulk annotation data (feature names, aliases, notes)
- bulk_load_phenotype.py: Bulk load phenotype data with experiments
- check_chebi.py: Compare CHEBI OBO files between versions
- fix_trna_anticodons.py: Fix tRNA names (T -> U in anticodons)
- generate_chibana_mapping.py: Generate Chibana to ORF mapping from GenBank
- load_aliases.py: Load extra aliases from external sources
- load_awg_orfs.py: Load Annotation Working Group ORFs from FASTA
- load_chibana_aliases.py: Load Chibana chromosome 7 aliases
- load_chr_coords_trnas.py: Load chromosome coordinates for tRNAs
- load_chromosome.py: Load chromosome sequences (supports Assembly 20/21)
- load_curation_status.py: Load curation status into REFERENCE table
- load_external_ids_orthogroups.py: Load external IDs for orthogroup associations
- load_external_links.py: Load external link information via DBXREF
- load_external_links_feat_url.py: Load external links via FEAT_URL
- load_feature_a21_locations.py: Load Assembly 21 locations (tRNA, CEN, rDNA)
- load_go_slim_terms.py: Load GO Slim terms into GO_SET table
- load_ipf_aliases.py: Load IPF (Induced Protein Fragment) aliases
- load_missing_feature_types.py: Add missing feature types for features
- load_mito_chr.py: Load mitochondrial chromosome data
- load_module_info.py: Load module info (external IDs and template URLs)
- load_orf_aliases.py: Load ORF version aliases (orf4, orf6 mappings)
- load_orthologs.py: Load ortholog/best hit information
- load_trnas.py: Load tRNA loci into the database
- update_pdf_status.py: Update PDF status for references by PMID

Usage:
    python scripts/load/bulk_annotation.py --help
    python scripts/load/bulk_load_phenotype.py --help
    python scripts/load/check_chebi.py --help
    python scripts/load/fix_trna_anticodons.py --help
    python scripts/load/generate_chibana_mapping.py --help
    python scripts/load/load_aliases.py --help
    python scripts/load/load_awg_orfs.py --help
    python scripts/load/load_chibana_aliases.py --help
    python scripts/load/load_chr_coords_trnas.py --help
    python scripts/load/load_chromosome.py --help
    python scripts/load/load_curation_status.py --help
    python scripts/load/load_external_ids_orthogroups.py --help
    python scripts/load/load_external_links.py --help
    python scripts/load/load_external_links_feat_url.py --help
    python scripts/load/load_feature_a21_locations.py --help
    python scripts/load/load_go_slim_terms.py --help
    python scripts/load/load_ipf_aliases.py --help
    python scripts/load/load_missing_feature_types.py --help
    python scripts/load/load_mito_chr.py --help
    python scripts/load/load_module_info.py --help
    python scripts/load/load_orf_aliases.py --help
    python scripts/load/load_orthologs.py --help
    python scripts/load/load_trnas.py --help
    python scripts/load/update_pdf_status.py --help
"""
