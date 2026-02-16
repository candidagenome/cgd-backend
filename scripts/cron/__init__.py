"""
CGD Cron Scripts

This package contains scripts typically run as scheduled cron jobs for
automated database updates, data processing, and maintenance tasks.

Categories:
-----------

GO (Gene Ontology) Scripts:
- load_go.py: Load/update GO terms from OBO file
- load_go_path.py: Load GO path relationships
- load_go_annotations_ortho.py: Load GO annotations via orthology
- load_go_annotations_ortho_transfer.py: Transfer GO annotations between species
- transfer_go.py: Transfer GO annotations via ortholog mappings
- process_obsolete_goids.py: Handle obsolete GO term transitions
- find_missing_go_child.py: Find missing child GO terms
- dump_go_annotation.py: Dump GO annotations to GAF format
- dump_go_slim_annotation.py: Dump GO Slim annotations
- go_synonym_transfer_check.py: Check GO synonym transfers
- checkin_associations_file.py: Check in GO association files

Reference/Literature Scripts:
- load_pubmed_references.py: Load references from PubMed
- load_ref_temp.py: Load temporary reference data
- load_gene_refs_to_reftemp.py: Load gene references to ref_temp table
- check_epubs.py: Check for electronic publication updates
- fulltext_url_update.py: Update full-text URLs for references
- fulltext_url_weekly_update.py: Weekly full-text URL updates

Data Export/Dump Scripts:
- dump_gff.py: Dump GFF3 files for genome browser
- dump_gff_assem20.py: Dump GFF for Assembly 20
- dump_gtf.py: Dump GTF files
- dump_sequence.py: Dump FASTA sequence files
- dump_phenotype_data.py: Dump phenotype data
- dump_gene_association.py: Dump gene association files
- dump_chromosomal_features.py: Dump chromosomal feature data
- dump_paula_sundstrom_files.py: Custom data dumps
- ftp_datadump.py: FTP data dump orchestration
- make_embl_files.py: Generate EMBL format files
- make_gpi.py: Generate Gene Product Information file

Ortholog/Homology Scripts:
- generate_best_hits.py: Generate best BLAST hits
- generate_ortholog_file_db_strains.py: Generate ortholog files for strains
- make_pairwise_orthogroup_file.py: Create pairwise orthogroup files
- make_transitive_ortholog_file.py: Create transitive ortholog files
- reciprocal_blast.py: Run reciprocal BLAST analysis
- generic_reciprocal.py: Generic reciprocal analysis
- run_inparanoid.py: Run InParanoid ortholog detection

BLAST/Sequence Analysis Scripts:
- create_blast_datasets.py: Create BLAST databases
- update_patmatch_dataset.py: Update pattern matching datasets
- update_multi_blast.py: Update multi-species BLAST
- update_seq_search_files.py: Update sequence search files
- check_seq_integrity.py: Check sequence data integrity
- various_checks_on_orf_seqs.py: ORF sequence validation
- various_checks_on_orf_seqs_aspgd.py: AspGD-specific ORF checks

Database/Feature Update Scripts:
- update_orf_classifications.py: Update ORF classifications
- update_biogrid_xref.py: Update BioGRID cross-references
- update_with_sgd.py: Update with SGD data
- update_with_interpro.py: Update with InterPro data
- update_a22_liftover.py: Assembly 22 coordinate liftover
- make_automatic_descriptions.py: Generate automatic gene descriptions

GBrowse/Browser Scripts:
- load_gbrowse_mysql.py: Load data into GBrowse MySQL
- load_gbrowse_mysql_new.py: New GBrowse MySQL loader
- rewrite_stanford_gff.py: Rewrite Stanford GFF files
- make_config_mapping_file.py: Generate config mapping files

Report/Notification Scripts:
- curator_reports.py: Generate curator progress reports
- genome_reports.py: Generate genome statistics reports
- annotation_num_for_goid.py: Annotation counts by GO term
- generate_gene_list.py: Generate gene list reports
- db_rebuild_reminder.py: Database rebuild reminders
- weekly_git_activity.py: Weekly git activity reports

Validation/Check Scripts:
- check_data.py: General data validation checks
- check_ptools.py: Pathway Tools validation
- check_recaptcha.py: reCAPTCHA validation
- check_with_support.py: Support data validation
- check_genome_version_dump.py: Genome version dump validation
- sandbox_check.py: Sandbox environment checks

Utility Scripts:
- make_cache_file.py: Generate cache files
- create_not_file.py: Create NOT annotation files
- generate_gap_files.py: Generate gap region files
- genespring_format.py: GeneSpring format conversion
- make_orf_coordinates_webprimer.py: ORF coordinates for WebPrimer
- wusage_transfer.py: Web usage statistics transfer
- convert_logs_weekly_to_monthly.py: Convert log files
- archive_website.py: Archive website snapshots
- download_ncbi_journal_file.py: Download NCBI journal data

Subdirectories:
- CGOB/: Candida Gene Order Browser scripts

Usage:
    Most scripts accept --help for usage information:
    python scripts/cron/<script_name>.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
    DATA_DIR: Directory for data files
    HTML_ROOT_DIR: Root directory for HTML files
    LOG_DIR: Directory for log files
    TMP_DIR: Temporary directory
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
    CURATOR_EMAIL: Email for notifications
    ADMIN_EMAIL: Admin email address
    SMTP_HOST: SMTP server host
"""
