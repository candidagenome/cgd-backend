"""
FTP dump scripts for data export.

This package contains scripts for generating various data export files
for FTP distribution. These scripts are typically run as cron jobs to
keep public data files up to date.

Scripts:
--------
create_utr_seq_files.py
    Generate 3' and 5' UTR sequence files in FASTA format.
    Usage: python create_utr_seq_files.py 500

export_seq_similarity_data.py
    Export sequence similarity data (BLAST hits, PDB homologs, domains).
    Usage: python export_seq_similarity_data.py pdb|domain|besthits|uniprot

create_intergenic_file.py
    Create intergenic (NOT feature) sequence file.
    Usage: python create_intergenic_file.py

dump_go_annotation.py
    Dump GO annotations to gene_association file (GAF format).
    Usage: python dump_go_annotation.py

pathway_ftp.py
    Dump biochemical pathways to tab-delimited file.
    Usage: python pathway_ftp.py /path/to/output.tab

recreate_fasta_files.py
    Recreate FASTA files for ORFs, RNAs, and other features.
    Usage: python recreate_fasta_files.py ORF|RNA|Other

gp2protein.py
    Generate gp2protein mapping file (gene IDs to UniProt/RefSeq).
    Usage: python gp2protein.py [--test]

gene_registry.py
    Create gene registry files (tab and text formats).
    Usage: python gene_registry.py

Environment Variables:
---------------------
All scripts use the following environment variables:

DATABASE_URL : Database connection URL
DB_SCHEMA : Database schema name (default: MULTI)
DATA_DIR : Directory for data files
FTP_DIR : FTP directory for output files
LOG_DIR : Directory for log files
PROJECT_ACRONYM : Project acronym (e.g., CGD, SGD)

Based on original Perl scripts from cgd/bin/ftp_dump/.
"""
