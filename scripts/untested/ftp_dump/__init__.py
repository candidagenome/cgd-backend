"""
FTP Dump Scripts

This package contains scripts for generating various data export files
for FTP distribution. These scripts are typically run as cron jobs to
keep public data files up to date.

Scripts:
- create_intergenic_file.py: Create intergenic (NOT feature) sequence file
- create_utr_seq_files.py: Generate 3' and 5' UTR sequence files in FASTA format
- dump_go_annotation.py: Dump GO annotations to gene_association file (GAF format)
- export_seq_similarity_data.py: Export sequence similarity data (BLAST, PDB, domains)
- ftp_datadump.py: FTP data dump coordinator (chromosomal features, lit curation)
- gene_registry.py: Create gene registry files (tab and text formats)
- generate_gff3.py: Generate GFF3 format files (ORFMAP, CloneGFF, Regulatory)
- gp2protein.py: Generate gp2protein mapping file (gene IDs to UniProt/RefSeq)
- pathway_ftp.py: Dump biochemical pathways to tab-delimited file
- recreate_fasta_files.py: Recreate FASTA files for ORFs, RNAs, and other features
- recreate_sgd_features.py: Recreate SGD-format feature files
- sequin.py: Create NCBI Sequin table files (.tbl format)
- update_chrom_sequence.py: Update chromosome sequence files
- update_seq_files.py: Update sequence files for FTP distribution

Usage:
    python scripts/ftp_dump/create_intergenic_file.py --help
    python scripts/ftp_dump/create_utr_seq_files.py 500
    python scripts/ftp_dump/dump_go_annotation.py --help
    python scripts/ftp_dump/export_seq_similarity_data.py pdb|domain|besthits|uniprot
    python scripts/ftp_dump/ftp_datadump.py --help
    python scripts/ftp_dump/gene_registry.py --help
    python scripts/ftp_dump/generate_gff3.py ORFMAP output.gff
    python scripts/ftp_dump/gp2protein.py --help
    python scripts/ftp_dump/pathway_ftp.py /path/to/output.tab
    python scripts/ftp_dump/recreate_fasta_files.py ORF|RNA|Other
    python scripts/ftp_dump/recreate_sgd_features.py --help
    python scripts/ftp_dump/sequin.py --help
    python scripts/ftp_dump/update_chrom_sequence.py --help
    python scripts/ftp_dump/update_seq_files.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
    DATA_DIR: Directory for data files
    FTP_DIR: FTP directory for output files
    LOG_DIR: Directory for log files
    PROJECT_ACRONYM: Project acronym (e.g., CGD, SGD)
    ORGANISM_NAME: Organism name for output files

Based on original Perl scripts from cgd/bin/ftp_dump/.
"""
