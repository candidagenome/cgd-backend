"""
CGD Data Scripts

This package contains scripts for data export, dump, and loading operations.
These scripts generate files for public download, genome browsers, and
external databases.

Scripts:
- create_blast_datasets.py: Create BLAST databases from sequence files
- dump_gff.py: Dump GFF3 files for genome browser and downloads
- dump_go_annotation.py: Dump GO annotations to GAF format
- dump_go_slim_annotation.py: Dump GO Slim annotations
- dump_gtf.py: Dump GTF files for genome analysis tools
- dump_intergenic_sequences.py: Dump intergenic sequence files
- dump_phenotype_data.py: Dump phenotype data to tab-delimited files
- dump_sequence.py: Dump FASTA sequence files (chromosome, ORF, protein)
- load_gbrowse_mysql.py: Load GFF/FASTA data into GBrowse MySQL database
- load_go_path.py: Load GO path data for term relationships
- make_gpi.py: Generate GPI (Gene Product Information) file for GO

Usage:
    python scripts/data/create_blast_datasets.py --help
    python scripts/data/dump_gff.py --help
    python scripts/data/dump_go_annotation.py --help
    python scripts/data/dump_go_slim_annotation.py --help
    python scripts/data/dump_gtf.py --help
    python scripts/data/dump_intergenic_sequences.py --help
    python scripts/data/dump_phenotype_data.py --help
    python scripts/data/dump_sequence.py --help
    python scripts/data/load_gbrowse_mysql.py --help
    python scripts/data/load_go_path.py --help
    python scripts/data/make_gpi.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
    DATA_DIR: Directory for data files
    HTML_ROOT_DIR: Root directory for HTML/web files
    LOG_DIR: Directory for log files
    TMP_DIR: Temporary directory
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
    CURATOR_EMAIL: Email for notifications
"""
