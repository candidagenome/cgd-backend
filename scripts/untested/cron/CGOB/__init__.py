"""
CGOB (Candida Gene Order Browser) Scripts

This package contains scripts for managing ortholog data from the
Candida Gene Order Browser (CGOB) project.

Scripts:
- download_cgob_files.py: Download CGOB data files from source
- format_sgd_blastdb.py: Format SGD BLAST databases for ortholog search
- get_cglab.py: Get C. glabrata ortholog data from YGOB
- cgob_to_cgd.py: Map CGOB identifiers to CGD identifiers
- prepare_cgd_orthologs.py: Prepare CGD ortholog clusters from CGOB data
- cgob_alignments.py: Create ortholog alignments and phylogenetic trees

Workflow:
1. download_cgob_files.py - Download latest CGOB data
2. format_sgd_blastdb.py - Prepare BLAST databases
3. get_cglab.py - Get C. glabrata data from YGOB
4. cgob_to_cgd.py - Create CGOB to CGD identifier mapping
5. prepare_cgd_orthologs.py - Generate CGD-format ortholog clusters
6. cgob_alignments.py - Create sequence alignments and trees

Usage:
    python scripts/cron/CGOB/download_cgob_files.py
    python scripts/cron/CGOB/cgob_to_cgd.py --debug
    python scripts/cron/CGOB/prepare_cgd_orthologs.py
    python scripts/cron/CGOB/cgob_alignments.py --debug --rounds 10

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files (CGOB data stored in DATA_DIR/CGOB/)
    LOG_DIR: Directory for log files
    BLASTP: Path to blastp executable
    MUSCLE: Path to MUSCLE alignment tool
    SEMPHY: Path to SEMPHY phylogenetic tool
    BLASTDBCMD: Path to blastdbcmd for sequence retrieval
"""
