"""
CGD Load Scripts

This package contains scripts for loading data into the CGD database.
These scripts are converted from the original Perl scripts in bin/load/.

Scripts:
- bulk_annotation.py: Load bulk annotation data (feature names, aliases, notes)
- load_curation_status.py: Load curation status into REFERENCE table
- load_external_links.py: Load external link information
- load_go_slim_terms.py: Load GO Slim terms into GO_SET table
- load_orthologs.py: Load ortholog/best hit information

Usage:
    python scripts/load/bulk_annotation.py --help
    python scripts/load/load_curation_status.py --help
    python scripts/load/load_external_links.py --help
    python scripts/load/load_go_slim_terms.py --help
    python scripts/load/load_orthologs.py --help
"""
