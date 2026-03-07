"""
CGD Web Utilities

This package contains scripts for web-related maintenance tasks.

Scripts (1 converted):
- delete_old_files.py: Delete files older than specified age

Not Converted:
- makeCacheFile.pl: Generate web cache files (requires lynx, CGD config)
- bp_bulk_load_gff.new.pl: Bulk load GFF files (BioPerl dependency)

Usage:

Delete old files:
    python delete_old_files.py /path/to/file.gif --days 7
    python delete_old_files.py /path/to/*.tmp --days 30 --dry-run

Original Perl Scripts:
    The scripts in this directory are conversions of Perl scripts from
    ~/cgd/bin/web/.
"""
