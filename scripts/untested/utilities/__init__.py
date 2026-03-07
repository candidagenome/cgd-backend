"""
CGD Standalone Utilities

This package contains standalone utility scripts that don't require
CGD-specific database modules.

Scripts (4 converted):
- valid_pcl.py: Validate PCL expression file format
- recompute_locations_after_seq_update.py: Update genomic locations after sequence changes
- get_gff_attrib_combinations.py: Extract attribute combinations from GFF files
- grep_list_match_files.py: Search for IDs in multiple files

Not Converted (require CGD-specific modules):
- countCodons.pl: Count codons in sequences (requires SGDObject)
- retrieveData.pl: Retrieve data from database (requires Tools::LuceneSearch)
- collect_tRNA_info.pl: Collect tRNA information (requires Database::Login)
- add_tRNA_introns.pl: Add tRNA introns (requires Database::Feature)
- mkCluster.pl: Make clusters (requires Repository::CdtDataset)

Usage:

Validate PCL file:
    python valid_pcl.py params.txt
    python valid_pcl.py params.txt -v  # suppress missing value warnings

Recompute locations after sequence update:
    python recompute_locations_after_seq_update.py input.gff GFF changes.txt

Get GFF attribute combinations:
    python get_gff_attrib_combinations.py input.gff

Search for IDs in files:
    python grep_list_match_files.py genes.txt file1.txt file2.txt

Original Perl Scripts:
    The scripts in this directory are conversions of Perl scripts from
    ~/cgd/bin/ and ~/cgd/bin/UMD_data_transfer/bin/.
"""
