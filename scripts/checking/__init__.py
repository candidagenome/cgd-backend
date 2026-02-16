"""
CGD Data Checking and Validation Scripts

This package contains scripts for data validation and sequence checking.

Scripts (2 converted):
- check_pmid_valid.py: Validate PubMed IDs using NCBI Entrez
- dump_chromosome_sequence.py: Dump chromosome sequences from database

Not Converted (require CGD-specific Perl modules):
- checkData.pl: Comprehensive data validation (64KB, very complex)
- checkChromosomeSequence.pl: Check chromosome sequence integrity
- checkFastaFeatureSequences.pl: Check FASTA feature sequences
- checkGCGFeatureSequences.pl: Check GCG feature sequences
- checkRefProp.pl: Check reference properties
- cleanup_go_ftp_files.pl: Clean up GO FTP files
- createFastaFeatureSequences.pl: Create FASTA feature sequences
- dbInternalSeqChecks.pl: Internal database sequence checks
- deleteWidowData.pl: Delete orphaned data

Usage:

Validate PubMed IDs:
    python check_pmid_valid.py --email user@example.com

Dump chromosome sequences:
    python dump_chromosome_sequence.py --chromosome 1 --output chr1.fasta
    python dump_chromosome_sequence.py --all --output-dir sequences/

Original Perl Scripts:
    The scripts in this directory are conversions of Perl scripts from
    ~/cgd/bin/checking/.
"""
