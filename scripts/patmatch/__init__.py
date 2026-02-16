"""
CGD Patmatch Utilities

This package contains scripts for pattern matching in biological sequences.

Scripts (2 total):
- generate_sequence_index.py: Generate byte offsets for FASTA sequence headers
- patmatch_to_nrgrep.py: Convert Patmatch patterns to nrgrep patterns

Usage:

Generate sequence index:
    python generate_sequence_index.py sequences.fasta > index.txt

Convert pattern (nucleotide):
    python patmatch_to_nrgrep.py -n "ATG{3,5}TAA"

Convert pattern (protein):
    python patmatch_to_nrgrep.py -p "M[AILV]{2,4}K"

Convert pattern (reverse complement):
    python patmatch_to_nrgrep.py -c "ATG{3,5}TAA"

Original Perl Scripts:
    The scripts in this directory are conversions of Perl scripts from
    ~/cgd/bin/patmatch/.
"""
