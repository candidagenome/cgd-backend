"""
CGD Check/Validation Scripts

This package contains scripts for validating data and performing checks
on CGD database content and input files.

Scripts (21 total):

File Validation and Conversion:
- check_blank_entries_embl.py: Check EMBL files for blank/missing entries
- convert_xml_blast.py: Convert BLAST XML output to text format
- embl_to_fasta.py: Convert EMBL files to FASTA format
- fix_embl_files.py: Fix unquoted multiline remarks in EMBL files
- get_feature_tags.py: Extract feature tags (product, remarks) from EMBL files

Sequence Analysis:
- compare_orf_lists.py: Compare two ORF lists and report differences
- find_internal_stops.py: Find internal stop codons in ORF sequences
- fix_partial_terminal_codons.py: Find/fix ORFs with partial terminal codons
- get_downstream_no_stop.py: Get downstream sequences for ORFs lacking stops
- parse_internal_stops_terminus.py: Filter internal stops close to terminus
- run_muscle.py: Run MUSCLE multiple sequence alignment
- run_blast.py: Run BLAST searches and parse results
- make_pairwise_alignments.py: Make pairwise sequence alignments

Database Queries:
- get_chr_sequences.py: Extract chromosome sequences from database
- get_feature_locations.py: Get feature locations from database
- get_orf_sequences.py: Get genomic sequences for ORFs from database
- get_protein_sequences.py: Get protein sequences for ORFs from database

Database Updates:
- add_aliases_merged_orfs.py: Add aliases from merged/deleted ORFs
- load_subfeatures.py: Load subfeatures (exons/introns) for features
- update_feature_type.py: Update feature_type for a list of features
- update_subfeature_type.py: Update subfeature_type for subfeatures

Usage Examples:
    # Check EMBL files for issues
    python scripts/check/check_blank_entries_embl.py *.embl --required-tags gene contig

    # Convert EMBL to FASTA
    python scripts/check/embl_to_fasta.py input.embl -o output.fa

    # Find internal stop codons
    python scripts/check/find_internal_stops.py --organism Ca --feature-type ORF

    # Get ORF sequences from database
    python scripts/check/get_orf_sequences.py --orf-list orfs.txt -o sequences.fa

    # Get protein sequences
    python scripts/check/get_protein_sequences.py orfs.txt -o proteins.fa --suffix '-A21'

    # Get feature locations in BED format
    python scripts/check/get_feature_locations.py features.txt -o locations.bed --format bed

    # Update feature types
    python scripts/check/update_feature_type.py orfs.txt --new-type pseudogene --dry-run

    # Add aliases from merged ORFs
    python scripts/check/add_aliases_merged_orfs.py merged_orfs.txt --dry-run

    # Run MUSCLE alignments
    python scripts/check/run_muscle.py sequences.fa -o alignments.clw

    # Run BLAST search
    python scripts/check/run_blast.py queries.fa blastdb -o results.tsv

    # Make pairwise alignments
    python scripts/check/make_pairwise_alignments.py sequences.fa -o alignments.txt --translate

Original Perl Scripts:
    The scripts in this directory are conversions of Perl scripts from
    ~/cgd/bin/check/. Many assembly-specific scripts (Ca19, Ca20, A20, A21)
    have been generalized to work with any organism/assembly.
"""
