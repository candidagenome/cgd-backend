"""
CGD Protein Analysis Scripts

This package contains scripts for protein analysis, domain annotation,
structural homology searches, and protein property updates.

Scripts (12 total):

Sequence Utilities:
- cull_seq.py: Remove sequences from a file based on a list

Domain Analysis (InterProScan):
- submit_iprscan.py: Submit sequences to InterProScan
- parse_iprscan_data.py: Parse InterProScan output for display/loading
- load_domain_data.py: Load domain/motif data into database
- update_signalp_tmhmm.py: Update SignalP/TMHMM protein details

PDB Structural Homology:
- download_pdb_seq.py: Download PDB sequences from NCBI
- blast_pdb.py: BLAST proteins against PDB database
- load_pdb.py: Load PDB homology data into database

UniProt Mapping:
- map_uniprot.py: Map proteins to UniProt (SwissProt/TrEMBL)

Protein Properties:
- protein_localization.py: Run SignalP and WoLF PSORT for localization
- prepare_protein_tracks.py: Prepare JBrowse protein tracks
- protein_prop_update.py: Calculate and update protein properties (MW, pI, etc.)

Workflow:

Domain Annotation Pipeline:
    1. submit_iprscan.py --strain STRAIN proteins.fa
    2. parse_iprscan_data.py --strain STRAIN --gff output.gff
    3. load_domain_data.py --strain STRAIN --data domain.data --dbuser USER

PDB Homology Pipeline:
    1. download_pdb_seq.py
    2. blast_pdb.py STRAIN proteins.fa
    3. load_pdb.py STRAIN USER

UniProt Mapping:
    1. map_uniprot.py --strain STRAIN

Protein Properties Update:
    python protein_prop_update.py --strain-abbrev SC5314 --created-by USER \\
        --coding-seq-file coding.fasta --protein-seq-file protein.fasta

Original Perl Scripts:
    The scripts in this directory are conversions of Perl scripts from
    ~/cgd/bin/proteins/.
"""
