"""
CGD InParanoid Utilities

This package contains utility scripts for working with InParanoid ortholog detection.

Scripts (3 converted):
- blast_parser.py: Parse BLAST XML output for InParanoid analysis
- inparanoid_xml_creator.py: Create XML cluster files from InParanoid SQL output
- cno_finder.py: Find closest non-orthologs for genes in InParanoid clusters

Note: The main InParanoid algorithm (inparanoid.pl, ~2000 lines) is a complex
orthology detection algorithm from Stockholm Bioinformatics Centre. It is
recommended to use the original Perl implementation or consider BioPython/OrthoFinder
for ortholog detection workflows.

Usage:

Parse BLAST output:
    python blast_parser.py 50 blast_results.xml > parsed.txt
    python blast_parser.py -a 50 blast_results.xml  # with alignments

Create XML from InParanoid results:
    python inparanoid_xml_creator.py sqltable.txt > clusters.xml

Find closest non-orthologs:
    python cno_finder.py ORG1 ORG2 clusters.xml blastAA.txt blastBB.txt

Workflow:
    1. Run BLAST searches (blastp -m7 for XML output)
    2. Parse BLAST with blast_parser.py
    3. Run InParanoid algorithm (Perl script or alternative)
    4. Create XML with inparanoid_xml_creator.py
    5. Find CNOs with cno_finder.py

Original Perl Scripts:
    The scripts in this directory are conversions of Perl scripts from
    ~/cgd/bin/inparanoid/.
"""
