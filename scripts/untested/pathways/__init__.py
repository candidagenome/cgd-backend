"""
CGD Pathway Tools Utilities

This package contains scripts for working with Pathway Tools (PTools)
data for metabolic pathway annotations.

Scripts (4 converted):
- get_coordinates_from_embl.py: Extract CDS coordinates from EMBL files
- swap_gene_for_orf19.py: Swap ORF19 IDs with gene names in PTools files
- produce_pathways_and_genes.py: Process pathways.col to generate download file
- update_ocelot_file.py: Update and compress Ocelot file for download

Not Converted:
- producePFFile.pl: Generate Pathologic format files (requires CGD Perl modules)
- replaceColumns.pl: Simple column replacement (specialized utility)

Usage:

Extract coordinates from EMBL:
    python get_coordinates_from_embl.py genome.embl

Swap ORF19 with gene names:
    python swap_gene_for_orf19.py --features chromosomal_feature.tab \\
        --input ptools_export.txt --output modified.txt

Generate pathways download file:
    python produce_pathways_and_genes.py --input pathways.col \\
        --output pathwaysAndGenes.tab

Pathway Tools Workflow:
    1. Export data from Pathway Tools
    2. Process with swap_gene_for_orf19.py if needed
    3. Re-import modified data into Pathway Tools
    4. Generate download files with produce_pathways_and_genes.py

Original Perl Scripts:
    The scripts in this directory are conversions of Perl scripts from
    ~/cgd/bin/pathways/.
"""
