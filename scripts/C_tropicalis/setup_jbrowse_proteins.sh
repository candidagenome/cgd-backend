#!/bin/bash
# Setup JBrowse2 protein domain data for C. tropicalis
# Run this on cgd-frontend-dev

set -e

ORGANISM="C_tropicalis_MYA3404"
DATA_DIR="/data/jbrowse2/protein_data"
DOMAIN_DIR="/data/domain/C_tropicalis_MYA3404"
FASTA_FILE="${DOMAIN_DIR}/C_tropicalis_MYA3404_default_proteins.fasta"
TSV_FILE="${DOMAIN_DIR}/C_tropicalis_MYA3404_proteins.tsv"

# Domain databases to create tracks for
DATABASES="Pfam PANTHER SUPERFAMILY Gene3D SMART ProSiteProfiles CDD PRINTS ProSitePatterns"

echo "=== Setting up JBrowse2 protein data for ${ORGANISM} ==="

# 1. Create symlink for protein FASTA
echo "Creating protein FASTA symlink..."
ln -sf ${FASTA_FILE} ${DATA_DIR}/${ORGANISM}_proteins.fasta

# 2. Create FASTA index
echo "Creating FASTA index..."
samtools faidx ${DATA_DIR}/${ORGANISM}_proteins.fasta

# 3. Create GFF files for each domain database
echo "Creating domain GFF files..."

# Create combined domains GFF
echo "##gff-version 3" > /tmp/${ORGANISM}_domains.gff

for DB in $DATABASES; do
    echo "Processing ${DB}..."

    # Map Gene3D to CATH for display
    if [ "$DB" == "Gene3D" ]; then
        DISPLAY_NAME="CATH"
    else
        DISPLAY_NAME="$DB"
    fi

    # Extract domains for this database and convert to GFF
    awk -F'\t' -v db="$DB" -v disp="$DISPLAY_NAME" '
    $3 == db && $10 == "T" {
        # protein_id, length, analysis, sig_acc, sig_desc, start, end, score, status, date, ipr_acc, ipr_desc
        protein_id = $1
        sig_acc = $4
        sig_desc = $5
        start = $6
        end = $7
        score = ($8 == "-" ? "." : $8)
        ipr_acc = $11

        # Escape special chars
        gsub(/;/, "%3B", sig_desc)
        gsub(/=/, "%3D", sig_desc)

        attrs = "ID=" sig_acc ";Name=" sig_acc ";description=" sig_desc
        if (ipr_acc != "" && ipr_acc != "-") {
            attrs = attrs ";interpro=" ipr_acc
        }

        print protein_id "\t" disp "\t" disp "\t" start "\t" end "\t" score "\t.\t.\t" attrs
    }' ${TSV_FILE} > /tmp/${ORGANISM}_${DISPLAY_NAME}.gff

    # Sort, compress and index
    if [ -s /tmp/${ORGANISM}_${DISPLAY_NAME}.gff ]; then
        sort -k1,1 -k4,4n /tmp/${ORGANISM}_${DISPLAY_NAME}.gff > /tmp/${ORGANISM}_${DISPLAY_NAME}_sorted.gff
        bgzip -c /tmp/${ORGANISM}_${DISPLAY_NAME}_sorted.gff > ${DATA_DIR}/${ORGANISM}_${DISPLAY_NAME}.gff.gz
        tabix -p gff ${DATA_DIR}/${ORGANISM}_${DISPLAY_NAME}.gff.gz

        # Append to combined file
        cat /tmp/${ORGANISM}_${DISPLAY_NAME}_sorted.gff >> /tmp/${ORGANISM}_domains.gff

        echo "  Created ${ORGANISM}_${DISPLAY_NAME}.gff.gz"
    else
        echo "  No data for ${DB}"
    fi
done

# 4. Create combined domains file
echo "Creating combined domains file..."
tail -n +2 /tmp/${ORGANISM}_domains.gff | sort -k1,1 -k4,4n > /tmp/${ORGANISM}_domains_sorted.gff
echo "##gff-version 3" | cat - /tmp/${ORGANISM}_domains_sorted.gff > /tmp/${ORGANISM}_domains_final.gff
bgzip -c /tmp/${ORGANISM}_domains_final.gff > ${DATA_DIR}/${ORGANISM}_domains.gff.gz
tabix -p gff ${DATA_DIR}/${ORGANISM}_domains.gff.gz

# 5. Cleanup temp files
rm -f /tmp/${ORGANISM}_*.gff

echo ""
echo "=== Done! ==="
echo "Files created in ${DATA_DIR}:"
ls -la ${DATA_DIR}/${ORGANISM}*

echo ""
echo "NOTE: You still need to update /data/jbrowse2/config.json to add:"
echo "  - Assembly: ${ORGANISM}_prot"
echo "  - Tracks for each domain database"
