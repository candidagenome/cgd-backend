#!/usr/bin/env bash
#
# Wrapper script to load PubMed references for all CGD organisms.
# Python equivalent of cgd-pubUpdate (Perl version).
#
# Usage:
#   ./cgd_pub_update.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Change to project root for relative paths to work
cd "$PROJECT_ROOT"

echo "Starting PubMed reference loading at $(date)"
echo "========================================"

# Load PubMed references for each organism
python3 "$SCRIPT_DIR/load_pubmed_references.py" \
    --species-query "Candida AND albicans" \
    --species-abbrev C_albicans \
    --link-genes Y

python3 "$SCRIPT_DIR/load_pubmed_references.py" \
    --species-query "Candida AND dubliniensis" \
    --species-abbrev C_dubliniensis \
    --link-genes Y

python3 "$SCRIPT_DIR/load_pubmed_references.py" \
    --species-query "Candida AND glabrata" \
    --species-abbrev C_glabrata \
    --link-genes Y

python3 "$SCRIPT_DIR/load_pubmed_references.py" \
    --species-query "Candida AND parapsilosis" \
    --species-abbrev C_parapsilosis \
    --link-genes Y

python3 "$SCRIPT_DIR/load_pubmed_references.py" \
    --species-query "Candida AND auris" \
    --species-abbrev C_auris \
    --link-genes Y

echo ""
echo "Loading ref_temp entries..."
echo "----------------------------------------"

# Load ref_temp for species and synonyms
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "albicans"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "glabrata" \
    --exclude "Biomphalaria,Arachis,Vitex,Littorinopsis,Pera,Velleia,Magonia,Ficus,Serjania,Disonycha,Lasiosphaeria"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "dubliniensis"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "parapsilosis"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "auris"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Torulopsis"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Candida" --exclude "Folsomia"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Nakaseomyces AND glabratus"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Nakaseomyces AND glabrata"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Candidozyma AND auris"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Candida AND krusei"
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Pichia AND kudriavzevii"

echo ""
echo "Updating full text URLs..."
echo "----------------------------------------"

python3 "$SCRIPT_DIR/fulltext_url_weekly_update.py"

echo "========================================"
echo "Finished PubMed reference loading at $(date)"
