#!/usr/bin/env bash
#
# Wrapper script to load PubMed references for all CGD organisms.
# Python equivalent of cgd-pubUpdate (Perl version).
#
# Usage:
#   ./cgd_pub_update.sh
#

# Note: We intentionally don't use 'set -e' here because minor errors
# (like duplicate PMID constraints) should not stop the entire pipeline.
# Each Python script handles its own errors gracefully.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Activate virtual environment only if it has required packages
# Otherwise use system Python (which may have packages in ~/.local)
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    # Check if venv has required packages
    if "$PROJECT_ROOT/venv/bin/python3" -c "import requests, Bio" 2>/dev/null; then
        source "$PROJECT_ROOT/venv/bin/activate"
    fi
elif [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    if "$PROJECT_ROOT/.venv/bin/python3" -c "import requests, Bio" 2>/dev/null; then
        source "$PROJECT_ROOT/.venv/bin/activate"
    fi
fi

# Change to project root for relative paths to work
cd "$PROJECT_ROOT"

# Arrays to track results
declare -a SPECIES_LIST=("C_albicans" "C_dubliniensis" "C_glabrata" "C_parapsilosis" "C_auris" "C_tropicalis")
declare -a SPECIES_QUERIES=("Candida AND albicans" "Candida AND dubliniensis" "Candida AND glabrata" "Candida AND parapsilosis" "Candida AND auris" "Candida AND tropicalis")
declare -a LOG_FILES=()
declare -a REFS_LOADED=()
declare -a ERRORS=()

echo "Starting PubMed reference loading at $(date)"
echo "========================================"

# Load PubMed references for each organism
for i in "${!SPECIES_LIST[@]}"; do
    species="${SPECIES_LIST[$i]}"
    query="${SPECIES_QUERIES[$i]}"

    echo ""
    echo "Processing $species..."

    python3 "$SCRIPT_DIR/load_pubmed_references.py" \
        --species-query "$query" \
        --species-abbrev "$species" \
        --link-genes Y

    # Find the most recent log file for this species
    log_file=$(ls -t "$LOG_DIR/${species}_PubMed_"*.log 2>/dev/null | head -1)
    if [ -n "$log_file" ]; then
        LOG_FILES+=("$log_file")
        # Extract summary from log file
        refs=$(grep "PubMed reference(s) were loaded" "$log_file" 2>/dev/null | grep -oE "^[0-9]+" || echo "0")
        errs=$(grep "ERROR(s) occurred while loading PubMed" "$log_file" 2>/dev/null | grep -oE "^[0-9]+" || echo "0")
        REFS_LOADED+=("$refs")
        ERRORS+=("$errs")
    else
        LOG_FILES+=("N/A")
        REFS_LOADED+=("0")
        ERRORS+=("0")
    fi
done

echo ""
echo "Loading ref_temp entries..."
echo "----------------------------------------"

# Load ref_temp for species and synonyms
REF_TEMP_QUERIES=("albicans" "glabrata" "dubliniensis" "parapsilosis" "auris" "tropicalis" "Torulopsis" "Candida" "Nakaseomyces AND glabratus" "Nakaseomyces AND glabrata" "Candidozyma AND auris" "Candida AND krusei" "Pichia AND kudriavzevii")
REF_TEMP_SUCCESS=0
REF_TEMP_TOTAL=${#REF_TEMP_QUERIES[@]}

python3 "$SCRIPT_DIR/load_ref_temp.py" --query "albicans" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "glabrata" \
    --exclude "Biomphalaria,Arachis,Vitex,Littorinopsis,Pera,Velleia,Magonia,Ficus,Serjania,Disonycha,Lasiosphaeria" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "dubliniensis" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "parapsilosis" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "auris" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "tropicalis" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Torulopsis" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Candida" --exclude "Folsomia" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Nakaseomyces AND glabratus" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Nakaseomyces AND glabrata" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Candidozyma AND auris" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Candida AND krusei" && ((REF_TEMP_SUCCESS++)) || true
python3 "$SCRIPT_DIR/load_ref_temp.py" --query "Pichia AND kudriavzevii" && ((REF_TEMP_SUCCESS++)) || true

echo ""
echo "Updating full text URLs..."
echo "----------------------------------------"

python3 "$SCRIPT_DIR/fulltext_url_weekly_update.py"

# Get fulltext URL summary (extract value after label, default to 0 if empty)
FULLTEXT_LOG="$LOG_DIR/load/NCBIfulltextURL.log"
_extract_value() {
    local val
    val=$(tail -20 "$FULLTEXT_LOG" 2>/dev/null | grep "$1" | tail -1 | sed "s/.*$1//" | tr -d ' ')
    echo "${val:-0}"
}
FULLTEXT_CHECKED=$(_extract_value "PubMed IDs checked: ")
FULLTEXT_FOUND=$(_extract_value "URLs found: ")
FULLTEXT_INSERTED=$(_extract_value "URLs inserted: ")
FULLTEXT_ERRORS=$(_extract_value "Errors: ")

echo ""
echo "========================================"
echo "SUMMARY"
echo "========================================"
echo ""
echo "PubMed References Loaded:"
echo "----------------------------------------"
total_refs=0
total_errors=0
for i in "${!SPECIES_LIST[@]}"; do
    species="${SPECIES_LIST[$i]}"
    refs="${REFS_LOADED[$i]}"
    errs="${ERRORS[$i]}"
    printf "  %-20s: %3s refs loaded, %s errors\n" "$species" "$refs" "$errs"
    total_refs=$((total_refs + refs))
    total_errors=$((total_errors + errs))
done
echo "----------------------------------------"
printf "  %-20s: %3s refs loaded, %s errors\n" "TOTAL" "$total_refs" "$total_errors"
echo ""
echo "ref_temp Queries: $REF_TEMP_SUCCESS/$REF_TEMP_TOTAL completed"
echo ""
echo "Fulltext URLs: $FULLTEXT_CHECKED checked, $FULLTEXT_FOUND found, $FULLTEXT_INSERTED inserted, $FULLTEXT_ERRORS errors"
echo ""
echo "Log Files:"
echo "----------------------------------------"
for log in "${LOG_FILES[@]}"; do
    echo "  $log"
done
echo "  $LOG_DIR/load/loadRefTemp.log"
echo "  $LOG_DIR/load/NCBIfulltextURL.log"
echo ""
echo "========================================"
echo "Finished PubMed reference loading at $(date)"
