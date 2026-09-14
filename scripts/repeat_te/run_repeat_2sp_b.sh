#!/usr/bin/env bash
# De novo repeat/TE pipeline for C. glabrata + C. dubliniensis — same as the
# tropicalis/auris/parapsilosis runs. Sequential (2-core box).
set -u
cd /data/HTS/repeat_te
DK="docker run --rm --user $(id -u):$(id -g) -v /data:/data dfam/tetools:latest"
MM="/data/HTS/trna_tropicalis/bin/micromamba"
export MAMBA_ROOT_PREFIX=/data/HTS/micromamba

run_sp() {
  local sp=$1 tag=$2 genome=$3
  echo "==== $sp RepeatModeler start $(date) ===="
  mkdir -p rmodel_$sp
  $DK bash -c "cd /data/HTS/repeat_te/rmodel_$sp && BuildDatabase -name $tag $genome" > rmodel_$sp/builddb.log 2>&1
  echo "-- BuildDatabase exit=$?"
  $DK bash -c "cd /data/HTS/repeat_te/rmodel_$sp && RepeatModeler -database $tag -threads 2 -LTRStruct" > rmodel_$sp/repeatmodeler.log 2>&1
  echo "-- RepeatModeler exit=$? $(date) families=$(grep -c '>' rmodel_$sp/$tag-families.fa 2>/dev/null)"
  [ -s rmodel_$sp/$tag-families.fa ] || { echo "!! no families for $sp, skipping rest"; return 1; }
  $MM run -n tesorter TEsorter rmodel_$sp/$tag-families.fa -db rexdb -p 2 -pre ${tag}_tesorter > ${tag}_tesorter.log 2>&1
  echo "-- TEsorter exit=$?"
  mkdir -p rm_denovo_$sp
  $DK RepeatMasker -lib /data/HTS/repeat_te/rmodel_$sp/$tag-families.fa -pa 1 -xsmall -no_is -a -dir /data/HTS/repeat_te/rm_denovo_$sp $genome > rm_denovo_$sp/run.log 2>&1
  echo "-- RepeatMasker exit=$? $(date)"
  python3 make_repeat_candidates_sp.py $sp $tag
  echo "==== $sp done $(date) ===="
}

run_sp C_glabrata_CBS138 CGLAB /data/genomes/C_glabrata_CBS138_current_chromosomes.fasta
run_sp C_dubliniensis_CD36 CDUB /data/genomes/C_dubliniensis_CD36_current_chromosomes.fasta
echo "==== ALL DONE B $(date) ===="
