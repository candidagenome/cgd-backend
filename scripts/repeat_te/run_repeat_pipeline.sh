#!/usr/bin/env bash
# Repeat/TE pipeline per docs/REPEAT_TE_ANNOTATION_STRATEGY.md:
# Phase 1: RepeatMasker albicans-seed homology survey across all 6 genomes (fast).
# Phase 2: RepeatModeler2 -LTRStruct de novo pilot on C. tropicalis (slow).
set -u
cd /data/HTS/repeat_te
IMG=dfam/tetools:latest
DK="docker run --rm --user $(id -u):$(id -g) -v /data:/data -w /data/HTS/repeat_te $IMG"
LIB=/data/HTS/repeat_te/albicans_te_seeds.fasta

declare -A GENOMES=(
  [C_albicans_SC5314]=/data/genomes/C_albicans_SC5314_A22_current_chromosomes.fasta
  [C_dubliniensis_CD36]=/data/genomes/C_dubliniensis_CD36_current_chromosomes.fasta
  [C_tropicalis]=/data/genomes/C_tropicalis_current_chromosomes.fasta
  [C_parapsilosis_CDC317]=/data/genomes/C_parapsilosis_CDC317_current_chromosomes.fasta
  [C_auris_B8441]=/data/genomes/C_auris_B8441_current_chromosomes.fasta
  [C_glabrata_CBS138]=/data/genomes/C_glabrata_CBS138_current_chromosomes.fasta
)

echo "==== PHASE 1: albicans-seed RepeatMasker survey start $(date) ===="
for sp in C_albicans_SC5314 C_dubliniensis_CD36 C_tropicalis C_parapsilosis_CDC317 C_auris_B8441 C_glabrata_CBS138; do
  g=${GENOMES[$sp]}
  out=rm_survey_$sp
  mkdir -p $out
  echo "-- RepeatMasker $sp start $(date)"
  $DK RepeatMasker -lib $LIB -pa 1 -xsmall -no_is -dir $out $g > $out/run.log 2>&1
  echo "-- RepeatMasker $sp exit=$? $(date)"
done
echo "==== PHASE 1 done $(date) ===="

echo "==== PHASE 2: RepeatModeler2 tropicalis pilot start $(date) ===="
mkdir -p rmodel_C_tropicalis && cd rmodel_C_tropicalis
$DK bash -c "cd /data/HTS/repeat_te/rmodel_C_tropicalis && BuildDatabase -name CTROP /data/genomes/C_tropicalis_current_chromosomes.fasta" > builddb.log 2>&1
echo "-- BuildDatabase exit=$?"
$DK bash -c "cd /data/HTS/repeat_te/rmodel_C_tropicalis && RepeatModeler -database CTROP -threads 2 -LTRStruct" > repeatmodeler.log 2>&1
echo "-- RepeatModeler exit=$? $(date)"
ls -l CTROP-families.fa 2>/dev/null
echo "==== ALL DONE $(date) ===="
