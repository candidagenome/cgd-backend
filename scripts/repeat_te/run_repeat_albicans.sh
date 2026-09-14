#!/usr/bin/env bash
set -u
cd /data/HTS/repeat_te
DK="docker run --rm --user $(id -u):$(id -g) -v /data:/data dfam/tetools:latest"
MM="/data/HTS/trna_tropicalis/bin/micromamba"
export MAMBA_ROOT_PREFIX=/data/HTS/micromamba
sp=C_albicans_SC5314; tag=CALB
genome=/data/genomes/C_albicans_SC5314_A22_current_chromosomes.fasta
echo "==== $sp RepeatModeler start $(date) ===="
mkdir -p rmodel_$sp
$DK bash -c "cd /data/HTS/repeat_te/rmodel_$sp && BuildDatabase -name $tag $genome" > rmodel_$sp/builddb.log 2>&1
echo "-- BuildDatabase exit=$?"
$DK bash -c "cd /data/HTS/repeat_te/rmodel_$sp && RepeatModeler -database $tag -threads 2 -LTRStruct" > rmodel_$sp/repeatmodeler.log 2>&1
echo "-- RepeatModeler exit=$? $(date) families=$(grep -c \">\" rmodel_$sp/$tag-families.fa 2>/dev/null)"
[ -s rmodel_$sp/$tag-families.fa ] || { echo "!! no families"; exit 1; }
$MM run -n tesorter TEsorter rmodel_$sp/$tag-families.fa -db rexdb -p 2 -pre ${tag}_tesorter > ${tag}_tesorter.log 2>&1
echo "-- TEsorter exit=$?"
mkdir -p rm_denovo_$sp
$DK RepeatMasker -lib /data/HTS/repeat_te/rmodel_$sp/$tag-families.fa -pa 1 -xsmall -no_is -a -dir /data/HTS/repeat_te/rm_denovo_$sp $genome > rm_denovo_$sp/run.log 2>&1
echo "-- RepeatMasker exit=$? $(date)"
echo "==== ALBICANS DONE $(date) ===="
