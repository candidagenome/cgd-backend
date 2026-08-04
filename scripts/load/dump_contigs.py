#!/usr/bin/env python3
"""Dump CGD-coordinate genomic contigs/chromosomes for a species to FASTA
(headers = feature_name), so cmscan hit coords == CGD coords."""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path("scripts/load/load_rrnas.py").resolve().parents[2]))
from sqlalchemy import text
from cgd.db.engine import SessionLocal
S = os.getenv("DB_SCHEMA", "MULTI")

organism_name = sys.argv[1]
out = Path(sys.argv[2])

with SessionLocal() as s:
    org = s.execute(text(f"SELECT organism_no FROM {S}.organism WHERE organism_name=:n"),
                    {"n": organism_name}).scalar()
    rows = s.execute(text(f"""
        SELECT f.feature_name, f.feature_no
        FROM {S}.feature f
        WHERE f.organism_no=:o AND f.feature_type IN ('contig','chromosome')
        ORDER BY f.feature_name"""), {"o": org}).fetchall()
    n = 0
    with open(out, "w") as fh:
        for name, fno in rows:
            seq = s.execute(text(f"""SELECT residues FROM {S}.seq WHERE feature_no=:f
                AND seq_type='genomic' AND is_seq_current='Y' AND ROWNUM=1"""), {"f": fno}).scalar()
            if seq is None:
                continue
            seq = str(seq)
            fh.write(f">{name}\n")
            for i in range(0, len(seq), 70):
                fh.write(seq[i:i + 70] + "\n")
            n += 1
    print(f"{organism_name}: wrote {n} contigs -> {out}")
