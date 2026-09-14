#!/usr/bin/env python3
"""Create make_repeat_candidates_sp.py (species-parameterized) from the tropicalis parser."""
src = open("/data/HTS/repeat_te/make_repeat_candidates.py").read()
src = src.replace(
    "import glob\nimport re\n",
    "import glob\nimport re\nimport sys\n\nSP = sys.argv[1]      # e.g. C_auris_B8441\nTAG = sys.argv[2]     # e.g. CAURIS\n")
src = src.replace('OUT = glob.glob("/data/HTS/repeat_te/rm_denovo_C_tropicalis/*.out")[0]',
                  'OUT = glob.glob(f"/data/HTS/repeat_te/rm_denovo_{SP}/*.out")[0]')
src = src.replace('FAM = "/data/HTS/repeat_te/rmodel_C_tropicalis/CTROP-families.fa"',
                  'FAM = f"/data/HTS/repeat_te/rmodel_{SP}/{TAG}-families.fa"')
src = src.replace('TES = "/data/HTS/repeat_te/CTROP_tesorter.cls.tsv"',
                  'TES = f"/data/HTS/repeat_te/{TAG}_tesorter.cls.tsv"')
src = src.replace('TSV = "/data/HTS/repeat_te/C_tropicalis_repeat_candidates.tsv"',
                  'TSV = f"/data/HTS/repeat_te/{SP}_repeat_candidates.tsv"')
src = src.replace('FSUM = "/data/HTS/repeat_te/C_tropicalis_repeat_family_summary.tsv"',
                  'FSUM = f"/data/HTS/repeat_te/{SP}_repeat_family_summary.tsv"')
assert "sys.argv" in src and "{SP}" in src and "{TAG}" in src
open("/data/HTS/repeat_te/make_repeat_candidates_sp.py", "w").write(src)
print("parser parameterized")
