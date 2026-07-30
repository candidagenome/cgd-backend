#!/usr/bin/env python3
"""Per-species feature-type inventory ("gap report").

Read-only. Counts FEATURE rows grouped by organism and feature_type so we can
see which species are missing tRNA / rRNA / snoRNA / snRNA / ncRNA / repeat /
transposable-element annotations.

Usage:
    python scripts/reports/feature_type_gap_report.py
"""

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from sqlalchemy import text  # noqa: E402

from cgd.db.engine import SessionLocal  # noqa: E402
from cgd.core.settings import settings  # noqa: E402

SCHEMA = settings.db_schema if hasattr(settings, "db_schema") else "MULTI"

# Feature types the PI cares about (non-coding RNA + repeats/TEs), in the order
# we want them displayed. Anything else found in the DB is still reported.
RNA_TYPES = ["tRNA", "rRNA", "snoRNA", "snRNA", "ncRNA"]
REPEAT_TYPES = [
    "repeat_region",
    "long_terminal_repeat",
    "retrotransposon",
    "transposable_element",
    "transposable_element_gene",
    "centromere",
]
CODING_TYPES = ["ORF", "pseudogene", "blocked_reading_frame"]


def main() -> None:
    with SessionLocal() as db:
        rows = db.execute(
            text(
                f"""
                SELECT o.organism_abbrev,
                       o.organism_name,
                       o.organism_order,
                       f.feature_type,
                       COUNT(*) AS cnt
                FROM {SCHEMA}.feature f
                JOIN {SCHEMA}.organism o ON f.organism_no = o.organism_no
                GROUP BY o.organism_abbrev, o.organism_name,
                         o.organism_order, f.feature_type
                ORDER BY o.organism_order, f.feature_type
                """
            )
        ).fetchall()

    # organism_abbrev -> {feature_type: count}
    per_org: dict[str, dict[str, int]] = defaultdict(dict)
    org_names: dict[str, str] = {}
    org_order: dict[str, int] = {}
    all_types: set[str] = set()
    for abbrev, name, order, ftype, cnt in rows:
        per_org[abbrev][ftype] = cnt
        org_names[abbrev] = name
        org_order[abbrev] = order or 0
        all_types.add(ftype)

    orgs = sorted(per_org.keys(), key=lambda a: (org_order[a], a))

    def block(title: str, types: list[str]) -> None:
        print(f"\n=== {title} ===")
        header = ["feature_type"] + orgs
        widths = [max(len(header[0]), max((len(t) for t in types), default=0))]
        widths += [max(len(a), 6) for a in orgs]
        print("  ".join(h.ljust(w) for h, w in zip(header, widths)))
        for t in types:
            cells = [t] + [str(per_org[a].get(t, 0)) for a in orgs]
            print("  ".join(c.ljust(w) for c, w in zip(cells, widths)))

    print("Per-species feature-type inventory")
    print(f"Schema: {SCHEMA}")
    print(f"Species (by organism_order): {', '.join(orgs)}")
    for a in orgs:
        print(f"  {a}: {org_names[a]}")

    block("Non-coding RNA genes", RNA_TYPES)
    block("Repeats / transposable elements", REPEAT_TYPES)
    block("Protein-coding (context)", CODING_TYPES)

    other = sorted(all_types - set(RNA_TYPES) - set(REPEAT_TYPES) - set(CODING_TYPES))
    if other:
        block("All other feature types present", other)


if __name__ == "__main__":
    main()
