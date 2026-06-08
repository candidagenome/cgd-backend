#!/usr/bin/env python3
"""
Resync ALL Oracle sequences with the max value of the PK column they feed.

Many MULTI tables auto-assign their primary key through a BEFORE INSERT
trigger of the form:

    IF (:new.<pk_col> IS NULL) THEN
        SELECT <seq>.NEXTVAL INTO :new.<pk_col> FROM DUAL;
    END IF;

When rows are bulk-loaded with explicit PK values that run ahead of the
sequence, the sequence later hands out values that already exist, raising
ORA-00001 (unique constraint <PK> violated) on the next insert.

This script DISCOVERS every (sequence, table, pk_column) triple by parsing the
BEFORE INSERT triggers in the connected schema, then for each one that is
behind its table's MAX(pk) it advances the sequence to MAX(pk) + MARGIN using
ALTER SEQUENCE ... RESTART START WITH (Oracle 12.2+).

Usage:
    python scripts/resync_all_sequences.py              # dry-run (default)
    python scripts/resync_all_sequences.py --apply       # actually resync
    python scripts/resync_all_sequences.py --margin 100  # safety margin (default 50)

Environment Variables:
    DATABASE_URL: Database connection URL (loaded from .env if present)
"""

import argparse
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Load environment variables BEFORE importing cgd modules
load_dotenv(PROJECT_ROOT / ".env")

sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402

# Matches "<seq>.NEXTVAL INTO :new.<col>" inside a trigger body.
NEXTVAL_RE = re.compile(
    r"(\w+)\.nextval\s+into\s+:new\.(\w+)",
    re.IGNORECASE,
)


def discover_sequence_assignments(conn):
    """
    Parse every BEFORE INSERT trigger in the connected schema and return a list
    of (sequence_name, table_name, pk_column) triples, all upper-cased.
    """
    triggers = conn.execute(text(
        "SELECT trigger_name, table_name FROM user_triggers "
        "WHERE triggering_event LIKE '%INSERT%' "
        "AND trigger_type LIKE 'BEFORE%'"
    )).fetchall()

    assignments = []
    seen = set()
    for trigger_name, table_name in triggers:
        rows = conn.execute(text(
            "SELECT text FROM user_source WHERE type = 'TRIGGER' "
            "AND name = :name ORDER BY line"
        ), {"name": trigger_name}).fetchall()
        body = "".join((r[0] or "") for r in rows)

        for seq_name, pk_col in NEXTVAL_RE.findall(body):
            key = (seq_name.upper(), table_name.upper(), pk_col.upper())
            if key not in seen:
                seen.add(key)
                assignments.append(key)

    assignments.sort()
    return assignments


def check_sequence(conn, seq_name, table_name, pk_col):
    """
    Return a status dict for one sequence:
      {seq, table, col, max_value, last_number, behind, target, error}
    'last_number' is the next value the sequence will hand out (the on-disk
    high-water mark; for cached sequences it is >= the real next value, so
    comparing it against MAX(pk) is a conservative 'behind' test).
    """
    info = {
        "seq": seq_name, "table": table_name, "col": pk_col,
        "max_value": None, "last_number": None, "behind": False,
        "target": None, "error": None,
    }

    seq_row = conn.execute(text(
        "SELECT last_number FROM user_sequences WHERE sequence_name = :s"
    ), {"s": seq_name}).fetchone()
    if seq_row is None:
        info["error"] = "sequence not found in this schema (skipped)"
        return info
    info["last_number"] = int(seq_row[0])

    max_value = conn.execute(
        text(f"SELECT MAX({pk_col}) FROM {table_name}")
    ).scalar()
    info["max_value"] = int(max_value) if max_value is not None else 0

    # Behind when the next value to be handed out is not strictly above the max.
    info["behind"] = info["last_number"] <= info["max_value"]
    if info["behind"]:
        info["target"] = info["max_value"] + 1  # margin added by caller
    return info


def main():
    parser = argparse.ArgumentParser(
        description="Resync all Oracle sequences to MAX(pk)."
    )
    parser.add_argument("--apply", action="store_true",
                        help="Actually resync (default is dry-run).")
    parser.add_argument("--margin", type=int, default=50,
                        help="Safety margin added above MAX(pk) (default 50).")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"Oracle Sequence Resync  [{mode}]  margin=+{args.margin}")
    print("=" * 78)

    behind, ok, errors, fixed, failed = [], [], [], [], []

    with SessionLocal() as session:
        conn = session.connection()

        assignments = discover_sequence_assignments(conn)
        print(f"Discovered {len(assignments)} sequence-backed PK column(s).\n")

        for seq_name, table_name, pk_col in assignments:
            try:
                info = check_sequence(conn, seq_name, table_name, pk_col)
            except Exception as e:  # noqa: BLE001
                info = {"seq": seq_name, "table": table_name,
                        "col": pk_col, "error": str(e)}

            if info.get("error"):
                errors.append(info)
                print(f"  SKIP  {seq_name:<28} {info['error']}")
                continue
            if not info["behind"]:
                ok.append(info)
                continue

            target = info["max_value"] + args.margin
            print(f"  BEHIND {seq_name:<28} "
                  f"next={info['last_number']} <= max={info['max_value']} "
                  f"-> restart at {target}")
            behind.append(info)

            if args.apply:
                try:
                    conn.execute(text(
                        f"ALTER SEQUENCE {seq_name} RESTART START WITH {target}"
                    ))
                    new_next = conn.execute(
                        text(f"SELECT {seq_name}.NEXTVAL FROM DUAL")
                    ).scalar()
                    if int(new_next) > info["max_value"]:
                        print(f"         OK -> NEXTVAL now {new_next}")
                        fixed.append(info)
                    else:
                        print(f"         WARNING -> NEXTVAL {new_next} "
                              f"still <= max {info['max_value']}")
                        failed.append(info)
                except Exception as e:  # noqa: BLE001
                    print(f"         ERROR -> {e}")
                    failed.append(info)

        if args.apply:
            session.commit()

    print("\n" + "=" * 78)
    print(f"Total discovered : {len(assignments)}")
    print(f"Already OK       : {len(ok)}")
    print(f"Behind           : {len(behind)}")
    print(f"Skipped (errors) : {len(errors)}")
    if args.apply:
        print(f"Resynced         : {len(fixed)}")
        print(f"Failed           : {len(failed)}")
        print("\nChanges committed." if not failed else
              "\nChanges committed (some failures — review above).")
    else:
        print("\nDRY-RUN: no changes made. Re-run with --apply to resync.")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
