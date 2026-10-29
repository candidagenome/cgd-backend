#!/usr/bin/env python3
"""
Monthly ortholog name-transfer candidate lists for curator review.

Read-only: runs one of the two name-transfer reports, saves a dated TSV with
an empty DECISION column, marks the rows that were not in the previous run,
prints a summary (posted to Slack by slack-cron.sh) and emails the summary
with the TSV attached to the curators.

Jobs:
    orthologs   qc_ortholog_name_transfers.py propose, TRANSFER rows only:
                genes whose ortholog groups (CGOB) hold at least two orthologs
                sharing one name and none carrying a different name.
    mbh         mutual_best_hit_name_candidates.py: genes CGOB left out of any
                group that pass the same rule once their mutual BLAST best
                hits are counted.

Nothing is written to the database. Rows curators approve are loaded by hand
with scripts/ortholog_loading/apply_name_transfers.py (dry-run first).

Usage:
    python name_transfer_candidates.py orthologs [--no-email]
    python name_transfer_candidates.py mbh [--no-email]

Environment Variables:
    NAME_TRANSFER_DIR: Output directory (default: /data/name_transfers)
    NAME_TRANSFER_EMAIL: Comma-separated recipients; no email if unset
    FROM_EMAIL, SMTP_HOST, SMTP_PORT: see cgd/utils/notifications.py
"""

import argparse
import csv
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

from cgd.utils.notifications import send_email  # noqa: E402

OUT_DIR = Path(os.getenv("NAME_TRANSFER_DIR", "/data/name_transfers"))

JOBS = {
    "orthologs": {
        "title": "ortholog groups",
        "prefix": "name_transfer_candidates",
        "cmd": ["scripts/reports/qc_ortholog_name_transfers.py", "propose"],
        "columns": ["feature_name", "organism", "proposed_name", "warn_codes",
                    "support_count", "support_detail", "homology_groups"],
        "rule": ("Each gene below has no standard name, and at least two of "
                 "its orthologs in the CGD ortholog groups (CGOB) share the "
                 "proposed name, with no ortholog carrying a different name."),
    },
    "mbh": {
        "title": "BLAST best hits",
        "prefix": "mbh_name_candidates",
        "cmd": ["scripts/reports/mutual_best_hit_name_candidates.py"],
        "columns": ["feature_name", "organism", "proposed_name", "warn_codes",
                    "baseline_fail_codes", "support_count", "support_detail",
                    "mutual_best_hits", "cgob_status"],
        "rule": ("Each gene below has no standard name and failed the regular "
                 "ortholog-group check, usually because CGOB left it out of "
                 "any group. It passes the same rule once its reciprocal BLAST "
                 "best hits are counted as orthologs. There is no gene-order "
                 "evidence behind these candidates."),
    },
}


def run_report(job):
    """Run the report script; return its rows as dicts and its stderr summary."""
    cmd = [sys.executable, *job["cmd"]]
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"ERROR: {' '.join(job['cmd'])} failed:\n{proc.stderr[-3000:]}")
    rows = list(csv.DictReader(proc.stdout.splitlines(), delimiter="\t"))
    if "status" in (rows[0] if rows else {}):
        rows = [r for r in rows if r["status"] == "TRANSFER"]
    return rows, proc.stderr


def previous_keys(prefix, today_file):
    """(feature_name, proposed_name) pairs of the most recent earlier run."""
    earlier = sorted(p for p in OUT_DIR.glob(f"{prefix}_*.tsv") if p.name < today_file.name)
    if not earlier:
        return None, None
    with open(earlier[-1], newline="") as fh:
        keys = {(r["feature_name"], r["proposed_name"])
                for r in csv.DictReader(fh, delimiter="\t")}
    return earlier[-1], keys


def write_tsv(path, columns, rows):
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerow(["new_this_run", *columns, "DECISION", "NOTES"])
        for r in rows:
            writer.writerow([r["new_this_run"], *(r.get(c, "") for c in columns), "", ""])


def candidate_line(r):
    warn = f"  [{r['warn_codes']}]" if r.get("warn_codes") else ""
    return (f"  {r['feature_name']:<14} {r['proposed_name']:<10} "
            f"{r['organism']}{warn}\n      support: {r['support_detail']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("job", choices=JOBS)
    parser.add_argument("--no-email", action="store_true",
                        help="write the TSV and print the summary only")
    args = parser.parse_args()
    job = JOBS[args.job]

    today = date.today().isoformat()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUT_DIR / f"{job['prefix']}_{today}.tsv"

    rows, report_log = run_report(job)
    prev_file, prev = previous_keys(job["prefix"], out_file)
    for r in rows:
        r["new_this_run"] = "yes" if prev is None or (r["feature_name"], r["proposed_name"]) not in prev else ""
    rows.sort(key=lambda r: (r["new_this_run"] != "yes", r["proposed_name"], r["organism"]))
    write_tsv(out_file, job["columns"], rows)

    new = [r for r in rows if r["new_this_run"]]
    since = f"since {prev_file.name}" if prev_file else "(first run)"
    headline = f"{len(rows)} candidates, {len(new)} new {since}"

    print(f"CGD name-transfer candidates: {job['title']}")
    print(f"Generated: {today}")
    print("=" * 40)
    print(report_log.strip())
    print(headline)
    print(f"TSV: {out_file}")

    if not rows:
        print("No candidates; no email sent.")
        return
    recipients = [a.strip() for a in os.getenv("NAME_TRANSFER_EMAIL", "").split(",") if a.strip()]
    if args.no_email or not recipients:
        print("Email skipped (--no-email or NAME_TRANSFER_EMAIL unset).")
        return

    body = "\n".join([
        f"Monthly CGD gene name-transfer candidates ({job['title']}): {headline}.",
        "",
        job["rule"],
        "",
        "Nothing has been applied. To review, fill the DECISION column of the",
        "attached TSV with APPROVE or REJECT (NOTES optional) and send it back;",
        "approved names are loaded with apply_name_transfers.py, which also cites",
        "the CGD name-transfer reference. Rows marked new_this_run=yes were not in",
        "the previous list; the others are still waiting for a decision.",
        "",
        f"New candidates ({len(new)}):" if new else "No new candidates this run.",
        *(candidate_line(r) for r in new),
        "",
        f"Server copy: {out_file}",
    ])
    subject = (f"CGD name-transfer candidates ({job['title']}), {today}: "
               f"{len(rows)} candidates, {len(new)} new")
    if not send_email(recipients, subject, body, attachments=[out_file]):
        sys.exit("ERROR: email to curators failed")
    print(f"Emailed: {', '.join(recipients)}")


if __name__ == "__main__":
    main()
