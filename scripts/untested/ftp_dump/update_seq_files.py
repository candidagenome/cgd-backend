#!/usr/bin/env python3
"""
Update sequence files coordinator script.

This script calls the programs that update the various sequence data files
(FTP site, BLAST dataset, etc.) after a sequence or annotation change.

Based on updateSeqFiles.pl.

Usage:
    python update_seq_files.py
    python update_seq_files.py --debug
    python update_seq_files.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    FTP_DIR: FTP directory for output files
    PROJECT_ACRONYM: Project acronym (e.g., CGD, SGD)
    CURATOR_EMAIL: Email address for notifications

Output:
    Calls various scripts to update:
    - NOT (intergenic) file
    - Feature FASTA files (ORF, RNA, Other)
    - NCBI genome source files (sequin tables)
    - Sends email report when complete
"""

import argparse
import logging
import os
import smtplib
import subprocess
import sys
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Load environment variables
load_dotenv()

# Configuration from environment
FTP_DIR = Path(os.getenv("FTP_DIR", "/var/ftp/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "curator@example.org")
BIN_DIR = Path(__file__).resolve().parent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def run_script(script_path: Path, *args) -> tuple[bool, str]:
    """
    Run a Python script and capture output.

    Returns tuple of (success, output).
    """
    cmd = [sys.executable, str(script_path)] + list(args)
    logger.info(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
        )

        output = result.stdout + result.stderr

        if result.returncode != 0:
            logger.error(f"Script failed with return code {result.returncode}")
            logger.error(output)
            return False, output

        logger.info(f"Script completed successfully")
        return True, output

    except subprocess.TimeoutExpired:
        logger.error("Script timed out after 1 hour")
        return False, "Timeout"
    except Exception as e:
        logger.error(f"Error running script: {e}")
        return False, str(e)


def send_email_report(report: str) -> None:
    """Send email report to curators."""
    try:
        msg = MIMEText(report)
        msg["Subject"] = "update_seq_files.py finished running"
        msg["From"] = f"noreply@{PROJECT_ACRONYM.lower()}.org"
        msg["To"] = CURATOR_EMAIL

        # Try to send via local SMTP
        with smtplib.SMTP("localhost") as smtp:
            smtp.send_message(msg)

        logger.info("Email report sent successfully")
    except Exception as e:
        logger.warning(f"Could not send email report: {e}")
        logger.info("Email report content:")
        logger.info(report)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update sequence files coordinator"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )
    parser.add_argument(
        "--skip-email",
        action="store_true",
        help="Skip sending email report",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    print("\nThis program updates the relevant data files after a sequence or annotation")
    print("change was made. See the curator help page for more info.\n")

    # Output directories
    ftp_dir = FTP_DIR / "data_download" / "sequence" / "genomic_sequence"
    data_dir = FTP_DIR / "data_download" / "sequence" / "NCBI_genome_source"

    report_lines = [
        "Updating sequence files is complete.\n",
    ]
    errors = []

    # 1. Update intergenic (NOT) file
    print("UPDATING FTP FILES...\n")
    print("Creating intergenic (NOT) file...")

    intergenic_script = BIN_DIR / "create_intergenic_file.py"
    if intergenic_script.exists():
        success, output = run_script(intergenic_script)
        if success:
            print("create_intergenic_file.py complete.\n")
            report_lines.append(f"1. Intergenic region sequences: {intergenic_script}")
            report_lines.append(f"\tFiles at {ftp_dir}/intergenic/\n")
        else:
            errors.append(f"create_intergenic_file.py failed: {output}")
    else:
        logger.warning(f"Script not found: {intergenic_script}")

    # 2. Update feature files (ORF, RNA, Other)
    print("Updating feature FASTA files...")

    fasta_script = BIN_DIR / "recreate_fasta_files.py"
    if fasta_script.exists():
        for feature_type in ["ORF", "RNA", "Other"]:
            print(f"Processing {feature_type}...")
            success, output = run_script(fasta_script, feature_type)
            if success:
                print(f"recreate_fasta_files.py {feature_type} complete.\n")
            else:
                errors.append(f"recreate_fasta_files.py {feature_type} failed: {output}")

        report_lines.append(f"2. Feature sequences - ORF, RNA, Other: {fasta_script}")
        report_lines.append(f"\tFiles at {ftp_dir}/orf_dna/")
        report_lines.append(f"\tFiles at {ftp_dir}/orf_protein/")
        report_lines.append(f"\tFiles at {ftp_dir}/rna/")
        report_lines.append(f"\tFiles at {ftp_dir}/other_features/\n")
    else:
        logger.warning(f"Script not found: {fasta_script}")

    # 3. Update NCBI genome source files (sequin)
    print("UPDATING NCBI_GENOME_SOURCE FILES...\n")

    sequin_script = BIN_DIR / "sequin.py"
    if sequin_script.exists():
        success, output = run_script(sequin_script)
        if success:
            print("sequin.py complete.\n")
            report_lines.append(f"3. NCBI genome source files: {sequin_script}")
            report_lines.append(f"\tFiles at {data_dir}\n")
        else:
            errors.append(f"sequin.py failed: {output}")
    else:
        logger.warning(f"Script not found: {sequin_script}")

    # 4. Update SGD features file
    print("UPDATING SGD FEATURES FILE...\n")

    features_script = BIN_DIR / "recreate_sgd_features.py"
    if features_script.exists():
        success, output = run_script(features_script)
        if success:
            print("recreate_sgd_features.py complete.\n")
            report_lines.append(f"4. SGD features file: {features_script}")
            report_lines.append(f"\tFiles at {FTP_DIR}/data_download/chromosomal_feature/\n")
        else:
            errors.append(f"recreate_sgd_features.py failed: {output}")
    else:
        logger.warning(f"Script not found: {features_script}")

    # Finishing message
    print("FINISHED with automated updates.\n")
    print("NOTE: If you got error messages for any of the programs")
    print("that were called, you may have to re-run the program.")
    print("ALSO, YOU MUST separately run programs to update chromosome files")
    print("(update_chrom_sequence.py). See the curator help page for more info.\n")

    # Add errors to report
    if errors:
        report_lines.append("\n--- ERRORS ---")
        for error in errors:
            report_lines.append(error)

    # Send email report
    report = "\n".join(report_lines)

    if not args.skip_email:
        send_email_report(report)

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
