#!/usr/bin/env python3
"""
Refresh the ChEBI ontology in CGD from the current EBI release.

CGD stores ChEBI in its generic Chado-style CV tables (there are no dedicated
chebi/chebi_url/chebi_alia tables like SGD has), so this loader writes into the
existing tables under the ``chebi_ontology`` CV (cv_no lookup by name):

    CV_TERM          one row per ChEBI term (term_name, cvterm_definition,
                     dbxref_id='CHEBI:NNN')
    DBXREF           source='EBI', dbxref_type='CHEBI', dbxref_id='CHEBI:NNN'
    CVTERM_DBXREF    links CV_TERM <-> DBXREF
    CVTERM_SYNONYM   ChEBI synonyms (with synonym_type)

The ChEBI accession is stored redundantly in CV_TERM.dbxref_id *and* in
DBXREF/CVTERM_DBXREF. The phenotype curation code reads CV_TERM.dbxref_id
directly (PhenotypeCurationService.get_chebi_term_name), so both must be kept
in sync for a new term to be usable in curation.

Mirrors SGD's scripts/loading/ontology/chebi.py:
- Downloads chebi.owl from EBI and filters to the 3-STAR (fully manually
  curated) subset only -- ChEBI is huge and the 2-star bulk is not curated.
- Adds new terms; updates changed names/definitions; syncs synonyms.

Deliberate differences from SGD (see scope notes):
- CV_TERM has no is_obsolete column, so this loader is *additive*: terms that
  dropped out of the release are reported but NOT deleted. Deleting is unsafe
  because phenotype annotations reference chemicals by name string
  (EXPT_PROPERTY.property_value), not by FK.
- Hierarchy (CVTERM_RELATIONSHIP) is out of scope here, exactly as in SGD's
  chebi.py, which loads terms/synonyms/urls only.
- Per-term URLs are not written; CGD renders ChEBI linkouts from a source-based
  URL template, not per-term DBXREF_URL rows.

Original SGD Perl/Python author: Shuai Weng (sweng66).
"""

import argparse
import logging
import os
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add repo root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Cv, CvTerm, CvtermDbxref, CvtermSynonym, Dbxref

load_dotenv()

logger = logging.getLogger(__name__)

# --- Constants mirroring the existing CGD ChEBI data conventions ---
CV_NAME = "chebi_ontology"
DBXREF_SOURCE = "EBI"
DBXREF_TYPE = "CHEBI"
CHEBI_OWL_URL = "https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.owl"

# Column limits from the CGD schema (chars)
MAX_TERM_NAME = 1024
MAX_SYNONYM = 1024
MAX_DEFINITION = 2900

# ChEBI emits Exact/Related/Narrow/Broad synonyms, but CGD's CODE table only
# registers these values for CVTERM_SYNONYM.synonym_type (enforced by the
# CHECKCODE trigger). Map ChEBI's types onto them, matching how the existing
# ChEBI synonyms are already stored (exact_synonym / related_synonym only).
SYNONYM_TYPE_MAP = {
    "EXACT": "exact_synonym",
    "RELATED": "related_synonym",
    "NARROW": "related_synonym",
    "BROAD": "related_synonym",
}
DEFAULT_SYNONYM_TYPE = "synonym"


def setup_logging(log_file: Path = None, verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, mode="w"))
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def _unescape(text: str) -> str:
    """Undo the XML entity escaping used in the OWL file."""
    return (
        text.replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&amp;", "&")
    )


def parse_chebi_owl(filepath: Path) -> list[dict]:
    """
    Parse a ChEBI OWL file into a list of 3-STAR term dicts.

    Ported from SGD's read_owl() (scripts/loading/ontology/__init__.py),
    reduced to the ChEBI-relevant fields. Only terms flagged as 3-STAR via
    ``<oboInOwl:inSubset ... 3_STAR>`` are returned; obsolete terms are skipped.

    Returns a list of dicts: {id, term, definition, aliases: [(name, type)]}.
    """
    data: list[dict] = []

    term = None
    term_id = None
    definition = None
    aliases: list[tuple] = []
    is_obsolete = False
    is_3_star = False

    term_start = '<owl:Class rdf:about='
    term_stop = '</owl:Class>'

    with open(filepath, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()

            # Term start: extract CHEBI id from the rdf:about URI
            if term_start in line:
                pieces = line.split(">")[0].split("/")
                candidate = pieces.pop().replace('"', "").replace("_", ":")
                # Skip anchors / non-CHEBI classes (e.g. owl:Thing references)
                if "#" in candidate or "CHEBI:" not in candidate:
                    term_id = None
                else:
                    term_id = candidate
                continue

            # Term end: emit if it is a valid, non-obsolete, 3-STAR term
            if term_stop in line:
                if term_id and term and is_3_star and not is_obsolete:
                    data.append(
                        {
                            "id": term_id,
                            "term": _unescape(term),
                            "definition": _unescape(definition) if definition else None,
                            "aliases": aliases,
                        }
                    )
                # reset state
                term = term_id = definition = None
                aliases = []
                is_obsolete = False
                is_3_star = False
                continue

            if term_id is None:
                continue

            # Label -> term name
            if "<rdfs:label" in line:
                try:
                    term = line.split(">")[1].split("<")[0].strip()
                except IndexError:
                    term = None

            # Definition (IAO_0000115)
            elif "<obo:IAO_0000115" in line or "<obo1:IAO_0000115" in line:
                try:
                    definition = line.split(">")[1].split("<")[0]
                except IndexError:
                    definition = None

            # Synonyms (Exact / Broad / Narrow / Related)
            elif re.search(r"has(Exact|Broad|Narrow|Related)Synonym", line):
                try:
                    alias_name = line.split(">")[1].split("<")[0]
                except IndexError:
                    alias_name = ""
                if alias_name:
                    raw_type = line.split("Synonym")[0].split("has")[-1].upper()
                    syn_type = SYNONYM_TYPE_MAP.get(raw_type, DEFAULT_SYNONYM_TYPE)
                    aliases.append((_unescape(alias_name), syn_type))

            # Obsoletion markers
            elif "<owl:deprecated" in line or "reason_for_obsolescence" in line:
                is_obsolete = True

            # 3-STAR subset flag
            elif "<oboInOwl:inSubset rdf:resource=" in line and (
                "#3_STAR" in line or "/3_STAR" in line
            ):
                is_3_star = True

    logger.info(f"Parsed {len(data)} 3-STAR terms from {filepath}")
    return data


def get_cv_no(session) -> int:
    """Look up the cv_no for the chebi_ontology CV."""
    cv = session.query(Cv).filter(Cv.cv_name == CV_NAME).first()
    if not cv:
        raise RuntimeError(
            f"CV '{CV_NAME}' not found; expected it to already exist (cv_no=3)."
        )
    return cv.cv_no


def get_or_create_dbxref(session, chebi_id: str, created_by: str) -> int:
    """Get existing dbxref_no for a CHEBI id, or create the DBXREF row."""
    existing = (
        session.query(Dbxref)
        .filter(
            Dbxref.source == DBXREF_SOURCE,
            Dbxref.dbxref_type == DBXREF_TYPE,
            Dbxref.dbxref_id == chebi_id,
        )
        .first()
    )
    if existing:
        return existing.dbxref_no

    dbxref = Dbxref(
        source=DBXREF_SOURCE,
        dbxref_type=DBXREF_TYPE,
        dbxref_id=chebi_id,
        created_by=created_by,
    )
    session.add(dbxref)
    session.flush()  # trigger populates dbxref_no
    return dbxref.dbxref_no


def link_cvterm_dbxref(session, cv_term_no: int, dbxref_no: int) -> None:
    """Ensure a CVTERM_DBXREF link exists."""
    existing = (
        session.query(CvtermDbxref)
        .filter(
            CvtermDbxref.cv_term_no == cv_term_no,
            CvtermDbxref.dbxref_no == dbxref_no,
        )
        .first()
    )
    if existing:
        return
    session.add(CvtermDbxref(cv_term_no=cv_term_no, dbxref_no=dbxref_no))
    session.flush()


def sync_synonyms(session, cv_term_no, new_aliases, created_by, counts, added_syn) -> None:
    """
    Add synonyms from the release that the term does not already have.

    Add-only and keyed by synonym *name* (ignoring type), because:
    - The same synonym text can arrive from ChEBI under a different type than
      it is already stored (e.g. an exact synonym reclassified as related).
      CVTERM_SYNONYM_UK is (term_synonym, cv_term_no, synonym_type), but keeping
      the existing row and not adding a second type avoids needless churn.
    - ``added_syn`` tracks (cv_term_no, name) inserted earlier in this run so a
      term reached twice (e.g. via an ID-change remap) cannot re-insert -- the
      session uses autoflush=False, so pending rows are not visible to the
      dedup query below.
    Existing synonyms are never deleted (refresh is additive, like the terms).
    """
    existing_names = {
        s.term_synonym
        for s in session.query(CvtermSynonym.term_synonym).filter(
            CvtermSynonym.cv_term_no == cv_term_no
        )
    }

    seen = set()
    for name, syn_type in new_aliases:
        if len(name) > MAX_SYNONYM or name in seen:
            continue
        seen.add(name)
        if name in existing_names or (cv_term_no, name) in added_syn:
            continue
        session.add(
            CvtermSynonym(
                cv_term_no=cv_term_no,
                term_synonym=name,
                synonym_type=syn_type,
                created_by=created_by,
            )
        )
        added_syn.add((cv_term_no, name))
        counts["synonyms_added"] += 1


def load_chebi(
    owl_file: Path,
    created_by: str,
    dry_run: bool = False,
    commit_every: int = 200,
    limit: int = 0,
) -> None:
    """Main entry point: parse the OWL and upsert into the CGD CV tables."""
    session = SessionLocal()
    counts = {
        "added": 0,
        "updated": 0,
        "synonyms_added": 0,
    }
    missing_from_release = []
    added_syn = set()  # (cv_term_no, term_synonym) inserted this run

    try:
        cv_no = get_cv_no(session)
        logger.info(f"Loading into CV '{CV_NAME}' (cv_no={cv_no})")

        # Index existing ChEBI terms both by CHEBI id (cv_term.dbxref_id) and by
        # name. term_name+cv_no is uniquely constrained (CV_TERM_UK), so a term
        # whose CHEBI id changed between releases must be re-mapped in place
        # rather than inserted, or it collides on the name.
        existing = list(
            session.query(CvTerm).filter(
                CvTerm.cv_no == cv_no, CvTerm.dbxref_id.isnot(None)
            )
        )
        by_id = {t.dbxref_id: t for t in existing}
        by_name = {t.term_name: t for t in existing}
        original_ids = {t.dbxref_id: t.term_name for t in existing}
        logger.info(f"{len(existing)} ChEBI terms currently in the database")

        data = parse_chebi_owl(owl_file)
        if limit:
            data = data[:limit]
            logger.info(f"--limit set: processing only the first {limit} terms")
        seen_ids = set()
        remapped_old_ids = set()

        for i, x in enumerate(data, start=1):
            chebi_id = x["id"]
            term_name = x["term"]
            definition = x["definition"]

            if len(term_name) > MAX_TERM_NAME:
                logger.warning(f"Skipping {chebi_id}: name exceeds {MAX_TERM_NAME} chars")
                continue
            if definition and len(definition) > MAX_DEFINITION:
                definition = definition[:MAX_DEFINITION]

            seen_ids.add(chebi_id)
            cv_term = by_id.get(chebi_id)

            # ID change: a term with this name exists under a different CHEBI id
            if cv_term is None and term_name in by_name:
                cv_term = by_name[term_name]
                old_id = cv_term.dbxref_id
                logger.info(f"ID change '{term_name}': {old_id} -> {chebi_id}")
                cv_term.dbxref_id = chebi_id
                by_id[chebi_id] = cv_term
                remapped_old_ids.add(old_id)
                counts["updated"] += 1

            if cv_term is not None:
                # Update changed name (guard against colliding with another term)
                if cv_term.term_name != term_name:
                    if term_name in by_name and by_name[term_name] is not cv_term:
                        logger.warning(
                            f"Skip rename of {cv_term.dbxref_id} "
                            f"'{cv_term.term_name}' -> '{term_name}': name in use"
                        )
                    else:
                        by_name.pop(cv_term.term_name, None)
                        logger.info(
                            f"{chebi_id} name: '{cv_term.term_name}' -> '{term_name}'"
                        )
                        cv_term.term_name = term_name
                        by_name[term_name] = cv_term
                        counts["updated"] += 1
                if (cv_term.cvterm_definition or None) != (definition or None):
                    cv_term.cvterm_definition = definition
                cv_term_no = cv_term.cv_term_no
            else:
                # Genuinely new term (guard against a name already present)
                if term_name in by_name:
                    logger.warning(
                        f"Skip new {chebi_id}: name '{term_name}' already exists"
                    )
                    continue
                cv_term = CvTerm(
                    cv_no=cv_no,
                    term_name=term_name,
                    dbxref_id=chebi_id,
                    cvterm_definition=definition,
                    created_by=created_by,
                )
                session.add(cv_term)
                session.flush()  # trigger populates cv_term_no
                cv_term_no = cv_term.cv_term_no
                by_id[chebi_id] = cv_term
                by_name[term_name] = cv_term
                counts["added"] += 1
                logger.info(f"NEW term {chebi_id}: {term_name}")

            # Keep DBXREF + CVTERM_DBXREF in sync with the denormalized id
            dbxref_no = get_or_create_dbxref(session, chebi_id, created_by)
            link_cvterm_dbxref(session, cv_term_no, dbxref_no)

            # Sync synonyms
            sync_synonyms(
                session, cv_term_no, x["aliases"], created_by, counts, added_syn
            )

            if i % commit_every == 0:
                # flush (not rollback) in dry-run so the in-memory index stays
                # valid while still exercising every trigger/constraint
                session.flush()
                if not dry_run:
                    session.commit()
                logger.info(f"  ...processed {i}/{len(data)} terms")

        # Report (do NOT delete) terms no longer in the current release.
        # Only meaningful for a full run -- a --limit run hasn't seen them all.
        if not limit:
            for chebi_id, name in original_ids.items():
                if chebi_id not in seen_ids and chebi_id not in remapped_old_ids:
                    missing_from_release.append((chebi_id, name))

        if dry_run:
            session.rollback()
            logger.info("DRY RUN - rolled back all changes")
        else:
            session.commit()

        _write_summary(counts, missing_from_release)

    except Exception:
        session.rollback()
        logger.exception("ChEBI load failed; rolled back")
        raise
    finally:
        session.close()


def _write_summary(counts: dict, missing: list) -> None:
    """Log a human-readable summary, mirroring SGD's report."""
    logger.info("=" * 60)
    logger.info("ChEBI load summary:")
    logger.info(f"  Terms added:       {counts['added']}")
    logger.info(f"  Terms updated:     {counts['updated']}")
    logger.info(f"  Synonyms added:    {counts['synonyms_added']}")
    logger.info(
        f"  In DB but not in current 3-STAR release (kept, not deleted): {len(missing)}"
    )
    for chebi_id, name in missing[:50]:
        logger.info(f"      {chebi_id}  {name}")
    if len(missing) > 50:
        logger.info(f"      ... and {len(missing) - 50} more")
    logger.info("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh the ChEBI ontology in CGD from the EBI release (3-STAR subset).",
    )
    parser.add_argument(
        "--owl-file",
        type=Path,
        help="Path to a local chebi.owl file. If omitted with --download, "
        "the current release is fetched from EBI.",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help=f"Download chebi.owl from {CHEBI_OWL_URL} before loading.",
    )
    parser.add_argument(
        "--created-by",
        default=os.environ.get("DEFAULT_USER", "CGDADMIN")[:12],
        help="Value for created_by columns. Must be a valid userid in the "
        "DBUSER table (enforced by the CHECKUSER trigger), e.g. CGDADMIN, "
        "SHUAI, MULTI.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and simulate the load without committing.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only the first N parsed terms (for quick validation). "
        "0 = all. Disables the missing-from-release report.",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("logs/load_chebi.log"),
        help="Log file path.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.log_file, args.verbose)
    logger.info(f"Started: {datetime.now()}")

    owl_file = args.owl_file
    if args.download or owl_file is None:
        owl_file = owl_file or Path("chebi.owl")
        logger.info(f"Downloading {CHEBI_OWL_URL} -> {owl_file}")
        urllib.request.urlretrieve(CHEBI_OWL_URL, owl_file)

    if not owl_file.exists():
        logger.error(f"OWL file not found: {owl_file}")
        sys.exit(1)

    load_chebi(
        owl_file=owl_file,
        created_by=args.created_by[:12],
        dry_run=args.dry_run,
        limit=args.limit,
    )
    logger.info(f"Done: {datetime.now()}")


if __name__ == "__main__":
    main()
