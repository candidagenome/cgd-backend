"""
BLAST Service - handles BLAST search execution and result parsing.
"""
from __future__ import annotations

import os
import re
import subprocess
import tempfile
import uuid
import json
import logging
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import quote, urlencode
from xml.etree import ElementTree as ET
from sqlalchemy.orm import Session
from sqlalchemy import func

from cgd.core.settings import settings
from cgd.core.blast_config import (
    BLAST_ORGANISMS,
    BLAST_TASKS,
    GENETIC_CODES,
    get_all_blast_organisms,
    get_organism_for_database,
    extract_organism_tag_from_database,
    build_database_names,
    get_database_type_for_dataset,
)
from cgd.models.models import Feature, Seq, FeatRelationship
from cgd.schemas.blast_schema import (
    BlastProgram,
    BlastDatabase,
    BlastTask,
    DownloadFormat,
    DatabaseType,
    BlastSearchRequest,
    BlastSearchResult,
    BlastSearchResponse,
    BlastHit,
    BlastHsp,
    BlastDatabaseInfo,
    BlastProgramInfo,
    BlastConfigResponse,
    BlastOrganismConfig,
    BlastTaskInfo,
    GeneticCodeInfo,
    BlastDownloadResponse,
)

logger = logging.getLogger(__name__)

# Use settings for paths (with fallbacks)
BLAST_BIN_PATH = settings.blast_bin_path
BLAST_DB_PATH = settings.blast_db_path

# Available BLAST programs with metadata
BLAST_PROGRAMS: Dict[BlastProgram, BlastProgramInfo] = {
    BlastProgram.BLASTN: BlastProgramInfo(
        name="blastn",
        display_name="BLASTN",
        description="Search nucleotide database with nucleotide query",
        query_type=DatabaseType.NUCLEOTIDE,
        database_type=DatabaseType.NUCLEOTIDE,
    ),
    BlastProgram.BLASTP: BlastProgramInfo(
        name="blastp",
        display_name="BLASTP",
        description="Search protein database with protein query",
        query_type=DatabaseType.PROTEIN,
        database_type=DatabaseType.PROTEIN,
    ),
    BlastProgram.BLASTX: BlastProgramInfo(
        name="blastx",
        display_name="BLASTX",
        description="Search protein database with translated nucleotide query",
        query_type=DatabaseType.NUCLEOTIDE,
        database_type=DatabaseType.PROTEIN,
    ),
    BlastProgram.TBLASTN: BlastProgramInfo(
        name="tblastn",
        display_name="TBLASTN",
        description="Search translated nucleotide database with protein query",
        query_type=DatabaseType.PROTEIN,
        database_type=DatabaseType.NUCLEOTIDE,
    ),
    BlastProgram.TBLASTX: BlastProgramInfo(
        name="tblastx",
        display_name="TBLASTX",
        description="Search translated nucleotide database with translated nucleotide query",
        query_type=DatabaseType.NUCLEOTIDE,
        database_type=DatabaseType.NUCLEOTIDE,
    ),
}

# Available BLAST databases with metadata
BLAST_DATABASES: Dict[BlastDatabase, BlastDatabaseInfo] = {
    # C. albicans Assembly 22 (default/current)
    BlastDatabase.CA22_GENOME: BlastDatabaseInfo(
        name="default_genomic_C_albicans_SC5314_A22",
        display_name="C. albicans SC5314 A22 - Genome",
        description="C. albicans SC5314 Assembly 22 chromosomes",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida albicans SC5314",
        assembly="A22",
    ),
    BlastDatabase.CA22_CODING: BlastDatabaseInfo(
        name="default_coding_C_albicans_SC5314_A22",
        display_name="C. albicans SC5314 A22 - Coding",
        description="C. albicans SC5314 Assembly 22 coding sequences",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida albicans SC5314",
        assembly="A22",
    ),
    BlastDatabase.CA22_PROTEIN: BlastDatabaseInfo(
        name="default_protein_C_albicans_SC5314_A22",
        display_name="C. albicans SC5314 A22 - Protein",
        description="C. albicans SC5314 Assembly 22 protein sequences",
        type=DatabaseType.PROTEIN,
        organism="Candida albicans SC5314",
        assembly="A22",
    ),
    # C. albicans Assembly 21
    BlastDatabase.CA21_GENOME: BlastDatabaseInfo(
        name="genomic_C_albicans_SC5314_A21",
        display_name="C. albicans SC5314 A21 - Genome",
        description="C. albicans SC5314 Assembly 21 chromosomes",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida albicans SC5314",
        assembly="A21",
    ),
    # C. albicans Assembly 19
    BlastDatabase.CA19_GENOME: BlastDatabaseInfo(
        name="genomic_C_albicans_SC5314_A19",
        display_name="C. albicans SC5314 A19 - Genome",
        description="C. albicans SC5314 Assembly 19 chromosomes",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida albicans SC5314",
        assembly="A19",
    ),
}

# Scoring matrices for protein BLAST
SCORING_MATRICES = [
    "BLOSUM62",
    "BLOSUM45",
    "BLOSUM80",
    "PAM30",
    "PAM70",
    "PAM250",
]


def get_blast_config() -> BlastConfigResponse:
    """Get BLAST configuration options."""
    return BlastConfigResponse(
        programs=list(BLAST_PROGRAMS.values()),
        databases=list(BLAST_DATABASES.values()),
        matrices=SCORING_MATRICES,
        default_evalue=10.0,
        default_max_hits=50,
    )


def get_compatible_databases(program: BlastProgram) -> List[BlastDatabaseInfo]:
    """Get databases compatible with a BLAST program."""
    program_info = BLAST_PROGRAMS.get(program)
    if not program_info:
        return []

    return [
        db_info for db_info in BLAST_DATABASES.values()
        if db_info.type == program_info.database_type
    ]


def get_compatible_programs(database: BlastDatabase) -> List[BlastProgramInfo]:
    """Get BLAST programs compatible with a database."""
    db_info = BLAST_DATABASES.get(database)
    if not db_info:
        return []

    return [
        prog_info for prog_info in BLAST_PROGRAMS.values()
        if prog_info.database_type == db_info.type
    ]


def get_blast_organisms() -> List[BlastOrganismConfig]:
    """Get list of all organisms available for BLAST searches."""
    organisms = get_all_blast_organisms(settings.blast_clade_conf)
    result = []
    for tag, config in organisms.items():
        result.append(BlastOrganismConfig(
            tag=tag,
            full_name=config.get("full_name", tag),
            trans_table=config.get("trans_table", 1),
            seq_sets=config.get("seq_sets", ["genomic", "gene", "coding", "protein"]),
            jbrowse_data=config.get("jbrowse_data"),
            is_cgd=config.get("is_cgd", False),
        ))
    return result


def get_tasks_for_program(program: BlastProgram) -> List[BlastTaskInfo]:
    """Get available BLAST tasks for a program."""
    program_name = program.value
    tasks = BLAST_TASKS.get(program_name, [])

    result = []
    for task in tasks:
        result.append(BlastTaskInfo(
            name=task["name"],
            display_name=task["display_name"],
            description=task["description"],
            programs=[program_name],
        ))
    return result


def get_genetic_codes() -> List[GeneticCodeInfo]:
    """Get list of available genetic codes."""
    return [
        GeneticCodeInfo(
            code=code,
            name=info["name"],
            description=info["description"],
        )
        for code, info in GENETIC_CODES.items()
    ]


def _select_blast_task(
    program: BlastProgram,
    query_length: int,
    user_task: Optional[BlastTask] = None
) -> Optional[str]:
    """
    Auto-select BLAST task based on program and query length.

    Args:
        program: BLAST program
        query_length: Length of query sequence
        user_task: User-specified task (takes precedence)

    Returns:
        Task name string or None for default behavior
    """
    # If user specified a task, use it
    if user_task:
        return user_task.value

    # Get tasks for this program
    program_name = program.value
    tasks = BLAST_TASKS.get(program_name, [])

    if not tasks:
        return None

    # Find the appropriate task based on query length
    for task in tasks:
        default_length = task.get("default_for_length")
        if default_length is not None:
            if default_length == 0 and query_length < 50:
                # Short sequence task
                return task["name"]
            elif default_length > 0 and query_length >= default_length:
                # Standard task for longer sequences
                return task["name"]

    # Return the first (default) task
    return tasks[0]["name"] if tasks else None


def _generate_jbrowse_url(
    organism_tag: str,
    chromosome: str,
    start: int,
    end: int,
) -> Optional[str]:
    """
    Generate JBrowse link for a genomic hit.

    Args:
        organism_tag: Organism identifier tag
        chromosome: Chromosome/contig name
        start: Start coordinate
        end: End coordinate

    Returns:
        JBrowse URL or None if not applicable
    """
    # Get organism config
    organisms = get_all_blast_organisms(settings.blast_clade_conf)
    config = organisms.get(organism_tag)

    if not config:
        # Try partial match
        for tag, cfg in organisms.items():
            if organism_tag in tag or tag in organism_tag:
                config = cfg
                organism_tag = tag
                break

    if not config or not config.get("jbrowse_data"):
        return None

    # Calculate coordinates with flanking region
    low = min(start, end)
    high = max(start, end)
    low_flanked = max(1, low - settings.jbrowse_flank)
    high_flanked = high + settings.jbrowse_flank

    # Build JBrowse URL
    data_encoded = quote(config["jbrowse_data"], safe='')
    loc_encoded = quote(f"{chromosome}:{low_flanked}..{high_flanked}", safe='')
    tracks_encoded = quote("DNA,Transcribed Features", safe='')

    base_url = settings.jbrowse_base_url
    url = f"{base_url}?data={data_encoded}&tracklist=1&nav=1&overview=1&tracks={tracks_encoded}&loc={loc_encoded}&highlight="

    return url


def _map_to_orf19_id(
    db: Session,
    feature_name: str,
    organism_tag: str
) -> Optional[str]:
    """
    Map Assembly 22 feature to orf19 ID via FeatRelationship.

    This maps A22 features back to their Assembly 21 (orf19) identifiers.

    Args:
        db: Database session
        feature_name: Feature name to look up
        organism_tag: Organism tag (only applies to C. albicans A22)

    Returns:
        orf19 identifier or None
    """
    # Only applicable to C. albicans Assembly 22
    if "A22" not in organism_tag and "SC5314" not in organism_tag:
        return None

    try:
        # Find the feature
        feature = (
            db.query(Feature)
            .filter(
                func.upper(Feature.feature_name) == feature_name.upper()
            )
            .first()
        )

        if not feature:
            return None

        # Look for Assembly 21 Primary Allele relationship
        relationship = (
            db.query(FeatRelationship)
            .filter(
                FeatRelationship.child_feature_no == feature.feature_no,
                FeatRelationship.relationship_type == 'Assembly 21 Primary Allele',
            )
            .first()
        )

        if not relationship:
            return None

        # Get the parent feature (orf19)
        parent_feature = (
            db.query(Feature)
            .filter(Feature.feature_no == relationship.parent_feature_no)
            .first()
        )

        if parent_feature:
            return parent_feature.feature_name

    except Exception as e:
        logger.warning(f"Error mapping to orf19: {e}")

    return None


def _parse_fasta_header(sequence: str) -> Tuple[str, str]:
    """
    Parse FASTA sequence, returning (header, sequence).

    If input is not FASTA format, returns (auto-generated header, sequence).
    """
    lines = sequence.strip().split('\n')
    if lines[0].startswith('>'):
        header = lines[0][1:].strip()
        seq = ''.join(lines[1:])
    else:
        header = f"Query_{uuid.uuid4().hex[:8]}"
        seq = ''.join(lines)

    # Clean sequence - remove whitespace and non-sequence characters
    seq = re.sub(r'\s+', '', seq)
    return header, seq


def _get_sequence_for_locus(db: Session, locus: str, seq_type: str = "genomic") -> Optional[Tuple[str, str]]:
    """
    Get sequence for a locus from the database.

    Returns (header, sequence) tuple or None if not found.
    """
    query_upper = locus.strip().upper()

    # Find feature by gene_name, feature_name, or dbxref_id
    feature = (
        db.query(Feature)
        .filter(func.upper(Feature.gene_name) == query_upper)
        .first()
    )
    if not feature:
        feature = (
            db.query(Feature)
            .filter(func.upper(Feature.feature_name) == query_upper)
            .first()
        )
    if not feature:
        feature = (
            db.query(Feature)
            .filter(func.upper(Feature.dbxref_id) == query_upper)
            .first()
        )

    if not feature:
        return None

    # Get sequence
    seq_record = (
        db.query(Seq)
        .filter(
            Seq.feature_no == feature.feature_no,
            Seq.seq_type == seq_type,
            Seq.is_seq_current == "Y"
        )
        .first()
    )

    if not seq_record:
        return None

    header = feature.gene_name or feature.feature_name
    return header, seq_record.residues.upper()


def _build_blast_command(
    program: BlastProgram,
    database_name: str,
    query_file: str,
    output_file: str,
    request: BlastSearchRequest,
    query_length: int = 0,
) -> List[str]:
    """
    Build BLAST command line.

    Args:
        program: BLAST program to run
        database_name: Database name (string, not enum)
        query_file: Path to query FASTA file
        output_file: Path for output file
        request: Search request with parameters
        query_length: Length of query sequence (for auto-task selection)

    Returns:
        Command line as list of strings
    """
    program_path = os.path.join(BLAST_BIN_PATH, program.value)
    db_path = os.path.join(BLAST_DB_PATH, database_name)

    cmd = [
        program_path,
        "-query", query_file,
        "-db", db_path,
        "-out", output_file,
        "-outfmt", "5",  # XML output format
        "-evalue", str(request.evalue),
        "-max_target_seqs", str(request.max_hits),
    ]

    # Task selection (auto-select or user-specified)
    task = _select_blast_task(program, query_length, request.task)
    if task:
        cmd.extend(["-task", task])

    # Word size
    if request.word_size:
        cmd.extend(["-word_size", str(request.word_size)])

    # Gap penalties
    if request.gap_open is not None:
        cmd.extend(["-gapopen", str(request.gap_open)])
    if request.gap_extend is not None:
        cmd.extend(["-gapextend", str(request.gap_extend)])

    # Low complexity filter
    if program in [BlastProgram.BLASTN]:
        if request.low_complexity_filter:
            cmd.extend(["-dust", "yes"])
        else:
            cmd.extend(["-dust", "no"])
    elif program in [BlastProgram.BLASTP, BlastProgram.BLASTX]:
        if request.low_complexity_filter:
            cmd.extend(["-seg", "yes"])
        else:
            cmd.extend(["-seg", "no"])

    # Scoring matrix (protein BLAST)
    if request.matrix and program in [BlastProgram.BLASTP, BlastProgram.BLASTX, BlastProgram.TBLASTN]:
        cmd.extend(["-matrix", request.matrix])

    # Strand (nucleotide BLAST)
    if request.strand and program in [BlastProgram.BLASTN, BlastProgram.BLASTX, BlastProgram.TBLASTX]:
        cmd.extend(["-strand", request.strand])

    # Genetic codes for translated searches
    if request.query_gencode and program in [BlastProgram.BLASTX, BlastProgram.TBLASTX]:
        cmd.extend(["-query_gencode", str(request.query_gencode)])
    if request.db_gencode and program in [BlastProgram.TBLASTN, BlastProgram.TBLASTX]:
        cmd.extend(["-db_gencode", str(request.db_gencode)])

    # Nucleotide match/mismatch scoring (BLASTN only)
    if program == BlastProgram.BLASTN:
        if request.reward is not None:
            cmd.extend(["-reward", str(request.reward)])
        if request.penalty is not None:
            cmd.extend(["-penalty", str(request.penalty)])

    # Ungapped alignment
    if request.ungapped:
        cmd.append("-ungapped")

    return cmd


def _parse_blast_xml(
    xml_content: str,
    db_session: Optional[Session] = None,
) -> BlastSearchResult:
    """
    Parse BLAST XML output (format 5).

    Args:
        xml_content: BLAST XML output string
        db_session: Optional database session for orf19 mapping

    Returns:
        Parsed BlastSearchResult
    """
    root = ET.fromstring(xml_content)

    # Get program info
    program = root.find(".//BlastOutput_program").text
    version = root.find(".//BlastOutput_version").text
    database = root.find(".//BlastOutput_db").text

    # Get query info
    query_id = root.find(".//BlastOutput_query-ID").text
    query_def_elem = root.find(".//BlastOutput_query-def")
    query_def = query_def_elem.text if query_def_elem is not None else None
    query_len = int(root.find(".//BlastOutput_query-len").text)

    # Get search parameters
    params_elem = root.find(".//Parameters")
    parameters = {}
    if params_elem is not None:
        for param in params_elem:
            tag_name = param.tag.replace("Parameters_", "")
            parameters[tag_name] = param.text

    # Get statistics
    stats_elem = root.find(".//Statistics")
    db_len = 0
    db_num = 0
    if stats_elem is not None:
        db_len = int(stats_elem.find("Statistics_db-len").text) if stats_elem.find("Statistics_db-len") is not None else 0
        db_num = int(stats_elem.find("Statistics_db-num").text) if stats_elem.find("Statistics_db-num") is not None else 0

    # Parse hits
    hits = []
    for hit_elem in root.findall(".//Hit"):
        hit_num = int(hit_elem.find("Hit_num").text)
        hit_id = hit_elem.find("Hit_id").text
        hit_def = hit_elem.find("Hit_def").text or ""
        hit_accession = hit_elem.find("Hit_accession").text
        hit_len = int(hit_elem.find("Hit_len").text)

        # Parse HSPs
        hsps = []
        total_score = 0
        best_evalue = float("inf")
        best_bit_score = 0
        query_coverage_set = set()

        for hsp_elem in hit_elem.findall(".//Hsp"):
            hsp_num = int(hsp_elem.find("Hsp_num").text)
            bit_score = float(hsp_elem.find("Hsp_bit-score").text)
            score = int(hsp_elem.find("Hsp_score").text)
            evalue = float(hsp_elem.find("Hsp_evalue").text)

            query_from = int(hsp_elem.find("Hsp_query-from").text)
            query_to = int(hsp_elem.find("Hsp_query-to").text)
            hit_from = int(hsp_elem.find("Hsp_hit-from").text)
            hit_to = int(hsp_elem.find("Hsp_hit-to").text)

            query_frame_elem = hsp_elem.find("Hsp_query-frame")
            query_frame = int(query_frame_elem.text) if query_frame_elem is not None else None
            hit_frame_elem = hsp_elem.find("Hsp_hit-frame")
            hit_frame = int(hit_frame_elem.text) if hit_frame_elem is not None else None

            identity = int(hsp_elem.find("Hsp_identity").text)
            positive_elem = hsp_elem.find("Hsp_positive")
            positive = int(positive_elem.text) if positive_elem is not None else None
            gaps = int(hsp_elem.find("Hsp_gaps").text) if hsp_elem.find("Hsp_gaps") is not None else 0
            align_len = int(hsp_elem.find("Hsp_align-len").text)

            query_seq = hsp_elem.find("Hsp_qseq").text
            hit_seq = hsp_elem.find("Hsp_hseq").text
            midline = hsp_elem.find("Hsp_midline").text

            percent_identity = (identity / align_len * 100) if align_len > 0 else 0
            percent_positive = (positive / align_len * 100) if positive and align_len > 0 else None

            hsp = BlastHsp(
                hsp_num=hsp_num,
                bit_score=bit_score,
                score=score,
                evalue=evalue,
                query_start=query_from,
                query_end=query_to,
                hit_start=hit_from,
                hit_end=hit_to,
                query_frame=query_frame,
                hit_frame=hit_frame,
                identity=identity,
                positive=positive,
                gaps=gaps,
                align_len=align_len,
                query_seq=query_seq,
                hit_seq=hit_seq,
                midline=midline,
                percent_identity=percent_identity,
                percent_positive=percent_positive,
            )
            hsps.append(hsp)

            total_score += score
            if evalue < best_evalue:
                best_evalue = evalue
            if bit_score > best_bit_score:
                best_bit_score = bit_score

            # Track query coverage
            for pos in range(query_from, query_to + 1):
                query_coverage_set.add(pos)

        query_cover = len(query_coverage_set) / query_len * 100 if query_len > 0 else 0

        # Try to extract locus link from hit_id or hit_def
        locus_link = _extract_locus_link(hit_id, hit_def, hit_accession)

        # Extract organism info from database path
        organism_tag = extract_organism_tag_from_database(os.path.basename(database))
        organism_config = get_organism_for_database(os.path.basename(database))
        organism_name = organism_config.get("full_name") if organism_config else None

        # Generate JBrowse URL for genomic hits
        jbrowse_url = None
        if organism_tag and hsps:
            # Use the first HSP's coordinates for the JBrowse link
            first_hsp = hsps[0]
            # For genomic databases, hit_id is typically the chromosome
            jbrowse_url = _generate_jbrowse_url(
                organism_tag,
                hit_id,  # chromosome/contig name
                first_hsp.hit_start,
                first_hsp.hit_end,
            )

        # Map to orf19 ID for Assembly 22 hits
        orf19_id = None
        if db_session and organism_tag and "A22" in organism_tag:
            # Try to extract feature name from hit info
            feature_name = hit_accession or hit_id
            orf19_id = _map_to_orf19_id(db_session, feature_name, organism_tag)

        hit = BlastHit(
            num=hit_num,
            id=hit_id,
            accession=hit_accession,
            description=hit_def,
            length=hit_len,
            hsps=hsps,
            best_evalue=best_evalue,
            best_bit_score=best_bit_score,
            total_score=total_score,
            query_cover=query_cover,
            locus_link=locus_link,
            jbrowse_url=jbrowse_url,
            organism_name=organism_name,
            organism_tag=organism_tag,
            orf19_id=orf19_id,
        )
        hits.append(hit)

    return BlastSearchResult(
        query_id=query_id,
        query_length=query_len,
        query_def=query_def,
        database=database,
        database_length=db_len,
        database_sequences=db_num,
        program=program,
        version=version,
        parameters=parameters,
        hits=hits,
        search_time=0,  # Not available in XML
        warnings=[],
    )


def _extract_locus_link(hit_id: str, hit_def: str, hit_accession: str) -> Optional[str]:
    """
    Extract locus link from BLAST hit information.

    CGD BLAST databases typically have headers like:
    - orf19.1234
    - Ca21chr1_C_albicans_SC5314_orf19.1234
    - ACT1 orf19.5007 CGDID:CAL0000191689
    """
    # Try to find orf pattern
    orf_match = re.search(r'(orf\d+\.\d+)', hit_id + " " + hit_def, re.IGNORECASE)
    if orf_match:
        return f"/locus/{orf_match.group(1)}"

    # Try to find CGDID pattern
    cgdid_match = re.search(r'(CAL\d+)', hit_id + " " + hit_def)
    if cgdid_match:
        return f"/locus/{cgdid_match.group(1)}"

    # Try to find standard gene name (uppercase, 3-4 letters + numbers)
    gene_match = re.search(r'\b([A-Z]{2,4}\d{1,3})\b', hit_def)
    if gene_match:
        return f"/locus/{gene_match.group(1)}"

    return None


def run_blast_search(
    db: Session,
    request: BlastSearchRequest,
) -> BlastSearchResponse:
    """
    Run a BLAST search.

    Args:
        db: Database session (for fetching locus sequences)
        request: BLAST search request

    Returns:
        BlastSearchResponse with results or error
    """
    # Debug logging
    logger.info(f"BLAST search request: program={request.program}, "
                f"genomes={request.genomes}, dataset_type={request.dataset_type}, "
                f"database={request.database}, databases={request.databases}")

    # Handle genomes + dataset_type selection (new approach)
    if request.genomes and request.dataset_type:
        # Convert genomes + dataset_type to database list
        database_names = build_database_names(
            request.genomes,
            request.dataset_type.value
        )
        logger.info(f"Built database names from genomes+dataset_type: {database_names}")

        # Validate program/database type compatibility
        program_info = BLAST_PROGRAMS.get(request.program)
        if not program_info:
            return BlastSearchResponse(
                success=False,
                error=f"Invalid program: {request.program}",
            )

        expected_db_type = get_database_type_for_dataset(request.dataset_type.value)
        if program_info.database_type.value != expected_db_type:
            return BlastSearchResponse(
                success=False,
                error=f"Program {request.program.value} requires {program_info.database_type.value} "
                      f"database, but dataset type {request.dataset_type.value} provides {expected_db_type} sequences",
            )

        # Use multi-database search
        request.databases = database_names
        return run_multi_database_blast(db, request)

    # Validate program/database compatibility (original flow)
    program_info = BLAST_PROGRAMS.get(request.program)
    db_info = BLAST_DATABASES.get(request.database)

    if not program_info or not db_info:
        return BlastSearchResponse(
            success=False,
            error="Invalid program or database specified",
        )

    if program_info.database_type != db_info.type:
        return BlastSearchResponse(
            success=False,
            error=f"Program {request.program.value} requires {program_info.database_type.value} "
                  f"database, but {request.database.value} is a {db_info.type.value} database",
        )

    # Get query sequence
    if request.locus:
        # Determine sequence type based on program
        seq_type = "protein" if program_info.query_type == DatabaseType.PROTEIN else "genomic"
        result = _get_sequence_for_locus(db, request.locus, seq_type)
        if not result:
            return BlastSearchResponse(
                success=False,
                error=f"Could not find {seq_type} sequence for locus: {request.locus}",
            )
        header, sequence = result
    elif request.sequence:
        header, sequence = _parse_fasta_header(request.sequence)
    else:
        return BlastSearchResponse(
            success=False,
            error="No query sequence or locus provided",
        )

    # Validate sequence
    if len(sequence) < 10:
        return BlastSearchResponse(
            success=False,
            error="Query sequence is too short (minimum 10 residues)",
        )

    # Validate sequence type
    if program_info.query_type == DatabaseType.PROTEIN:
        if re.search(r'[^ACDEFGHIKLMNPQRSTVWXY*]', sequence, re.IGNORECASE):
            return BlastSearchResponse(
                success=False,
                error="Invalid protein sequence characters detected",
            )
    else:
        if re.search(r'[^ACGTUNRYWSMKHBVD]', sequence, re.IGNORECASE):
            return BlastSearchResponse(
                success=False,
                error="Invalid nucleotide sequence characters detected",
            )

    # Check if BLAST databases exist
    db_path = os.path.join(BLAST_DB_PATH, request.database.value)
    # BLAST databases have extensions like .nsq, .nin, .nhr for nucleotide
    # and .psq, .pin, .phr for protein
    expected_ext = ".nsq" if db_info.type == DatabaseType.NUCLEOTIDE else ".psq"
    if not os.path.exists(db_path + expected_ext):
        return BlastSearchResponse(
            success=False,
            error=f"BLAST database not found: {request.database.value}. "
                  f"Please contact the administrator.",
        )

    # Get database name (handle both enum and string)
    database_name = request.database.value if request.database else None
    if not database_name:
        return BlastSearchResponse(
            success=False,
            error="No database specified",
        )

    # Run BLAST
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            query_file = os.path.join(tmpdir, "query.fasta")
            output_file = os.path.join(tmpdir, "output.xml")

            # Write query sequence
            with open(query_file, "w") as f:
                f.write(f">{header}\n{sequence}\n")

            # Build and run command
            cmd = _build_blast_command(
                request.program,
                database_name,
                query_file,
                output_file,
                request,
                query_length=len(sequence),
            )

            logger.info(f"Running BLAST command: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=settings.blast_timeout,
            )

            if result.returncode != 0:
                error_msg = result.stderr or "Unknown BLAST error"
                logger.error(f"BLAST failed: {error_msg}")
                return BlastSearchResponse(
                    success=False,
                    error=f"BLAST search failed: {error_msg}",
                )

            # Parse results
            with open(output_file, "r") as f:
                xml_content = f.read()

            search_result = _parse_blast_xml(xml_content, db_session=db)

            return BlastSearchResponse(
                success=True,
                result=search_result,
            )

    except subprocess.TimeoutExpired:
        return BlastSearchResponse(
            success=False,
            error="BLAST search timed out (maximum 5 minutes)",
        )
    except Exception as e:
        logger.exception("BLAST search error")
        return BlastSearchResponse(
            success=False,
            error=f"BLAST search error: {str(e)}",
        )


def format_blast_results_text(result: BlastSearchResult) -> str:
    """Format BLAST results as plain text."""
    lines = []
    lines.append(f"BLAST {result.program} {result.version}")
    lines.append(f"Database: {result.database}")
    lines.append(f"Query: {result.query_def or result.query_id} ({result.query_length} letters)")
    lines.append("")
    lines.append(f"Sequences producing significant alignments:")
    lines.append("")

    # Summary table
    lines.append(f"{'Description':<60} {'Score':>7} {'E-value':>10} {'Ident':>6}")
    lines.append("-" * 90)
    for hit in result.hits:
        desc = hit.description[:57] + "..." if len(hit.description) > 60 else hit.description
        lines.append(f"{desc:<60} {hit.best_bit_score:>7.1f} {hit.best_evalue:>10.2e} {hit.hsps[0].percent_identity:>5.1f}%")

    lines.append("")
    lines.append("Alignments:")
    lines.append("")

    # Detailed alignments
    for hit in result.hits:
        lines.append(f"> {hit.description}")
        lines.append(f"  Length = {hit.length}")
        lines.append("")

        for hsp in hit.hsps:
            lines.append(f"  Score = {hsp.bit_score:.1f} bits ({hsp.score}), "
                        f"Expect = {hsp.evalue:.2e}")
            lines.append(f"  Identities = {hsp.identity}/{hsp.align_len} ({hsp.percent_identity:.0f}%), "
                        f"Gaps = {hsp.gaps}/{hsp.align_len}")
            if hsp.query_frame:
                lines.append(f"  Frame = {hsp.query_frame}")
            lines.append("")

            # Show alignment in chunks
            chunk_size = 60
            for i in range(0, len(hsp.query_seq), chunk_size):
                q_chunk = hsp.query_seq[i:i+chunk_size]
                m_chunk = hsp.midline[i:i+chunk_size]
                s_chunk = hsp.hit_seq[i:i+chunk_size]

                q_start = hsp.query_start + i
                s_start = hsp.hit_start + i

                lines.append(f"Query  {q_start:<6} {q_chunk} {q_start + len(q_chunk) - 1}")
                lines.append(f"       {'':6} {m_chunk}")
                lines.append(f"Sbjct  {s_start:<6} {s_chunk} {s_start + len(s_chunk) - 1}")
                lines.append("")

    return "\n".join(lines)


def run_multi_database_blast(
    db: Session,
    request: BlastSearchRequest,
) -> BlastSearchResponse:
    """
    Run BLAST against multiple databases and merge results.

    Args:
        db: Database session
        request: BLAST search request with 'databases' list

    Returns:
        BlastSearchResponse with merged results from all databases
    """
    if not request.databases:
        return BlastSearchResponse(
            success=False,
            error="No databases specified for multi-database search",
        )

    # Validate program
    program_info = BLAST_PROGRAMS.get(request.program)
    if not program_info:
        return BlastSearchResponse(
            success=False,
            error=f"Invalid program: {request.program}",
        )

    # Get query sequence
    if request.locus:
        seq_type = "protein" if program_info.query_type == DatabaseType.PROTEIN else "genomic"
        result = _get_sequence_for_locus(db, request.locus, seq_type)
        if not result:
            return BlastSearchResponse(
                success=False,
                error=f"Could not find {seq_type} sequence for locus: {request.locus}",
            )
        header, sequence = result
    elif request.sequence:
        header, sequence = _parse_fasta_header(request.sequence)
    else:
        return BlastSearchResponse(
            success=False,
            error="No query sequence or locus provided",
        )

    # Validate sequence
    if len(sequence) < 10:
        return BlastSearchResponse(
            success=False,
            error="Query sequence is too short (minimum 10 residues)",
        )

    # Run BLAST against each database and collect results
    all_hits = []
    all_warnings = []
    merged_result = None

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            query_file = os.path.join(tmpdir, "query.fasta")

            # Write query sequence
            with open(query_file, "w") as f:
                f.write(f">{header}\n{sequence}\n")

            for db_name in request.databases:
                output_file = os.path.join(tmpdir, f"output_{db_name}.xml")

                # Check if database exists
                db_path = os.path.join(BLAST_DB_PATH, db_name)
                if not (os.path.exists(db_path + ".nsq") or os.path.exists(db_path + ".psq")):
                    all_warnings.append(f"Database not found: {db_name}")
                    continue

                # Build and run command
                cmd = _build_blast_command(
                    request.program,
                    db_name,
                    query_file,
                    output_file,
                    request,
                    query_length=len(sequence),
                )

                logger.info(f"Running multi-DB BLAST against {db_name}")

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=settings.blast_timeout,
                )

                if result.returncode != 0:
                    all_warnings.append(f"BLAST failed for {db_name}: {result.stderr}")
                    continue

                # Parse results
                with open(output_file, "r") as f:
                    xml_content = f.read()

                search_result = _parse_blast_xml(xml_content, db_session=db)

                # Collect hits
                all_hits.extend(search_result.hits)

                # Keep first result as template
                if merged_result is None:
                    merged_result = search_result

            if merged_result is None:
                return BlastSearchResponse(
                    success=False,
                    error="No databases could be searched",
                )

            # Sort hits by best E-value and re-number
            all_hits.sort(key=lambda h: h.best_evalue)
            for i, hit in enumerate(all_hits, 1):
                hit.num = i

            # Limit hits to max_hits
            if request.max_hits:
                all_hits = all_hits[:request.max_hits]

            # Update merged result
            merged_result.hits = all_hits
            merged_result.database = ", ".join(request.databases)
            merged_result.warnings = all_warnings

            return BlastSearchResponse(
                success=True,
                result=merged_result,
            )

    except subprocess.TimeoutExpired:
        return BlastSearchResponse(
            success=False,
            error="BLAST search timed out",
        )
    except Exception as e:
        logger.exception("Multi-database BLAST error")
        return BlastSearchResponse(
            success=False,
            error=f"BLAST search error: {str(e)}",
        )


def generate_fasta_download(result: BlastSearchResult) -> BlastDownloadResponse:
    """
    Generate FASTA file of hit sequences.

    Args:
        result: BLAST search result

    Returns:
        BlastDownloadResponse with FASTA content
    """
    lines = []

    for hit in result.hits:
        # Use the hit sequence from the best HSP
        if hit.hsps:
            best_hsp = max(hit.hsps, key=lambda h: h.bit_score)
            # Build header with hit info
            header = f">{hit.id} {hit.description}"
            if hit.organism_name:
                header += f" [{hit.organism_name}]"
            lines.append(header)

            # Add the hit sequence (from alignment, with gaps removed)
            seq = best_hsp.hit_seq.replace("-", "")
            # Wrap sequence at 60 chars
            for i in range(0, len(seq), 60):
                lines.append(seq[i:i+60])
            lines.append("")

    content = "\n".join(lines)
    query_name = result.query_def or result.query_id
    filename = f"blast_hits_{query_name[:20]}.fasta"

    return BlastDownloadResponse(
        format=DownloadFormat.FASTA,
        content=content,
        filename=filename,
        content_type="text/plain",
    )


def generate_tab_download(result: BlastSearchResult) -> BlastDownloadResponse:
    """
    Generate tab-delimited results table.

    Args:
        result: BLAST search result

    Returns:
        BlastDownloadResponse with tab-delimited content
    """
    lines = []

    # Header
    headers = [
        "Query", "Subject", "Identity%", "AlignLen", "Mismatches", "Gaps",
        "QueryStart", "QueryEnd", "SubjStart", "SubjEnd", "E-value", "BitScore",
        "Organism", "orf19_ID", "JBrowse"
    ]
    lines.append("\t".join(headers))

    # Data rows
    for hit in result.hits:
        for hsp in hit.hsps:
            mismatches = hsp.align_len - hsp.identity - hsp.gaps
            row = [
                result.query_id,
                hit.id,
                f"{hsp.percent_identity:.2f}",
                str(hsp.align_len),
                str(mismatches),
                str(hsp.gaps),
                str(hsp.query_start),
                str(hsp.query_end),
                str(hsp.hit_start),
                str(hsp.hit_end),
                f"{hsp.evalue:.2e}",
                f"{hsp.bit_score:.1f}",
                hit.organism_name or "",
                hit.orf19_id or "",
                hit.jbrowse_url or "",
            ]
            lines.append("\t".join(row))

    content = "\n".join(lines)
    query_name = result.query_def or result.query_id
    filename = f"blast_results_{query_name[:20]}.tsv"

    return BlastDownloadResponse(
        format=DownloadFormat.TAB,
        content=content,
        filename=filename,
        content_type="text/tab-separated-values",
    )


def generate_raw_download(result: BlastSearchResult) -> BlastDownloadResponse:
    """
    Generate raw BLAST text output.

    Args:
        result: BLAST search result

    Returns:
        BlastDownloadResponse with raw text content
    """
    content = format_blast_results_text(result)
    query_name = result.query_def or result.query_id
    filename = f"blast_output_{query_name[:20]}.txt"

    return BlastDownloadResponse(
        format=DownloadFormat.RAW,
        content=content,
        filename=filename,
        content_type="text/plain",
    )


def generate_download(
    result: BlastSearchResult,
    format: DownloadFormat
) -> BlastDownloadResponse:
    """
    Generate downloadable BLAST results in the specified format.

    Args:
        result: BLAST search result
        format: Download format

    Returns:
        BlastDownloadResponse with content
    """
    if format == DownloadFormat.FASTA:
        return generate_fasta_download(result)
    elif format == DownloadFormat.TAB:
        return generate_tab_download(result)
    elif format == DownloadFormat.RAW:
        return generate_raw_download(result)
    else:
        raise ValueError(f"Unsupported download format: {format}")
