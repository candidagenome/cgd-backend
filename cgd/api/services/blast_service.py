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
from xml.etree import ElementTree as ET
from sqlalchemy.orm import Session
from sqlalchemy import func

from cgd.core.settings import settings
from cgd.models.models import Feature, Seq
from cgd.schemas.blast_schema import (
    BlastProgram,
    BlastDatabase,
    DatabaseType,
    BlastSearchRequest,
    BlastSearchResult,
    BlastSearchResponse,
    BlastHit,
    BlastHsp,
    BlastDatabaseInfo,
    BlastProgramInfo,
    BlastConfigResponse,
)

logger = logging.getLogger(__name__)

# BLAST+ binary path - can be overridden via environment
BLAST_BIN_PATH = os.environ.get("BLAST_BIN_PATH", "/usr/bin")

# BLAST database path - can be overridden via environment
BLAST_DB_PATH = os.environ.get(
    "BLAST_DB_PATH",
    os.path.join(settings.cgd_data_dir, "blast_db")
)

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
    # C. albicans Assembly 22
    BlastDatabase.CA22_GENOME: BlastDatabaseInfo(
        name="C_albicans_SC5314_A22_genome",
        display_name="C. albicans SC5314 A22 - Genome",
        description="C. albicans SC5314 Assembly 22 chromosomes",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida albicans SC5314",
        assembly="A22",
    ),
    BlastDatabase.CA22_ORFS: BlastDatabaseInfo(
        name="C_albicans_SC5314_A22_ORFs",
        display_name="C. albicans SC5314 A22 - ORFs",
        description="C. albicans SC5314 Assembly 22 ORF sequences (genomic)",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida albicans SC5314",
        assembly="A22",
    ),
    BlastDatabase.CA22_CODING: BlastDatabaseInfo(
        name="C_albicans_SC5314_A22_coding",
        display_name="C. albicans SC5314 A22 - Coding",
        description="C. albicans SC5314 Assembly 22 coding sequences",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida albicans SC5314",
        assembly="A22",
    ),
    BlastDatabase.CA22_PROTEIN: BlastDatabaseInfo(
        name="C_albicans_SC5314_A22_protein",
        display_name="C. albicans SC5314 A22 - Protein",
        description="C. albicans SC5314 Assembly 22 protein sequences",
        type=DatabaseType.PROTEIN,
        organism="Candida albicans SC5314",
        assembly="A22",
    ),
    # C. albicans Assembly 21
    BlastDatabase.CA21_GENOME: BlastDatabaseInfo(
        name="C_albicans_SC5314_A21_genome",
        display_name="C. albicans SC5314 A21 - Genome",
        description="C. albicans SC5314 Assembly 21 chromosomes",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida albicans SC5314",
        assembly="A21",
    ),
    BlastDatabase.CA21_ORFS: BlastDatabaseInfo(
        name="C_albicans_SC5314_A21_ORFs",
        display_name="C. albicans SC5314 A21 - ORFs",
        description="C. albicans SC5314 Assembly 21 ORF sequences (genomic)",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida albicans SC5314",
        assembly="A21",
    ),
    BlastDatabase.CA21_CODING: BlastDatabaseInfo(
        name="C_albicans_SC5314_A21_coding",
        display_name="C. albicans SC5314 A21 - Coding",
        description="C. albicans SC5314 Assembly 21 coding sequences",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida albicans SC5314",
        assembly="A21",
    ),
    BlastDatabase.CA21_PROTEIN: BlastDatabaseInfo(
        name="C_albicans_SC5314_A21_protein",
        display_name="C. albicans SC5314 A21 - Protein",
        description="C. albicans SC5314 Assembly 21 protein sequences",
        type=DatabaseType.PROTEIN,
        organism="Candida albicans SC5314",
        assembly="A21",
    ),
    # C. glabrata
    BlastDatabase.CG_GENOME: BlastDatabaseInfo(
        name="C_glabrata_CBS138_genome",
        display_name="C. glabrata CBS138 - Genome",
        description="C. glabrata CBS138 chromosomes",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida glabrata CBS138",
    ),
    BlastDatabase.CG_ORFS: BlastDatabaseInfo(
        name="C_glabrata_CBS138_ORFs",
        display_name="C. glabrata CBS138 - ORFs",
        description="C. glabrata CBS138 ORF sequences (genomic)",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida glabrata CBS138",
    ),
    BlastDatabase.CG_CODING: BlastDatabaseInfo(
        name="C_glabrata_CBS138_coding",
        display_name="C. glabrata CBS138 - Coding",
        description="C. glabrata CBS138 coding sequences",
        type=DatabaseType.NUCLEOTIDE,
        organism="Candida glabrata CBS138",
    ),
    BlastDatabase.CG_PROTEIN: BlastDatabaseInfo(
        name="C_glabrata_CBS138_protein",
        display_name="C. glabrata CBS138 - Protein",
        description="C. glabrata CBS138 protein sequences",
        type=DatabaseType.PROTEIN,
        organism="Candida glabrata CBS138",
    ),
    # All Candida
    BlastDatabase.ALL_CANDIDA_GENOME: BlastDatabaseInfo(
        name="all_candida_genome",
        display_name="All Candida - Genome",
        description="All Candida species chromosomes combined",
        type=DatabaseType.NUCLEOTIDE,
        organism="All Candida species",
    ),
    BlastDatabase.ALL_CANDIDA_ORFS: BlastDatabaseInfo(
        name="all_candida_ORFs",
        display_name="All Candida - ORFs",
        description="All Candida species ORF sequences combined",
        type=DatabaseType.NUCLEOTIDE,
        organism="All Candida species",
    ),
    BlastDatabase.ALL_CANDIDA_CODING: BlastDatabaseInfo(
        name="all_candida_coding",
        display_name="All Candida - Coding",
        description="All Candida species coding sequences combined",
        type=DatabaseType.NUCLEOTIDE,
        organism="All Candida species",
    ),
    BlastDatabase.ALL_CANDIDA_PROTEIN: BlastDatabaseInfo(
        name="all_candida_protein",
        display_name="All Candida - Protein",
        description="All Candida species protein sequences combined",
        type=DatabaseType.PROTEIN,
        organism="All Candida species",
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
    database: BlastDatabase,
    query_file: str,
    output_file: str,
    request: BlastSearchRequest,
) -> List[str]:
    """Build BLAST command line."""
    program_path = os.path.join(BLAST_BIN_PATH, program.value)
    db_path = os.path.join(BLAST_DB_PATH, database.value)

    cmd = [
        program_path,
        "-query", query_file,
        "-db", db_path,
        "-out", output_file,
        "-outfmt", "5",  # XML output format
        "-evalue", str(request.evalue),
        "-max_target_seqs", str(request.max_hits),
    ]

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

    return cmd


def _parse_blast_xml(xml_content: str) -> BlastSearchResult:
    """Parse BLAST XML output (format 5)."""
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
    # Validate program/database compatibility
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
                request.database,
                query_file,
                output_file,
                request,
            )

            logger.info(f"Running BLAST command: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
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

            search_result = _parse_blast_xml(xml_content)

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
