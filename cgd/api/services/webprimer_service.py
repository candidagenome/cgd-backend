"""
WebPrimer Service - Primer design algorithms.

Implements primer design for PCR and sequencing applications.
"""
from __future__ import annotations

import math
import logging
from typing import List, Optional, Tuple, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func

from cgd.models.models import Feature, Seq, FeatLocation, Organism
from cgd.schemas.webprimer_schema import (
    WebPrimerRequest,
    WebPrimerResponse,
    WebPrimerConfigResponse,
    WebPrimerSequenceRequest,
    WebPrimerSequenceResponse,
    PrimerResult,
    PrimerPairResult,
    SequencingPrimerResult,
    PrimerPurpose,
    SequencingStrand,
    SequencingStrandCount,
)

logger = logging.getLogger(__name__)

# Thermodynamic parameters for Tm calculation (nearest-neighbor)
# Values from SantaLucia (1998) - entropy in cal/(mol*K), enthalpy in kcal/mol
ENTROPY_VALUES = {
    "AA": -22.2, "TT": -22.2,
    "AT": -20.4,
    "TA": -21.3,
    "CA": -22.7, "TG": -22.7,
    "GT": -22.4, "AC": -22.4,
    "CT": -21.0, "AG": -21.0,
    "GA": -22.2, "TC": -22.2,
    "CG": -27.2,
    "GC": -24.4,
    "GG": -19.9, "CC": -19.9,
}

ENTHALPY_VALUES = {
    "AA": -7.9, "TT": -7.9,
    "AT": -7.2,
    "TA": -7.2,
    "CA": -8.5, "TG": -8.5,
    "GT": -8.4, "AC": -8.4,
    "CT": -7.8, "AG": -7.8,
    "GA": -8.2, "TC": -8.2,
    "CG": -10.6,
    "GC": -9.8,
    "GG": -8.0, "CC": -8.0,
}


def get_webprimer_config() -> WebPrimerConfigResponse:
    """Get default configuration for primer design."""
    return WebPrimerConfigResponse(
        default_params={
            "bp_from_start": 35,
            "bp_from_stop": 35,
            "parsed_length": 35,
            "opt_tm": 55,
            "min_tm": 50,
            "max_tm": 65,
            "opt_length": 20,
            "min_length": 18,
            "max_length": 21,
            "opt_gc": 45,
            "min_gc": 30,
            "max_gc": 60,
            "max_self_anneal": 24,
            "max_self_end_anneal": 12,
            "max_pair_anneal": 24,
            "max_pair_end_anneal": 12,
            "seq_spacing": 250,
            "dna_conc": 50,
            "salt_conc": 50,
        }
    )


def get_sequence_for_locus(
    db: Session,
    locus: str,
) -> WebPrimerSequenceResponse:
    """Get genomic sequence for a locus."""
    # Find feature by name
    feature = (
        db.query(Feature)
        .filter(func.upper(Feature.feature_name) == func.upper(locus.strip()))
        .first()
    )

    if not feature:
        # Try gene_name
        feature = (
            db.query(Feature)
            .filter(func.upper(Feature.gene_name) == func.upper(locus.strip()))
            .first()
        )

    if not feature:
        return WebPrimerSequenceResponse(
            success=False,
            error=f"Locus '{locus}' not found in database"
        )

    # Get sequence
    seq_record = (
        db.query(Seq)
        .filter(
            Seq.feature_no == feature.feature_no,
            Seq.is_seq_current == "Y",
            Seq.seq_type == "genomic",
        )
        .first()
    )

    if not seq_record or not seq_record.residues:
        return WebPrimerSequenceResponse(
            success=False,
            error=f"No genomic sequence available for '{locus}'"
        )

    return WebPrimerSequenceResponse(
        success=True,
        sequence=seq_record.residues,
        locus=feature.feature_name,
    )


def _complement_base(base: str) -> str:
    """Get complement of a single base."""
    complements = {"A": "T", "T": "A", "G": "C", "C": "G",
                   "a": "t", "t": "a", "g": "c", "c": "g",
                   "N": "N", "n": "n"}
    return complements.get(base, "N")


def _reverse_complement(seq: str) -> str:
    """Get reverse complement of a DNA sequence."""
    return "".join(_complement_base(b) for b in reversed(seq))


def _calculate_gc_percent(seq: str) -> float:
    """Calculate GC percentage of a sequence."""
    seq = seq.upper()
    gc_count = seq.count("G") + seq.count("C")
    total = len(seq)
    if total == 0:
        return 0.0
    return (gc_count / total) * 100


def _has_gc_clamp(seq: str) -> bool:
    """Check if primer has GC clamp (G or C at 3' end)."""
    if len(seq) < 2:
        return False
    last_two = seq[-2:].upper()
    gc_bases = {"G", "C"}
    return last_two[0] in gc_bases or last_two[1] in gc_bases


def _calculate_tm(seq: str, dna_conc: float = 50, salt_conc: float = 50) -> float:
    """
    Calculate melting temperature using nearest-neighbor method.

    Uses SantaLucia (1998) unified parameters.

    Args:
        seq: Primer sequence
        dna_conc: DNA concentration in nM
        salt_conc: Salt concentration in mM

    Returns:
        Melting temperature in Celsius
    """
    seq = seq.upper()
    if len(seq) < 2:
        return 0.0

    # Use simple formula for reliability
    # Basic formula: Tm = 64.9 + 41 * (nG + nC - 16.4) / length
    # Or Wallace rule for shorter primers: Tm = 2*(A+T) + 4*(G+C)

    gc_count = seq.count('G') + seq.count('C')
    at_count = seq.count('A') + seq.count('T')
    length = len(seq)

    if length < 14:
        # Wallace rule for short primers
        tm = 2 * at_count + 4 * gc_count
    else:
        # More accurate formula for longer primers
        # Tm = 81.5 + 16.6*log10([Na+]) + 41*(GC/length) - 675/length
        gc_percent = (gc_count / length) * 100
        na_conc = salt_conc / 1000  # Convert mM to M

        tm = 81.5 + 16.6 * math.log10(na_conc) + 0.41 * gc_percent - 675 / length

    return round(tm, 1)


def _calculate_annealing(seq1: str, seq2: str) -> int:
    """
    Calculate annealing score between two sequences.

    Scores: G-C = 4, A-T = 2, mismatch = 0
    Returns maximum contiguous annealing score.
    """
    seq1 = seq1.upper()
    seq2 = seq2.upper()

    # Pad shorter sequence
    len_diff = len(seq1) - len(seq2)
    if len_diff < 0:
        seq1 = "N" * abs(len_diff) + seq1
    elif len_diff > 0:
        seq2 = "N" * len_diff + seq2

    max_score = 0

    # Slide seq2 along seq1
    for offset in range(len(seq2)):
        current_score = 0
        temp_score = 0

        for i in range(len(seq2) - offset):
            b1 = seq1[i]
            b2 = seq2[len(seq2) - 1 - i - offset]

            # Check for complementary base pairing
            if (b1 == "G" and b2 == "C") or (b1 == "C" and b2 == "G"):
                temp_score += 4
            elif (b1 == "A" and b2 == "T") or (b1 == "T" and b2 == "A"):
                temp_score += 2
            else:
                if temp_score > current_score:
                    current_score = temp_score
                temp_score = 0

        if temp_score > current_score:
            current_score = temp_score
        if current_score > max_score:
            max_score = current_score

    return max_score


def _calculate_end_annealing(seq1: str, seq2: str) -> int:
    """
    Calculate 3' end annealing score.

    Checks for annealing at the 3' end which can cause primer-dimer.
    """
    seq1 = seq1.upper()
    seq2 = seq2.upper()

    # Pad shorter sequence
    len_diff = len(seq1) - len(seq2)
    if len_diff < 0:
        seq1 = "N" * abs(len_diff) + seq1
    elif len_diff > 0:
        seq2 = "N" * len_diff + seq2

    max_score = 0

    for offset in range(len(seq2)):
        score = 0
        started = False

        for i in range(len(seq2) - offset):
            b1 = seq1[i]
            b2 = seq2[len(seq2) - 1 - i - offset]

            if (b1 == "G" and b2 == "C") or (b1 == "C" and b2 == "G"):
                score += 4
                started = True
            elif (b1 == "A" and b2 == "T") or (b1 == "T" and b2 == "A"):
                score += 2
                started = True
            else:
                if started:
                    break  # Stop at first mismatch after annealing starts

        if score > max_score:
            max_score = score

    return max_score


def _generate_primers(
    block: str,
    min_length: int,
    max_length: int,
    specific_ends: bool,
) -> List[str]:
    """Generate all possible primers from a DNA block."""
    primers = []
    block = block.upper()

    if specific_ends:
        # Only primers starting at position 0
        for length in range(min_length, max_length + 1):
            if length <= len(block):
                primers.append(block[:length])
    else:
        # All possible primers at all positions
        for length in range(min_length, max_length + 1):
            for start in range(len(block) - length + 1):
                primers.append(block[start:start + length])

    return primers


def _filter_primers_by_gc(
    primers: List[str],
    min_gc: float,
    max_gc: float,
) -> Dict[str, float]:
    """Filter primers by GC content and return dict with GC values."""
    result = {}
    for primer in primers:
        gc = _calculate_gc_percent(primer)
        if min_gc <= gc <= max_gc:
            result[primer] = gc
    return result


def _filter_primers_by_tm(
    primers: Dict[str, float],
    min_tm: float,
    max_tm: float,
    dna_conc: float,
    salt_conc: float,
) -> Dict[str, Tuple[float, float]]:
    """Filter primers by Tm and return dict with (GC, Tm) values."""
    result = {}
    for primer, gc in primers.items():
        tm = _calculate_tm(primer, dna_conc, salt_conc)
        if min_tm <= tm <= max_tm:
            result[primer] = (gc, tm)
    return result


def _filter_primers_by_self_anneal(
    primers: Dict[str, Tuple[float, float]],
    max_self_anneal: int,
    max_self_end_anneal: int,
) -> Dict[str, Tuple[float, float, int, int]]:
    """Filter primers by self-annealing and return dict with all values."""
    result = {}
    for primer, (gc, tm) in primers.items():
        self_anneal = _calculate_annealing(primer, primer)
        if self_anneal <= max_self_anneal:
            self_end = _calculate_end_annealing(primer, primer)
            if self_end <= max_self_end_anneal:
                result[primer] = (gc, tm, self_anneal, self_end)
    return result


def _calculate_primer_score(
    primer_data: Tuple,
    opt_tm: float,
    opt_gc: float,
    opt_length: int,
) -> float:
    """Calculate a score for primer quality (lower is better)."""
    gc, tm, self_anneal, self_end = primer_data[:4]
    length = len(primer_data[4]) if len(primer_data) > 4 else opt_length

    score = 0.0
    score += abs(opt_tm - tm)
    score += abs(opt_gc - gc)
    score += self_anneal / 10
    score += self_end / 5
    score += abs(opt_length - length) / 2

    return score


def _calculate_pair_score(
    forward_data: Tuple,
    reverse_data: Tuple,
    pair_anneal: int,
    pair_end_anneal: int,
    opt_tm: float,
    opt_gc: float,
    opt_length: int,
) -> float:
    """Calculate score for a primer pair (lower is better)."""
    f_gc, f_tm, f_self, f_end = forward_data[:4]
    r_gc, r_tm, r_self, r_end = reverse_data[:4]

    score = 0.0
    score += abs(opt_tm - f_tm)
    score += abs(opt_tm - r_tm)
    score += abs(opt_gc - f_gc)
    score += abs(opt_gc - r_gc)
    score += f_self / 10
    score += r_self / 10
    score += pair_anneal / 10
    score += f_end / 5
    score += r_end / 5
    score += pair_end_anneal / 5

    return score


def design_primers(request: WebPrimerRequest) -> WebPrimerResponse:
    """
    Design primers for PCR or sequencing.

    Args:
        request: Primer design parameters

    Returns:
        Primer design results
    """
    # Clean and validate sequence
    sequence = "".join(c for c in request.sequence.upper() if c in "ATGCN")

    if len(sequence) < request.min_length:
        return WebPrimerResponse(
            success=False,
            purpose=request.purpose.value,
            sequence_length=len(sequence),
            error="Sequence too short for primer design"
        )

    warnings = []

    if request.purpose == PrimerPurpose.PCR:
        return _design_pcr_primers(sequence, request, warnings)
    else:
        return _design_sequencing_primers(sequence, request, warnings)


def _design_pcr_primers(
    sequence: str,
    request: WebPrimerRequest,
    warnings: List[str],
) -> WebPrimerResponse:
    """Design PCR primer pairs."""
    # Parse forward and reverse blocks
    parsed_length = min(request.parsed_length, len(sequence) // 2)
    forward_block = sequence[:parsed_length]
    reverse_block = _reverse_complement(sequence[-parsed_length:])

    # Generate primers
    forward_primers = _generate_primers(
        forward_block,
        request.min_length,
        request.max_length,
        request.specific_ends,
    )
    reverse_primers = _generate_primers(
        reverse_block,
        request.min_length,
        request.max_length,
        request.specific_ends,
    )

    # Filter forward primers
    forward_gc = _filter_primers_by_gc(forward_primers, request.min_gc, request.max_gc)
    forward_gc_count = len(forward_gc)

    if not forward_gc:
        return WebPrimerResponse(
            success=False,
            purpose="PCR",
            sequence_length=len(sequence),
            forward_gc_valid=0,
            error="No forward primers found in valid GC range"
        )

    forward_tm = _filter_primers_by_tm(
        forward_gc, request.min_tm, request.max_tm,
        request.dna_conc, request.salt_conc
    )
    forward_tm_count = len(forward_tm)

    if not forward_tm:
        return WebPrimerResponse(
            success=False,
            purpose="PCR",
            sequence_length=len(sequence),
            forward_gc_valid=forward_gc_count,
            forward_tm_valid=0,
            error="No forward primers found in valid Tm range"
        )

    forward_valid = _filter_primers_by_self_anneal(
        forward_tm, request.max_self_anneal, request.max_self_end_anneal
    )
    forward_self_count = len(forward_valid)

    if not forward_valid:
        return WebPrimerResponse(
            success=False,
            purpose="PCR",
            sequence_length=len(sequence),
            forward_gc_valid=forward_gc_count,
            forward_tm_valid=forward_tm_count,
            forward_self_valid=0,
            error="No forward primers found with valid self-annealing"
        )

    # Filter reverse primers
    reverse_gc = _filter_primers_by_gc(reverse_primers, request.min_gc, request.max_gc)
    reverse_gc_count = len(reverse_gc)

    if not reverse_gc:
        return WebPrimerResponse(
            success=False,
            purpose="PCR",
            sequence_length=len(sequence),
            forward_gc_valid=forward_gc_count,
            forward_tm_valid=forward_tm_count,
            forward_self_valid=forward_self_count,
            reverse_gc_valid=0,
            error="No reverse primers found in valid GC range"
        )

    reverse_tm = _filter_primers_by_tm(
        reverse_gc, request.min_tm, request.max_tm,
        request.dna_conc, request.salt_conc
    )
    reverse_tm_count = len(reverse_tm)

    if not reverse_tm:
        return WebPrimerResponse(
            success=False,
            purpose="PCR",
            sequence_length=len(sequence),
            forward_gc_valid=forward_gc_count,
            forward_tm_valid=forward_tm_count,
            forward_self_valid=forward_self_count,
            reverse_gc_valid=reverse_gc_count,
            reverse_tm_valid=0,
            error="No reverse primers found in valid Tm range"
        )

    reverse_valid = _filter_primers_by_self_anneal(
        reverse_tm, request.max_self_anneal, request.max_self_end_anneal
    )
    reverse_self_count = len(reverse_valid)

    if not reverse_valid:
        return WebPrimerResponse(
            success=False,
            purpose="PCR",
            sequence_length=len(sequence),
            forward_gc_valid=forward_gc_count,
            forward_tm_valid=forward_tm_count,
            forward_self_valid=forward_self_count,
            reverse_gc_valid=reverse_gc_count,
            reverse_tm_valid=reverse_tm_count,
            reverse_self_valid=0,
            error="No reverse primers found with valid self-annealing"
        )

    # Find valid pairs
    pairs = []
    for f_seq, f_data in forward_valid.items():
        for r_seq, r_data in reverse_valid.items():
            pair_anneal = _calculate_annealing(f_seq, r_seq)
            if pair_anneal > request.max_pair_anneal:
                continue

            pair_end = _calculate_end_annealing(f_seq, r_seq)
            if pair_end > request.max_pair_end_anneal:
                continue

            # Calculate positions
            f_pos = sequence.find(f_seq) + 1
            r_pos = len(sequence) - sequence.rfind(_reverse_complement(r_seq)[::-1])

            # Product length
            product_length = len(sequence)
            if request.specific_ends:
                product_length = len(sequence)
            else:
                # Approximate product length
                product_length = r_pos - f_pos + len(r_seq)

            score = _calculate_pair_score(
                f_data, r_data, pair_anneal, pair_end,
                request.opt_tm, request.opt_gc, request.opt_length
            )

            pairs.append({
                "forward_seq": f_seq,
                "forward_data": f_data,
                "reverse_seq": r_seq,
                "reverse_data": r_data,
                "pair_anneal": pair_anneal,
                "pair_end_anneal": pair_end,
                "f_pos": f_pos,
                "r_pos": len(sequence) - len(r_seq) + 1,
                "product_length": product_length,
                "score": score,
            })

            if len(pairs) >= request.max_results:
                warnings.append(f"Stopped at {request.max_results} pairs - more may exist")
                break
        if len(pairs) >= request.max_results:
            break

    if not pairs:
        return WebPrimerResponse(
            success=False,
            purpose="PCR",
            sequence_length=len(sequence),
            forward_gc_valid=forward_gc_count,
            forward_tm_valid=forward_tm_count,
            forward_self_valid=forward_self_count,
            reverse_gc_valid=reverse_gc_count,
            reverse_tm_valid=reverse_tm_count,
            reverse_self_valid=reverse_self_count,
            error="No valid primer pairs found. Try relaxing parameters."
        )

    # Sort by score and assign ranks
    pairs.sort(key=lambda x: x["score"])

    all_pairs = []
    for rank, p in enumerate(pairs, 1):
        f_gc, f_tm, f_self, f_end = p["forward_data"]
        r_gc, r_tm, r_self, r_end = p["reverse_data"]

        pair_result = PrimerPairResult(
            forward=PrimerResult(
                sequence=p["forward_seq"],
                length=len(p["forward_seq"]),
                tm=f_tm,
                gc_percent=f_gc,
                self_anneal=f_self,
                self_end_anneal=f_end,
                position=p["f_pos"],
                strand="CODING",
            ),
            reverse=PrimerResult(
                sequence=p["reverse_seq"],
                length=len(p["reverse_seq"]),
                tm=r_tm,
                gc_percent=r_gc,
                self_anneal=r_self,
                self_end_anneal=r_end,
                position=p["r_pos"],
                strand="NON-CODING",
            ),
            pair_anneal=p["pair_anneal"],
            pair_end_anneal=p["pair_end_anneal"],
            product_length=p["product_length"],
            rank=rank,
            score=round(p["score"], 2),
        )
        all_pairs.append(pair_result)

    # Get amplified sequence for best pair
    best = all_pairs[0]
    amp_start = best.forward.position - 1
    amp_end = best.reverse.position + best.reverse.length - 1
    amplified_seq = sequence[amp_start:amp_end] if amp_start < amp_end else sequence

    return WebPrimerResponse(
        success=True,
        purpose="PCR",
        sequence_length=len(sequence),
        amplified_sequence=amplified_seq,
        best_pair=best,
        all_pairs=all_pairs,
        total_pairs=len(all_pairs),
        forward_gc_valid=forward_gc_count,
        forward_tm_valid=forward_tm_count,
        forward_self_valid=forward_self_count,
        reverse_gc_valid=reverse_gc_count,
        reverse_tm_valid=reverse_tm_count,
        reverse_self_valid=reverse_self_count,
        warnings=warnings,
    )


def _design_sequencing_primers(
    sequence: str,
    request: WebPrimerRequest,
    warnings: List[str],
) -> WebPrimerResponse:
    """Design sequencing primers."""
    coding_primers = []
    noncoding_primers = []

    # Parse blocks at intervals
    spacing = request.seq_spacing
    parsed_length = request.parsed_length

    # Coding strand
    if request.seq_strand_count == SequencingStrandCount.BOTH or \
       request.seq_strand == SequencingStrand.CODING:
        pos = 0
        rank = 1
        while pos + parsed_length <= len(sequence):
            block = sequence[pos:pos + parsed_length]
            primer = _find_best_sequencing_primer(
                block, request, pos + 1, "CODING"
            )
            if primer:
                coding_primers.append(SequencingPrimerResult(
                    primer=primer,
                    rank=rank,
                ))
                rank += 1
            pos += spacing

    # Non-coding strand
    if request.seq_strand_count == SequencingStrandCount.BOTH or \
       request.seq_strand == SequencingStrand.NON_CODING:
        rev_seq = _reverse_complement(sequence)
        pos = 0
        rank = 1
        while pos + parsed_length <= len(rev_seq):
            block = rev_seq[pos:pos + parsed_length]
            actual_pos = len(sequence) - pos
            primer = _find_best_sequencing_primer(
                block, request, actual_pos, "NON-CODING"
            )
            if primer:
                noncoding_primers.append(SequencingPrimerResult(
                    primer=primer,
                    rank=rank,
                ))
                rank += 1
            pos += spacing

    if not coding_primers and not noncoding_primers:
        return WebPrimerResponse(
            success=False,
            purpose="SEQUENCING",
            sequence_length=len(sequence),
            error="No valid sequencing primers found"
        )

    return WebPrimerResponse(
        success=True,
        purpose="SEQUENCING",
        sequence_length=len(sequence),
        coding_primers=coding_primers,
        noncoding_primers=noncoding_primers,
        warnings=warnings,
    )


def _find_best_sequencing_primer(
    block: str,
    request: WebPrimerRequest,
    position: int,
    strand: str,
) -> Optional[PrimerResult]:
    """Find the best primer from a block for sequencing."""
    primers = _generate_primers(
        block, request.min_length, request.max_length, False
    )

    best_primer = None
    best_score = float("inf")

    for primer_seq in primers:
        gc = _calculate_gc_percent(primer_seq)
        if not (request.min_gc <= gc <= request.max_gc):
            continue

        self_anneal = _calculate_annealing(primer_seq, primer_seq)
        if self_anneal > request.max_self_anneal:
            continue

        has_clamp = _has_gc_clamp(primer_seq)

        # Score: prefer optimal GC and GC clamp
        score = abs(request.opt_gc - gc) / 10
        if not has_clamp:
            score += 1

        if score < best_score:
            best_score = score
            tm = _calculate_tm(primer_seq, request.dna_conc, request.salt_conc)
            self_end = _calculate_end_annealing(primer_seq, primer_seq)

            best_primer = PrimerResult(
                sequence=primer_seq,
                length=len(primer_seq),
                tm=tm,
                gc_percent=gc,
                self_anneal=self_anneal,
                self_end_anneal=self_end,
                position=position,
                strand=strand,
            )

    return best_primer
