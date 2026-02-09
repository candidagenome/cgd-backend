"""
Sequence Alignment Service - Compare and align two sequences.

Provides a simple pairwise sequence alignment tool for curators
to compare old vs new sequences during curation.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class SeqAlignmentService:
    """Service for sequence alignment and comparison."""

    def __init__(self):
        pass

    def align_sequences(
        self,
        seq1: str,
        seq2: str,
        seq1_name: str = "Sequence 1",
        seq2_name: str = "Sequence 2",
    ) -> dict:
        """
        Align two sequences and show differences.

        Uses a simple character-by-character comparison with gap handling.

        Args:
            seq1: First sequence
            seq2: Second sequence
            seq1_name: Name/label for first sequence
            seq2_name: Name/label for second sequence

        Returns:
            Alignment results with statistics
        """
        # Clean sequences - remove whitespace and convert to uppercase
        seq1_clean = "".join(seq1.upper().split())
        seq2_clean = "".join(seq2.upper().split())

        # Simple Needleman-Wunsch-like alignment
        aligned1, aligned2, symbols = self._simple_align(seq1_clean, seq2_clean)

        # Calculate statistics
        matches = symbols.count("*")
        mismatches = symbols.count(".")
        gaps = symbols.count(" ")
        total_aligned = len(symbols)

        identity = (matches / total_aligned * 100) if total_aligned > 0 else 0

        # Format aligned sequences in blocks
        blocks = self._format_alignment_blocks(
            aligned1, aligned2, symbols, seq1_name, seq2_name
        )

        return {
            "seq1_name": seq1_name,
            "seq2_name": seq2_name,
            "seq1_length": len(seq1_clean),
            "seq2_length": len(seq2_clean),
            "aligned_length": total_aligned,
            "matches": matches,
            "mismatches": mismatches,
            "gaps": gaps,
            "identity_percent": round(identity, 2),
            "aligned_seq1": aligned1,
            "aligned_seq2": aligned2,
            "symbols": symbols,
            "blocks": blocks,
        }

    def _simple_align(self, seq1: str, seq2: str) -> tuple[str, str, str]:
        """
        Perform simple global alignment of two sequences.

        Uses dynamic programming for optimal alignment.
        """
        # For very different length sequences, use a simpler approach
        if abs(len(seq1) - len(seq2)) > max(len(seq1), len(seq2)) * 0.5:
            return self._simple_comparison(seq1, seq2)

        # Scoring parameters
        match_score = 2
        mismatch_score = -1
        gap_penalty = -2

        m, n = len(seq1), len(seq2)

        # Limit matrix size for very long sequences
        if m * n > 10_000_000:  # ~10MB limit
            return self._simple_comparison(seq1, seq2)

        # Initialize scoring matrix
        score = [[0] * (n + 1) for _ in range(m + 1)]

        # Initialize first row and column
        for i in range(m + 1):
            score[i][0] = gap_penalty * i
        for j in range(n + 1):
            score[0][j] = gap_penalty * j

        # Fill scoring matrix
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                match = score[i - 1][j - 1] + (
                    match_score if seq1[i - 1] == seq2[j - 1] else mismatch_score
                )
                delete = score[i - 1][j] + gap_penalty
                insert = score[i][j - 1] + gap_penalty
                score[i][j] = max(match, delete, insert)

        # Traceback
        aligned1, aligned2, symbols = [], [], []
        i, j = m, n

        while i > 0 or j > 0:
            if i > 0 and j > 0:
                current = score[i][j]
                diag = score[i - 1][j - 1]
                up = score[i - 1][j]
                left = score[i][j - 1]

                if seq1[i - 1] == seq2[j - 1]:
                    expected_diag = diag + match_score
                else:
                    expected_diag = diag + mismatch_score

                if current == expected_diag:
                    aligned1.append(seq1[i - 1])
                    aligned2.append(seq2[j - 1])
                    symbols.append("*" if seq1[i - 1] == seq2[j - 1] else ".")
                    i -= 1
                    j -= 1
                elif current == up + gap_penalty:
                    aligned1.append(seq1[i - 1])
                    aligned2.append("-")
                    symbols.append(" ")
                    i -= 1
                else:
                    aligned1.append("-")
                    aligned2.append(seq2[j - 1])
                    symbols.append(" ")
                    j -= 1
            elif i > 0:
                aligned1.append(seq1[i - 1])
                aligned2.append("-")
                symbols.append(" ")
                i -= 1
            else:
                aligned1.append("-")
                aligned2.append(seq2[j - 1])
                symbols.append(" ")
                j -= 1

        # Reverse since we traced back
        return "".join(reversed(aligned1)), "".join(reversed(aligned2)), "".join(reversed(symbols))

    def _simple_comparison(self, seq1: str, seq2: str) -> tuple[str, str, str]:
        """
        Simple character-by-character comparison for very different sequences.
        """
        max_len = max(len(seq1), len(seq2))
        aligned1 = seq1.ljust(max_len, "-")
        aligned2 = seq2.ljust(max_len, "-")

        symbols = []
        for c1, c2 in zip(aligned1, aligned2):
            if c1 == "-" or c2 == "-":
                symbols.append(" ")
            elif c1 == c2:
                symbols.append("*")
            else:
                symbols.append(".")

        return aligned1, aligned2, "".join(symbols)

    def _format_alignment_blocks(
        self,
        aligned1: str,
        aligned2: str,
        symbols: str,
        name1: str,
        name2: str,
        block_size: int = 60,
    ) -> list[dict]:
        """Format alignment into display blocks."""
        blocks = []
        seq1_pos = 0
        seq2_pos = 0

        for i in range(0, len(aligned1), block_size):
            block_seq1 = aligned1[i : i + block_size]
            block_seq2 = aligned2[i : i + block_size]
            block_symbols = symbols[i : i + block_size]

            # Calculate actual positions (excluding gaps)
            seq1_start = seq1_pos + 1
            seq2_start = seq2_pos + 1

            for c in block_seq1:
                if c != "-":
                    seq1_pos += 1
            for c in block_seq2:
                if c != "-":
                    seq2_pos += 1

            blocks.append({
                "seq1": block_seq1,
                "seq2": block_seq2,
                "symbols": block_symbols,
                "seq1_start": seq1_start,
                "seq1_end": seq1_pos,
                "seq2_start": seq2_start,
                "seq2_end": seq2_pos,
            })

        return blocks

    def compare_sequences(self, seq1: str, seq2: str) -> dict:
        """
        Quick comparison of two sequences without full alignment.

        Args:
            seq1: First sequence
            seq2: Second sequence

        Returns:
            Comparison statistics
        """
        seq1_clean = "".join(seq1.upper().split())
        seq2_clean = "".join(seq2.upper().split())

        length_diff = len(seq2_clean) - len(seq1_clean)

        # Find differences at each position (up to shorter sequence length)
        min_len = min(len(seq1_clean), len(seq2_clean))
        differences = []

        for i in range(min_len):
            if seq1_clean[i] != seq2_clean[i]:
                differences.append({
                    "position": i + 1,
                    "seq1_char": seq1_clean[i],
                    "seq2_char": seq2_clean[i],
                })

        return {
            "seq1_length": len(seq1_clean),
            "seq2_length": len(seq2_clean),
            "length_difference": length_diff,
            "identical": seq1_clean == seq2_clean,
            "difference_count": len(differences),
            "differences": differences[:100],  # Limit to first 100
        }
