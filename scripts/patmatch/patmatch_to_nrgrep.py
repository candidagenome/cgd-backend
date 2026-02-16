#!/usr/bin/env python3
"""
Convert Patmatch patterns to nrgrep patterns.

A utility to convert a pattern expression in Patmatch to an expression that
is understood by nrgrep. This program does not check the syntax of a
Patmatch pattern. It is assumed that it has been done before calling this
program.

Original Perl: patmatch_to_nrgrep.pl (by Thomas Yan, 2004/06/01)
Converted to Python: 2024

Usage:
    python patmatch_to_nrgrep.py -n PATTERN  # nucleotide pattern
    python patmatch_to_nrgrep.py -p PATTERN  # protein pattern
    python patmatch_to_nrgrep.py -c PATTERN  # reverse complement nucleotide
"""

import argparse
import sys
from typing import Tuple

# Constants
INFINITE = -1
NUCLEOTIDE = "N"
PEPTIDE = "P"
COMPLEMENT = "C"

# IUPAC nucleotide codes
NUCLEOTIDE_IUPAC = {
    'R': '[AG]',
    'Y': '[CT]',
    'S': '[GC]',
    'W': '[AT]',
    'M': '[AC]',
    'K': '[GT]',
    'V': '[ACG]',
    'H': '[ACT]',
    'D': '[AGT]',
    'B': '[CGT]',
}

# IUPAC peptide codes
PEPTIDE_IUPAC = {
    'J': '[IFVLWMAGCY]',
    'O': '[TSHEDQNKR]',
    'B': '[DN]',
    'Z': '[EQ]',
}

# Complement mapping for nucleotides
COMPLEMENT_MAP = str.maketrans('ATCGRYSWMKVHDB<>', 'TAGCYRSWKMBDHV><')


class PatmatchConverter:
    """Convert Patmatch patterns to nrgrep patterns."""

    def __init__(self, pattern_class: str, debug: bool = False):
        """
        Initialize converter.

        Args:
            pattern_class: One of NUCLEOTIDE, PEPTIDE, or COMPLEMENT
            debug: Enable debug output
        """
        self.pattern_class = pattern_class
        self.debug = debug

    def convert(self, pattern: str) -> str:
        """
        Convert a Patmatch pattern to nrgrep pattern.

        Args:
            pattern: Patmatch pattern string

        Returns:
            nrgrep pattern string
        """
        pattern = self._prepare_pattern(pattern)
        pattern = self._fix_wildcards(pattern)
        pattern = self._fix_repetitions(pattern)
        pattern = self._substitute_characters(pattern)
        pattern = self._finalize_pattern(pattern)
        return pattern

    def _prepare_pattern(self, pattern: str) -> str:
        """Make uppercase, remove spaces, get reverse complement if needed."""
        pattern = ''.join(pattern.split())  # Remove whitespace
        pattern = pattern.upper()

        if self.pattern_class == COMPLEMENT:
            pattern = self._get_reverse_complement(pattern)

        return pattern

    def _fix_wildcards(self, pattern: str) -> str:
        """Substitute Patmatch wildcards with nrgrep wildcards."""
        if self.pattern_class == PEPTIDE:
            pattern = pattern.replace('X', '.')
        else:  # nucleotide
            pattern = pattern.replace('N', '.').replace('X', '.')
        return pattern

    def _fix_repetitions(self, pattern: str) -> str:
        """Convert Patmatch repetitions {m}, {m,}, {,m}, {m,n} to nrgrep."""
        if '{' not in pattern:
            return pattern

        nrgrep = []
        patmatch = list(pattern)

        for char in patmatch:
            if char == '}':
                nrgrep = self._process_repetition(nrgrep)
            else:
                nrgrep.append(char)

        return ''.join(nrgrep)

    def _process_repetition(self, nrgrep: list) -> list:
        """Process a repetition pattern."""
        rep_info = self._extract_repetition_info(nrgrep)
        repeat_pattern = self._extract_repeat_pattern(nrgrep)
        self._append_nrgrep_repeats(nrgrep, rep_info, repeat_pattern)
        return nrgrep

    def _extract_repetition_info(self, nrgrep: list) -> str:
        """Extract repetition info from pattern array."""
        rep_info = []
        while nrgrep:
            char = nrgrep.pop()
            if char == '{':
                break
            rep_info.insert(0, char)
        return ''.join(rep_info)

    def _extract_repeat_pattern(self, nrgrep: list) -> str:
        """
        Extract the pattern to repeat.

        Examples:
            ATG -> G
            AT(TATA) -> (TATA)
            AT[TAG] -> [TAG]
        """
        if not nrgrep:
            return ''

        char = nrgrep.pop()

        if char in ')':
            return self._extract_bracketed_pattern(nrgrep, '(', ')')
        elif char == ']':
            return self._extract_bracketed_pattern(nrgrep, '[', ']')
        else:
            return char

    def _extract_bracketed_pattern(
        self, nrgrep: list, left: str, right: str
    ) -> str:
        """Extract a bracketed pattern like (xxx) or [xxx]."""
        bracket_stack = [right]
        repeat = [right]

        while bracket_stack:
            if not nrgrep:
                break
            char = nrgrep.pop()
            repeat.insert(0, char)
            if char == right:
                bracket_stack.append(char)
            elif char == left:
                bracket_stack.pop()

        return ''.join(repeat)

    def _append_nrgrep_repeats(
        self, nrgrep: list, repeat_info: str, repeat_pattern: str
    ) -> None:
        """Append nrgrep repeat pattern to array."""
        lower, upper = self._process_repeat_info(repeat_info)
        repeats = self._build_nrgrep_repeat(lower, upper, repeat_pattern)
        nrgrep.append(repeats)

    def _process_repeat_info(self, repeat_info: str) -> Tuple[int, int]:
        """
        Process repeat info to determine bounds.

        Formats: m, ,m, m,, m,n
        """
        lower = 0
        upper = 0

        if repeat_info.startswith(','):
            # ,m format
            upper = int(repeat_info[1:])
        elif repeat_info.endswith(','):
            # m, format
            lower = int(repeat_info[:-1])
            upper = INFINITE
        elif ',' in repeat_info:
            # m,n format
            parts = repeat_info.split(',')
            lower = int(parts[0])
            upper = int(parts[1])
        else:
            # m format
            lower = int(repeat_info)
            upper = int(repeat_info)

        return lower, upper

    def _build_nrgrep_repeat(
        self, lower: int, upper: int, pattern: str
    ) -> str:
        """Build nrgrep repeat pattern."""
        repeat_parts = []

        # Add pattern 'lower' times
        for _ in range(lower):
            repeat_parts.append(pattern)

        # Handle upper bound
        if upper == INFINITE:
            repeat_parts.append(f"{pattern}*")
        else:
            # Add optional patterns for upper - lower times
            for _ in range(upper - lower):
                repeat_parts.append(f"{pattern}?")

        return ''.join(repeat_parts)

    def _substitute_characters(self, pattern: str) -> str:
        """Substitute IUPAC wildcard characters with subsets."""
        if self.pattern_class == PEPTIDE:
            for code, replacement in PEPTIDE_IUPAC.items():
                pattern = pattern.replace(code, replacement)
        else:  # nucleotide
            for code, replacement in NUCLEOTIDE_IUPAC.items():
                pattern = pattern.replace(code, replacement)

        pattern = self._remove_nested_brackets(pattern)
        return pattern

    def _remove_nested_brackets(self, pattern: str) -> str:
        """
        Remove nested brackets and duplicate characters within brackets.

        Examples:
            TA[A[CT]] -> TA[ACT]
            TA[ATAG] -> TA[ATG]
        """
        result = []
        bracket_stack = []
        char_set = set()

        for char in pattern:
            if char == '[':
                if not bracket_stack:
                    result.append(char)
                bracket_stack.append(char)
            elif char == ']':
                bracket_stack.pop()
                if not bracket_stack:
                    result.append(char)
                    char_set.clear()
            else:
                if not bracket_stack:
                    result.append(char)
                else:
                    if char not in char_set:
                        result.append(char)
                        char_set.add(char)

        return ''.join(result)

    def _finalize_pattern(self, pattern: str) -> str:
        """Add parentheses and convert anchors to nrgrep format."""
        has_start_anchor = pattern.startswith('<')
        has_end_anchor = pattern.endswith('>')

        # Remove anchors
        if has_start_anchor:
            pattern = pattern[1:]
        if has_end_anchor:
            pattern = pattern[:-1]

        # Add parentheses and anchors
        if has_start_anchor and has_end_anchor:
            pattern = f'^({pattern})$'
        elif has_start_anchor:
            pattern = f'^({pattern})'
        elif has_end_anchor:
            pattern = f'({pattern})$'
        else:
            pattern = f'({pattern})'

        return pattern

    def _get_reverse_complement(self, pattern: str) -> str:
        """Get reverse complement of a pattern."""
        pattern = self._complement_nucleotides(pattern)
        pattern = self._reverse_pattern(pattern)
        if self.debug:
            print(f"DEBUG: Reverse complement: {pattern}", file=sys.stderr)
        return pattern

    def _complement_nucleotides(self, pattern: str) -> str:
        """Complement nucleotides in pattern."""
        # Translate nucleotides
        pattern = pattern.translate(COMPLEMENT_MAP)

        # Swap anchors (< becomes >, > becomes <)
        # Note: already handled in COMPLEMENT_MAP
        return pattern

    def _reverse_pattern(self, pattern: str) -> str:
        """Reverse a pattern while preserving groupings."""
        chars = list(pattern)
        result = []

        while chars:
            char = chars.pop()
            if char in ')]}':
                group = self._extract_group_reverse(char, chars)
                result.append(group)
            else:
                result.append(char)

        return ''.join(result)

    def _extract_group_reverse(self, closer: str, chars: list) -> str:
        """Extract a group for reverse complement."""
        opener = {'(': ')', '[': ']', '{': '}',
                  ')': '(', ']': '[', '}': '{'}[closer]

        group = [closer]
        internal = []

        while chars:
            char = chars.pop()
            if char == opener:
                if opener != '{':
                    # For () and [], reverse internal content
                    group.insert(0, ''.join(internal))
                    group.insert(0, char)
                    break
                else:
                    # For {}, get the character/group being repeated
                    group.insert(0, char)
                    if chars:
                        repeater = chars.pop()
                        if repeater in ')]}':
                            inner_group = self._extract_group_reverse(
                                repeater, chars
                            )
                            group.insert(0, inner_group)
                        else:
                            group.insert(0, repeater)
                    break
            elif char in ')]}':
                inner = self._extract_group_reverse(char, chars)
                internal.append(inner)
            else:
                if closer == '}':
                    group.insert(0, char)
                else:
                    internal.append(char)

        return ''.join(group)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Convert Patmatch patterns to nrgrep patterns"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-n",
        dest="nucleotide",
        action="store_true",
        help="Nucleotide pattern",
    )
    group.add_argument(
        "-p",
        dest="peptide",
        action="store_true",
        help="Protein pattern",
    )
    group.add_argument(
        "-c",
        dest="complement",
        action="store_true",
        help="Reverse complement nucleotide pattern",
    )

    parser.add_argument(
        "pattern",
        help="Patmatch pattern to convert",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    # Determine pattern class
    if args.nucleotide:
        pattern_class = NUCLEOTIDE
    elif args.peptide:
        pattern_class = PEPTIDE
    else:
        pattern_class = COMPLEMENT

    converter = PatmatchConverter(pattern_class, args.debug)
    result = converter.convert(args.pattern)
    print(result, end='')


if __name__ == "__main__":
    main()
