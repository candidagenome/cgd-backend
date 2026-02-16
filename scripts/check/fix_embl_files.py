#!/usr/bin/env python3
"""
Fix EMBL files with unquoted multiline remarks.

This script fixes EMBL files that have unquoted multiline /remarks= fields,
which can cause BioPython parsing errors.

Original Perl: fixEmblFiles.pl
Converted to Python: 2024
"""

import argparse
import re
import sys
from pathlib import Path


def fix_embl_file(input_file: Path, output_file: Path = None) -> int:
    """
    Fix unquoted remarks in EMBL file.

    Args:
        input_file: Input EMBL file
        output_file: Output file (default: input_file.out)

    Returns:
        Number of remarks fields fixed
    """
    if output_file is None:
        output_file = input_file.with_suffix(input_file.suffix + '.out')

    fixes = 0
    lines_buffer = []
    in_remarks = False

    with open(input_file) as f_in, open(output_file, 'w') as f_out:
        for line in f_in:
            line = line.rstrip('\n')

            if '/remarks=$' in line:
                # Empty remarks - add quotes
                line = line + '""'
                f_out.write(line + '\n')
                fixes += 1

            elif '/remarks=' in line and not line.endswith('"'):
                # Start of multiline remarks - add opening quote
                line = line.replace('/remarks=', '/remarks="')
                lines_buffer.append(line)
                in_remarks = True

            elif in_remarks:
                # Check if we've reached next feature or sequence section
                if re.match(r'^FT   \w', line) or line.startswith('SQ   '):
                    # Previous line was last of remarks - add closing quote
                    if lines_buffer:
                        lines_buffer[-1] += '"'
                        fixes += 1

                    # Write buffered remarks lines
                    for buf_line in lines_buffer:
                        f_out.write(buf_line + '\n')
                    lines_buffer = []
                    in_remarks = False

                    # Write current line
                    f_out.write(line + '\n')
                else:
                    # Still in remarks - buffer the line
                    lines_buffer.append(line)

            else:
                # Regular line - write directly
                f_out.write(line + '\n')

    return fixes


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Fix EMBL files with unquoted multiline remarks"
    )
    parser.add_argument(
        "input_files",
        type=Path,
        nargs="+",
        help="Input EMBL file(s)",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Modify files in place (backup created as .bak)",
    )
    parser.add_argument(
        "--output-suffix",
        default=".out",
        help="Suffix for output files (default: .out)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    total_fixes = 0

    for input_file in args.input_files:
        if not input_file.exists():
            print(f"Warning: File not found: {input_file}", file=sys.stderr)
            continue

        if args.in_place:
            # Create backup
            backup = input_file.with_suffix(input_file.suffix + '.bak')
            input_file.rename(backup)
            output_file = input_file
            actual_input = backup
        else:
            output_file = input_file.with_suffix(input_file.suffix + args.output_suffix)
            actual_input = input_file

        fixes = fix_embl_file(actual_input, output_file)
        total_fixes += fixes

        if args.verbose:
            print(f"{input_file}: {fixes} fixes applied -> {output_file}")

    if args.verbose:
        print(f"\nTotal fixes applied: {total_fixes}")


if __name__ == "__main__":
    main()
