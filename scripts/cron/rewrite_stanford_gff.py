#!/usr/bin/env python3
"""
Rewrite GFF files to a more standard format.

This script rewrites GFF files generated for CGD/AspGD to more closely match
standard GFF3 format. It:
- Adds GFF3 ID attributes to features that don't have them
- Converts each ORF feature to gene + mRNA features
- Optionally replaces multiple CDS features with a single CDS
- Optionally creates exon features from CDS/UTR features

Based on rewrite-stanford-gff.pl by Jonathan Crabtree.
Modified by Prachi Shah (Oct 10, 2012).

Usage:
    python rewrite_stanford_gff.py --file input.gff
    python rewrite_stanford_gff.py --file input.gff --make-single-cds --print-exons
    python rewrite_stanford_gff.py --file input.gff --output output.gff

Options:
    --file: Input GFF file
    --make-single-cds: Replace multiple CDS with a single CDS spanning introns
    --print-exons: Print exon features derived from CDS/UTR
    --output: Output file (default: stdout)
"""

import argparse
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)


@dataclass
class GFFFeature:
    """Represents a GFF feature."""

    line: str
    seqid: str
    source: str
    type: str
    start: int
    end: int
    score: str
    strand: str
    phase: str
    attributes: str


@dataclass
class GeneModel:
    """Stores features for a gene model."""

    orf_feat: GFFFeature | None = None
    orf_line_num: int = 0
    cds_feats: list[GFFFeature] = field(default_factory=list)
    intron_feats: list[GFFFeature] = field(default_factory=list)
    exon_feats: list[GFFFeature] = field(default_factory=list)
    utr_feats: list[GFFFeature] = field(default_factory=list)

    def clear(self):
        """Clear all features."""
        self.orf_feat = None
        self.orf_line_num = 0
        self.cds_feats = []
        self.intron_feats = []
        self.exon_feats = []
        self.utr_feats = []


def parse_gff_line(line: str) -> GFFFeature:
    """Parse a GFF line into a GFFFeature object."""
    fields = line.split("\t")
    if len(fields) != 9:
        raise ValueError(f"Wrong number of fields: {len(fields)}")

    return GFFFeature(
        line=line,
        seqid=fields[0],
        source=fields[1],
        type=fields[2],
        start=int(fields[3]),
        end=int(fields[4]),
        score=fields[5],
        strand=fields[6],
        phase=fields[7],
        attributes=fields[8],
    )


def get_id_from_attributes(attributes: str) -> str | None:
    """Extract ID from GFF attributes."""
    match = re.search(r"ID=([^;]+)", attributes)
    return match.group(1) if match else None


def format_gff_line(feat: GFFFeature) -> str:
    """Format a GFFFeature as a GFF line."""
    return "\t".join([
        feat.seqid,
        feat.source,
        feat.type,
        str(feat.start),
        str(feat.end),
        feat.score,
        feat.strand,
        feat.phase,
        feat.attributes,
    ])


def remove_utr_introns(
    oid: str,
    introns: list[GFFFeature],
    orf_line_num: int,
) -> tuple[list[GFFFeature], int]:
    """
    Remove UTR introns, keeping only coding introns.

    Returns (filtered_introns, error_count).
    """
    if not introns:
        return [], 0

    errors = 0
    introns_only = []

    strand = introns[0].strand
    if strand not in ("+", "-"):
        logger.error(f"Problem parsing intron strand (got '{strand}' for {oid} at line {orf_line_num})")
        return [], 1

    # Sort introns by position
    if strand == "-":
        sorted_introns = sorted(introns, key=lambda x: x.start, reverse=True)
    else:
        sorted_introns = sorted(introns, key=lambda x: x.start)

    intron_seen = False
    three_prime_utr_intron_seen = False

    for feat in sorted_introns:
        if feat.type in ("intron", "gap"):
            if three_prime_utr_intron_seen:
                logger.error(f"{feat.type} appears after three_prime_UTR_intron for {oid} at line {orf_line_num}")
                errors += 1
            intron_seen = True
            introns_only.append(feat)
        elif feat.type == "five_prime_UTR_intron":
            if intron_seen:
                logger.error(f"five_prime_UTR_intron appears after intron for {oid} at line {orf_line_num}")
                errors += 1
        elif feat.type == "three_prime_UTR_intron":
            three_prime_utr_intron_seen = True
        else:
            logger.error(f"Unexpected intron type ({feat.type}) for {oid} at line {orf_line_num}")

    return introns_only, errors


def process_rna(
    model: GeneModel,
    output_lines: list[str],
) -> int:
    """
    Process RNA features (tRNA, ncRNA, etc.).

    Returns error count.
    """
    if not model.orf_feat:
        return 0

    errors = 0
    feat = model.orf_feat
    oid = get_id_from_attributes(feat.attributes)

    if model.cds_feats:
        logger.error(f"Found unexpected CDS feature(s) for RNA {oid} at line {model.orf_line_num}")
        return 1

    # Print the RNA feature
    output_lines.append(format_gff_line(feat))

    # Remove UTR introns
    intron_feats_only, err = remove_utr_introns(oid, model.intron_feats, model.orf_line_num)
    errors += err

    # Combine exons and introns
    subfeats = model.exon_feats + intron_feats_only

    # Sort by position
    if feat.strand == "-":
        sorted_subfeats = sorted(subfeats, key=lambda x: x.start, reverse=True)
    else:
        sorted_subfeats = sorted(subfeats, key=lambda x: x.start)

    last_start = None
    last_end = None
    enum = 1
    local_errors = []

    for i, sf in enumerate(sorted_subfeats):
        is_even = (i % 2) == 0

        if sf.type.endswith("exon"):
            if not is_even:
                local_errors.append(
                    f"Expected intron at position {i} of gene {oid} at line {model.orf_line_num}, found {sf.type}"
                )
            pid = oid if sf.type == "noncoding_exon" else f"{oid}-T"
            new_attrs = f"ID={pid}-T-E{enum};Parent={pid}"
            output_lines.append(
                "\t".join([
                    sf.seqid, sf.source, sf.type, str(sf.start), str(sf.end),
                    sf.score, sf.strand, sf.phase, new_attrs
                ])
            )
            enum += 1
        elif sf.type in ("intron", "gap"):
            if is_even:
                local_errors.append(
                    f"Expected exon at position {i} of gene {oid} at line {model.orf_line_num}, found {sf.type}"
                )
            # Validate intron attributes
            match = re.match(r"^Parent=([^;]+)(;parent_feature_type=[^;]+)?$", sf.attributes)
            if match:
                if match.group(1) != oid:
                    logger.error(f"Parent={match.group(1)} doesn't match ORF id {oid}")
            else:
                logger.error(f"Unexpected attributes in intron: '{sf.attributes}'")
        else:
            logger.error(f"Unexpected feat type {sf.type}")

        # Check coordinate consistency
        if feat.strand == "+" and last_end is not None:
            if sf.start != last_end + 1:
                local_errors.append(f"{sf.type} start={sf.start}, expected {last_end + 1} in gene {oid}")
        elif feat.strand == "-" and last_start is not None:
            if sf.end != last_start - 1:
                local_errors.append(f"{sf.type} end={sf.end}, expected {last_start - 1} in gene {oid}")

        last_start = sf.start
        last_end = sf.end

    if local_errors:
        logger.error(f"Errors found for {oid}")
        for err in local_errors:
            logger.error(err)
        errors += len(local_errors)

    return errors


def process_orf(
    model: GeneModel,
    output_lines: list[str],
    make_single_cds: bool,
    print_exons: bool,
) -> int:
    """
    Process ORF features.

    Returns error count.
    """
    if not model.orf_feat:
        return 0

    errors = 0
    feat = model.orf_feat
    oid = get_id_from_attributes(feat.attributes)

    if not oid:
        logger.error(f"Unable to parse ID from ORF at line {model.orf_line_num}")
        return 1

    # Extend ORF coords using UTRs
    gstart = feat.start
    gend = feat.end

    utrs_by_start = {}
    utrs_by_end = {}
    utrs = {}

    for uf in model.utr_feats:
        if uf.start < gstart:
            gstart = uf.start
        if uf.end > gend:
            gend = uf.end
        utrs_by_start[uf.start] = uf
        utrs_by_end[uf.end] = uf
        utrs[id(uf)] = uf

    # Gene feature
    gtype = "pseudogene" if feat.type == "pseudogene" else "gene"
    if feat.type == "uORF":
        gtype = "gene"

    output_lines.append(
        "\t".join([
            feat.seqid, feat.source, gtype, str(gstart), str(gend),
            feat.score, feat.strand, feat.phase, feat.attributes
        ])
    )

    # mRNA feature
    if "Parent" in feat.attributes:
        logger.error(f"ORF already has parent at line {model.orf_line_num}")
        return 1

    mrna_attrs = re.sub(r"ID=([^;]+);", rf"ID=\1-T;Parent={oid};", feat.attributes)
    output_lines.append(
        "\t".join([
            feat.seqid, feat.source, "mRNA", str(gstart), str(gend),
            feat.score, feat.strand, feat.phase, mrna_attrs
        ])
    )

    # Process CDS features
    cds_min = None
    cds_max = None
    cds_orf_classification = None

    for cds in model.cds_feats:
        if cds_min is None or cds.start < cds_min:
            cds_min = cds.start
        if cds_max is None or cds.end > cds_max:
            cds_max = cds.end

        if cds.score != ".":
            logger.error(f"Unexpected CDS score '{cds.score}'")
        if cds.phase != ".":
            logger.error(f"Unexpected CDS phase '{cds.phase}'")
        if cds.strand != feat.strand:
            logger.error(f"CDS strand doesn't match ORF at line {model.orf_line_num}")
        if cds.seqid != feat.seqid:
            logger.error(f"CDS seqid ({cds.seqid}) doesn't match ORF ({feat.seqid})")

        # Parse orf_classification
        oc_match = re.search(r"orf_classification=([^;]+)", cds.attributes)
        oc = oc_match.group(1) if oc_match else None

        if feat.type == "uORF":
            oc = "Uncharacterized"

        if make_single_cds:
            if oc is None and gtype != "pseudogene" and feat.type != "uORF":
                logger.error(f"Couldn't parse orf_classification for non-pseudogene CDS")

            if cds_orf_classification is not None and oc != cds_orf_classification:
                logger.error(f"CDS orf_classification mismatch ('{oc}' vs. '{cds_orf_classification}')")
            else:
                cds_orf_classification = oc

    # Validate CDS coords
    if cds_min is None or cds_max is None:
        logger.error(f"Couldn't parse CDS min or max for ORF {oid} at line {model.orf_line_num}")
        return 1

    if make_single_cds and cds_orf_classification is None and gtype != "pseudogene":
        logger.error(f"Couldn't parse CDS orf classification for ORF {oid}")

    # Check CDS coords match ORF
    if cds_min != feat.start:
        logger.error(f"Minimum CDS coordinate ({cds_min}) not equal to min ORF coordinate ({feat.start}) for ORF {oid}")
        errors += 1

    if cds_max != feat.end:
        logger.error(f"Maximum CDS coordinate ({cds_max}) not equal to max ORF coordinate ({feat.end}) for ORF {oid}")
        errors += 1

    # Print single CDS if requested
    if make_single_cds:
        oclass = f";orf_classification={cds_orf_classification}" if cds_orf_classification else ""
        output_lines.append(
            "\t".join([
                feat.seqid, feat.source, "CDS", str(cds_min), str(cds_max),
                feat.score, feat.strand, feat.phase,
                f"ID={oid}-P;Parent={oid}-T{oclass}"
            ])
        )

    # Sort CDS and intron features
    intron_feats_only, err = remove_utr_introns(oid, model.intron_feats, model.orf_line_num)
    errors += err

    cds_and_introns = model.cds_feats + intron_feats_only

    if feat.strand == "-":
        sorted_features = sorted(cds_and_introns, key=lambda x: x.start, reverse=True)
    else:
        sorted_features = sorted(cds_and_introns, key=lambda x: x.start)

    last_start = None
    last_end = None
    local_errors = []
    exons = []

    for i, sf in enumerate(sorted_features):
        is_even = (i % 2) == 0

        if sf.type == "CDS":
            if not is_even and oid != "CAGL0G07183g":
                local_errors.append(
                    f"Expected intron at position {i} of gene {oid}, found {sf.type}"
                )

            if print_exons:
                estart = sf.start
                eend = sf.end

                # Check if exon can be extended by UTR
                if sf.start - 1 in utrs_by_end:
                    u1 = utrs_by_end[sf.start - 1]
                    estart = u1.start
                    utrs.pop(id(u1), None)

                if sf.end + 1 in utrs_by_start:
                    u2 = utrs_by_start[sf.end + 1]
                    eend = u2.end
                    utrs.pop(id(u2), None)

                exon_feat = GFFFeature(
                    line="",
                    seqid=sf.seqid,
                    source=sf.source,
                    type="exon",
                    start=estart,
                    end=eend,
                    score=sf.score,
                    strand=sf.strand,
                    phase=sf.phase,
                    attributes=f"ID={oid}-T-E;Parent={oid}-T",
                )
                exons.append(exon_feat)

            if not make_single_cds:
                # Update parent to mRNA
                new_attrs = re.sub(r"Parent=([^;]+)", r"Parent=\1-T", sf.attributes)
                output_lines.append(
                    "\t".join([
                        sf.seqid, sf.source, sf.type, str(sf.start), str(sf.end),
                        sf.score, sf.strand, sf.phase, f"ID={oid}-P;{new_attrs}"
                    ])
                )

        elif sf.type in ("intron", "gap"):
            if is_even:
                local_errors.append(
                    f"Expected CDS at position {i} of gene {oid}, found {sf.type}"
                )

            # Validate intron attributes
            match = re.match(r"^Parent=([^;]+)(;parent_feature_type=[^;]+)?$", sf.attributes)
            if match:
                if match.group(1) != oid:
                    logger.error(f"Parent={match.group(1)} doesn't match ORF id {oid}")
            else:
                logger.error(f"Unexpected attributes in intron: '{sf.attributes}'")
        else:
            logger.error(f"Unexpected feat type {sf.type}")

        # Check coordinate consistency
        if feat.strand == "+" and last_end is not None:
            if sf.start != last_end + 1:
                local_errors.append(f"{sf.type} start={sf.start}, expected {last_end + 1} in gene {oid}")
        elif feat.strand == "-" and last_start is not None:
            if sf.end != last_start - 1 and oid != "CAGL0G07183g":
                local_errors.append(f"{sf.type} end={sf.end}, expected {last_start - 1} in gene {oid}")

        last_start = sf.start
        last_end = sf.end

    # Add exons for unaccounted UTRs
    for uf in utrs.values():
        exon_feat = GFFFeature(
            line="",
            seqid=uf.seqid,
            source=uf.source,
            type="exon",
            start=uf.start,
            end=uf.end,
            score=uf.score,
            strand=uf.strand,
            phase=uf.phase,
            attributes=f"ID={oid}-T-E;Parent={oid}-T",
        )
        exons.append(exon_feat)

    # Sort and print exons
    if print_exons and exons:
        if feat.strand == "-":
            sorted_exons = sorted(exons, key=lambda x: x.start, reverse=True)
        else:
            sorted_exons = sorted(exons, key=lambda x: x.start)

        for i, exon in enumerate(sorted_exons, 1):
            new_attrs = exon.attributes.replace("-E;", f"-E{i};")
            output_lines.append(
                "\t".join([
                    exon.seqid, exon.source, exon.type, str(exon.start), str(exon.end),
                    exon.score, exon.strand, exon.phase, new_attrs
                ])
            )

    if local_errors:
        logger.error(f"Errors found for {oid}")
        for err in local_errors:
            logger.error(err)

    return errors


def process_orf_or_rna(
    model: GeneModel,
    output_lines: list[str],
    make_single_cds: bool,
    print_exons: bool,
) -> int:
    """
    Process either ORF or RNA feature based on type.

    Returns error count.
    """
    if not model.orf_feat:
        return 0

    feat = model.orf_feat
    attrs = feat.attributes

    # Determine if this is an RNA or ORF
    rna_types = ("tRNA", "ncRNA", "rRNA", "snoRNA", "snRNA", "repeat_region")

    if feat.type in rna_types or "tRNAscan" in attrs:
        return process_rna(model, output_lines)
    elif feat.type in ("ORF", "uORF"):
        return process_orf(model, output_lines, make_single_cds, print_exons)
    elif feat.type == "pseudogene":
        # Pseudogenes can be coding or non-coding
        if model.cds_feats:
            return process_orf(model, output_lines, make_single_cds, print_exons)
        else:
            return process_rna(model, output_lines)
    else:
        logger.error(f"Don't know how to handle gene feature of type '{feat.type}'")
        return 1


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Rewrite GFF files to more standard format"
    )
    parser.add_argument(
        "--file",
        required=True,
        type=Path,
        help="Input GFF file",
    )
    parser.add_argument(
        "--make-single-cds",
        action="store_true",
        help="Replace multiple CDS with a single spanning CDS",
    )
    parser.add_argument(
        "--print-exons",
        action="store_true",
        help="Print exon features derived from CDS/UTR",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=None,
        help="Log file path",
    )

    args = parser.parse_args()

    if not args.file.exists():
        logger.error(f"Input file does not exist: {args.file}")
        return 1

    # Set up output
    if args.output:
        out_file = open(args.output, "w")
    else:
        out_file = sys.stdout

    model = GeneModel()
    output_lines: list[str] = []
    total_errors = 0
    lines_processed = 0
    lnum = 0
    in_fasta = False

    # Top-level feature types
    top_level_types = {
        "ORF", "tRNA", "pseudogene", "uORF", "ncRNA",
        "rRNA", "snoRNA", "snRNA", "repeat_region"
    }

    # Pass-through types
    passthrough_types = {
        "chromosome", "contig", "long_terminal_repeat",
        "blocked_reading_frame", "centromere", "retrotransposon", "LTR"
    }

    try:
        with open(args.file) as f:
            for line in f:
                line = line.rstrip("\n\r")
                lnum += 1

                # Handle FASTA section and comments
                if line.startswith("##FASTA") or in_fasta or line.startswith("#"):
                    if line.startswith("##FASTA"):
                        in_fasta = True
                    out_file.write(line + "\n")
                    lines_processed += 1
                    continue

                # Parse GFF feature
                try:
                    feat = parse_gff_line(line)
                except ValueError as e:
                    logger.error(f"Error at line {lnum}: {e}")
                    continue

                # Handle top-level gene features
                if feat.type in top_level_types:
                    # Process previous gene model
                    errors = process_orf_or_rna(
                        model, output_lines,
                        args.make_single_cds, args.print_exons
                    )
                    total_errors += errors

                    # Write buffered output
                    for out_line in output_lines:
                        out_file.write(out_line + "\n")
                    output_lines.clear()

                    # Start new gene model
                    model.clear()
                    model.orf_feat = feat
                    model.orf_line_num = lnum

                elif feat.type == "CDS":
                    if not model.orf_feat:
                        logger.error(f"CDS at line {lnum} with no preceding ORF feature")
                    else:
                        model.cds_feats.append(feat)

                elif feat.type in ("intron", "gap", "five_prime_UTR_intron", "three_prime_UTR_intron"):
                    if not model.orf_feat:
                        logger.error(f"{feat.type} at line {lnum} with no preceding ORF feature")
                    else:
                        model.intron_feats.append(feat)

                elif feat.type == "noncoding_exon":
                    if not model.orf_feat:
                        logger.error(f"noncoding_exon at line {lnum} with no preceding RNA feature")
                    else:
                        model.exon_feats.append(feat)

                elif feat.type in ("five_prime_UTR", "three_prime_UTR"):
                    if not model.orf_feat:
                        logger.error(f"{feat.type} at line {lnum} with no preceding ORF feature")
                    else:
                        model.utr_feats.append(feat)

                elif feat.type in passthrough_types:
                    out_file.write(line + "\n")
                    lines_processed += 1

                else:
                    logger.error(f"Unexpected feature type ({feat.type}) at line {lnum}")

        # Process final gene model
        errors = process_orf_or_rna(
            model, output_lines,
            args.make_single_cds, args.print_exons
        )
        total_errors += errors

        # Write remaining output
        for out_line in output_lines:
            out_file.write(out_line + "\n")

        logger.info(f"Processed {lines_processed}/{lnum} lines from {args.file}, found {total_errors} error(s)")

        return total_errors

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    finally:
        if args.output and out_file:
            out_file.close()


if __name__ == "__main__":
    sys.exit(main())
