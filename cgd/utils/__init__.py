"""
CGD Utility Library.

This package provides reusable utility functions for CGD scripts and applications.

Modules:
--------
logging_setup
    Logging configuration utilities.
config
    Environment and configuration management.
fasta
    FASTA file reading and writing.
compression
    File compression and archiving utilities.
sequence
    DNA/protein sequence manipulation.
ids
    ID formatting utilities (GO IDs, chromosome names, etc.).
database
    Common database query utilities.
notifications
    Email and notification utilities.
file_io
    General file I/O utilities.
"""

from cgd.utils.logging_setup import setup_logging, get_logger
from cgd.utils.config import load_config, get_config_value, Config
from cgd.utils.fasta import read_fasta, write_fasta, format_fasta_entry
from cgd.utils.compression import compress_file, decompress_file, archive_file
from cgd.utils.sequence import reverse_complement, translate_dna, extract_subsequence
from cgd.utils.ids import format_goid, normalize_chromosome_name
from cgd.utils.database import (
    get_organism_no,
    get_seq_source,
    get_strain_config,
    CachedLookup,
)
from cgd.utils.notifications import send_email, send_error_email
from cgd.utils.file_io import (
    read_tab_delimited,
    write_tab_delimited,
    open_file,
    ensure_directory,
)

__all__ = [
    # logging_setup
    "setup_logging",
    "get_logger",
    # config
    "load_config",
    "get_config_value",
    "Config",
    # fasta
    "read_fasta",
    "write_fasta",
    "format_fasta_entry",
    # compression
    "compress_file",
    "decompress_file",
    "archive_file",
    # sequence
    "reverse_complement",
    "translate_dna",
    "extract_subsequence",
    # ids
    "format_goid",
    "normalize_chromosome_name",
    # database
    "get_organism_no",
    "get_seq_source",
    "get_strain_config",
    "CachedLookup",
    # notifications
    "send_email",
    "send_error_email",
    # file_io
    "read_tab_delimited",
    "write_tab_delimited",
    "open_file",
    "ensure_directory",
]
