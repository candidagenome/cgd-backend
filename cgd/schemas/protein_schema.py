from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class ProteinInfoOut(BaseModel):
    protein_length: typing.Optional[int] = None
    molecular_weight: typing.Optional[int] = None
    pi: typing.Optional[float] = None  # Isoelectric point
    cai: typing.Optional[float] = None  # Codon Adaptation Index
    codon_bias: typing.Optional[float] = None
    fop_score: typing.Optional[float] = None  # Frequency of optimal codons
    n_term_seq: typing.Optional[str] = None
    c_term_seq: typing.Optional[str] = None
    gravy_score: typing.Optional[float] = None
    aromaticity_score: typing.Optional[float] = None
    amino_acids: typing.Optional[dict[str, int]] = None  # {"ala": 10, "arg": 5, ...}


class ProteinDetailsForOrganism(BaseModel):
    locus_display_name: str
    protein_info: typing.Optional[ProteinInfoOut] = None


class ProteinDetailsResponse(BaseModel):
    """
    {
      "results": {
        "Candida albicans": { "locus_display_name": "ACT1", "protein_info": {...} },
        "Candida glabrata": { ... }
      }
    }
    """
    results: dict[str, ProteinDetailsForOrganism]
