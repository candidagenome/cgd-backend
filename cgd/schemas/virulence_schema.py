"""
Virulence Factor Browser Schemas - Pydantic models for virulence factor API.
"""
from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# PHENOTYPE EVIDENCE TIER DEFINITIONS
# =============================================================================

# Evidence tiers rank phenotype evidence by biological relevance to virulence
# Tier 1 = most direct virulence evidence, Tier 4 = indirect/weak evidence
PHENOTYPE_EVIDENCE_TIERS = {
    1: {
        "name": "Direct Virulence",
        "description": "Direct evidence of virulence/pathogenesis",
        "patterns": [
            "%virulence%", "%pathogenesis%", "%host killing%",
            "%infection%", "%lethality%", "%colonization%"
        ],
        "score_contribution": 5,
    },
    2: {
        "name": "Host Interaction",
        "description": "Host cell/tissue interactions",
        "patterns": [
            "%host cell%", "%phagocytosis%", "%macrophage%",
            "%epithelial%", "%endothelial%", "%invasion%",
            "%galleria%", "%mouse%", "%animal model%"
        ],
        "score_contribution": 4,
    },
    3: {
        "name": "Stress Response",
        "description": "Stress resistance that may enable survival in host",
        "patterns": [
            "%oxidative stress%", "%heat shock%", "%antifungal%",
            "%azole%", "%echinocandin%"
        ],
        "score_contribution": 2,
    },
    4: {
        "name": "Indirect",
        "description": "Broad phenotypes with indirect virulence relevance",
        "patterns": ["%resistance%", "%susceptibility%", "%sensitivity%"],
        "score_contribution": 1,
    },
}

# =============================================================================
# HOUSEKEEPING GENE GO TERMS
# =============================================================================

# GO terms that indicate housekeeping/essential cellular functions
HOUSEKEEPING_GO_TERMS = [
    "GO:0006412",  # translation
    "GO:0006414",  # translational elongation
    "GO:0006260",  # DNA replication
    "GO:0006350",  # transcription
    "GO:0006457",  # protein folding
    "GO:0007049",  # cell cycle
    "GO:0006096",  # glycolysis
    "GO:0006099",  # tricarboxylic acid cycle
    "GO:0015031",  # protein transport
    "GO:0006886",  # intracellular protein transport
    "GO:0000715",  # nucleotide-excision repair
    "GO:0006281",  # DNA repair
]

# =============================================================================
# DIRECT VS INDIRECT EVIDENCE CLASSIFICATION
# =============================================================================

# GO terms that indicate direct virulence evidence
DIRECT_VIRULENCE_GO_TERMS = [
    "pathogenesis", "virulence", "host", "invasion", "adhesion",
    "biofilm", "filament", "hyphal", "morphogenesis",
]

# Phenotype patterns that indicate direct evidence (Tier 1 & 2)
DIRECT_PHENOTYPE_PATTERNS = [
    # Tier 1 - Direct Virulence
    "virulence", "pathogenesis", "host killing", "infection",
    "lethality", "colonization",
    # Tier 2 - Host Interaction
    "host cell", "phagocytosis", "macrophage", "epithelial",
    "endothelial", "invasion", "galleria", "mouse", "animal model",
]


def split_evidence(match_reasons: list[str]) -> tuple[list[str], list[str]]:
    """
    Split match reasons into direct and indirect evidence.

    Direct evidence includes:
    - Virulence model evidence
    - Tier 1 & 2 phenotypes (Direct Virulence, Host Interaction)
    - Pathogenesis/host-related GO terms

    Indirect evidence includes:
    - Tier 3 & 4 phenotypes (Stress Response, Indirect)
    - Gene pattern matches
    - Headline matches
    - Literature topic matches
    - Non-virulence GO terms

    Args:
        match_reasons: List of match reason strings

    Returns:
        Tuple of (direct_evidence, indirect_evidence) lists
    """
    direct = []
    indirect = []

    for reason in match_reasons:
        reason_lower = reason.lower()

        # Virulence model is always direct
        if reason_lower.startswith("virulence model:"):
            direct.append(reason)
            continue

        # Check phenotype evidence
        if reason_lower.startswith("phenotype:"):
            phenotype_text = reason_lower.replace("phenotype:", "").strip()
            is_direct = any(
                pattern in phenotype_text
                for pattern in DIRECT_PHENOTYPE_PATTERNS
            )
            if is_direct:
                direct.append(reason)
            else:
                indirect.append(reason)
            continue

        # Check GO evidence
        if reason_lower.startswith("go:"):
            is_direct = any(
                term in reason_lower
                for term in DIRECT_VIRULENCE_GO_TERMS
            )
            if is_direct:
                direct.append(reason)
            else:
                indirect.append(reason)
            continue

        # Everything else is indirect (gene pattern, headline, literature)
        indirect.append(reason)

    return direct, indirect


# =============================================================================
# CONFIDENCE TIER DEFINITIONS
# =============================================================================

# Simple confidence tiers mapped from 0-20 score
CONFIDENCE_TIERS = {
    "High": {"min_score": 10, "description": "Strong direct virulence evidence"},
    "Medium": {"min_score": 5, "description": "Moderate evidence with host interaction"},
    "Low": {"min_score": 0, "description": "Indirect or weak evidence"},
}


def get_confidence_tier(score: int) -> str:
    """Map a 0-20 confidence score to High/Medium/Low tier."""
    if score >= 10:
        return "High"
    elif score >= 5:
        return "Medium"
    else:
        return "Low"


# =============================================================================
# EVIDENCE-CALIBRATED LANGUAGE SYSTEM
# =============================================================================

# Evidence tiers determine allowed verb strength (scientific precision)
# Based on: confidence level + evidence type (in_vivo, phenotype, GO/KW)
EVIDENCE_LANGUAGE_TIERS = {
    "in_vivo_strong": {
        "description": "in vivo evidence + high confidence",
        "allowed_verbs": ["required for", "promotes", "drives", "mediates"],
        "avoid_verbs": ["may", "suggests", "plays a role in", "is involved in"],
        "ending_phrase": ", with in vivo evidence supporting a role in virulence.",
    },
    "experimental_strong": {
        "description": "phenotype/model evidence + high confidence, no in vivo",
        "allowed_verbs": ["required for", "contributes to", "promotes", "mediates"],
        "avoid_verbs": ["plays a role in", "is involved in"],
        "ending_phrase": None,  # No extra ending for experimental
    },
    "experimental_moderate": {
        "description": "phenotype/model evidence + medium confidence",
        "allowed_verbs": ["is associated with", "has been linked to", "is implicated in", "may contribute to"],
        "avoid_verbs": ["controls", "drives", "is essential for"],
        "ending_phrase": None,  # No ending for moderate evidence
    },
    "annotation_supported": {
        "description": "GO/KW only",
        "allowed_verbs": ["is associated with", "is linked to", "is annotated to", "has annotation support for"],
        "avoid_verbs": ["required for", "contributes to", "pathogenesis", "controls"],
        "ending_phrase": None,  # No ending for annotation-only
    },
    "indirect_low": {
        "description": "low confidence / weak indirect evidence",
        "allowed_verbs": ["has limited evidence for", "shows possible association with", "may be linked to"],
        "avoid_verbs": ["required for", "contributes to", "plays a role in", "controls"],
        "ending_phrase": None,  # No strong ending for weak evidence
    },
}

# Category phrases describe VIRULENCE RELEVANCE, not gene identity
CATEGORY_VIRULENCE_PHRASES = {
    "Adhesins": "host adhesion",
    "Secreted Enzymes": "secreted enzymatic activity",
    "Morphogenesis": "morphogenesis",
    "Host Interaction": "host interaction",
    "Biofilm Formation": "biofilm formation",
    "Immune Evasion": "immune evasion",
    "Drug Resistance": "drug response",
}

# Semantic normalization to deduplicate similar concepts
# Maps phrases to canonical form to avoid "adhesion and host adhesion"
CONCEPT_NORMALIZATION = {
    # Adhesion family - all collapse to "adhesion"
    "biofilm formation": "biofilm",
    "biofilm": "biofilm",
    "cell adhesion": "adhesion",
    "host adhesion": "adhesion",
    "adhesion": "adhesion",
    # Host interaction family
    "host interaction": "host interaction",
    "immune evasion": "host interaction",
    # Other concepts stay distinct
    "secreted enzymatic activity": "enzymatic activity",
    "morphogenesis": "morphogenesis",
    "drug response": "drug response",
    "hyphal growth": "morphogenesis",
    "filamentous growth": "morphogenesis",
}

# Protein-type-specific verb mappings
# Different protein classes use different verbs for scientific accuracy
PROTEIN_TYPE_VERBS = {
    "transcription factor": "regulating",
    "transcriptional regulator": "regulating",
    "zinc finger protein": "regulating",
    "DNA-binding protein": "regulating",
    "kinase": "involved in",
    "phosphatase": "involved in",
    "dehydrogenase": "involved in",
    "oxidase": "involved in",
    "reductase": "involved in",
    "synthase": "involved in",
    "transferase": "involved in",
    "isomerase": "involved in",
    "adhesin": "mediating",
    "cell surface protein": "mediating",
    "GPI-anchored protein": "mediating",
    "membrane protein": "involved in",
    "transporter": "involved in",
    "efflux pump": "involved in",
    "protease": "involved in",
    "secreted aspartyl protease": "involved in",
    "secreted protease": "involved in",
    "lipase": "involved in",
    "phospholipase": "involved in",
    "chaperone": "supporting",
    "heat shock protein": "supporting",
}

# Role normalization for verbose headline-derived phrases
ROLE_NORMALIZATION = {
    "protein that is required for the normal transcriptional response": "transcriptional regulator",
    "protein that plays a role in proteolysis": "protease",
    "protein that is involved in proteolysis": "protease",
    "protein that plays a role in transcription": "transcriptional regulator",
    "protein that is associated with transcription": "transcriptional regulator",
}


def determine_evidence_language_tier(
    confidence_tier: str,
    direct_evidence: list[str],
    indirect_evidence: list[str],
) -> str:
    """
    Determine evidence tier for calibrated language selection.

    Returns one of: in_vivo_strong, experimental_strong, experimental_moderate,
                    annotation_supported, indirect_low
    """
    confidence = confidence_tier.lower() if confidence_tier else "low"
    all_evidence = direct_evidence + indirect_evidence

    # Check for in vivo / virulence model evidence
    has_in_vivo = any(
        "virulence model" in e.lower() or
        "mouse" in e.lower() or
        "galleria" in e.lower() or
        "in vivo" in e.lower()
        for e in all_evidence
    )

    # Check for phenotype evidence
    has_phenotype = any("phenotype:" in e.lower() for e in all_evidence)

    # Check for GO evidence
    has_go = any(e.lower().startswith("go:") for e in all_evidence)

    # Check for keyword/pattern evidence only
    has_kw_only = not has_phenotype and not has_go and len(all_evidence) > 0

    # Determine tier based on evidence + confidence
    if has_in_vivo and confidence == "high":
        return "in_vivo_strong"

    if has_phenotype and confidence == "high":
        return "experimental_strong"

    if has_phenotype and confidence in ("medium", "med"):
        return "experimental_moderate"

    if (has_go or has_kw_only) and not has_phenotype:
        return "annotation_supported"

    return "indirect_low"


def get_verb_for_protein_type(role: str) -> str:
    """
    Get the appropriate verb for a protein type.

    Transcription factors "regulate", enzymes are "involved in",
    adhesins "mediate", etc.
    """
    role_lower = role.lower()
    for protein_type, verb in PROTEIN_TYPE_VERBS.items():
        if protein_type in role_lower:
            return verb
    return "involved in"  # Default for unknown types


def normalize_concept(concept: str) -> str:
    """Normalize a concept to its canonical form for deduplication."""
    concept_lower = concept.lower().strip()
    return CONCEPT_NORMALIZATION.get(concept_lower, concept_lower)


def dedupe_concepts(concepts: list[str]) -> list[str]:
    """
    Deduplicate concepts semantically.

    "adhesion" and "host adhesion" both normalize to "adhesion",
    so only keep the first one encountered.
    """
    seen_normalized = set()
    result = []
    for concept in concepts:
        normalized = normalize_concept(concept)
        if normalized not in seen_normalized:
            seen_normalized.add(normalized)
            result.append(concept)
    return result


def get_virulence_phrase(categories: list[str], max_items: int = 1) -> str | None:
    """
    Get virulence relevance phrase from categories.

    These describe virulence context, NOT gene identity.
    Deduplicates semantically similar concepts (e.g., adhesion + biofilm -> adhesion).
    Limited to max_items=1 by default to avoid repetition.
    """
    phrases = []
    seen_normalized = set()

    for cat in categories:
        if cat in CATEGORY_VIRULENCE_PHRASES:
            phrase = CATEGORY_VIRULENCE_PHRASES[cat]
            # Normalize to detect semantic duplicates
            normalized = CONCEPT_NORMALIZATION.get(phrase, phrase)
            if normalized not in seen_normalized:
                seen_normalized.add(normalized)
                phrases.append(phrase)

    if not phrases:
        return None

    # Limit to max_items (default 1 to keep summaries tight)
    phrases = phrases[:max_items]
    if len(phrases) == 1:
        return phrases[0]
    elif len(phrases) == 2:
        return f"{phrases[0]} and {phrases[1]}"
    else:
        return ", ".join(phrases[:-1]) + f", and {phrases[-1]}"


# =============================================================================
# EVIDENCE TYPE DEFINITIONS
# =============================================================================

# Evidence types for filtering
EVIDENCE_TYPES = {
    "GO": {
        "name": "GO Annotation",
        "description": "Gene Ontology annotation evidence",
        "match_prefixes": ["GO:"],
    },
    "PHE": {
        "name": "Phenotype",
        "description": "Phenotype and virulence model evidence",
        "match_prefixes": ["phenotype:", "virulence model:"],
    },
    "KW": {
        "name": "Keyword",
        "description": "Gene pattern, headline, or literature matches",
        "match_prefixes": ["gene pattern:", "headline:", "literature topic:"],
    },
}


def extract_evidence_types(match_reasons: list[str]) -> list[str]:
    """Extract evidence type codes (GO/PHE/KW) from match reasons."""
    types = set()
    for reason in match_reasons:
        reason_lower = reason.lower()
        for type_code, config in EVIDENCE_TYPES.items():
            for prefix in config["match_prefixes"]:
                if reason_lower.startswith(prefix.lower()):
                    types.add(type_code)
                    break
    return sorted(types)


def generate_inclusion_reason(match_reasons: list[str], categories: list[str]) -> str:
    """
    Generate a short human-readable reason for inclusion.

    Examples:
    - "Virulence model + GO (pathogenesis)"
    - "Gene pattern (ALS family) + Phenotype (adhesion)"
    - "GO (host interaction) | Adhesins, Host Interaction"
    """
    parts = []

    # Group reasons by type
    has_virulence_model = False
    go_terms = []
    phenotypes = []
    gene_patterns = []
    headlines = []
    literature = []

    for reason in match_reasons:
        reason_lower = reason.lower()
        if reason_lower.startswith("virulence model:"):
            has_virulence_model = True
        elif reason_lower.startswith("go:"):
            # Extract just the term name
            term = reason.split("(")[0].replace("GO:", "").strip()
            if term and term not in go_terms:
                go_terms.append(term[:20])  # Truncate long terms
        elif reason_lower.startswith("phenotype:"):
            pheno = reason.replace("phenotype:", "").strip()[:20]
            if pheno and pheno not in phenotypes:
                phenotypes.append(pheno)
        elif reason_lower.startswith("gene pattern:"):
            pattern = reason.replace("gene pattern:", "").strip()
            # Extract family name (e.g., ALS1 -> ALS)
            import re
            family = re.sub(r'\d+$', '', pattern)
            if family and f"{family} family" not in gene_patterns:
                gene_patterns.append(f"{family} family")
        elif reason_lower.startswith("headline:"):
            headlines.append("headline match")
        elif reason_lower.startswith("literature topic:"):
            topic = reason.replace("literature topic:", "").strip()
            if topic and topic not in literature:
                literature.append(topic)

    # Build the reason string
    if has_virulence_model:
        parts.append("Virulence model")
    if gene_patterns:
        parts.append(f"Gene pattern ({', '.join(gene_patterns[:2])})")
    if go_terms:
        parts.append(f"GO ({', '.join(go_terms[:2])})")
    if phenotypes:
        parts.append(f"Phenotype ({', '.join(phenotypes[:2])})")
    if literature:
        parts.append(f"Literature ({', '.join(literature[:2])})")
    if headlines and not parts:
        parts.append("Headline match")

    if not parts:
        parts.append("Category match")

    # Combine with categories if space allows
    reason_str = " + ".join(parts[:3])
    if len(reason_str) < 60 and categories:
        reason_str += f" | {', '.join(categories[:2])}"

    return reason_str[:100]  # Max 100 chars


def _extract_function_from_headline(headline: str) -> str | None:
    """
    Extract the CORE MOLECULAR FUNCTION from headline text.

    IMPORTANT: Prioritizes actual biological role (actin, kinase, etc.)
    over category-derived labels. This ensures summaries reflect true
    gene function, not just virulence category membership.

    Returns the function phrase or None.
    """
    import re

    if not headline:
        return None

    headline_lower = headline.lower()

    # First check: Does headline START with a protein name/type?
    # Many headlines begin with "Actin;", "Kinase;", etc.
    first_word_patterns = [
        (r'^actin\b', 'actin cytoskeletal protein'),
        (r'^tubulin\b', 'tubulin cytoskeletal protein'),
        (r'^histone\b', 'histone protein'),
        (r'^ribosomal\b', 'ribosomal protein'),
        (r'^ubiquitin\b', 'ubiquitin-related protein'),
        (r'^cyclophilin\b', 'cyclophilin'),
        (r'^thioredoxin\b', 'thioredoxin'),
        (r'^glutathione\b', 'glutathione-related protein'),
        (r'^superoxide dismutase\b', 'superoxide dismutase'),
        (r'^catalase\b', 'catalase'),
        (r'^peroxidase\b', 'peroxidase'),
        (r'^elongation factor\b', 'elongation factor'),
        (r'^initiation factor\b', 'translation initiation factor'),
        (r'^glyceraldehyde\b', 'glycolytic enzyme'),
        (r'^enolase\b', 'enolase'),
        (r'^pyruvate\b', 'metabolic enzyme'),
    ]

    for pattern, func_name in first_word_patterns:
        if re.search(pattern, headline_lower):
            return func_name

    # Function patterns - ordered by specificity
    function_patterns = [
        # Transcription/regulation
        (r'\btranscription factor\b', 'transcription factor'),
        (r'\btranscriptional (?:activator|repressor|regulator)\b', 'transcriptional regulator'),
        (r'\bDNA[- ]binding (?:protein|factor)\b', 'DNA-binding protein'),
        (r'\bzinc finger (?:protein|transcription factor)\b', 'zinc finger protein'),
        # Enzymes
        (r'\baspartyl protease\b', 'secreted aspartyl protease'),
        (r'\bsecretory? (?:aspartyl )?protease\b', 'secreted protease'),
        (r'\bprotease\b', 'protease'),
        (r'\blipase\b', 'lipase'),
        (r'\bphospholipase\b', 'phospholipase'),
        (r'\bkinase\b', 'kinase'),
        (r'\bphosphatase\b', 'phosphatase'),
        (r'\bsynthase\b', 'synthase'),
        (r'\btransferase\b', 'transferase'),
        (r'\boxidase\b', 'oxidase'),
        (r'\breductase\b', 'reductase'),
        (r'\bdehydrogenase\b', 'dehydrogenase'),
        (r'\bisomerase\b', 'isomerase'),
        # Cytoskeleton/structural (anywhere in headline)
        (r'\bactin\b', 'actin cytoskeletal protein'),
        (r'\btubulin\b', 'tubulin'),
        (r'\bcytoskeletal\b', 'cytoskeletal protein'),
        # Surface/structural
        (r'\bcell (?:surface|wall) (?:protein|adhesin)\b', 'cell surface protein'),
        (r'\badhesin\b', 'adhesin'),
        (r'\bGPI[- ]anchored (?:protein|adhesin)\b', 'GPI-anchored protein'),
        (r'\bmembrane protein\b', 'membrane protein'),
        (r'\btransporter\b', 'transporter'),
        (r'\befflux pump\b', 'efflux pump'),
        # Signaling
        (r'\bMAP kinase\b', 'MAP kinase'),
        (r'\bsignal(?:ing)? (?:protein|molecule)\b', 'signaling protein'),
        (r'\breceptor\b', 'receptor'),
        # Chaperones/stress
        (r'\bheat shock protein\b', 'heat shock protein'),
        (r'\bchaperone\b', 'chaperone'),
        (r'\bsuperoxide dismutase\b', 'superoxide dismutase'),
        # Generic regulators
        (r'\bregulator\b', 'regulator'),
    ]

    for pattern, func_name in function_patterns:
        if re.search(pattern, headline_lower):
            return func_name

    return None


def _extract_actions_from_headline(headline: str) -> list[str]:
    """
    Extract biological actions/mechanisms from headline.

    Looks for patterns like "regulates X", "required for Y", "promotes Z".
    Returns list of action phrases.
    """
    import re

    if not headline:
        return []

    actions = []

    # Action verb patterns with their targets
    action_patterns = [
        # Regulation patterns
        (r'regulates? ([^;,\.]+?)(?:;|,|\.|$)', 'regulates'),
        (r'controls? ([^;,\.]+?)(?:;|,|\.|$)', 'controls'),
        (r'modulates? ([^;,\.]+?)(?:;|,|\.|$)', 'modulates'),
        # Requirement patterns
        (r'required for ([^;,\.]+?)(?:;|,|\.|$)', 'required for'),
        (r'essential for ([^;,\.]+?)(?:;|,|\.|$)', 'essential for'),
        (r'necessary for ([^;,\.]+?)(?:;|,|\.|$)', 'necessary for'),
        # Promotion/induction
        (r'promotes? ([^;,\.]+?)(?:;|,|\.|$)', 'promotes'),
        (r'induces? ([^;,\.]+?)(?:;|,|\.|$)', 'induces'),
        (r'activates? ([^;,\.]+?)(?:;|,|\.|$)', 'activates'),
        (r'enhances? ([^;,\.]+?)(?:;|,|\.|$)', 'enhances'),
        # Inhibition
        (r'inhibits? ([^;,\.]+?)(?:;|,|\.|$)', 'inhibits'),
        (r'represses? ([^;,\.]+?)(?:;|,|\.|$)', 'represses'),
        (r'suppresses? ([^;,\.]+?)(?:;|,|\.|$)', 'suppresses'),
        # Involvement
        (r'involved in ([^;,\.]+?)(?:;|,|\.|$)', 'involved in'),
        (r'contributes? to ([^;,\.]+?)(?:;|,|\.|$)', 'contributes to'),
        (r'mediates? ([^;,\.]+?)(?:;|,|\.|$)', 'mediates'),
        (r'participates? in ([^;,\.]+?)(?:;|,|\.|$)', 'participates in'),
        # Role patterns
        (r'role in ([^;,\.]+?)(?:;|,|\.|$)', 'role in'),
        (r'functions? in ([^;,\.]+?)(?:;|,|\.|$)', 'functions in'),
    ]

    for pattern, verb in action_patterns:
        matches = re.findall(pattern, headline, re.IGNORECASE)
        for match in matches:
            target = match.strip()
            # Clean up and limit length
            if target and len(target) > 3 and len(target) < 60:
                # Skip if it's just a gene name reference
                if not re.match(r'^[A-Z][a-z]+\d*$', target):
                    actions.append(f"{verb} {target}")

    return actions[:3]  # Limit to top 3 actions


def _extract_model_systems(headline: str, direct_evidence: list[str]) -> list[str]:
    """
    Extract experimental model systems from headline and evidence.

    Returns list of model system descriptions.
    """
    import re

    models = []

    # Check headline for model mentions
    if headline:
        headline_lower = headline.lower()

        model_patterns = [
            (r'\bmouse\b|\bmurine\b|\bmice\b', 'mouse infection model'),
            (r'\bgalleria\b', 'Galleria mellonella model'),
            (r'\bcatheter\b', 'catheter biofilm model'),
            (r'\brat\b', 'rat model'),
            (r'\bmacrophage\b', 'macrophage interaction'),
            (r'\bepithelial\b', 'epithelial cell interaction'),
            (r'\bendothelial\b', 'endothelial cell interaction'),
            (r'\bin vivo\b', 'in vivo studies'),
        ]

        for pattern, model_name in model_patterns:
            if re.search(pattern, headline_lower):
                if model_name not in models:
                    models.append(model_name)

    # Check direct evidence for virulence models
    for evidence in direct_evidence:
        ev_lower = evidence.lower()
        if 'virulence model' in ev_lower:
            if 'mouse' in ev_lower and 'mouse infection model' not in models:
                models.append('mouse infection model')
            elif 'galleria' in ev_lower and 'Galleria mellonella model' not in models:
                models.append('Galleria mellonella model')

    return models[:2]  # Limit to 2


def _get_primary_action(actions: list[str]) -> str | None:
    """Get the most important action, cleaned up for template use."""
    if not actions:
        return None

    # Priority order for action verbs
    priority = [
        "regulates", "controls", "required for", "essential for",
        "mediates", "promotes", "activates", "inhibits",
        "contributes to", "involved in", "role in"
    ]

    for verb in priority:
        for action in actions:
            if action.startswith(verb + " "):
                # Extract just the target (what it regulates/controls/etc)
                target = action[len(verb) + 1:].strip()
                # Truncate long targets
                if len(target) > 40:
                    target = target[:37] + "..."
                return (verb, target)

    # Fallback to first action
    if actions:
        parts = actions[0].split(" ", 1)
        if len(parts) == 2:
            verb, target = parts
            if len(target) > 40:
                target = target[:37] + "..."
            return (verb, target)

    return None


def calculate_importance_level(
    direct_evidence: list[str],
    indirect_evidence: list[str],
    paper_count: int,
    confidence_score: int,
) -> tuple[str, str]:
    """
    Calculate importance level based on evidence strength.

    Returns:
        Tuple of (level, label) where:
        - level: "high", "medium", or "low"
        - label: Human-readable badge text
    """
    # Count key evidence types
    has_virulence_model = any("virulence model" in e.lower() for e in direct_evidence)
    direct_count = len(direct_evidence)
    phenotype_count = sum(1 for e in direct_evidence + indirect_evidence if "phenotype:" in e.lower())
    go_count = sum(1 for e in direct_evidence + indirect_evidence if e.lower().startswith("go:"))

    # Calculate importance score
    importance_score = 0

    # Virulence model is strongest signal
    if has_virulence_model:
        importance_score += 4

    # Direct evidence count
    if direct_count >= 3:
        importance_score += 3
    elif direct_count >= 1:
        importance_score += 1

    # Paper count (well-studied)
    if paper_count >= 10:
        importance_score += 3
    elif paper_count >= 5:
        importance_score += 2
    elif paper_count >= 2:
        importance_score += 1

    # Multiple evidence types
    if phenotype_count >= 2 and go_count >= 1:
        importance_score += 2

    # Confidence score factor
    if confidence_score >= 15:
        importance_score += 2
    elif confidence_score >= 10:
        importance_score += 1

    # Determine level and label
    if importance_score >= 8:
        level = "high"
        if has_virulence_model and paper_count >= 10:
            label = "Core virulence factor"
        elif has_virulence_model:
            label = "Validated in vivo"
        elif paper_count >= 10:
            label = "Well-characterized"
        else:
            label = "Strong evidence"
    elif importance_score >= 4:
        level = "medium"
        if paper_count >= 5:
            label = "Multiple studies"
        elif direct_count >= 2:
            label = "Direct evidence"
        else:
            label = "Moderate evidence"
    else:
        level = "low"
        if phenotype_count >= 1:
            label = "Phenotype support"
        elif go_count >= 1:
            label = "GO annotation"
        else:
            label = "Indirect evidence"

    return level, label


def _is_generic_gene(role: str) -> bool:
    """Check if gene role is generic (housekeeping-like)."""
    generic_roles = {
        "actin cytoskeletal protein", "tubulin cytoskeletal protein",
        "ribosomal protein", "elongation factor", "translation initiation factor",
        "histone protein", "glycolytic enzyme", "metabolic enzyme",
    }
    return role.lower() in generic_roles


def _has_causal_evidence(direct_evidence: list[str]) -> bool:
    """
    Check if evidence supports causal claims ("required for").

    Only returns True if there's:
    - Tier 1 phenotype evidence (virulence, pathogenesis, lethality)
    - OR strong knockout/mutant phenotype

    This prevents overuse of "required for" when evidence is weak.
    """
    causal_patterns = [
        "virulence", "pathogenesis", "lethality", "killing",
        "colonization", "infection", "mutant", "knockout",
        "deletion", "null", "loss of function"
    ]

    for evidence in direct_evidence:
        ev_lower = evidence.lower()
        if any(pattern in ev_lower for pattern in causal_patterns):
            return True

    return False


def _get_paper_scaled_phrase(paper_count: int, has_in_vivo: bool) -> str:
    """
    Get evidence phrase scaled by paper count.

    - 10+ papers: "with strong/multiple studies supporting"
    - 3-9 papers: "with evidence supporting"
    - 1-2 papers: "with in vivo evidence suggesting" or "suggesting"
    """
    if paper_count >= 10:
        return "with multiple studies supporting a role in virulence"
    elif paper_count >= 5:
        return "with evidence supporting a role in virulence"
    elif has_in_vivo:
        return "with in vivo evidence supporting a role in virulence"
    else:
        return ""


def repair_summary(summary: str) -> str:
    """
    Post-process summary to remove generated-feeling patterns.

    Fixes:
    - "plays a role in X, contributing to Y" -> "required for X and Y"
    - "is involved in X, involved in Y" -> "is involved in X and Y"
    - Double spaces, trailing punctuation issues
    - "that that" and other stutter patterns
    """
    import re

    # Fix "plays a role in X, contributing to Y" patterns
    summary = re.sub(
        r'plays a role in ([^,]+),\s*contributing to ([^\.]+)',
        r'required for \1 and \2',
        summary
    )

    # Fix "plays a key role in X, contributing to Y"
    summary = re.sub(
        r'plays a key role in ([^,]+),\s*contributing to ([^\.]+)',
        r'required for \1 and \2',
        summary
    )

    # Fix "is involved in X, involved in Y"
    summary = re.sub(
        r'is involved in ([^,]+),\s*involved in ([^\.]+)',
        r'is involved in \1 and \2',
        summary
    )

    # Fix "is associated with X, associated with Y"
    summary = re.sub(
        r'is associated with ([^,]+),\s*associated with ([^\.]+)',
        r'is associated with \1 and \2',
        summary
    )

    # Remove redundant "that that"
    summary = re.sub(r'\bthat\s+that\b', 'that', summary)

    # Remove double spaces
    summary = re.sub(r'  +', ' ', summary)

    # Fix ", ." pattern
    summary = re.sub(r',\s*\.', '.', summary)

    # Fix ".. " pattern
    summary = re.sub(r'\.\.+', '.', summary)

    # Ensure single period at end
    summary = summary.rstrip('.')
    summary += '.'

    return summary


def generate_summary(
    gene_name: str,
    categories: list[str],
    direct_evidence: list[str],
    indirect_evidence: list[str],
    headline: str | None,
    confidence_tier: str,
    paper_count: int = 0,
) -> str:
    """
    Generate a concise biological summary (~150 chars) for table display.

    Key principles:
    1. "required for" ONLY with causal evidence (tier 1 phenotype, knockout)
    2. Protein-type-specific verbs (TF → regulates, enzyme → involved in)
    3. Deduplicate concepts semantically (adhesion vs host adhesion)
    4. Scale evidence phrase by paper count

    Templates by evidence strength:
    - Strong + causal: "X is a TF regulating morphogenesis, with multiple studies..."
    - Strong + no causal: "X is an adhesin mediating adhesion, with in vivo evidence..."
    - Moderate: "X is a kinase involved in drug response"
    - Weak: "X is a protein with limited evidence linking it to Y"
    """
    # Helper for a/an article selection
    def article(word: str) -> str:
        vowels = 'aeiouAEIOU'
        return 'an' if word and word[0] in vowels else 'a'

    # Determine evidence tier
    evidence_tier = determine_evidence_language_tier(
        confidence_tier, direct_evidence, indirect_evidence
    )

    # Extract biological information
    function = _extract_function_from_headline(headline)
    actions = _extract_actions_from_headline(headline)

    # Determine role - prioritize molecular function
    role = function if function else "protein"

    # Get primary action target
    action_info = _get_primary_action(actions)
    action_target = action_info[1] if action_info else None

    # Get virulence relevance phrase
    virulence_phrase = get_virulence_phrase(categories, max_items=1)

    # Collect and deduplicate concepts
    concepts = []
    if action_target:
        concepts.append(action_target)
    if virulence_phrase:
        concepts.append(virulence_phrase)
    concepts = dedupe_concepts(concepts)[:2]  # Max 2, deduplicated

    # Check evidence quality
    has_in_vivo = any(
        "virulence model" in e.lower() or "mouse" in e.lower() or "galleria" in e.lower()
        for e in direct_evidence + indirect_evidence
    )
    has_causal = _has_causal_evidence(direct_evidence)
    confidence = confidence_tier.lower() if confidence_tier else "low"

    # Get protein-type-specific verb
    protein_verb = get_verb_for_protein_type(role)

    # BUILD SUMMARY based on evidence tier
    if evidence_tier in ("in_vivo_strong", "experimental_strong"):
        # STRONG EVIDENCE
        if concepts:
            concepts_str = " and ".join(concepts)

            # Use "required for" ONLY with causal evidence
            if has_causal and confidence == "high":
                summary = f"{gene_name} is {article(role)} {role} required for {concepts_str}"
            else:
                # Use protein-type-specific verb
                summary = f"{gene_name} is {article(role)} {role} {protein_verb} {concepts_str}"
        else:
            summary = f"{gene_name} is {article(role)} {role}"

        # Add paper-scaled evidence phrase for strong evidence
        if confidence == "high" and not _is_generic_gene(role):
            ending = _get_paper_scaled_phrase(paper_count, has_in_vivo)
            if ending:
                summary = summary.rstrip(".") + ", " + ending

    elif evidence_tier == "experimental_moderate":
        # MODERATE EVIDENCE: Use "involved in" or "associated with"
        if concepts:
            concepts_str = " and ".join(concepts)
            summary = f"{gene_name} is {article(role)} {role} {protein_verb} {concepts_str}"
        else:
            summary = f"{gene_name} is {article(role)} {role}"

    elif evidence_tier == "annotation_supported":
        # ANNOTATION ONLY: Hedged language
        if concepts:
            summary = f"{gene_name} is {article(role)} {role} linked to {concepts[0]}"
        else:
            summary = f"{gene_name} is {article(role)} {role}"

    else:  # indirect_low
        # WEAK EVIDENCE: "limited evidence"
        if concepts:
            summary = f"{gene_name} is {article(role)} {role} with limited evidence linking it to {concepts[0]}"
        else:
            summary = f"{gene_name} is {article(role)} {role}"

    # Ensure period
    if not summary.endswith("."):
        summary += "."

    # Normalize verbose role patterns
    for verbose, normalized in ROLE_NORMALIZATION.items():
        if verbose in summary.lower():
            summary = summary.replace(verbose, normalized)
            break

    # Apply post-processing repairs
    summary = repair_summary(summary)

    # Cap at 180 chars
    if len(summary) > 180:
        truncate_at = summary.rfind(' ', 0, 177)
        if truncate_at > 100:
            summary = summary[:truncate_at] + "..."
        else:
            summary = summary[:177] + "..."

    return summary


def generate_summary_full(
    gene_name: str,
    categories: list[str],
    direct_evidence: list[str],
    indirect_evidence: list[str],
    headline: str | None,
    confidence_tier: str,
    paper_count: int = 0,
) -> str:
    """
    Generate a detailed biological summary for tooltips/expansion.

    Uses same principles as generate_summary():
    - Protein-type-specific verbs
    - "required for" only with causal evidence
    - Deduplicated concepts
    - Paper-count scaling
    """
    # Helper for a/an article selection
    def article(word: str) -> str:
        vowels = 'aeiouAEIOU'
        return 'an' if word and word[0] in vowels else 'a'

    # Determine evidence tier
    evidence_tier = determine_evidence_language_tier(
        confidence_tier, direct_evidence, indirect_evidence
    )

    # Extract biological information
    function = _extract_function_from_headline(headline)
    actions = _extract_actions_from_headline(headline)

    # Determine role
    role = function if function else "protein"

    # Get virulence context
    virulence_phrase = get_virulence_phrase(categories, max_items=1)

    # Collect concepts from actions
    concepts = []
    for action in actions[:2]:
        if " " in action:
            _, target = action.split(" ", 1)
            if target and len(target) > 3:
                concepts.append(target)

    # Add virulence phrase
    if virulence_phrase:
        concepts.append(virulence_phrase)

    # Deduplicate concepts
    concepts = dedupe_concepts(concepts)[:3]

    # Check evidence quality
    has_in_vivo = any(
        "virulence model" in e.lower() or "mouse" in e.lower()
        for e in direct_evidence + indirect_evidence
    )
    has_causal = _has_causal_evidence(direct_evidence)
    confidence = confidence_tier.lower() if confidence_tier else "low"

    # Get protein-type-specific verb
    protein_verb = get_verb_for_protein_type(role)

    # Build summary based on evidence tier
    if evidence_tier in ("in_vivo_strong", "experimental_strong"):
        if concepts:
            concepts_str = " and ".join(concepts)
            # Use "required for" only with causal evidence
            if has_causal and confidence == "high":
                summary = f"{gene_name} is {article(role)} {role} required for {concepts_str}."
            else:
                summary = f"{gene_name} is {article(role)} {role} {protein_verb} {concepts_str}."
        else:
            summary = f"{gene_name} is {article(role)} {role}."

        # Add paper-scaled evidence phrase
        if confidence == "high" and not _is_generic_gene(role):
            ending = _get_paper_scaled_phrase(paper_count, has_in_vivo)
            if ending:
                summary = summary.rstrip(".") + ", " + ending + "."

    elif evidence_tier == "experimental_moderate":
        if concepts:
            concepts_str = " and ".join(concepts[:2])
            summary = f"{gene_name} is {article(role)} {role} {protein_verb} {concepts_str}."
        else:
            summary = f"{gene_name} is {article(role)} {role}."

    elif evidence_tier == "annotation_supported":
        if concepts:
            summary = f"{gene_name} is {article(role)} {role} linked to {concepts[0]}."
        else:
            summary = f"{gene_name} is {article(role)} {role}."

    else:  # indirect_low
        if concepts:
            summary = f"{gene_name} is {article(role)} {role} with limited evidence linking it to {concepts[0]}."
        else:
            summary = f"{gene_name} is {article(role)} {role}."

    # Apply post-processing repairs
    summary = repair_summary(summary)

    # Add study count for well-studied genes
    if paper_count >= 10:
        summary = summary.rstrip(".") + f". ({paper_count} publications)"
    elif paper_count >= 5:
        summary = summary.rstrip(".") + f". ({paper_count} studies)"

    return summary[:400]


def generate_evidence_breakdown(
    direct_evidence: list[str],
    indirect_evidence: list[str],
    paper_count: int,
    confidence_score: int,
) -> dict:
    """
    Generate a structured breakdown of evidence types.

    Returns dict like:
    {
        "virulence_models": 2,
        "go_annotations": 5,
        "phenotypes": 3,
        "keyword_matches": 1,
        "papers": 12,
        "score_breakdown": {
            "direct_evidence_points": 15,
            "indirect_evidence_points": 5,
            "paper_bonus": 2,
            "total": 22
        }
    }
    """
    breakdown = {
        "virulence_models": 0,
        "go_annotations": 0,
        "phenotypes": 0,
        "keyword_matches": 0,
        "papers": paper_count,
    }

    # Count direct evidence types
    for evidence in direct_evidence:
        ev_lower = evidence.lower()
        if "virulence model:" in ev_lower:
            breakdown["virulence_models"] += 1
        elif ev_lower.startswith("go:"):
            breakdown["go_annotations"] += 1
        elif "phenotype:" in ev_lower:
            breakdown["phenotypes"] += 1
        else:
            breakdown["keyword_matches"] += 1

    # Count indirect evidence types
    for evidence in indirect_evidence:
        ev_lower = evidence.lower()
        if ev_lower.startswith("go:"):
            breakdown["go_annotations"] += 1
        elif "phenotype:" in ev_lower:
            breakdown["phenotypes"] += 1
        else:
            breakdown["keyword_matches"] += 1

    # Add score breakdown explanation
    direct_points = (
        breakdown["virulence_models"] * 5 +
        len([e for e in direct_evidence if e.lower().startswith("go:")]) * 3
    )
    indirect_points = (
        breakdown["go_annotations"] * 2 +
        breakdown["phenotypes"] * 2
    )
    paper_bonus = min(3, paper_count // 5)  # +1 point per 5 papers, max 3

    breakdown["score_explanation"] = {
        "virulence_models": f"{breakdown['virulence_models']} x 5 = {breakdown['virulence_models'] * 5}",
        "go_direct": f"GO (direct) x 3",
        "phenotypes": f"{breakdown['phenotypes']} x 2 = {breakdown['phenotypes'] * 2}",
        "papers": f"{paper_count} papers",
        "total_score": confidence_score,
    }

    return breakdown


# =============================================================================
# CONFIDENCE SCORE WEIGHTS
# =============================================================================

# Weights for calculating confidence scores (0-20 range)
EVIDENCE_WEIGHTS = {
    "virulence_model": 5,       # Tested in mouse/Galleria
    "tier1_phenotype": 4,       # Direct virulence phenotype
    "tier2_phenotype": 3,       # Host interaction phenotype
    "virulence_go": 3,          # Pathogenesis/host GO terms
    "disease_literature": 2,    # Disease literature topic
    "gene_pattern": 1,          # Gene name pattern match
    "keyword_match": 1,         # Headline keyword
    "housekeeping_penalty": -3,  # Housekeeping gene penalty
}

# =============================================================================
# VIRULENCE CATEGORY DEFINITIONS
# =============================================================================

# Category rules define how genes are mapped to virulence categories
VIRULENCE_CATEGORIES = {
    "adhesins": {
        "name": "Adhesins",
        "description": "Cell surface adhesion proteins including ALS family, Hwp1",
        "rules": {
            "gene_patterns": ["ALS%", "HWP%", "EAP%", "PGA%"],
            "phenotype_observables": ["adhesion%", "cell wall%"],
            "go_terms": ["GO:0007155", "GO:0044406"],  # cell adhesion, adhesion to host
            "headlines": ["%adhesin%", "%cell surface%"],
        }
    },
    "secreted_enzymes": {
        "name": "Secreted Enzymes",
        "description": "Secreted aspartyl proteases (SAPs), lipases, phospholipases",
        "rules": {
            "gene_patterns": ["SAP%", "LIP%", "PLC%", "PLB%"],
            "go_terms": ["GO:0008233", "GO:0016298", "GO:0008970"],  # protease, lipase, phospholipase
        }
    },
    "morphogenesis": {
        "name": "Morphogenesis",
        "description": "Genes involved in yeast-hyphal transition and morphological switching",
        "rules": {
            "gene_patterns": ["ECE%", "HGC%", "TUP%", "NRG%", "EFG%"],
            "phenotype_observables": ["filamentous growth%", "hyphal%", "cell morphology%"],
            "go_terms": ["GO:0001403"],  # invasive filamentous growth
        }
    },
    "host_interaction": {
        "name": "Host Interaction",
        "description": "Factors mediating host-pathogen interactions",
        "rules": {
            "phenotype_has_virulence_model": True,  # genes tested in animal models
            "literature_topics": ["Disease"],
            "go_terms": ["GO:0009405", "GO:0044419"],  # pathogenesis, host-pathogen interaction
        }
    },
    "biofilm": {
        "name": "Biofilm Formation",
        "description": "Genes required for biofilm development and maintenance",
        "rules": {
            "phenotype_observables": ["biofilm%"],
            "go_terms": ["GO:0044010", "GO:0043709"],  # biofilm formation, biofilm matrix
        }
    },
    "immune_evasion": {
        "name": "Immune Evasion",
        "description": "Genes involved in evading host immune responses",
        "rules": {
            "go_terms": ["GO:0042832", "GO:0009615"],  # defense response to protozoan, defense evasion
            "phenotype_observables": ["%immune%", "%phagocyt%"],
        }
    },
    "drug_resistance": {
        "name": "Drug Resistance",
        "description": "Genes conferring antifungal drug resistance",
        "rules": {
            "gene_patterns": ["CDR%", "MDR%", "ERG%", "FKS%"],
            "phenotype_observables": ["%resistance%", "%susceptibility%"],
            "go_terms": ["GO:0042493", "GO:0046677"],  # response to drug, response to antibiotic
        }
    }
}


# =============================================================================
# API RESPONSE SCHEMAS
# =============================================================================

class VirulenceCategory(BaseModel):
    """Single virulence category with metadata and gene count."""
    key: str
    name: str
    description: str
    count: int = 0


class VirulenceCategoriesResponse(BaseModel):
    """Response from categories endpoint."""
    categories: list[VirulenceCategory]
    total_genes: int = 0


class VirulenceFactor(BaseModel):
    """Single virulence factor (gene) with its category mappings."""
    feature_no: int
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism: str
    organism_abbrev: str
    headline: typing.Optional[str] = None
    description: typing.Optional[str] = None
    categories: list[str] = []  # List of category names this gene belongs to
    match_reasons: list[str] = []  # Why this gene matched (e.g., "gene pattern: ALS1", "GO: pathogenesis")

    # Evidence quality fields
    evidence_tier: int = 4                          # 1=best, 4=weakest
    evidence_tier_name: str = "Indirect"
    confidence_score: int = 0                       # 0-20 range
    confidence_tier: str = "Low"                    # High/Medium/Low
    is_housekeeping: bool = False
    housekeeping_reason: typing.Optional[str] = None
    ortholog_count: int = 0                         # Candida species with orthologs

    # Quick win fields
    inclusion_reason: str = ""                      # Short human-readable reason
    evidence_types: list[str] = []                  # List of GO/PHE/KW codes

    # Paper/reference fields
    paper_count: int = 0                            # Number of associated papers
    pmids: list[int] = []                           # List of PubMed IDs (sorted by recency)

    # Split evidence fields
    direct_evidence: list[str] = []                 # Direct virulence evidence
    indirect_evidence: list[str] = []               # Indirect/supporting evidence

    # Auto-generated summary (#1 improvement)
    summary: str = ""                               # Concise summary for table (~150 chars)
    summary_full: str = ""                          # Detailed summary for tooltip/expansion

    # Evidence breakdown (#2 improvement)
    evidence_breakdown: dict = {}                   # Structured: {virulence_models: 2, go_terms: 5, phenotypes: 3, ...}

    # Importance/prioritization fields (#3 improvement)
    importance_level: str = "low"                   # high/medium/low
    importance_label: str = "Indirect evidence"    # Human-readable badge (e.g., "Core virulence factor")

    # Structural data links (#4 improvement)
    uniprot_id: typing.Optional[str] = None         # UniProt accession (SwissProt/TrEMBL)
    alphafold_url: typing.Optional[str] = None      # AlphaFold structure link


class VirulenceFactorsResponse(BaseModel):
    """Response from factors search endpoint."""
    items: list[VirulenceFactor]
    total_count: int
    page: int
    page_size: int
    categories_searched: list[str] = []


class VirulenceCategoryMatch(BaseModel):
    """Details about why a gene matched a category."""
    category_key: str
    category_name: str
    match_type: str  # "gene_pattern", "phenotype", "go_term", "literature", "virulence_model"
    match_value: str  # The specific value that matched (e.g., "ALS1", "biofilm formation", "GO:0044010")


class VirulenceFactorDetail(BaseModel):
    """Detailed virulence information for a specific gene."""
    feature_no: int
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism: str
    organism_abbrev: str
    headline: typing.Optional[str] = None
    description: typing.Optional[str] = None
    categories: list[VirulenceCategoryMatch] = []


class VirulenceCategoryStats(BaseModel):
    """Stats for a single category."""
    key: str
    name: str
    count: int


class VirulenceOrganismStats(BaseModel):
    """Stats for a single organism."""
    organism_abbrev: str
    organism_name: str
    count: int


class VirulenceStats(BaseModel):
    """Summary statistics for virulence factors."""
    total_genes: int
    categories: list[VirulenceCategoryStats]
    organisms: list[VirulenceOrganismStats]


class VirulenceDownloadRequest(BaseModel):
    """Request parameters for download endpoint."""
    categories: list[str] = []
    organisms: list[str] = []
    search_term: typing.Optional[str] = None
    format: str = "tsv"  # "tsv" or "csv"


# Forward reference resolution
VirulenceFactorDetail.model_rebuild()
