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
        "allowed_verbs": ["required for", "plays a key role in", "promotes", "drives"],
        "avoid_verbs": ["may", "suggests"],
        "ending_phrase": "Virulence demonstrated in vivo.",
    },
    "experimental_strong": {
        "description": "phenotype/model evidence + high confidence, no in vivo",
        "allowed_verbs": ["contributes to", "is involved in", "supports", "promotes"],
        "avoid_verbs": ["required for", "is essential for"],
        "ending_phrase": "Experimental evidence supports virulence role.",
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
# Maps phrases to canonical form to avoid "adhesion and biofilm and adhesion"
CONCEPT_NORMALIZATION = {
    "biofilm formation": "adhesion",
    "cell adhesion": "adhesion",
    "host adhesion": "adhesion",
    "host interaction": "host interaction",
    "immune evasion": "host interaction",
    "secreted enzymatic activity": "secreted enzymatic activity",
    "morphogenesis": "morphogenesis",
    "drug response": "drug response",
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

    Uses evidence-calibrated language:
    - Strong evidence → strong verbs ("required for", "contributes to")
    - Weak evidence → hedged language ("associated with", "may be linked to")

    Template: "[Gene] is a [core role] [core function], [virulence clause]. [evidence clause]"
    """
    # Helper for a/an article selection
    def article(word: str) -> str:
        vowels = 'aeiouAEIOU'
        return 'an' if word and word[0] in vowels else 'a'

    # Determine evidence tier for calibrated language
    evidence_tier = determine_evidence_language_tier(
        confidence_tier, direct_evidence, indirect_evidence
    )
    tier_config = EVIDENCE_LANGUAGE_TIERS.get(evidence_tier, EVIDENCE_LANGUAGE_TIERS["indirect_low"])

    # Extract biological information
    function = _extract_function_from_headline(headline)
    actions = _extract_actions_from_headline(headline)

    # Determine role - PRIORITIZE molecular function over category
    if function:
        role = function
    else:
        # No clear molecular function found - use neutral "protein"
        role = "protein"

    # Get primary action (core function clause)
    action_info = _get_primary_action(actions)

    # Get virulence relevance phrase from categories (max 1 to avoid repetition)
    virulence_phrase = get_virulence_phrase(categories, max_items=1)

    # Build first clause: core identity + function
    action_phrase = ""
    has_plays_role = False

    if action_info:
        verb, target = action_info

        # Calibrate verb based on evidence tier
        # Strong verbs like "controls", "required for" need strong evidence
        if evidence_tier in ("indirect_low", "annotation_supported"):
            # Downgrade strong verbs for weak evidence
            if verb in ("controls", "required for", "essential for", "drives"):
                verb = "involved in"
            elif verb in ("regulates", "promotes", "activates"):
                verb = "associated with"
            # For annotation_supported, skip "role in" entirely - too assertive
            if verb == "role in":
                verb = "involved in"

        # Format action phrase with proper grammar
        # Verbs needing "that X" construction
        if verb in ("regulates", "controls", "promotes", "activates", "inhibits",
                    "mediates", "modulates", "induces", "represses", "suppresses"):
            action_phrase = f"that {verb} {target}"
        # Verbs needing "that is X" construction
        elif verb in ("required for", "essential for", "involved in", "associated with"):
            action_phrase = f"that is {verb} {target}"
        # "role in" gets special phrasing (only for strong evidence)
        elif verb == "role in":
            action_phrase = f"that plays a role in {target}"
            has_plays_role = True
        # Verbs with "may" stay as-is
        elif verb.startswith("may "):
            action_phrase = f"that {verb} {target}"
        # Default: use "that" + verb
        else:
            action_phrase = f"that {verb} {target}"

        first_clause = f"{gene_name} is {article(role)} {role} {action_phrase}"
    else:
        first_clause = f"{gene_name} is {article(role)} {role}"

    # Build virulence clause based on evidence tier
    # AVOID redundant phrasing: don't use "contributing to" if we already have "plays a role"
    virulence_clause = ""
    if virulence_phrase:
        if evidence_tier == "in_vivo_strong" and not has_plays_role:
            virulence_clause = f", contributing to {virulence_phrase}"
        elif evidence_tier == "in_vivo_strong" and has_plays_role:
            # Skip virulence clause to avoid "plays a role ... contributing to"
            virulence_clause = ""
        elif evidence_tier == "experimental_strong" and not has_plays_role:
            virulence_clause = f", involved in {virulence_phrase}"
        elif evidence_tier == "experimental_strong" and has_plays_role:
            # Already have "plays a role", skip redundant clause
            virulence_clause = ""
        elif evidence_tier == "experimental_moderate":
            virulence_clause = f", associated with {virulence_phrase}"
        elif evidence_tier == "annotation_supported":
            virulence_clause = f", with annotation linking it to {virulence_phrase}"
        else:  # indirect_low
            virulence_clause = f", with limited evidence for {virulence_phrase}"

    # Combine clauses
    summary = first_clause + virulence_clause
    if not summary.endswith("."):
        summary += "."

    # Check for verbose role patterns and normalize
    for verbose, normalized in ROLE_NORMALIZATION.items():
        if verbose in summary.lower():
            summary = summary.replace(verbose, normalized)
            break

    # Add evidence ending phrase ONLY for strong evidence AND non-generic genes
    ending = tier_config.get("ending_phrase")
    if ending and evidence_tier == "in_vivo_strong":
        # Only add "Virulence demonstrated in vivo" if:
        # - confidence is High
        # - gene is not generic (e.g., ACT1, housekeeping)
        confidence = confidence_tier.lower() if confidence_tier else "low"
        if confidence == "high" and not _is_generic_gene(role):
            combined = summary.rstrip(".") + ". " + ending
            if len(combined) <= 180:
                summary = combined

    # Cap at 180 chars for table display, truncate at word boundary
    if len(summary) > 180:
        # Find last space before limit to truncate at word boundary
        truncate_at = summary.rfind(' ', 0, 177)
        if truncate_at > 100:  # Ensure we keep a reasonable amount
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

    Uses evidence-calibrated language with same principles as generate_summary().
    Includes: function, mechanisms, virulence context, evidence quality, study count.
    """
    # Helper for a/an article selection
    def article(word: str) -> str:
        vowels = 'aeiouAEIOU'
        return 'an' if word and word[0] in vowels else 'a'

    # Determine evidence tier
    evidence_tier = determine_evidence_language_tier(
        confidence_tier, direct_evidence, indirect_evidence
    )
    tier_config = EVIDENCE_LANGUAGE_TIERS.get(evidence_tier, EVIDENCE_LANGUAGE_TIERS["indirect_low"])

    # Extract biological information
    function = _extract_function_from_headline(headline)
    actions = _extract_actions_from_headline(headline)
    models = _extract_model_systems(headline, direct_evidence)

    # Determine role - prioritize molecular function
    if function:
        role = function
    else:
        role = "protein"

    parts = [f"{gene_name} is {article(role)} {role}"]

    # Add mechanisms (up to 2), with calibrated verbs
    if actions:
        formatted = []
        for action in actions[:2]:
            verb_part = action.split(" ", 1)[0] if " " in action else action

            # Calibrate verbs for weak evidence
            if evidence_tier in ("indirect_low", "annotation_supported"):
                if verb_part in ("controls", "required", "essential", "drives"):
                    action = action.replace(verb_part, "is associated with", 1)

            if action.startswith("required for "):
                formatted.append("is " + action)
            elif action.startswith("essential for "):
                formatted.append("is " + action)
            elif action.startswith("involved in "):
                formatted.append("is " + action)
            elif action.startswith("role in "):
                formatted.append("plays a " + action)
            else:
                formatted.append(action)

        if len(formatted) == 1:
            parts.append(f"that {formatted[0]}")
        else:
            parts.append(f"that {formatted[0]} and {formatted[1]}")

    # Add virulence context based on evidence tier (max 1 concept to avoid repetition)
    virulence_phrase = get_virulence_phrase(categories, max_items=1)

    # Check if we already mentioned similar concepts in the action phrase
    has_role_phrase = any("plays a" in p or "role in" in p for p in parts)

    if virulence_phrase and not has_role_phrase:
        if evidence_tier == "in_vivo_strong":
            parts.append(f"and contributes to {virulence_phrase}")
        elif evidence_tier == "experimental_strong":
            parts.append(f"and is involved in {virulence_phrase}")
        elif evidence_tier == "experimental_moderate":
            parts.append(f"and is associated with {virulence_phrase}")
        elif evidence_tier == "annotation_supported":
            parts.append(f"with annotation support for {virulence_phrase}")
        else:
            parts.append(f"with limited evidence for {virulence_phrase}")

    summary = " ".join(parts).strip()
    if not summary.endswith("."):
        summary += "."

    # Add evidence ending for strong evidence (same rules as generate_summary)
    ending = tier_config.get("ending_phrase")
    if ending and evidence_tier == "in_vivo_strong":
        confidence = confidence_tier.lower() if confidence_tier else "low"
        if confidence == "high" and not _is_generic_gene(role):
            summary += " " + ending

    # Add study signal
    if paper_count >= 10:
        summary += f" ({paper_count} publications)"
    elif paper_count >= 5:
        summary += f" ({paper_count} studies)"

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
