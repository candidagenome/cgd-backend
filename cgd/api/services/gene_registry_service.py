"""
Gene Registry Service.

Provides gene name validation and registration functionality.
"""
from __future__ import annotations

import re
import json
import logging
from typing import List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func, or_

from cgd.models.models import (
    Feature, FeatAlias, Alias, FeatProperty, Organism, Colleague, CollUrl, Url,
)
from cgd.schemas.gene_registry_schema import (
    GeneValidationResult,
    ColleagueMatch,
    GeneRegistrySearchResponse,
    SpeciesOption,
    GeneRegistryConfigResponse,
)

logger = logging.getLogger(__name__)

# Gene name pattern: 3 letters + 1 or more numbers
GENE_NAME_PATTERN = re.compile(r'^[a-zA-Z]{3}[0-9]+$')


def _mask_email(email: Optional[str]) -> Optional[str]:
    """Partially mask email for display."""
    if not email or '@' not in email:
        return email
    local, domain = email.split('@', 1)
    if len(local) <= 2:
        masked_local = local[0] + '*'
    else:
        masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
    return f"{masked_local}@{domain}"


def _get_organism_display_name(db: Session, organism_abbrev: str) -> str:
    """Get display name for organism."""
    organism = (
        db.query(Organism)
        .filter(Organism.organism_abbrev == organism_abbrev)
        .first()
    )
    if not organism:
        return organism_abbrev

    # Try to get species name from parent
    if organism.taxonomic_rank == 'Species':
        return organism.organism_name

    # For strains, get parent species
    if organism.parent_organism_no:
        parent = (
            db.query(Organism)
            .filter(Organism.organism_no == organism.parent_organism_no)
            .first()
        )
        if parent and parent.taxonomic_rank == 'Species':
            return parent.organism_name

    return organism.organism_name


def _get_feature_qualifier(db: Session, feature_no: int) -> Optional[str]:
    """Get feature qualifier (Verified, Uncharacterized, Dubious, Deleted, etc.)."""
    prop = (
        db.query(FeatProperty.property_value)
        .filter(
            FeatProperty.feature_no == feature_no,
            FeatProperty.property_type == 'Qualifier',
        )
        .first()
    )
    return prop[0] if prop else None


def validate_gene_name(
    db: Session,
    gene_name: str,
    orf_name: Optional[str],
    organism_abbrev: str,
) -> GeneValidationResult:
    """
    Validate a proposed gene name.

    Checks:
    - Gene name format (3 letters + numbers)
    - Gene name not already in database
    - Gene name not an alias
    - ORF exists (if provided)
    - ORF not deleted/merged
    - ORF not already named
    """
    warnings = []
    errors = []

    result = GeneValidationResult(
        is_valid=True,
        format_valid=True,
    )

    # Get organism
    organism = (
        db.query(Organism)
        .filter(Organism.organism_abbrev == organism_abbrev)
        .first()
    )
    if not organism:
        result.is_valid = False
        result.errors = [f"Unknown organism: {organism_abbrev}"]
        return result

    # Check gene name format
    if not GENE_NAME_PATTERN.match(gene_name):
        result.format_valid = False
        errors.append(
            f"'{gene_name}' is not an acceptable gene name. "
            "Gene names must be three letters followed by one or more numbers."
        )

    # Check if gene name already exists
    existing_gene = (
        db.query(Feature)
        .filter(
            func.upper(Feature.gene_name) == gene_name.upper(),
            Feature.organism_no == organism.organism_no,
        )
        .first()
    )

    if existing_gene:
        result.gene_exists = True
        warnings.append(
            f"The gene name '{gene_name}' already exists in the database."
        )

    # Check if gene name is an alias
    alias_match = (
        db.query(Alias, Feature)
        .join(FeatAlias, FeatAlias.alias_no == Alias.alias_no)
        .join(Feature, Feature.feature_no == FeatAlias.feature_no)
        .filter(
            func.upper(Alias.alias_name) == gene_name.upper(),
            Feature.organism_no == organism.organism_no,
        )
        .first()
    )

    if alias_match:
        alias, feature = alias_match
        result.gene_is_alias = True
        result.alias_for = feature.gene_name or feature.feature_name
        warnings.append(
            f"'{gene_name}' is an alias for '{result.alias_for}'."
        )

    # Validate ORF if provided
    if orf_name:
        orf_feature = (
            db.query(Feature)
            .filter(
                func.upper(Feature.feature_name) == orf_name.upper(),
                Feature.organism_no == organism.organism_no,
            )
            .first()
        )

        if orf_feature:
            result.orf_exists = True

            # Check if ORF is deleted/merged
            qualifier = _get_feature_qualifier(db, orf_feature.feature_no)
            if qualifier and 'deleted' in qualifier.lower():
                result.orf_is_deleted = True
                errors.append(
                    f"The ORF '{orf_name}' is a Deleted ORF."
                )
            elif qualifier and 'dubious' in qualifier.lower():
                result.orf_is_dubious = True
                warnings.append(
                    f"The ORF '{orf_name}' is a Dubious ORF."
                )

            # Check if ORF already has a gene name
            if orf_feature.gene_name and orf_feature.gene_name != orf_feature.feature_name:
                result.orf_has_gene = True
                result.orf_gene_name = orf_feature.gene_name

                # Check if it matches proposed gene
                if orf_feature.gene_name.upper() != gene_name.upper():
                    warnings.append(
                        f"The ORF '{orf_name}' already has gene name "
                        f"'{orf_feature.gene_name}'."
                    )
        else:
            result.orf_exists = False
            errors.append(
                f"The ORF '{orf_name}' is not in the database."
            )

    result.warnings = warnings
    result.errors = errors
    result.is_valid = len(errors) == 0

    return result


def search_gene_registry(
    db: Session,
    last_name: str,
    gene_name: str,
    orf_name: Optional[str],
    organism_abbrev: str,
) -> GeneRegistrySearchResponse:
    """
    Search for gene registry - validate gene and find colleagues.
    """
    # Get organism display name
    organism_name = _get_organism_display_name(db, organism_abbrev)

    # Validate gene name
    validation = validate_gene_name(db, gene_name, orf_name, organism_abbrev)

    # Determine if user can proceed
    can_proceed = validation.is_valid and not validation.gene_exists

    # Allow proceeding with warnings (dubious ORF, etc.)
    if validation.orf_is_deleted:
        can_proceed = False

    # Search for colleagues
    search_term = last_name.strip()
    sql_pattern = search_term.replace('*', '%')
    wildcard_appended = False

    colleagues_query = (
        db.query(Colleague)
        .filter(
            or_(
                func.upper(Colleague.last_name).like(func.upper(sql_pattern)),
                func.upper(Colleague.other_last_name).like(func.upper(sql_pattern)),
            )
        )
    )

    colleagues = colleagues_query.all()

    # If no results and no wildcard, try with wildcard
    if not colleagues and '%' not in sql_pattern:
        wildcard_appended = True
        sql_pattern = sql_pattern + '%'
        colleagues = (
            db.query(Colleague)
            .filter(
                or_(
                    func.upper(Colleague.last_name).like(func.upper(sql_pattern)),
                    func.upper(Colleague.other_last_name).like(func.upper(sql_pattern)),
                )
            )
            .order_by(Colleague.last_name, Colleague.first_name)
            .all()
        )

    # Build colleague matches
    colleague_matches = []
    for coll in colleagues:
        # Get URLs
        urls = (
            db.query(Url.url)
            .join(CollUrl, CollUrl.url_no == Url.url_no)
            .filter(CollUrl.colleague_no == coll.colleague_no)
            .all()
        )

        full_name = f"{coll.last_name}, {coll.first_name}"
        if coll.suffix:
            full_name += f" {coll.suffix}"

        colleague_matches.append(ColleagueMatch(
            colleague_no=coll.colleague_no,
            full_name=full_name,
            institution=coll.institution,
            email=_mask_email(coll.email),
            work_phone=coll.work_phone,
            urls=[u[0] for u in urls],
        ))

    display_term = search_term + '*' if wildcard_appended else search_term

    return GeneRegistrySearchResponse(
        success=True,
        validation=validation,
        can_proceed=can_proceed,
        wildcard_appended=wildcard_appended,
        search_term=display_term,
        colleagues=colleague_matches,
        organism_name=organism_name,
    )


def get_gene_registry_config(db: Session) -> GeneRegistryConfigResponse:
    """
    Get configuration for gene registry form.

    Returns available species and default species.
    """
    species_options = []

    # Get organisms that have features (strains with genomic data)
    # Query distinct organisms from the feature table
    organism_nos = (
        db.query(Feature.organism_no)
        .distinct()
        .all()
    )
    organism_no_list = [o[0] for o in organism_nos]

    if organism_no_list:
        organisms = (
            db.query(Organism)
            .filter(Organism.organism_no.in_(organism_no_list))
            .order_by(Organism.organism_order)
            .all()
        )

        for org in organisms:
            # Get species name - either from parent or self
            species_name = org.organism_name
            if org.parent_organism_no:
                parent = (
                    db.query(Organism)
                    .filter(Organism.organism_no == org.parent_organism_no)
                    .first()
                )
                if parent and 'species' in parent.taxonomic_rank.lower():
                    species_name = parent.organism_name

            species_options.append(SpeciesOption(
                abbrev=org.organism_abbrev,
                name=species_name,
            ))

    # Fallback to hardcoded defaults if database query fails
    if not species_options:
        species_options = [
            SpeciesOption(abbrev='C_albicans_SC5314', name='Candida albicans'),
            SpeciesOption(abbrev='C_glabrata_CBS138', name='Candida glabrata'),
        ]

    # Default to C_albicans_SC5314 or first option
    default_species = 'C_albicans_SC5314'
    if not any(s.abbrev == default_species for s in species_options):
        default_species = species_options[0].abbrev if species_options else ''

    return GeneRegistryConfigResponse(
        species=species_options,
        default_species=default_species,
        gene_name_pattern=GENE_NAME_PATTERN.pattern,
        nomenclature_url='/Nomenclature.shtml',
    )


def submit_gene_registry(
    db: Session,
    data: dict,
) -> dict:
    """
    Submit gene registry request.

    Creates a submission record for curator review.
    """
    errors = []

    # Validate required fields
    gene_name = data.get('gene_name', '').strip()
    organism = data.get('organism', '').strip()

    if not gene_name:
        errors.append("Gene name is required")
    if not organism:
        errors.append("Organism is required")

    # Check if we have colleague info
    colleague_no = data.get('colleague_no')
    if not colleague_no:
        # New colleague - validate required fields
        if not data.get('last_name'):
            errors.append("Last name is required")
        if not data.get('first_name'):
            errors.append("First name is required")
        if not data.get('email'):
            errors.append("Email is required")
        if not data.get('institution'):
            errors.append("Organization is required")

    if errors:
        return {
            'success': False,
            'errors': errors,
        }

    # Log the submission (in production, save to database or send email)
    submission_data = {
        'type': 'gene_registry',
        'submitted_at': datetime.now().isoformat(),
        'data': data,
    }

    logger.info(f"Gene registry submission: {json.dumps(submission_data, default=str)}")

    return {
        'success': True,
        'message': (
            "Your gene name reservation has been submitted and will be reviewed "
            "by our curators. You will receive a confirmation email."
        ),
    }
