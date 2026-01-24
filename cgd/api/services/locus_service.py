from sqlalchemy.orm import Session

from cgd.api.crud.locus_crud import get_features_for_locus_name
from cgd.schemas.locus_schema import LocusByOrganismResponse, FeatureOut


def get_locus_by_organism(db: Session, name: str) -> LocusByOrganismResponse:
    features = get_features_for_locus_name(db, name)

    out: dict[str, FeatureOut] = {}

    for f in features:
        # Pick the correct organism display field for your schema.
        # Your Feature model shows: organism: relationship('Organism', ...)
        # So organism likely has something like organism_name or display_name.
        org = f.organism

        organism_name = None
        if org is not None:
            organism_name = (
                getattr(org, "organism_name", None)
                or getattr(org, "display_name", None)
                or getattr(org, "name", None)
            )

        # Fallback if relationship isn’t loaded / doesn’t exist:
        if not organism_name:
            organism_name = str(f.organism_no)

        out[organism_name] = FeatureOut.model_validate(f)

    return LocusByOrganismResponse(results=out)
