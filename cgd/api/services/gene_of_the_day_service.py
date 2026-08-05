"""
Gene-of-the-day service.

Deterministically picks one named, characterized C. albicans SC5314 ORF per
calendar day, so everyone sees the same gene for the whole day and the pick
rotates over the set over time. Mirrors the SGD /gene_of_the_day behavior.
"""
import datetime
import logging

from sqlalchemy.orm import Session

from cgd.models.models import Feature, Organism
from cgd.schemas.stats_schema import GeneOfTheDayResponse

logger = logging.getLogger(__name__)

# The reference strain the featured gene is drawn from.
_REFERENCE_ORGANISM_ABBREV = "C_albicans_SC5314"

# Cached per calendar day so the pick is stable within a day and the DB query
# runs at most once per day regardless of landing-page traffic.
_cache = {"ordinal": None, "data": None}


def get_gene_of_the_day(db: Session) -> GeneOfTheDayResponse:
    try:
        today_ordinal = datetime.date.today().toordinal()
        if _cache["ordinal"] == today_ordinal and _cache["data"] is not None:
            return _cache["data"]

        organism = (
            db.query(Organism)
            .filter(Organism.organism_abbrev == _REFERENCE_ORGANISM_ABBREV)
            .first()
        )
        if not organism:
            return GeneOfTheDayResponse(success=False, error="Reference organism not found")

        # Named, characterized ORFs (has a gene name and a headline), ordered
        # deterministically so offset-by-ordinal is stable across processes.
        query = (
            db.query(Feature)
            .filter(
                Feature.organism_no == organism.organism_no,
                Feature.feature_type == "ORF",
                Feature.gene_name.isnot(None),
                Feature.headline.isnot(None),
            )
            .order_by(Feature.feature_name)
        )
        total = query.count()
        if total == 0:
            return GeneOfTheDayResponse(success=False, error="No candidate genes found")

        gene = query.offset(today_ordinal % total).limit(1).one()
        name = gene.gene_name or gene.feature_name
        data = GeneOfTheDayResponse(
            display_name=name,
            systematic_name=gene.feature_name,
            headline=gene.headline,
            organism=organism.organism_name,
            link=f"/locus/{name}",
            dbxref_id=gene.dbxref_id,
            success=True,
        )
        _cache["ordinal"] = today_ordinal
        _cache["data"] = data
        return data
    except Exception as exc:  # noqa: BLE001
        logger.error("Error computing gene of the day: %s", exc)
        return GeneOfTheDayResponse(success=False, error=str(exc))
