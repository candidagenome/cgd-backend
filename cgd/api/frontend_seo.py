from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import quote

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DEFAULT_SITE_TITLE = "Candida Genome Database"
LOCUS_TITLE_SUFFIX = "Candida Genome Database (CGD)"
CANONICAL_ORIGIN = "https://www.candidagenome.org"
FRONTEND_DIST_DIR = Path(os.getenv("FRONTEND_DIST_DIR", "/opt/cgd_frontend/dist"))


@dataclass(frozen=True)
class LocusSeo:
    title: str
    description: str
    canonical_url: str
    display_name: str
    feature_name: str | None = None
    organism: str | None = None
    feature_type: str | None = None


def _strip_html(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]*>", " ", str(value))).strip()


def _truncate(value: str, max_length: int = 170) -> str:
    if len(value) <= max_length:
        return value
    shortened = value[: max_length - 3]
    return re.sub(r"\s+\S*$", "", shortened) + "..."


def _remove_existing_seo_tags(html: str) -> str:
    replacements = [
        r"<title>[\s\S]*?</title>",
        r"\s*<meta\s+name=[\"']description[\"'][^>]*>",
        r"\s*<link\s+rel=[\"']canonical[\"'][^>]*>",
        r"\s*<meta\s+property=[\"']og:[^\"']+[\"'][^>]*>",
        r"\s*<script\s+type=[\"']application/ld\+json[\"'][^>]*>[\s\S]*?</script>",
    ]
    for pattern in replacements:
        html = re.sub(pattern, "", html, flags=re.IGNORECASE)
    return html


def _feature_value(feature: Any, field: str) -> Any:
    if isinstance(feature, dict):
        return feature.get(field)
    return getattr(feature, field, None)


def _select_primary_feature(locus_data: Any) -> tuple[Any | None, str | None]:
    results = _feature_value(locus_data, "results")
    if not isinstance(results, dict) or not results:
        return None, None

    query_organism = _feature_value(locus_data, "query_organism")
    if query_organism and query_organism in results:
        return results[query_organism], query_organism

    organism, feature = next(iter(results.items()))
    return feature, organism


def build_locus_seo(name: str, locus_data: Any) -> LocusSeo | None:
    feature, organism_name = _select_primary_feature(locus_data)
    if not feature:
        return None

    display_name = _feature_value(feature, "gene_name") or _feature_value(feature, "feature_name") or name
    feature_name = _feature_value(feature, "feature_name")
    organism = organism_name or _feature_value(feature, "organism") or _feature_value(feature, "organism_name")
    feature_type = _feature_value(feature, "feature_type") or "locus"
    qualifier = _feature_value(feature, "feature_qualifier")
    summary = _strip_html(
        _feature_value(feature, "headline")
        or _feature_value(feature, "description")
        or _feature_value(feature, "description_with_refs")
        or _feature_value(feature, "name_description")
    )

    identifiers = [
        display_name,
        feature_name if feature_name and feature_name != display_name else None,
        organism,
        " ".join(part for part in [qualifier, feature_type] if part),
    ]
    identifier_text = " - ".join(part for part in identifiers if part)
    description = _truncate(
        f"{identifier_text}. {summary}" if summary else f"{identifier_text} in the Candida Genome Database."
    )

    return LocusSeo(
        title=f"{display_name} | {LOCUS_TITLE_SUFFIX}",
        description=description,
        canonical_url=f"{CANONICAL_ORIGIN}/locus/{quote(name, safe='')}",
        display_name=display_name,
        feature_name=feature_name,
        organism=organism,
        feature_type=feature_type,
    )


def _seo_head(seo: LocusSeo) -> str:
    json_ld = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": seo.title,
        "description": seo.description,
        "url": seo.canonical_url,
        "isPartOf": {
            "@type": "Dataset",
            "name": DEFAULT_SITE_TITLE,
            "description": "Curated genomic, gene, protein, phenotype, literature, and sequence data for Candida species.",
            "url": CANONICAL_ORIGIN,
        },
    }
    json_ld_text = json.dumps(json_ld, separators=(",", ":")).replace("<", "\\u003c")

    return "\n    ".join(
        [
            f"<title>{escape(seo.title)}</title>",
            f'<meta name="description" content="{escape(seo.description, quote=True)}">',
            f'<link rel="canonical" href="{escape(seo.canonical_url, quote=True)}">',
            f'<meta property="og:title" content="{escape(seo.title, quote=True)}">',
            f'<meta property="og:description" content="{escape(seo.description, quote=True)}">',
            '<meta property="og:type" content="profile">',
            f'<meta property="og:url" content="{escape(seo.canonical_url, quote=True)}">',
            f'<script type="application/ld+json">{json_ld_text}</script>',
        ]
    )


def _seo_noscript(seo: LocusSeo) -> str:
    subtitle = " - ".join(
        part for part in [seo.feature_name, seo.organism, seo.feature_type] if part
    )
    subtitle_html = f"\n      <p>{escape(subtitle)}</p>" if subtitle else ""
    return (
        '<noscript id="seo-locus-summary">\n'
        f"      <main><h1>{escape(seo.display_name)}</h1>{subtitle_html}\n"
        f"      <p>{escape(seo.description)}</p></main>\n"
        "    </noscript>"
    )


def inject_locus_seo(html: str, seo: LocusSeo) -> str:
    html = _remove_existing_seo_tags(html)
    html = html.replace("</head>", f"    {_seo_head(seo)}\n  </head>", 1)
    return html.replace(
        '<div id="root"></div>',
        f'<div id="root"></div>\n    {_seo_noscript(seo)}',
        1,
    )


def read_frontend_index() -> str:
    index_path = FRONTEND_DIST_DIR / "index.html"
    return index_path.read_text(encoding="utf-8")


# Display order for multi-organism names (albicans first), mirroring
# es_search_service.ORGANISM_PRIORITY without importing the service stack.
_ORGANISM_PRIORITY = [
    "Candida albicans SC5314",
    "Candida glabrata CBS138",
    "Candida auris B8441",
    "Candida dubliniensis CD36",
    "Candida parapsilosis CDC317",
    "Candida tropicalis MYA-3404",
]

# The SEO tags need seven scalar fields, so the render must NOT call the
# full locus assembly: /locus/:name is fetched by every crawler for every
# gene, and a distributed crawl of the ~42k sitemap URLs through the heavy
# path exhausts the DB pool (prod incident 2026-09-15). One indexed query
# plus a TTL cache keeps the route effectively free.
_SEO_QUERY = text("""
    SELECT f.gene_name, f.feature_name, o.organism_name, f.feature_type,
           f.headline, f.name_description,
           (SELECT MAX(fp.property_value) FROM feat_property fp
             WHERE fp.feature_no = f.feature_no
               AND fp.property_type = 'feature_qualifier'
               AND fp.property_value NOT LIKE 'Deleted%') AS qualifier
      FROM feature f
      JOIN organism o ON o.organism_no = f.organism_no
     WHERE UPPER(f.feature_name) = :name OR UPPER(f.gene_name) = :name
""")

_seo_cache: dict[str, tuple[float, LocusSeo | None]] = {}
_SEO_CACHE_TTL = 6 * 3600
_SEO_CACHE_MAX = 60000


def _fetch_locus_seo(db: Session, name: str) -> LocusSeo | None:
    rows = db.execute(_SEO_QUERY, {"name": name.upper()}).fetchall()
    if not rows:
        return None

    def rank(row):
        org = row.organism_name
        priority = (_ORGANISM_PRIORITY.index(org)
                    if org in _ORGANISM_PRIORITY else 999)
        return (priority, row.feature_name.endswith("_B"),
                row.headline is None)

    row = min(rows, key=rank)
    feature = {
        "gene_name": row.gene_name,
        "feature_name": row.feature_name,
        "feature_type": row.feature_type,
        "feature_qualifier": row.qualifier,
        "headline": row.headline,
        "name_description": row.name_description,
    }
    return build_locus_seo(
        name,
        {"results": {row.organism_name: feature},
         "query_organism": row.organism_name},
    )


def get_locus_seo(db: Session, name: str) -> LocusSeo | None:
    """TTL-cached lightweight SEO metadata lookup for /locus/:name."""
    key = name.strip().upper()
    now = time.monotonic()
    hit = _seo_cache.get(key)
    if hit and hit[0] > now:
        return hit[1]
    try:
        seo = _fetch_locus_seo(db, name)
    except Exception:
        logger.exception("Unable to build locus SEO metadata for %s", name)
        return None
    if len(_seo_cache) >= _SEO_CACHE_MAX:
        _seo_cache.clear()
    _seo_cache[key] = (now + _SEO_CACHE_TTL, seo)
    return seo


def render_locus_html(db: Session, name: str) -> str:
    html = read_frontend_index()
    seo = get_locus_seo(db, name)
    return inject_locus_seo(html, seo) if seo else html
