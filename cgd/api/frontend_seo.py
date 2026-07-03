from __future__ import annotations

import importlib
import json
import logging
import os
import re
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import quote

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


def render_locus_html(db: Session, name: str) -> str:
    html = read_frontend_index()
    try:
        locus_service = importlib.import_module("cgd.api.services.locus_service")
        seo = build_locus_seo(name, locus_service.get_locus_by_organism(db, name))
    except Exception:
        logger.exception("Unable to build locus SEO metadata for %s", name)
        seo = None

    return inject_locus_seo(html, seo) if seo else html
