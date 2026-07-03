from cgd.api.frontend_seo import build_locus_seo, inject_locus_seo


def test_build_locus_seo_uses_query_organism_feature():
    locus_data = {
        "query_organism": "Candida albicans SC5314",
        "results": {
            "Candida glabrata CBS138": {
                "feature_name": "CAGL0A00100g",
                "gene_name": "ACT1",
                "feature_type": "ORF",
                "feature_qualifier": "Verified",
                "headline": "Actin in another species",
            },
            "Candida albicans SC5314": {
                "feature_name": "C1_13700W_A",
                "gene_name": "ACT1",
                "feature_type": "ORF",
                "feature_qualifier": "Verified",
                "headline": "Actin; gene has intron",
            },
        },
    }

    seo = build_locus_seo("ACT1", locus_data)

    assert seo.title == "ACT1 | Candida Genome Database (CGD)"
    assert seo.canonical_url == "https://www.candidagenome.org/locus/ACT1"
    assert "C1_13700W_A" in seo.description
    assert "Candida albicans SC5314" in seo.description
    assert "Actin; gene has intron" in seo.description


def test_inject_locus_seo_replaces_default_tags_and_adds_noscript():
    html = """<!doctype html>
<html lang="en">
  <head>
    <title>Candida Genome Database</title>
    <meta name="description" content="Default">
    <link rel="canonical" href="https://www.candidagenome.org/">
    <meta property="og:title" content="Default">
  </head>
  <body>
    <div id="root"></div>
  </body>
</html>"""
    seo = build_locus_seo(
        "ACT1",
        {
            "query_organism": "Candida albicans SC5314",
            "results": {
                "Candida albicans SC5314": {
                    "feature_name": "C1_13700W_A",
                    "gene_name": "ACT1",
                    "feature_type": "ORF",
                    "feature_qualifier": "Verified",
                    "headline": "Actin; gene has intron",
                }
            },
        },
    )

    rendered = inject_locus_seo(html, seo)

    assert "<title>ACT1 | Candida Genome Database (CGD)</title>" in rendered
    assert 'content="Default"' not in rendered
    assert 'href="https://www.candidagenome.org/locus/ACT1"' in rendered
    assert '<noscript id="seo-locus-summary">' in rendered
    assert "<h1>ACT1</h1>" in rendered
