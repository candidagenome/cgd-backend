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


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeDb:
    def __init__(self, rows):
        self.rows = rows
        self.calls = 0

    def execute(self, *_args, **_kw):
        self.calls += 1
        return _FakeResult(self.rows)


def _row(**kw):
    from types import SimpleNamespace
    base = dict(gene_name=None, feature_name="X_A", organism_name="Candida albicans SC5314",
                feature_type="ORF", headline="a headline", name_description=None, qualifier=None)
    base.update(kw)
    return SimpleNamespace(**base)


def test_lightweight_seo_prefers_priority_organism_and_a_allele():
    from cgd.api import frontend_seo
    db = _FakeDb([
        _row(feature_name="CTRG_1", organism_name="Candida tropicalis MYA-3404", gene_name="ACT1"),
        _row(feature_name="C1_001W_B", gene_name="ACT1"),
        _row(feature_name="C1_001W_A", gene_name="ACT1"),
    ])
    seo = frontend_seo._fetch_locus_seo(db, "ACT1")
    assert seo is not None
    assert seo.feature_name == "C1_001W_A"
    assert seo.organism == "Candida albicans SC5314"


def test_get_locus_seo_caches_lookups():
    from cgd.api import frontend_seo
    frontend_seo._seo_cache.clear()
    db = _FakeDb([_row(gene_name="TESTGENE")])
    first = frontend_seo.get_locus_seo(db, "TestGene")
    second = frontend_seo.get_locus_seo(db, "TESTGENE")
    assert first == second
    assert db.calls == 1
    frontend_seo._seo_cache.clear()


def test_get_locus_seo_unknown_name_not_cached_as_error():
    from cgd.api import frontend_seo
    frontend_seo._seo_cache.clear()
    db = _FakeDb([])
    assert frontend_seo.get_locus_seo(db, "NOPE") is None
    assert db.calls == 1
    # negative results are cached too (crawlers re-hit 404-ish names)
    assert frontend_seo.get_locus_seo(db, "NOPE") is None
    assert db.calls == 1
    frontend_seo._seo_cache.clear()
