"""
Tests for phenotype enrichment: merging phenotype records for enrichment and
counting every tested phenotype in the multiple-testing correction.
"""
from unittest.mock import MagicMock

from cgd.api.services.phenotype_enrichment_service import (
    _apply_multiple_testing_correction,
    _get_merged_phenotype_map,
    _get_phenotype_annotations,
)
from cgd.schemas.phenotype_enrichment_schema import MultipleCorrectionMethod


def _db_returning(rows):
    """Mock session whose query(...)...all() returns rows."""
    db = MagicMock()
    query = db.query.return_value
    query.order_by.return_value.all.return_value = rows
    query.filter.return_value.all.return_value = rows
    return db


class TestMergedPhenotypeMap:
    def test_same_observable_mutant_qualifier_merge(self):
        # Same phenotype recorded under two experiment types (rows 10 and 25)
        db = _db_returning([
            (10, 'resistance to chemicals', 'null', 'decreased'),
            (11, 'viable', 'null', None),
            (25, 'resistance to chemicals', 'null', 'decreased'),
            (30, 'resistance to chemicals', 'null', 'increased'),
        ])
        merged = _get_merged_phenotype_map(db)
        assert merged == {10: 10, 11: 11, 25: 10, 30: 30}

    def test_different_mutant_type_not_merged(self):
        db = _db_returning([
            (1, 'viable', 'null', None),
            (2, 'viable', 'repressible', None),
        ])
        assert _get_merged_phenotype_map(db) == {1: 1, 2: 2}


class TestPhenotypeAnnotationsMerged:
    def test_annotations_use_representative(self):
        # Gene 100 annotated via row 25, gene 200 via row 10: same phenotype
        db = _db_returning([(100, 25), (200, 10), (200, 11)])
        annotations = _get_phenotype_annotations(db, [100, 200], {10: 10, 11: 11, 25: 10})
        assert annotations == {100: {10}, 200: {10, 11}}

    def test_without_map_keeps_phenotype_nos(self):
        db = _db_returning([(100, 25)])
        assert _get_phenotype_annotations(db, [100]) == {100: {25}}


class TestCorrectionCountsAllTests:
    RESULTS = [(1, 3, 10, 30, 1000, 0.01)] + [(i, 2, 10, 300, 1000, 0.6) for i in range(2, 11)]

    def test_bh_uses_all_tested_phenotypes(self):
        corrected = _apply_multiple_testing_correction(
            self.RESULTS, MultipleCorrectionMethod.BENJAMINI_HOCHBERG, 0.05)
        assert corrected == []

    def test_no_correction_applies_raw_cutoff(self):
        corrected = _apply_multiple_testing_correction(self.RESULTS, MultipleCorrectionMethod.NONE, 0.05)
        assert [r[0] for r in corrected] == [1]
