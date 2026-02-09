"""
Tests for Feature Curation Service.

Tests cover:
- Organism and chromosome lookups
- Feature existence check
- Feature creation with validation
- Location management
- Feature deletion
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from cgd.api.services.curation.feature_curation_service import (
    FeatureCurationService,
    FeatureCurationError,
)


class MockOrganism:
    """Mock Organism model."""

    def __init__(
        self,
        organism_no: int,
        organism_name: str,
        organism_abbrev: str,
        organism_order: int = 1,
    ):
        self.organism_no = organism_no
        self.organism_name = organism_name
        self.organism_abbrev = organism_abbrev
        self.organism_order = organism_order


class MockFeature:
    """Mock Feature model."""

    def __init__(
        self,
        feature_no: int,
        feature_name: str,
        organism_no: int = 1,
        gene_name: str = None,
        feature_type: str = "ORF",
        dbxref_id: str = None,
        source: str = "CGD",
        created_by: str = None,
    ):
        self.feature_no = feature_no
        self.feature_name = feature_name
        self.organism_no = organism_no
        self.gene_name = gene_name
        self.feature_type = feature_type
        self.dbxref_id = dbxref_id or feature_name
        self.source = source
        self.created_by = created_by


class MockFeatLocation:
    """Mock FeatLocation model."""

    def __init__(
        self,
        feat_location_no: int,
        feature_no: int,
        root_seq_no: int,
        seq_no: int,
        start_coord: int,
        stop_coord: int,
        strand: str = "W",
        is_loc_current: str = "Y",
    ):
        self.feat_location_no = feat_location_no
        self.feature_no = feature_no
        self.root_seq_no = root_seq_no
        self.seq_no = seq_no
        self.start_coord = start_coord
        self.stop_coord = stop_coord
        self.strand = strand
        self.is_loc_current = is_loc_current


class MockSeq:
    """Mock Seq model."""

    def __init__(
        self,
        seq_no: int,
        feature_no: int,
        seq_type: str = "genomic",
        is_seq_current: str = "Y",
        source: str = "GenBank",
    ):
        self.seq_no = seq_no
        self.feature_no = feature_no
        self.seq_type = seq_type
        self.is_seq_current = is_seq_current
        self.source = source


class MockGenomeVersion:
    """Mock GenomeVersion model."""

    def __init__(
        self,
        genome_version_no: int,
        organism_no: int,
        is_ver_current: str = "Y",
    ):
        self.genome_version_no = genome_version_no
        self.organism_no = organism_no
        self.is_ver_current = is_ver_current


class MockReference:
    """Mock Reference model."""

    def __init__(self, reference_no: int, citation: str = "Test Citation"):
        self.reference_no = reference_no
        self.citation = citation


class MockRefLink:
    """Mock RefLink model."""

    def __init__(
        self,
        ref_link_no: int,
        reference_no: int,
        tab_name: str,
        primary_key: int,
        col_name: str,
    ):
        self.ref_link_no = ref_link_no
        self.reference_no = reference_no
        self.tab_name = tab_name
        self.primary_key = primary_key
        self.col_name = col_name


class MockQuery:
    """Mock SQLAlchemy query."""

    def __init__(self, results=None):
        self._results = results or []

    def filter(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def order_by(self, *args):
        return self

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results

    def delete(self):
        pass


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    db = MagicMock()
    db.query.return_value = MockQuery([])
    return db


@pytest.fixture
def sample_organisms():
    """Create sample organisms."""
    return [
        MockOrganism(1, "Candida albicans SC5314", "C_albicans_SC5314", 1),
        MockOrganism(2, "Candida glabrata CBS138", "C_glabrata_CBS138", 2),
    ]


@pytest.fixture
def sample_features():
    """Create sample features."""
    return [
        MockFeature(1, "ChrA", feature_type="chromosome"),
        MockFeature(2, "CAL0001", gene_name="ALS1", feature_type="ORF"),
        MockFeature(3, "CAL0002", feature_type="ORF"),
    ]


class TestConstants:
    """Tests for service constants."""

    def test_feature_types_includes_orf(self):
        """Should include ORF as a valid feature type."""
        assert "ORF" in FeatureCurationService.FEATURE_TYPES

    def test_feature_types_includes_pseudogene(self):
        """Should include pseudogene."""
        assert "pseudogene" in FeatureCurationService.FEATURE_TYPES

    def test_feature_types_includes_trna(self):
        """Should include tRNA gene."""
        assert "tRNA gene" in FeatureCurationService.FEATURE_TYPES

    def test_qualifiers_includes_verified(self):
        """Should include Verified qualifier."""
        assert "Verified" in FeatureCurationService.FEATURE_QUALIFIERS

    def test_qualifiers_includes_dubious(self):
        """Should include Dubious qualifier."""
        assert "Dubious" in FeatureCurationService.FEATURE_QUALIFIERS

    def test_strands(self):
        """Should define Watson and Crick strands."""
        assert FeatureCurationService.STRANDS == ["W", "C"]

    def test_source(self):
        """Should define source as CGD."""
        assert FeatureCurationService.SOURCE == "CGD"


class TestGetOrganisms:
    """Tests for getting organisms."""

    def test_returns_organism_list(self, mock_db, sample_organisms):
        """Should return list of organisms."""
        mock_db.query.return_value = MockQuery(sample_organisms)

        service = FeatureCurationService(mock_db)
        results = service.get_organisms()

        assert len(results) == 2
        assert results[0]["organism_no"] == 1
        assert results[0]["organism_name"] == "Candida albicans SC5314"
        assert results[0]["organism_abbrev"] == "C_albicans_SC5314"

    def test_returns_empty_for_no_organisms(self, mock_db):
        """Should return empty list when no organisms."""
        mock_db.query.return_value = MockQuery([])

        service = FeatureCurationService(mock_db)
        results = service.get_organisms()

        assert len(results) == 0


class TestGetChromosomes:
    """Tests for getting chromosomes."""

    def test_returns_chromosomes_for_organism(self, mock_db, sample_organisms, sample_features):
        """Should return chromosomes for given organism."""
        mock_db.query.side_effect = [
            MockQuery([sample_organisms[0]]),  # Organism lookup
            MockQuery([sample_features[0]]),   # Chromosome features
        ]

        service = FeatureCurationService(mock_db)
        results = service.get_chromosomes("C_albicans_SC5314")

        assert len(results) == 1
        assert results[0]["feature_name"] == "ChrA"

    def test_returns_empty_for_unknown_organism(self, mock_db):
        """Should return empty list for unknown organism."""
        mock_db.query.return_value = MockQuery([])

        service = FeatureCurationService(mock_db)
        results = service.get_chromosomes("Unknown_organism")

        assert len(results) == 0


class TestCheckFeatureExists:
    """Tests for checking if feature exists."""

    def test_returns_feature_info_if_found(self, mock_db, sample_features):
        """Should return feature info when found."""
        mock_db.query.return_value = MockQuery([sample_features[1]])

        service = FeatureCurationService(mock_db)
        result = service.check_feature_exists("CAL0001")

        assert result is not None
        assert result["feature_no"] == 2
        assert result["feature_name"] == "CAL0001"
        assert result["gene_name"] == "ALS1"

    def test_returns_none_if_not_found(self, mock_db):
        """Should return None when not found."""
        mock_db.query.return_value = MockQuery([])

        service = FeatureCurationService(mock_db)
        result = service.check_feature_exists("NONEXISTENT")

        assert result is None


class TestCreateFeature:
    """Tests for creating features."""

    def test_raises_for_existing_feature(self, mock_db, sample_features):
        """Should raise error if feature already exists."""
        mock_db.query.return_value = MockQuery([sample_features[1]])

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.create_feature(
                feature_name="CAL0001",
                feature_type="ORF",
                organism_abbrev="C_albicans_SC5314",
                curator_userid="curator1",
            )

        assert "already exists" in str(exc_info.value)

    def test_raises_for_invalid_feature_type(self, mock_db):
        """Should raise error for invalid feature type."""
        mock_db.query.return_value = MockQuery([])  # Feature doesn't exist

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.create_feature(
                feature_name="NEW001",
                feature_type="invalid_type",
                organism_abbrev="C_albicans_SC5314",
                curator_userid="curator1",
            )

        assert "Invalid feature type" in str(exc_info.value)

    def test_raises_for_unknown_organism(self, mock_db):
        """Should raise error for unknown organism."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Feature doesn't exist
            MockQuery([]),  # Organism not found
        ]

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.create_feature(
                feature_name="NEW001",
                feature_type="ORF",
                organism_abbrev="Unknown",
                curator_userid="curator1",
            )

        assert "not found" in str(exc_info.value)

    def test_raises_when_chromosome_without_coords(self, mock_db, sample_organisms):
        """Should raise error when chromosome given without coordinates."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Feature doesn't exist
            MockQuery([sample_organisms[0]]),  # Organism found
        ]

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.create_feature(
                feature_name="NEW001",
                feature_type="ORF",
                organism_abbrev="C_albicans_SC5314",
                curator_userid="curator1",
                chromosome_name="ChrA",
            )

        assert "coordinates are required" in str(exc_info.value)

    def test_raises_when_strand_required_but_missing(self, mock_db, sample_organisms):
        """Should raise error when strand required but not provided."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Feature doesn't exist
            MockQuery([sample_organisms[0]]),  # Organism found
        ]

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.create_feature(
                feature_name="NEW001",
                feature_type="ORF",
                organism_abbrev="C_albicans_SC5314",
                curator_userid="curator1",
                chromosome_name="ChrA",
                start_coord=1000,
                stop_coord=2000,
            )

        assert "Strand is required" in str(exc_info.value)

    def test_raises_for_invalid_strand(self, mock_db, sample_organisms):
        """Should raise error for invalid strand."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Feature doesn't exist
            MockQuery([sample_organisms[0]]),  # Organism found
        ]

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.create_feature(
                feature_name="NEW001",
                feature_type="ORF",
                organism_abbrev="C_albicans_SC5314",
                curator_userid="curator1",
                chromosome_name="ChrA",
                start_coord=1000,
                stop_coord=2000,
                strand="X",
            )

        assert "Invalid strand" in str(exc_info.value)

    def test_raises_for_wrong_watson_coords(self, mock_db, sample_organisms):
        """Should raise error when Watson coords are backwards."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Feature doesn't exist
            MockQuery([sample_organisms[0]]),  # Organism found
        ]

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.create_feature(
                feature_name="NEW001",
                feature_type="ORF",
                organism_abbrev="C_albicans_SC5314",
                curator_userid="curator1",
                chromosome_name="ChrA",
                start_coord=2000,
                stop_coord=1000,
                strand="W",
            )

        assert "Watson strand" in str(exc_info.value)

    def test_raises_for_wrong_crick_coords(self, mock_db, sample_organisms):
        """Should raise error when Crick coords are backwards."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Feature doesn't exist
            MockQuery([sample_organisms[0]]),  # Organism found
        ]

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.create_feature(
                feature_name="NEW001",
                feature_type="ORF",
                organism_abbrev="C_albicans_SC5314",
                curator_userid="curator1",
                chromosome_name="ChrA",
                start_coord=1000,
                stop_coord=2000,
                strand="C",
            )

        assert "Crick strand" in str(exc_info.value)

    def test_creates_feature_without_coords(self, mock_db, sample_organisms):
        """Should create feature without coordinates."""
        mock_db.query.side_effect = [
            MockQuery([]),  # Feature doesn't exist
            MockQuery([sample_organisms[0]]),  # Organism found
        ]

        service = FeatureCurationService(mock_db)
        service.create_feature(
            feature_name="NEW001",
            feature_type="not physically mapped",
            organism_abbrev="C_albicans_SC5314",
            curator_userid="curator1",
        )

        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()
        mock_db.commit.assert_called_once()


class TestAddLocationToFeature:
    """Tests for adding location to existing feature."""

    def test_raises_for_unknown_feature(self, mock_db):
        """Should raise error for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.add_location_to_feature(
                feature_name="UNKNOWN",
                organism_abbrev="C_albicans_SC5314",
                chromosome_name="ChrA",
                start_coord=1000,
                stop_coord=2000,
                strand="W",
                curator_userid="curator1",
            )

        assert "not found" in str(exc_info.value)

    def test_raises_for_unmappable_feature_type(self, mock_db, sample_organisms):
        """Should raise error for feature types that can't have locations."""
        feature = MockFeature(1, "GENE1", feature_type="not in systematic sequence")

        mock_db.query.side_effect = [
            MockQuery([feature]),  # Feature found
        ]

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.add_location_to_feature(
                feature_name="GENE1",
                organism_abbrev="C_albicans_SC5314",
                chromosome_name="ChrA",
                start_coord=1000,
                stop_coord=2000,
                strand="W",
                curator_userid="curator1",
            )

        assert "Cannot add location" in str(exc_info.value)

    def test_raises_when_strand_required_but_missing(self, mock_db):
        """Should raise error when strand required but not provided."""
        feature = MockFeature(1, "GENE1", feature_type="ORF")

        mock_db.query.side_effect = [
            MockQuery([feature]),  # Feature found
        ]

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.add_location_to_feature(
                feature_name="GENE1",
                organism_abbrev="C_albicans_SC5314",
                chromosome_name="ChrA",
                start_coord=1000,
                stop_coord=2000,
                strand=None,
                curator_userid="curator1",
            )

        assert "Strand is required" in str(exc_info.value)


class TestGetFeatureInfo:
    """Tests for getting feature info."""

    def test_returns_none_for_unknown_feature(self, mock_db):
        """Should return None for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = FeatureCurationService(mock_db)
        result = service.get_feature_info("UNKNOWN", "C_albicans_SC5314")

        assert result is None

    def test_returns_feature_info(self, mock_db, sample_features):
        """Should return feature info with locations."""
        mock_db.query.side_effect = [
            MockQuery([sample_features[1]]),  # Feature found
            MockQuery([]),  # No locations
        ]

        service = FeatureCurationService(mock_db)
        result = service.get_feature_info("CAL0001", "C_albicans_SC5314")

        assert result is not None
        assert result["feature_no"] == 2
        assert result["feature_name"] == "CAL0001"
        assert result["gene_name"] == "ALS1"
        assert result["feature_type"] == "ORF"
        assert "locations" in result

    def test_includes_location_info(self, mock_db, sample_features):
        """Should include location information."""
        location = MockFeatLocation(1, 2, 100, 101, 1000, 2000, "W")
        seq = MockSeq(100, 1)  # Chromosome seq
        chromosome = sample_features[0]

        mock_db.query.side_effect = [
            MockQuery([sample_features[1]]),  # Feature found
            MockQuery([location]),  # Locations
            MockQuery([seq]),  # Chromosome seq
            MockQuery([chromosome]),  # Chromosome feature
        ]

        service = FeatureCurationService(mock_db)
        result = service.get_feature_info("CAL0001", "C_albicans_SC5314")

        assert len(result["locations"]) == 1
        assert result["locations"][0]["start_coord"] == 1000
        assert result["locations"][0]["stop_coord"] == 2000
        assert result["locations"][0]["strand"] == "W"


class TestDeleteFeature:
    """Tests for deleting features."""

    def test_raises_for_unknown_feature(self, mock_db):
        """Should raise error for unknown feature."""
        mock_db.query.return_value = MockQuery([])

        service = FeatureCurationService(mock_db)

        with pytest.raises(FeatureCurationError) as exc_info:
            service.delete_feature(999, "curator1")

        assert "not found" in str(exc_info.value)

    def test_deletes_feature(self, mock_db, sample_features):
        """Should delete feature and related records."""
        mock_db.query.return_value = MockQuery([sample_features[1]])

        service = FeatureCurationService(mock_db)
        service.delete_feature(2, "curator1")

        mock_db.delete.assert_called_once()
        mock_db.commit.assert_called_once()


class TestServiceInitialization:
    """Tests for service initialization."""

    def test_stores_db_session(self, mock_db):
        """Should store database session."""
        service = FeatureCurationService(mock_db)
        assert service.db is mock_db


class TestFeatureCurationError:
    """Tests for the error class."""

    def test_exception_message(self):
        """Should store error message."""
        error = FeatureCurationError("Test error")
        assert str(error) == "Test error"

    def test_is_exception(self):
        """Should be an Exception."""
        assert issubclass(FeatureCurationError, Exception)
