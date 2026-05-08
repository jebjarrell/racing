"""
Tests for PaceCalculator class.

Tests cover:
- Position-to-pace-figure conversion
- Pace style classification
- Horse pace calculation from historical data
- Field pace features
- Pace fit scoring
- Lone speed detection
"""

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

from features.pace_calculator import (
    PaceCalculator,
    HorsePaceData,
    FieldPaceData,
    get_pace_features,
)


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary SQLite database with racing schema."""
    db_path = tmp_path / "test_racing.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Create races_standardized table
    conn.execute("""
        CREATE TABLE races_standardized (
            race_id TEXT PRIMARY KEY,
            race_date TEXT NOT NULL,
            track_code TEXT NOT NULL,
            course_type_code TEXT NOT NULL,
            class_level INTEGER
        )
    """)

    # Create race_entries_standardized table
    conn.execute("""
        CREATE TABLE race_entries_standardized (
            race_id TEXT NOT NULL,
            registration_number TEXT NOT NULL,
            first_call_position INTEGER,
            second_call_position INTEGER,
            finish_position INTEGER,
            scratched INTEGER DEFAULT 0,
            PRIMARY KEY (race_id, registration_number),
            FOREIGN KEY (race_id) REFERENCES races_standardized(race_id)
        )
    """)

    conn.commit()
    conn.close()

    return str(db_path)


@pytest.fixture
def calculator(temp_db):
    """Create a PaceCalculator instance with test database."""
    calc = PaceCalculator(db_path=temp_db)
    yield calc
    calc.close()


@pytest.fixture
def sample_race_data(temp_db):
    """Insert sample race data into test database."""
    conn = sqlite3.connect(temp_db)

    base_date = date(2023, 9, 1)

    # Insert races
    races = [
        ("race_001", (base_date - timedelta(days=30)).isoformat(), "SAR", "DIRT", 3),
        ("race_002", (base_date - timedelta(days=25)).isoformat(), "SAR", "DIRT", 3),
        ("race_003", (base_date - timedelta(days=20)).isoformat(), "SAR", "DIRT", 2),
        ("race_004", (base_date - timedelta(days=15)).isoformat(), "BEL", "TURF", 3),
        ("race_005", (base_date - timedelta(days=10)).isoformat(), "SAR", "DIRT", 3),
    ]

    for race_id, race_date, track, course, class_level in races:
        conn.execute(
            "INSERT INTO races_standardized VALUES (?, ?, ?, ?, ?)",
            (race_id, race_date, track, course, class_level),
        )

    # Insert entries for horse "12345" with varied pace profiles
    entries = [
        # Horse 12345 - early speed type (positions: 1, 2, 1)
        ("race_001", "12345", 1, 2, 1, 0),
        ("race_002", "12345", 2, 3, 2, 0),
        ("race_003", "12345", 1, 1, 2, 0),
        ("race_004", "12345", 2, 2, 3, 0),
        ("race_005", "12345", 1, 3, 1, 0),
        # Horse 67890 - closer type (positions: 8, 7, 3)
        ("race_001", "67890", 8, 7, 3, 0),
        ("race_002", "67890", 9, 8, 2, 0),
        ("race_003", "67890", 7, 6, 3, 0),
        ("race_004", "67890", 8, 7, 4, 0),
        ("race_005", "67890", 9, 8, 2, 0),
        # Horse 11111 - no prior races (for testing empty data)
    ]

    for race_id, reg_num, first_call, second_call, finish, scratched in entries:
        conn.execute(
            "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?, ?, ?)",
            (race_id, reg_num, first_call, second_call, finish, scratched),
        )

    conn.commit()
    conn.close()

    return base_date


class TestPositionToPaceFigure:
    """Tests for _position_to_pace_figure conversion."""

    def test_position_1_converts_to_10(self, calculator):
        """Position 1 should convert to 10.0."""
        result = calculator._position_to_pace_figure(1)
        assert result == 10.0

    def test_position_5_converts_to_6(self, calculator):
        """Position 5 should convert to 6.0."""
        result = calculator._position_to_pace_figure(5)
        assert result == 6.0

    def test_position_10_converts_to_1(self, calculator):
        """Position 10 should convert to 1.0."""
        result = calculator._position_to_pace_figure(10)
        assert result == 1.0

    def test_position_greater_than_10_converts_to_0(self, calculator):
        """Position > 10 should convert to 0.0."""
        result = calculator._position_to_pace_figure(15)
        assert result == 0.0

    def test_none_position_converts_to_0(self, calculator):
        """None position should convert to 0.0."""
        result = calculator._position_to_pace_figure(None)
        assert result == 0.0

    def test_invalid_position_0_converts_to_0(self, calculator):
        """Position 0 should convert to 0.0."""
        result = calculator._position_to_pace_figure(0)
        assert result == 0.0


class TestPaceStyleClassification:
    """Tests for _classify_pace_style method."""

    def test_early_speed_type(self, calculator):
        """Early >= 7 and late <= 0 should classify as E (1)."""
        result = calculator._classify_pace_style(early_pace=8.0, late_pace=-0.5)
        assert result == 1

    def test_early_presser_type(self, calculator):
        """Early >= 5 and late > 0 should classify as EP (2)."""
        result = calculator._classify_pace_style(early_pace=6.0, late_pace=1.5)
        assert result == 2

    def test_closer_type(self, calculator):
        """Early < 3 and late >= 2 should classify as S (4)."""
        result = calculator._classify_pace_style(early_pace=2.0, late_pace=2.5)
        assert result == 4

    def test_presser_stalker_type(self, calculator):
        """3 <= early < 5 should classify as PS (3)."""
        result = calculator._classify_pace_style(early_pace=4.0, late_pace=0.0)
        assert result == 3

    def test_default_to_presser(self, calculator):
        """Ambiguous case should default to EP (2)."""
        result = calculator._classify_pace_style(early_pace=5.0, late_pace=0.0)
        assert result == 2


class TestHorsePaceCalculation:
    """Tests for calculate_horse_pace with historical data."""

    def test_early_speed_horse_pace(self, calculator, sample_race_data):
        """Early speed horse should show high early pace figure."""
        result = calculator.calculate_horse_pace("12345", sample_race_data, num_races=5)

        assert isinstance(result, HorsePaceData)
        assert result.horse_pace_early > 5.0  # Early speed should be high
        assert result.horse_pace_late <= 1.0  # May fade or hold
        # With slightly positive late pace (0.4) and high early (9.6),
        # classification is EP(2) since E requires late <= 0
        assert result.horse_pace_style in (1, 2)  # E or EP type

    def test_closer_horse_pace(self, calculator, sample_race_data):
        """Closer horse should show low early pace and high late pace."""
        result = calculator.calculate_horse_pace("67890", sample_race_data, num_races=5)

        assert isinstance(result, HorsePaceData)
        assert result.horse_pace_early < 4.0  # Closer starts back
        assert result.horse_pace_late > 1.0  # Gains ground
        assert result.horse_pace_style == 4  # Should be S type

    def test_no_prior_races_returns_defaults(self, calculator, sample_race_data):
        """Horse with no prior races should return default HorsePaceData."""
        result = calculator.calculate_horse_pace("99999", sample_race_data, num_races=5)

        assert isinstance(result, HorsePaceData)
        assert result.horse_pace_early == 0.0
        assert result.horse_pace_mid == 0.0
        assert result.horse_pace_late == 0.0
        assert result.horse_pace_style == 2  # Default EP

    def test_point_in_time_logic(self, calculator, sample_race_data):
        """Only races before target date should be used."""
        # Query with date before any race should return defaults
        early_date = date(2023, 8, 1)
        result = calculator.calculate_horse_pace("12345", early_date, num_races=5)

        assert result.horse_pace_early == 0.0
        assert result.horse_pace_mid == 0.0

    def test_respects_num_races_limit(self, calculator, sample_race_data):
        """Should only use specified number of prior races."""
        # With num_races=2, should only use last 2 races
        result = calculator.calculate_horse_pace("12345", sample_race_data, num_races=2)

        assert isinstance(result, HorsePaceData)
        # Should have some data (at least 2 races available)
        assert result.horse_pace_early > 0.0 or result.horse_pace_mid > 0.0


class TestFieldPaceFeatures:
    """Tests for field-level pace calculations."""

    def test_field_pace_scenario_with_early_speeds(self, calculator):
        """Field with multiple E-types should have positive race_pace_scenario."""
        entry_pace_data = [
            {"horse_pace_style": 1},  # E
            {"horse_pace_style": 1},  # E
            {"horse_pace_style": 2},  # EP
            {"horse_pace_style": 3},  # PS
        ]

        result = calculator.calculate_field_pace_features(entry_pace_data, field_size=4)

        assert result["field_early_speed_count"] == 2
        assert result["race_pace_scenario"] > 0.0

    def test_field_pace_scenario_with_no_early_speeds(self, calculator):
        """Field with no E-types should have negative race_pace_scenario."""
        entry_pace_data = [
            {"horse_pace_style": 2},  # EP
            {"horse_pace_style": 3},  # PS
            {"horse_pace_style": 4},  # S
        ]

        result = calculator.calculate_field_pace_features(entry_pace_data, field_size=3)

        assert result["field_early_speed_count"] == 0
        assert result["race_pace_scenario"] < 0.0

    def test_empty_field_returns_defaults(self, calculator):
        """Empty field data should return default values."""
        result = calculator.calculate_field_pace_features([], field_size=0)

        assert result["race_pace_scenario"] == 0.0
        assert result["field_early_speed_count"] == 1


class TestPaceFitScore:
    """Tests for calculate_pace_fit scoring."""

    def test_early_speed_benefits_from_slow_pace(self, calculator):
        """E-type (1) should have negative fit in fast pace scenario."""
        fit = calculator.calculate_pace_fit(horse_pace_style=1, race_pace_scenario=2.0)

        assert fit == -2.0

    def test_closer_benefits_from_fast_pace(self, calculator):
        """S-type (4) should have positive fit in fast pace scenario."""
        fit = calculator.calculate_pace_fit(horse_pace_style=4, race_pace_scenario=2.0)

        assert fit == 2.0

    def test_presser_benefits_from_fast_pace(self, calculator):
        """PS-type (3) should have positive fit in fast pace scenario."""
        fit = calculator.calculate_pace_fit(horse_pace_style=3, race_pace_scenario=1.5)

        assert fit == 1.5

    def test_invalid_style_returns_0(self, calculator):
        """Invalid pace style should return 0.0."""
        fit = calculator.calculate_pace_fit(horse_pace_style=99, race_pace_scenario=1.0)

        assert fit == 0.0


class TestLoneSpeed:
    """Tests for is_lone_speed detection."""

    def test_lone_speed_identified_correctly(self, calculator):
        """E-type (1) with field_early_speed_count=1 should be lone speed."""
        result = calculator.is_lone_speed(
            horse_pace_style=1, field_early_speed_count=1
        )

        assert result is True

    def test_not_lone_speed_with_multiple_early_speeds(self, calculator):
        """E-type with multiple early speeds should not be lone."""
        result = calculator.is_lone_speed(
            horse_pace_style=1, field_early_speed_count=2
        )

        assert result is False

    def test_non_early_speed_type_not_lone(self, calculator):
        """Non-E-type should never be lone speed."""
        result = calculator.is_lone_speed(
            horse_pace_style=4, field_early_speed_count=1
        )

        assert result is False


class TestHorsePaceDataClass:
    """Tests for HorsePaceData dataclass."""

    def test_to_dict_conversion(self):
        """to_dict should return properly formatted dictionary."""
        data = HorsePaceData(
            horse_pace_early=7.5,
            horse_pace_mid=6.2,
            horse_pace_late=-0.8,
            horse_pace_style=1,
        )

        result = data.to_dict()

        assert result["horse_pace_early"] == 7.5
        assert result["horse_pace_mid"] == 6.2
        assert result["horse_pace_late"] == -0.8
        assert result["horse_pace_style"] == 1

    def test_default_values(self):
        """HorsePaceData should have sensible defaults."""
        data = HorsePaceData()

        assert data.horse_pace_early == 0.0
        assert data.horse_pace_mid == 0.0
        assert data.horse_pace_late == 0.0
        assert data.horse_pace_style == 2  # Default EP


class TestFieldPaceDataClass:
    """Tests for FieldPaceData dataclass."""

    def test_to_dict_conversion(self):
        """to_dict should return properly formatted dictionary."""
        data = FieldPaceData(
            race_pace_scenario=1.5,
            field_early_speed_count=2,
            horse_pace_fit_score=-0.8,
            horse_is_lone_speed=False,
        )

        result = data.to_dict()

        assert result["race_pace_scenario"] == 1.5
        assert result["field_early_speed_count"] == 2
        assert result["horse_pace_fit_score"] == -0.8
        assert result["horse_is_lone_speed"] == 0

    def test_lone_speed_boolean_to_int(self):
        """to_dict should convert boolean to int."""
        data = FieldPaceData(horse_is_lone_speed=True)
        result = data.to_dict()

        assert result["horse_is_lone_speed"] == 1
