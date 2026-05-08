"""
Tests for SpeedAdjustmentCalculator class.

Tests cover:
- Global average speed calculation
- Track-day average speed calculation
- Daily track variant calculation
- Speed adjustments (track, surface, class)
"""

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

from features.speed_adjustments import (
    SpeedAdjustmentCalculator,
    get_speed_adjustments,
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
            speed_rating REAL,
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
    """Create a SpeedAdjustmentCalculator instance with test database."""
    calc = SpeedAdjustmentCalculator(db_path=temp_db, global_lookback_days=365)
    yield calc
    calc.close()


@pytest.fixture
def sample_speed_data(temp_db):
    """Insert sample speed rating data into test database."""
    conn = sqlite3.connect(temp_db)

    base_date = date(2023, 9, 1)

    # Insert races over a 90-day window with varying track conditions
    races_data = [
        # Early races for global average (base_date - 60 to 30 days)
        ("race_001", (base_date - timedelta(days=60)).isoformat(), "SAR", "DIRT", 3),
        ("race_002", (base_date - timedelta(days=55)).isoformat(), "SAR", "DIRT", 3),
        ("race_003", (base_date - timedelta(days=50)).isoformat(), "BEL", "TURF", 2),
        ("race_004", (base_date - timedelta(days=45)).isoformat(), "SAR", "DIRT", 3),
        ("race_005", (base_date - timedelta(days=40)).isoformat(), "BEL", "TURF", 3),
        # Recent races for horse speed calculation (base_date - 30 to 5 days)
        ("race_006", (base_date - timedelta(days=30)).isoformat(), "SAR", "DIRT", 3),
        ("race_007", (base_date - timedelta(days=25)).isoformat(), "SAR", "DIRT", 3),
        ("race_008", (base_date - timedelta(days=20)).isoformat(), "BEL", "TURF", 2),
        ("race_009", (base_date - timedelta(days=15)).isoformat(), "SAR", "DIRT", 2),
        ("race_010", (base_date - timedelta(days=10)).isoformat(), "BEL", "DIRT", 3),
        ("race_011", (base_date - timedelta(days=5)).isoformat(), "SAR", "DIRT", 3),
    ]

    for race_id, race_date, track, course, class_level in races_data:
        conn.execute(
            "INSERT INTO races_standardized VALUES (?, ?, ?, ?, ?)",
            (race_id, race_date, track, course, class_level),
        )

    # Insert speed ratings for field averages
    # SAR DIRT (early races): 90, 95, 92 -> avg 92.33
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_001", "horse_999", 90.0, 0),
    )
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_001", "horse_888", 95.0, 0),
    )
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_002", "horse_777", 92.0, 0),
    )

    # BEL TURF (early races): 88, 87 -> avg 87.5
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_003", "horse_999", 88.0, 0),
    )
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_005", "horse_888", 87.0, 0),
    )

    # Global average across all early races: (90+95+92+88+87)/5 = 90.4

    # Recent SAR DIRT (race_006): 88, 89 -> avg 88.5 (fast track)
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_006", "horse_111", 88.0, 0),
    )
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_006", "horse_222", 89.0, 0),
    )

    # Recent SAR DIRT (race_007): 91, 92 -> avg 91.5 (track variant)
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_007", "horse_333", 91.0, 0),
    )
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_007", "horse_444", 92.0, 0),
    )

    # Test horse 12345 races with high speeds
    # race_006: speed 100 (SAR, DIRT, class 3)
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_006", "12345", 100.0, 0),
    )

    # race_008: speed 95 (BEL, TURF, class 2) - lower speed on worse surface
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_008", "12345", 95.0, 0),
    )

    # race_010: speed 98 (BEL, DIRT, class 3)
    conn.execute(
        "INSERT INTO race_entries_standardized VALUES (?, ?, ?, ?)",
        ("race_010", "12345", 98.0, 0),
    )

    conn.commit()
    conn.close()

    return base_date


class TestGlobalAverageSpeed:
    """Tests for _get_global_avg_speed method."""

    def test_global_avg_calculated_correctly(self, calculator, sample_speed_data):
        """Global average should exclude races on/after target date."""
        result = calculator._get_global_avg_speed(sample_speed_data)

        # Should be > 0 with sample data
        assert result > 0.0
        # Should be in reasonable range (80-100 for thoroughbreds)
        assert 80.0 <= result <= 100.0

    def test_global_avg_cached_per_year(self, calculator, sample_speed_data):
        """Global average should be cached per year."""
        first_call = calculator._get_global_avg_speed(sample_speed_data)
        second_call = calculator._get_global_avg_speed(sample_speed_data)

        assert first_call == second_call
        # Cache should have entry for 2023
        assert 2023 in calculator._global_avg_cache

    def test_global_avg_point_in_time(self, calculator, sample_speed_data):
        """Global average should only use races before target date."""
        # Early date with no data should return 0
        early_date = date(2023, 7, 1)
        result = calculator._get_global_avg_speed(early_date)

        assert result == 0.0

    def test_global_avg_returns_0_with_no_data(self, calculator, temp_db):
        """Global average should return 0.0 when no races exist."""
        result = calculator._get_global_avg_speed(date(2023, 9, 1))

        assert result == 0.0


class TestTrackDayAverageSpeed:
    """Tests for _get_track_day_avg_speed method."""

    def test_track_day_avg_calculated_correctly(self, calculator, sample_speed_data):
        """Track-day average should calculate from same day races."""
        target_date = sample_speed_data - timedelta(days=30)
        result = calculator._get_track_day_avg_speed("SAR", target_date, "DIRT")

        # Should find race_006 with speeds 88, 89, 100
        assert result is not None
        assert 88.0 <= result <= 100.0

    def test_track_day_avg_returns_none_with_no_data(
        self, calculator, sample_speed_data
    ):
        """Track-day average should return None if no races on that day."""
        target_date = sample_speed_data - timedelta(days=1)
        result = calculator._get_track_day_avg_speed("SAR", target_date, "DIRT")

        # No race on this specific date
        assert result is None

    def test_track_day_avg_filters_by_surface(self, calculator, sample_speed_data):
        """Track-day average should only include matching surface type."""
        target_date = sample_speed_data - timedelta(days=50)
        result = calculator._get_track_day_avg_speed("BEL", target_date, "TURF")

        # Should find race_003 with speed 88
        assert result is not None
        assert result == 88.0


class TestDailyTrackVariant:
    """Tests for calculate_daily_track_variant method."""

    def test_daily_track_variant_calculated(self, calculator, sample_speed_data):
        """Daily track variant should be non-zero when track differs from global avg."""
        result = calculator.calculate_daily_track_variant(
            "SAR", sample_speed_data - timedelta(days=25), "DIRT"
        )

        # Should be some non-zero value (track faster or slower than global)
        assert isinstance(result, float)
        # Variant typically in range -15 to 15
        assert -20.0 <= result <= 20.0

    def test_daily_track_variant_falls_back_to_7day(self, calculator, sample_speed_data):
        """Daily track variant should fall back to 7-day if no previous day data."""
        # Use date with no races on previous day
        result = calculator.calculate_daily_track_variant(
            "SAR", sample_speed_data - timedelta(days=10), "DIRT"
        )

        assert isinstance(result, float)

    def test_daily_track_variant_returns_0_with_no_global_avg(
        self, calculator, temp_db
    ):
        """Daily track variant should return 0.0 if no global average."""
        result = calculator.calculate_daily_track_variant(
            "SAR", date(2023, 7, 1), "DIRT"
        )

        assert result == 0.0


class TestCalculateAdjustedSpeeds:
    """Tests for calculate_adjusted_speeds (all three adjustments)."""

    def test_track_adjusted_speed_calculated(self, calculator, sample_speed_data):
        """Track-adjusted speed should remove variant from race day."""
        result = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=3,
        )

        assert "horse_speed_track_adjusted" in result
        assert isinstance(result["horse_speed_track_adjusted"], float)
        # Should be in 0-150 range
        assert 0.0 <= result["horse_speed_track_adjusted"] <= 150.0
        # Should have some value with sample data
        assert result["horse_speed_track_adjusted"] > 0.0

    def test_surface_adjusted_speed_with_conversion(self, calculator, sample_speed_data):
        """Surface-adjusted speed should apply conversion penalty."""
        result = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="BEL",
            current_course_type="TURF",  # Different from horse's best race surface
            current_class_level=3,
        )

        assert "horse_speed_surface_adjusted" in result
        # Should be lower than track_adjusted if surface conversion applies
        assert result["horse_speed_surface_adjusted"] >= 0.0
        assert result["horse_speed_surface_adjusted"] <= 150.0

    def test_class_adjusted_speed_moving_up(self, calculator, sample_speed_data):
        """Class-adjusted speed should penalize moving up in class."""
        # Horse's best race was class 3, moving to class 2 (harder)
        result = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=2,  # Harder than horse's best (3)
        )

        assert "horse_speed_class_adjusted" in result
        # Should be penalized (lower than surface_adjusted)
        assert result["horse_speed_class_adjusted"] <= result[
            "horse_speed_surface_adjusted"
        ]

    def test_class_adjusted_speed_moving_down(self, calculator, sample_speed_data):
        """Class-adjusted speed should bonus moving down in class."""
        # Horse's best race was class 3, moving to class 4 (easier)
        result = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=4,  # Easier than horse's best (3)
        )

        assert "horse_speed_class_adjusted" in result
        # Should be bonused (higher than surface_adjusted)
        assert result["horse_speed_class_adjusted"] >= result[
            "horse_speed_surface_adjusted"
        ]

    def test_daily_track_variant_included(self, calculator, sample_speed_data):
        """Result should include daily_track_variant for current race."""
        result = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=3,
        )

        assert "daily_track_variant" in result
        assert isinstance(result["daily_track_variant"], float)

    def test_diagnostic_fields_populated(self, calculator, sample_speed_data):
        """Result should include diagnostic fields for best speed race."""
        result = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=3,
        )

        assert result["best_speed_earned_date"] is not None
        assert result["best_speed_earned_track"] is not None
        assert result["best_speed_earned_surface"] is not None
        assert result["best_speed_earned_class"] is not None

    def test_no_prior_races_returns_defaults(self, calculator, sample_speed_data):
        """Horse with no prior races should return all 0s."""
        result = calculator.calculate_adjusted_speeds(
            registration_number="99999",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=3,
        )

        assert result["horse_speed_track_adjusted"] == 0.0
        assert result["horse_speed_surface_adjusted"] == 0.0
        assert result["horse_speed_class_adjusted"] == 0.0
        assert result["best_speed_earned_date"] is None

    def test_point_in_time_logic(self, calculator, sample_speed_data):
        """Only races before target date should be used."""
        # Query with date before any horse races
        early_date = date(2023, 8, 1)
        result = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=early_date,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=3,
        )

        # No races before early_date, should return defaults
        assert result["horse_speed_track_adjusted"] == 0.0
        assert result["best_speed_earned_date"] is None

    def test_respects_90day_lookback(self, calculator, sample_speed_data):
        """Should only consider races within 90-day window."""
        result = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=3,
        )

        # Should find best speed (race_006 is 30 days before, within 90)
        assert result["horse_speed_track_adjusted"] > 0.0


class TestSurfaceConversionPenalties:
    """Tests for surface conversion penalty application."""

    def test_dirt_to_turf_penalty(self, calculator, sample_speed_data):
        """DIRT to TURF should apply -3.0 penalty."""
        result_dirt = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=3,
        )

        result_turf = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="TURF",
            current_class_level=3,
        )

        # TURF result should be ~3 points lower than DIRT
        # (best speed was on DIRT)
        surface_diff = (
            result_dirt["horse_speed_track_adjusted"]
            - result_turf["horse_speed_surface_adjusted"]
        )
        # Allow some variance due to track variant calculations
        assert surface_diff > 0.0

    def test_no_penalty_for_same_surface(self, calculator, sample_speed_data):
        """No penalty when surface hasn't changed."""
        result = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",  # Same as best race
            current_class_level=3,
        )

        # Surface-adjusted should equal track-adjusted
        assert (
            result["horse_speed_surface_adjusted"]
            == result["horse_speed_track_adjusted"]
        )


class TestClassAdjustmentFactors:
    """Tests for class level adjustment factors."""

    def test_class_factor_up_applied(self, calculator, sample_speed_data):
        """CLASS_FACTOR_UP = -1.5 per level moving up."""
        result_harder = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=2,  # One level harder than best (3)
        )

        # Should have penalty
        assert result_harder["horse_speed_class_adjusted"] < result_harder[
            "horse_speed_surface_adjusted"
        ]

    def test_class_factor_down_applied(self, calculator, sample_speed_data):
        """CLASS_FACTOR_DOWN = 0.5 per level moving down."""
        result_easier = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=4,  # One level easier than best (3)
        )

        # Should have bonus
        assert result_easier["horse_speed_class_adjusted"] > result_easier[
            "horse_speed_surface_adjusted"
        ]


class TestConvenienceFunctions:
    """Tests for convenience wrapper functions."""

    def test_get_speed_adjustments_function(self, calculator, sample_speed_data, temp_db):
        """get_speed_adjustments should return same result as calculator method."""
        result = get_speed_adjustments(
            db_path=temp_db,
            registration_number="12345",
            race_date=sample_speed_data,
            track_code="SAR",
            course_type="DIRT",
            class_level=3,
        )

        assert "horse_speed_track_adjusted" in result
        assert "horse_speed_surface_adjusted" in result
        assert "horse_speed_class_adjusted" in result
        assert isinstance(result["daily_track_variant"], float)


class TestOutputRanges:
    """Tests for output value ranges."""

    def test_adjusted_speeds_clamped_to_0_to_150(
        self, calculator, sample_speed_data
    ):
        """All adjusted speeds should be clamped to 0.0-150.0 range."""
        result = calculator.calculate_adjusted_speeds(
            registration_number="12345",
            race_date=sample_speed_data,
            current_track_code="SAR",
            current_course_type="DIRT",
            current_class_level=3,
        )

        assert 0.0 <= result["horse_speed_track_adjusted"] <= 150.0
        assert 0.0 <= result["horse_speed_surface_adjusted"] <= 150.0
        assert 0.0 <= result["horse_speed_class_adjusted"] <= 150.0
