"""
Speed Adjustment Calculator for Horse Racing Features

Calculates speed-based adjustments for horses, including track variants,
surface conversions, and class-level adjustments. All adjustments use
speed ratings to produce comparable features across different conditions.

All calculations use STRICT point-in-time logic: only data from races that
occurred BEFORE the target date is used.

Example:
    calculator = SpeedAdjustmentCalculator(db_path='racing_data.db')
    adjustments = calculator.calculate_adjusted_speeds(
        registration_number='12345A',
        race_date=date(2023, 9, 1),
        current_track_code='SAR',
        current_course_type='DIRT',
        current_class_level=3
    )
    calculator.close()
"""

import logging
from datetime import date, timedelta
from typing import Dict, Optional, Any

import sqlite3

logger = logging.getLogger(__name__)


class SpeedAdjustmentCalculator:
    """
    Calculates speed adjustments for horses across multiple conditions.

    Features computed:
    - horse_speed_track_adjusted: Best speed adjusted for daily track variant
    - horse_speed_surface_adjusted: Speed adjusted for surface change penalty
    - horse_speed_class_adjusted: Speed adjusted for class level differential
    - daily_track_variant: Track variant for current race date (race-level feature)

    All methods use strict point-in-time logic: only data from races that
    occurred BEFORE the target date is used.

    Attributes:
        db_path: Path to SQLite database
        global_lookback_days: Number of days to look back for global speed average
    """

    DEFAULT_GLOBAL_LOOKBACK = 365

    # Surface type conversions
    SURFACE_CONVERSION = {
        ('DIRT', 'TURF'): -3.0,      # Dirt to turf penalty
        ('TURF', 'DIRT'): -2.0,      # Turf to dirt penalty
    }

    # Class level adjustment factors
    CLASS_FACTOR_UP = -1.5          # Penalty per class level moving up (to harder)
    CLASS_FACTOR_DOWN = 0.5         # Bonus per class level moving down (to easier)

    def __init__(self, db_path: str = 'racing_data.db', global_lookback_days: int = DEFAULT_GLOBAL_LOOKBACK):
        """
        Initialize the speed adjustment calculator.

        Args:
            db_path: Path to SQLite database
            global_lookback_days: Days to look back for global speed average (default 365)
        """
        self.db_path = db_path
        self.global_lookback_days = global_lookback_days
        self._conn: Optional[sqlite3.Connection] = None
        self._global_avg_cache: Dict[int, float] = {}

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection with performance optimizations."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, timeout=30)
            self._conn.row_factory = sqlite3.Row
            try:
                self._conn.execute("PRAGMA mmap_size = 268435456")
                self._conn.execute("PRAGMA cache_size = -64000")
                self._conn.execute("PRAGMA journal_mode = WAL")
            except sqlite3.OperationalError:
                pass
        return self._conn

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def _get_global_avg_speed(self, race_date: date) -> float:
        """
        Get global average speed_rating over the lookback window.

        Cached per year to improve performance across multiple horses
        in the same race period.

        POINT-IN-TIME: Only uses races BEFORE race_date.

        Args:
            race_date: Target date (exclusive)

        Returns:
            Global average speed rating, or 0.0 if no data
        """
        year = race_date.year
        if year in self._global_avg_cache:
            return self._global_avg_cache[year]

        conn = self._get_connection()
        lookback_start = race_date - timedelta(days=self.global_lookback_days)

        query = """
            SELECT AVG(re.speed_rating) as avg_speed
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE rs.race_date >= ?
                AND rs.race_date < ?
                AND re.speed_rating IS NOT NULL
                AND re.scratched = 0
        """

        cursor = conn.execute(
            query,
            (lookback_start.isoformat(), race_date.isoformat())
        )
        row = cursor.fetchone()

        avg_speed = 0.0
        if row and row['avg_speed'] is not None:
            avg_speed = float(row['avg_speed'])

        self._global_avg_cache[year] = avg_speed
        logger.debug(f"Cached global avg speed for {year}: {avg_speed:.2f}")

        return avg_speed

    def _get_track_day_avg_speed(
        self,
        track_code: str,
        target_date: date,
        course_type: str
    ) -> Optional[float]:
        """
        Get average speed_rating for races at a specific track on a specific date.

        POINT-IN-TIME: Only uses races BEFORE target_date.

        Args:
            track_code: Track code (e.g., 'SAR')
            target_date: Target date (exclusive)
            course_type: Surface type ('DIRT', 'TURF', 'SYNTHETIC')

        Returns:
            Average speed rating for that track+date+surface, or None if no data
        """
        conn = self._get_connection()

        query = """
            SELECT AVG(re.speed_rating) as avg_speed
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE rs.track_code = ?
                AND rs.race_date = ?
                AND rs.course_type_code = ?
                AND re.speed_rating IS NOT NULL
                AND re.scratched = 0
        """

        cursor = conn.execute(
            query,
            (track_code, target_date.isoformat(), course_type)
        )
        row = cursor.fetchone()

        if row and row['avg_speed'] is not None:
            return float(row['avg_speed'])

        return None

    def calculate_daily_track_variant(
        self,
        track_code: str,
        race_date: date,
        course_type: str
    ) -> float:
        """
        Calculate track variant for the current race's track on this date.

        Uses PREVIOUS day's variant (point-in-time: can't use today's results).
        Falls back to 7-day trailing average if previous day has no data.
        Returns 0.0 if insufficient data.

        POINT-IN-TIME: Queries data strictly before race_date.

        Args:
            track_code: Track code
            race_date: Race date
            course_type: Surface type

        Returns:
            Daily track variant (-15 to 15 typical range)
        """
        conn = self._get_connection()
        global_avg = self._get_global_avg_speed(race_date)

        if global_avg == 0.0:
            logger.debug(f"No global avg for {race_date}, returning 0.0 variant")
            return 0.0

        # Try previous day
        previous_day = race_date - timedelta(days=1)
        prev_day_avg = self._get_track_day_avg_speed(track_code, race_date, course_type)

        if prev_day_avg is not None:
            variant = prev_day_avg - global_avg
            logger.debug(
                f"Track variant for {track_code} on {race_date}: "
                f"{variant:.2f} (prev_day_avg={prev_day_avg:.2f}, global={global_avg:.2f})"
            )
            return variant

        # Fall back to 7-day trailing average (not including today)
        seven_days_ago = race_date - timedelta(days=7)

        query = """
            SELECT AVG(re.speed_rating) as avg_speed
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE rs.track_code = ?
                AND rs.race_date >= ?
                AND rs.race_date < ?
                AND rs.course_type_code = ?
                AND re.speed_rating IS NOT NULL
                AND re.scratched = 0
        """

        cursor = conn.execute(
            query,
            (track_code, seven_days_ago.isoformat(), race_date.isoformat(), course_type)
        )
        row = cursor.fetchone()

        if row and row['avg_speed'] is not None:
            seven_day_avg = float(row['avg_speed'])
            variant = seven_day_avg - global_avg
            logger.debug(
                f"Track variant for {track_code} on {race_date} (7-day fallback): "
                f"{variant:.2f} (seven_day_avg={seven_day_avg:.2f}, global={global_avg:.2f})"
            )
            return variant

        logger.debug(f"No variant data for {track_code} on {race_date}, returning 0.0")
        return 0.0

    def calculate_adjusted_speeds(
        self,
        registration_number: str,
        race_date: date,
        current_track_code: str,
        current_course_type: str,
        current_class_level: int
    ) -> Dict[str, Any]:
        """
        Calculate all speed adjustments for a horse in a specific race context.

        Finds the horse's best speed_rating from the last 90 days and applies
        adjustments for:
        1. Track variant on the day it was earned
        2. Surface conversion penalty
        3. Class level differential

        POINT-IN-TIME: Only uses races BEFORE race_date.

        Args:
            registration_number: Horse registration number
            race_date: Target race date (exclusive)
            current_track_code: Current race's track code
            current_course_type: Current race's surface type
            current_class_level: Current race's class level

        Returns:
            Dict with keys:
            - horse_speed_track_adjusted (0-150 range)
            - horse_speed_surface_adjusted (0-150 range)
            - horse_speed_class_adjusted (0-150 range)
            - daily_track_variant (for current race)
            - best_speed_earned_date (for diagnostics)
            - best_speed_earned_track (for diagnostics)
            - best_speed_earned_surface (for diagnostics)
            - best_speed_earned_class (for diagnostics)
        """
        conn = self._get_connection()

        # Initialize all features to defaults
        result = {
            'horse_speed_track_adjusted': 0.0,
            'horse_speed_surface_adjusted': 0.0,
            'horse_speed_class_adjusted': 0.0,
            'daily_track_variant': 0.0,
            'best_speed_earned_date': None,
            'best_speed_earned_track': None,
            'best_speed_earned_surface': None,
            'best_speed_earned_class': None,
        }

        # Get current race's daily track variant
        result['daily_track_variant'] = self.calculate_daily_track_variant(
            current_track_code, race_date, current_course_type
        )

        # Find horse's best speed in last 90 days
        cutoff_date = race_date - timedelta(days=90)

        query = """
            SELECT
                re.speed_rating,
                rs.race_date,
                rs.track_code,
                rs.course_type_code,
                rs.class_level
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE re.registration_number = ?
                AND rs.race_date >= ?
                AND rs.race_date < ?
                AND re.speed_rating IS NOT NULL
                AND re.scratched = 0
            ORDER BY re.speed_rating DESC
            LIMIT 1
        """

        cursor = conn.execute(
            query,
            (registration_number, cutoff_date.isoformat(), race_date.isoformat())
        )
        best_race = cursor.fetchone()

        if not best_race:
            logger.debug(f"No speed rating found for {registration_number} in last 90 days")
            return result

        best_speed = float(best_race['speed_rating'])
        earned_date = date.fromisoformat(best_race['race_date'])
        earned_track = best_race['track_code']
        earned_surface = best_race['course_type_code']
        earned_class = best_race['class_level']

        # Store diagnostic info
        result['best_speed_earned_date'] = earned_date
        result['best_speed_earned_track'] = earned_track
        result['best_speed_earned_surface'] = earned_surface
        result['best_speed_earned_class'] = earned_class

        # 1. TRACK-ADJUSTED SPEED
        # Adjustment = raw_speed - track_variant_of_day_it_was_earned
        track_variant_earned_day = self.calculate_daily_track_variant(
            earned_track, earned_date, earned_surface
        )
        track_adjusted = best_speed - track_variant_earned_day
        result['horse_speed_track_adjusted'] = max(0.0, min(150.0, track_adjusted))

        logger.debug(
            f"Horse {registration_number}: best_speed={best_speed:.1f}, "
            f"track_variant_earned={track_variant_earned_day:.2f}, "
            f"track_adjusted={track_adjusted:.2f}"
        )

        # 2. SURFACE-ADJUSTED SPEED
        surface_adjusted = result['horse_speed_track_adjusted']
        if earned_surface != current_course_type:
            penalty = self.SURFACE_CONVERSION.get(
                (earned_surface, current_course_type),
                0.0
            )
            surface_adjusted = result['horse_speed_track_adjusted'] + penalty
            logger.debug(
                f"Surface conversion {earned_surface} -> {current_course_type}: "
                f"penalty={penalty:.1f}, adjusted={surface_adjusted:.2f}"
            )

        result['horse_speed_surface_adjusted'] = max(0.0, min(150.0, surface_adjusted))

        # 3. CLASS-ADJUSTED SPEED
        class_adjusted = result['horse_speed_surface_adjusted']
        if earned_class is not None and current_class_level is not None:
            class_diff = earned_class - current_class_level

            if class_diff > 0:
                # Moving up in class (harder): apply negative factor
                adjustment = class_diff * self.CLASS_FACTOR_UP
            else:
                # Moving down in class (easier): apply positive factor
                adjustment = abs(class_diff) * self.CLASS_FACTOR_DOWN

            class_adjusted = result['horse_speed_surface_adjusted'] + adjustment
            logger.debug(
                f"Class adjustment: diff={class_diff}, factor={'UP' if class_diff > 0 else 'DOWN'}, "
                f"adjustment={adjustment:.2f}, adjusted={class_adjusted:.2f}"
            )

        result['horse_speed_class_adjusted'] = max(0.0, min(150.0, class_adjusted))

        return result


# Convenience function for quick lookup
def get_speed_adjustments(
    db_path: str,
    registration_number: str,
    race_date: date,
    track_code: str,
    course_type: str,
    class_level: int
) -> Dict[str, Any]:
    """
    Convenience function to get all speed adjustments for a horse.

    Args:
        db_path: Path to SQLite database
        registration_number: Horse registration number
        race_date: Target race date
        track_code: Race track code
        course_type: Race surface type
        class_level: Race class level

    Returns:
        Dict with all speed adjustment features
    """
    calc = SpeedAdjustmentCalculator(db_path=db_path)

    try:
        return calc.calculate_adjusted_speeds(
            registration_number, race_date, track_code, course_type, class_level
        )
    finally:
        calc.close()
