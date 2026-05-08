"""
Pace Calculator for Horse Racing Features

Calculates pace-related features for horses and race fields, including:
- Early pace figures (position at first call)
- Mid pace figures (position at second call)
- Late pace dynamics (ground gained/lost)
- Pace style classification (E, EP, PS, S)
- Field-level pace scenarios
- Pace fit scores

All calculations use STRICT point-in-time logic: only data from races that
occurred BEFORE the target date is used. This prevents data leakage.

Example:
    calculator = PaceCalculator('racing_data.db')
    horse_pace = calculator.calculate_horse_pace(
        registration_number='12345',
        race_date=date(2023, 9, 1),
        num_races=5
    )
    field_features = calculator.calculate_field_pace_features(
        entry_pace_data=[...],
        field_size=10
    )
"""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Any

import sqlite3

logger = logging.getLogger(__name__)


@dataclass
class HorsePaceData:
    """Container for horse-level pace features."""
    horse_pace_early: float = 0.0           # 0-10 scale, default 0.0
    horse_pace_mid: float = 0.0             # 0-10 scale, default 0.0
    horse_pace_late: float = 0.0            # -5 to 5 scale, default 0.0
    horse_pace_style: int = 2               # 1=E, 2=EP, 3=PS, 4=S, default 2

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            'horse_pace_early': round(self.horse_pace_early, 2),
            'horse_pace_mid': round(self.horse_pace_mid, 2),
            'horse_pace_late': round(self.horse_pace_late, 2),
            'horse_pace_style': self.horse_pace_style,
        }


@dataclass
class FieldPaceData:
    """Container for field-level pace features."""
    race_pace_scenario: float = 0.0         # -3 to 3 scale, default 0.0
    field_early_speed_count: int = 1        # 0-14, default 1
    horse_pace_fit_score: float = 0.0       # -3 to 3 scale, default 0.0
    horse_is_lone_speed: bool = False       # Binary, default False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            'race_pace_scenario': round(self.race_pace_scenario, 2),
            'field_early_speed_count': self.field_early_speed_count,
            'horse_pace_fit_score': round(self.horse_pace_fit_score, 2),
            'horse_is_lone_speed': 1 if self.horse_is_lone_speed else 0,
        }


class PaceCalculator:
    """
    Calculates pace-related features for horse racing predictions.

    All methods use strict point-in-time logic: only data from races that
    occurred BEFORE the target date is used.

    Attributes:
        db_path: Path to SQLite database
    """

    def __init__(self, db_path: str = 'racing_data.db'):
        """
        Initialize the pace calculator.

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

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

    def _position_to_pace_figure(self, position: Optional[int]) -> float:
        """
        Convert running position to pace figure on 0-10 scale.

        Position 1 = 10, position 2 = 9, ..., position 10 = 0.
        Positions > 10 are capped at 0.

        Args:
            position: Running position (1-indexed), or None

        Returns:
            Pace figure on 0-10 scale
        """
        if position is None or position < 1:
            return 0.0
        if position > 10:
            return 0.0
        return max(0.0, 10.0 - (position - 1))

    def calculate_horse_pace(
        self,
        registration_number: str,
        race_date: date,
        num_races: int = 5
    ) -> HorsePaceData:
        """
        Calculate horse-level pace features from historical data.

        POINT-IN-TIME: Only uses races WHERE race_date < target_date.

        Calculates:
        - horse_pace_early: Average early pace figure (first call position)
        - horse_pace_mid: Average mid pace figure (second call position)
        - horse_pace_late: Average late pace gain/loss (2nd call - finish position)
        - horse_pace_style: Classification (1=E, 2=EP, 3=PS, 4=S)

        Args:
            registration_number: Horse registration number
            race_date: Target race date (exclusive upper bound)
            num_races: Number of prior races to include

        Returns:
            HorsePaceData with calculated features
        """
        conn = self._get_connection()
        data = HorsePaceData()

        # Query: Get last num_races from horse before race_date
        query = """
            SELECT
                re.race_id,
                re.first_call_position,
                re.second_call_position,
                re.finish_position
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE re.registration_number = ?
                AND rs.race_date < ?
                AND re.scratched = 0
            ORDER BY rs.race_date DESC
            LIMIT ?
        """

        cursor = conn.execute(query, (registration_number, race_date.isoformat(), num_races))
        rows = cursor.fetchall()

        if not rows:
            logger.debug(
                f"No prior races found for horse {registration_number} before {race_date}"
            )
            return data

        # Calculate early pace figures
        early_figures = []
        mid_figures = []
        late_gains = []

        for row in rows:
            # Early pace from first call position
            first_call = row['first_call_position']
            if first_call is not None:
                early_fig = self._position_to_pace_figure(first_call)
                early_figures.append(early_fig)

            # Mid pace from second call position
            second_call = row['second_call_position']
            if second_call is not None:
                mid_fig = self._position_to_pace_figure(second_call)
                mid_figures.append(mid_fig)

            # Late pace: gain/loss = second_call_position - finish_position
            # Positive = closed ground (better), negative = faded
            if second_call is not None and row['finish_position'] is not None:
                gain_loss = second_call - row['finish_position']
                late_gains.append(gain_loss)

        # Calculate averages
        if early_figures:
            data.horse_pace_early = sum(early_figures) / len(early_figures)

        if mid_figures:
            data.horse_pace_mid = sum(mid_figures) / len(mid_figures)

        if late_gains:
            data.horse_pace_late = sum(late_gains) / len(late_gains)

        # Classify pace style
        data.horse_pace_style = self._classify_pace_style(
            data.horse_pace_early,
            data.horse_pace_late
        )

        return data

    def _classify_pace_style(self, early_pace: float, late_pace: float) -> int:
        """
        Classify pace style based on early and late pace averages.

        Style codes:
        - 1 = E (Early speed): early >= 7 AND late <= 0
        - 2 = EP (Early/Presser): early >= 5 AND late > 0
        - 3 = PS (Presser/Stalker): 3 <= early < 5 OR other
        - 4 = S (Closer/Sustained): early < 3 AND late >= 2
        - 2 = EP (default for ambiguous cases)

        Args:
            early_pace: Average early pace figure
            late_pace: Average late pace gain/loss

        Returns:
            Style code (1-4), default 2
        """
        # E (Early speed)
        if early_pace >= 7 and late_pace <= 0:
            return 1

        # EP (Early/Presser)
        if early_pace >= 5 and late_pace > 0:
            return 2

        # S (Closer/Sustained)
        if early_pace < 3 and late_pace >= 2:
            return 4

        # PS (Presser/Stalker) - catch-all middle ground
        if 3 <= early_pace < 5:
            return 3

        # Default to EP (2) for ambiguous cases
        return 2

    def calculate_field_pace_features(
        self,
        entry_pace_data: List[Dict[str, Any]],
        field_size: int
    ) -> Dict[str, Any]:
        """
        Calculate field-level pace features from all entries' horse-level pace data.

        Computes:
        - race_pace_scenario: Normalized count of E-types in field
        - field_early_speed_count: Count of E-types

        Args:
            entry_pace_data: List of horse pace data dicts with 'horse_pace_style' key
            field_size: Total number of entries in field

        Returns:
            Dict with race_pace_scenario and field_early_speed_count
        """
        if not entry_pace_data or field_size == 0:
            return {
                'race_pace_scenario': 0.0,
                'field_early_speed_count': 1,
            }

        # Count E-types (style == 1)
        e_count = sum(1 for entry in entry_pace_data if entry.get('horse_pace_style') == 1)

        # race_pace_scenario: (count_E - 1.5) normalized
        # Rough scaling: if field_size=10, divide by ~4 to get range of roughly -3 to 3
        normalization_factor = max(1, field_size / 3.0)
        race_pace_scenario = (e_count - 1.5) / normalization_factor

        return {
            'race_pace_scenario': race_pace_scenario,
            'field_early_speed_count': e_count,
        }

    def calculate_pace_fit(
        self,
        horse_pace_style: int,
        race_pace_scenario: float
    ) -> float:
        """
        Calculate horse's pace fit score relative to the race scenario.

        Logic:
        - If style is E(1) or EP(2): fit = -race_pace_scenario
          (E-types prefer slow pace, suffer in fast pace)
        - If style is PS(3) or S(4): fit = race_pace_scenario
          (Closers prefer fast pace)

        Args:
            horse_pace_style: Pace style code (1-4)
            race_pace_scenario: Field-level pace scenario score

        Returns:
            Pace fit score (-3 to 3), default 0.0
        """
        if horse_pace_style in (1, 2):  # E or EP
            return -race_pace_scenario
        elif horse_pace_style in (3, 4):  # PS or S
            return race_pace_scenario
        else:
            return 0.0

    def is_lone_speed(
        self,
        horse_pace_style: int,
        field_early_speed_count: int
    ) -> bool:
        """
        Determine if horse is the only early speed in the field.

        Returns True if:
        - This horse IS an E-type (style == 1)
        - AND there is exactly 1 E-type in the field

        Args:
            horse_pace_style: This horse's pace style code
            field_early_speed_count: Count of E-types in field

        Returns:
            True if horse is lone speed, False otherwise
        """
        return horse_pace_style == 1 and field_early_speed_count == 1

    def calculate_all_pace_features(
        self,
        registration_number: str,
        race_date: date,
        entry_pace_data: Optional[List[Dict[str, Any]]] = None,
        field_size: int = 0,
        num_races: int = 5
    ) -> Dict[str, Any]:
        """
        Calculate all 8 pace features in one call.

        This is a convenience method that computes horse-level features first,
        then field-level features if field data is provided.

        POINT-IN-TIME: Only uses races before race_date.

        Args:
            registration_number: Horse registration number
            race_date: Target race date
            entry_pace_data: List of all entries' pace data for field calculations
            field_size: Total entries in the race
            num_races: Number of prior races to include

        Returns:
            Dict with all 8 features:
            - horse_pace_early, horse_pace_mid, horse_pace_late, horse_pace_style
            - race_pace_scenario, field_early_speed_count
            - horse_pace_fit_score, horse_is_lone_speed
        """
        # Calculate horse-level pace features
        horse_pace = self.calculate_horse_pace(registration_number, race_date, num_races)
        result = horse_pace.to_dict()

        # If field data provided, calculate field-level features
        if entry_pace_data is not None and field_size > 0:
            field_pace = self.calculate_field_pace_features(entry_pace_data, field_size)
            result.update(field_pace)

            # Calculate fit and lone speed using horse and field data
            fit_score = self.calculate_pace_fit(
                horse_pace.horse_pace_style,
                field_pace['race_pace_scenario']
            )
            result['horse_pace_fit_score'] = round(fit_score, 2)

            is_lone = self.is_lone_speed(
                horse_pace.horse_pace_style,
                field_pace['field_early_speed_count']
            )
            result['horse_is_lone_speed'] = 1 if is_lone else 0
        else:
            # Default values when field data not available
            result['race_pace_scenario'] = 0.0
            result['field_early_speed_count'] = 1
            result['horse_pace_fit_score'] = 0.0
            result['horse_is_lone_speed'] = 0

        return result


# Convenience function for quick pace lookup
def get_pace_features(
    db_path: str,
    registration_number: str,
    race_date: date,
    entry_pace_data: Optional[List[Dict[str, Any]]] = None,
    field_size: int = 0,
    num_races: int = 5
) -> Dict[str, Any]:
    """
    Convenience function to get all pace features for a horse in a race.

    Args:
        db_path: Path to SQLite database
        registration_number: Horse registration number
        race_date: Target race date
        entry_pace_data: List of all entries' pace data (optional)
        field_size: Total entries in the race (optional)
        num_races: Number of prior races to include

    Returns:
        Dict with all 8 pace features
    """
    calc = PaceCalculator(db_path=db_path)

    try:
        return calc.calculate_all_pace_features(
            registration_number=registration_number,
            race_date=race_date,
            entry_pace_data=entry_pace_data,
            field_size=field_size,
            num_races=num_races
        )
    finally:
        calc.close()
