"""
Feature Engine - Main Orchestration for Horse Racing Features

This module provides the main FeatureEngine class that orchestrates
all feature calculations across horse form, connections, track bias,
speed/pace, class, and equipment categories.

All features are calculated using strict point-in-time logic.

Example:
    engine = FeatureEngine(db_path='racing_data.db')
    features_df = engine.calculate_all_features(
        race_id='SAR-2023-09-01-5',
        race_date=date(2023, 9, 1)
    )

    # For a single entry
    entry_features = engine.calculate_entry_features(
        race_id='SAR-2023-09-01-5',
        entry_id='SAR-2023-09-01-5_H12345',
        race_date=date(2023, 9, 1)
    )
"""

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Any, Tuple

from .rolling_stats import RollingStatsCalculator, HorseForm
from .track_bias import TrackBiasCalculator, PostPositionBias, SpeedBias

logger = logging.getLogger(__name__)


def parse_fractional_odds(odds_str: Any) -> float:
    """
    Parse fractional odds string to decimal odds.

    Examples:
        '5/1' -> 5.0
        '9/2' -> 4.5
        '7/5' -> 1.4
        '1/2' -> 0.5
        'EVEN' or '1/1' -> 1.0
        5.0 -> 5.0 (already numeric)
        None or '' -> 0.0

    Args:
        odds_str: Fractional odds string like '5/1' or numeric value

    Returns:
        Decimal odds as float
    """
    if odds_str is None or odds_str == '':
        return 0.0

    # If already numeric, return as float
    if isinstance(odds_str, (int, float)):
        return float(odds_str)

    # Convert to string and clean
    odds_str = str(odds_str).strip().upper()

    # Handle EVEN odds
    if odds_str in ('EVEN', 'EVS', 'EV'):
        return 1.0

    # Try to parse as fractional odds (e.g., '5/1', '9/2')
    if '/' in odds_str:
        try:
            parts = odds_str.split('/')
            if len(parts) == 2:
                numerator = float(parts[0])
                denominator = float(parts[1])
                if denominator != 0:
                    return numerator / denominator
        except (ValueError, ZeroDivisionError):
            pass

    # Try to parse as simple float
    try:
        return float(odds_str)
    except ValueError:
        return 0.0


@dataclass
class RaceContext:
    """Container for race-level context."""
    race_id: str
    race_date: date
    track_code: str
    course_type: str
    distance_yards: int
    class_level: int
    purse_usd: float
    field_size: int
    track_condition: str

    distance_bucket: str = 'route'

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> 'RaceContext':
        """Create from database row."""
        distance = row['distance_yards'] or 1760

        # Determine distance bucket
        if distance < 1540:
            bucket = 'sprint'
        elif distance < 2200:
            bucket = 'route'
        else:
            bucket = 'marathon'

        return cls(
            race_id=row['race_id'],
            race_date=date.fromisoformat(row['race_date']),
            track_code=row['track_code'],
            course_type=row['course_type_code'] or 'UNKNOWN',
            distance_yards=distance,
            class_level=row['class_level'] or 3,
            purse_usd=float(row['purse_usd'] or 0),
            field_size=row['field_size'] if 'field_size' in row.keys() else 0,
            track_condition=row['track_condition'] or 'UNKNOWN',
            distance_bucket=bucket
        )


@dataclass
class EntryContext:
    """Container for entry-level context."""
    entry_id: str
    race_id: str
    registration_number: str
    trainer_id: str
    jockey_id: str
    post_position: int
    morning_line_odds: float
    age_at_race: int
    weight_lbs: int

    # Equipment flags
    has_blinkers: bool = False
    has_lasix: bool = False
    lasix_first_time: bool = False
    blinkers_first_time: bool = False
    blinkers_off: bool = False

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> 'EntryContext':
        """Create from database row."""
        return cls(
            entry_id=row['entry_id'],
            race_id=row['race_id'],
            registration_number=row['registration_number'],
            trainer_id=row['trainer_id'] or '',
            jockey_id=row['jockey_id'] or '',
            post_position=row['post_position'] or 0,
            morning_line_odds=parse_fractional_odds(row['morning_line_odds']),
            age_at_race=row['age_at_race'] or 0,
            weight_lbs=row['weight_lbs'] or 0,
            has_blinkers=bool(row['has_blinkers']),
            has_lasix=bool(row['has_lasix']),
            lasix_first_time=bool(row['lasix_first_time']),
            blinkers_first_time=bool(row['blinkers_first_time']),
            blinkers_off=bool(row['blinkers_off']),
        )


class FeatureEngine:
    """
    Main orchestrator for feature calculation.

    Combines rolling stats, track bias, and other features into
    a complete feature set for each race entry.

    Attributes:
        db_path: Path to SQLite database
        rolling_windows: List of rolling window sizes in days
        sample_thresholds: Minimum sample sizes by entity type
    """

    DEFAULT_ROLLING_WINDOWS = [14, 30, 60]
    DEFAULT_SAMPLE_THRESHOLDS = {
        'trainer': 20,
        'jockey': 20,
        'combo': 5,
        'horse': 3,
        'track_bias': 50,
    }

    def __init__(
        self,
        db_path: str = 'racing_data.db',
        rolling_windows: Optional[List[int]] = None,
        sample_thresholds: Optional[Dict[str, int]] = None
    ):
        """
        Initialize the feature engine.

        Args:
            db_path: Path to SQLite database
            rolling_windows: List of rolling window sizes
            sample_thresholds: Minimum sample sizes by entity
        """
        self.db_path = db_path
        self.rolling_windows = rolling_windows or self.DEFAULT_ROLLING_WINDOWS
        self.sample_thresholds = sample_thresholds or self.DEFAULT_SAMPLE_THRESHOLDS

        self._conn: Optional[sqlite3.Connection] = None

        # Initialize sub-calculators
        self.rolling_stats = RollingStatsCalculator(
            db_path=db_path,
            windows=self.rolling_windows,
            sample_thresholds=self.sample_thresholds
        )
        self.track_bias = TrackBiasCalculator(
            db_path=db_path,
            min_sample_size=self.sample_thresholds['track_bias']
        )

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        """Close all database connections."""
        if self._conn:
            self._conn.close()
            self._conn = None
        self.rolling_stats.close()
        self.track_bias.close()

    def get_race_context(self, race_id: str) -> Optional[RaceContext]:
        """
        Get race context information.

        Args:
            race_id: Race identifier

        Returns:
            RaceContext or None if not found
        """
        conn = self._get_connection()

        cursor = conn.execute("""
            SELECT
                race_id, race_date, track_code, course_type_code,
                distance_yards, class_level, purse_usd, track_condition,
                (SELECT COUNT(*) FROM race_entries_standardized
                 WHERE race_id = rs.race_id AND scratched = 0) as field_size
            FROM races_standardized rs
            WHERE race_id = ?
        """, (race_id,))

        row = cursor.fetchone()
        return RaceContext.from_row(row) if row else None

    def get_entry_context(self, entry_id: str) -> Optional[EntryContext]:
        """
        Get entry context information.

        Args:
            entry_id: Entry identifier

        Returns:
            EntryContext or None if not found
        """
        conn = self._get_connection()

        cursor = conn.execute("""
            SELECT *
            FROM race_entries_standardized
            WHERE entry_id = ?
        """, (entry_id,))

        row = cursor.fetchone()
        return EntryContext.from_row(row) if row else None

    def get_race_entries(self, race_id: str) -> List[EntryContext]:
        """
        Get all non-scratched entries for a race.

        Args:
            race_id: Race identifier

        Returns:
            List of EntryContext objects
        """
        conn = self._get_connection()

        cursor = conn.execute("""
            SELECT *
            FROM race_entries_standardized
            WHERE race_id = ?
                AND scratched = 0
            ORDER BY post_position
        """, (race_id,))

        return [EntryContext.from_row(row) for row in cursor.fetchall()]

    def calculate_horse_features(
        self,
        registration_number: str,
        race_date: date,
        race_context: RaceContext
    ) -> Dict[str, Any]:
        """
        Calculate horse form features.

        POINT-IN-TIME: Only uses data from before race_date.

        Args:
            registration_number: Horse registration number
            race_date: Target race date
            race_context: Race context for class comparisons

        Returns:
            Dict of horse form features
        """
        form = self.rolling_stats.calculate_horse_form(registration_number, race_date)

        # Surface preference
        surface_pref = self.rolling_stats.calculate_surface_preference(
            registration_number, race_date, race_context.course_type
        )

        # Distance preference
        distance_pref = self.rolling_stats.calculate_distance_preference(
            registration_number, race_date, race_context.distance_yards
        )

        features = {
            # Recent form
            'days_since_last': form.days_since_last_race,
            'last_3_finishes': form.last_3_finishes,
            'layoff_indicator': form.days_since_last_race is not None and form.days_since_last_race > 60,
            'first_time_starter': form.days_since_last_race is None,

            # Career stats
            'total_starts': form.dirt_starts + form.turf_starts,
            'total_wins': form.dirt_wins + form.turf_wins,
            'career_win_rate': (form.dirt_wins + form.turf_wins) / max(1, form.dirt_starts + form.turf_starts),

            # Surface performance
            'surface_starts': form.dirt_starts if race_context.course_type == 'DIRT' else form.turf_starts,
            'surface_wins': form.dirt_wins if race_context.course_type == 'DIRT' else form.turf_wins,
            'surface_win_rate': 0.0,  # Calculated below
            'surface_preference': surface_pref,

            # Distance performance
            'distance_starts': form.sprint_starts if race_context.distance_bucket == 'sprint' else form.route_starts,
            'distance_wins': form.sprint_wins if race_context.distance_bucket == 'sprint' else form.route_wins,
            'distance_preference': distance_pref,

            # Speed figures
            'best_speed_90_days': form.best_speed_90_days,
            'avg_speed_90_days': form.avg_speed_90_days,
            'speed_trend': form.speed_trend,

            # Class metrics
            'last_class_level': form.last_class_level,
            'avg_class_level': form.avg_class_level,
            'class_change': (race_context.class_level - form.last_class_level) if form.last_class_level else 0,
        }

        # Calculate surface win rate
        surface_starts = features['surface_starts']
        if surface_starts > 0:
            features['surface_win_rate'] = features['surface_wins'] / surface_starts

        # Calculate distance win rate
        distance_starts = features['distance_starts']
        if distance_starts > 0:
            features['distance_win_rate'] = features['distance_wins'] / distance_starts

        return features

    def calculate_connection_features(
        self,
        trainer_id: str,
        jockey_id: str,
        race_date: date,
        race_context: RaceContext
    ) -> Dict[str, Any]:
        """
        Calculate trainer, jockey, and combo features.

        POINT-IN-TIME: Only uses data from before race_date.

        Args:
            trainer_id: Trainer external party ID
            jockey_id: Jockey external party ID
            race_date: Target race date
            race_context: Race context

        Returns:
            Dict of connection features
        """
        features = {}

        # Trainer stats
        trainer_stats = self.rolling_stats.calculate_trainer_stats(trainer_id, race_date)
        for window in self.rolling_windows:
            stats = trainer_stats.get(window)
            if stats:
                features[f'trainer_win_rate_{window}d'] = stats.win_rate
                features[f'trainer_roi_{window}d'] = stats.roi
                if window == 30:
                    features['trainer_starts'] = stats.starts
                    features['trainer_sample_flag'] = stats.sufficient_sample

        # Trainer hot streak (3+ wins in 14 days)
        features['trainer_hot_streak'] = self.rolling_stats.get_hot_streak_indicator(
            'trainer', trainer_id, race_date
        )

        # Jockey stats
        jockey_stats = self.rolling_stats.calculate_jockey_stats(jockey_id, race_date)
        for window in self.rolling_windows:
            stats = jockey_stats.get(window)
            if stats:
                features[f'jockey_win_rate_{window}d'] = stats.win_rate
                features[f'jockey_roi_{window}d'] = stats.roi
                if window == 30:
                    features['jockey_starts'] = stats.starts
                    features['jockey_sample_flag'] = stats.sufficient_sample

        # Jockey hot streak
        features['jockey_hot_streak'] = self.rolling_stats.get_hot_streak_indicator(
            'jockey', jockey_id, race_date
        )

        # Combo stats
        combo_stats = self.rolling_stats.calculate_combo_stats(trainer_id, jockey_id, race_date)
        features['combo_win_rate'] = combo_stats.get('win_rate', 0)
        features['combo_synergy_score'] = combo_stats.get('synergy_score', 0)
        features['combo_sample_flag'] = combo_stats.get('sufficient_sample', False)

        return features

    def calculate_track_features(
        self,
        entry: EntryContext,
        race_context: RaceContext
    ) -> Dict[str, Any]:
        """
        Calculate track and post position features.

        POINT-IN-TIME: Only uses data from before race_date.

        Args:
            entry: Entry context
            race_context: Race context

        Returns:
            Dict of track/position features
        """
        features = {
            'post_position': entry.post_position,
            'field_size': race_context.field_size,
        }

        # Post position bias
        post_bias = self.track_bias.calculate_post_position_bias(
            race_context.track_code,
            race_context.course_type,
            race_context.distance_bucket,
            race_context.race_date
        )

        features['post_position_win_rate'] = post_bias.get_post_win_rate(entry.post_position)
        features['inside_bias_score'] = post_bias.inside_bias_score
        features['track_bias_sample_flag'] = post_bias.sufficient_sample

        # Rail adjustment
        features['rail_bias_adjustment'] = self.track_bias.calculate_rail_adjustment(
            race_context.track_code,
            race_context.course_type,
            entry.post_position,
            race_context.distance_yards,
            race_context.race_date
        )

        # Speed bias
        speed_bias = self.track_bias.calculate_speed_bias(
            race_context.track_code,
            race_context.course_type,
            race_context.race_date
        )
        features['speed_bias_score'] = speed_bias.speed_bias_score

        return features

    def calculate_equipment_features(self, entry: EntryContext) -> Dict[str, Any]:
        """
        Calculate equipment-related features.

        Args:
            entry: Entry context with equipment flags

        Returns:
            Dict of equipment features
        """
        return {
            'blinkers_on': entry.has_blinkers,
            'blinkers_first_time': entry.blinkers_first_time,
            'blinkers_off': entry.blinkers_off,
            'lasix_on': entry.has_lasix,
            'lasix_first_time': entry.lasix_first_time,
            'equipment_change': entry.blinkers_first_time or entry.blinkers_off or entry.lasix_first_time,
        }

    def calculate_class_features(
        self,
        race_context: RaceContext,
        horse_features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate class-related features.

        Args:
            race_context: Race context
            horse_features: Already calculated horse features

        Returns:
            Dict of class features
        """
        class_change = horse_features.get('class_change', 0)

        return {
            'class_level': race_context.class_level,
            'class_change': class_change,
            'class_drop_indicator': class_change < 0,
            'class_rise_indicator': class_change > 0,
            'purse_usd': race_context.purse_usd,
        }

    def calculate_entry_features(
        self,
        race_id: str,
        entry_id: str,
        race_date: date
    ) -> Dict[str, Any]:
        """
        Calculate all features for a single race entry.

        This is the main method for generating features for inference.

        Args:
            race_id: Race identifier
            entry_id: Entry identifier
            race_date: Race date

        Returns:
            Dict with all features for the entry
        """
        # Get contexts
        race_context = self.get_race_context(race_id)
        if not race_context:
            logger.warning(f"Race not found: {race_id}")
            return {}

        entry = self.get_entry_context(entry_id)
        if not entry:
            logger.warning(f"Entry not found: {entry_id}")
            return {}

        # Calculate all feature categories
        features = {
            'race_id': race_id,
            'entry_id': entry_id,
            'registration_number': entry.registration_number,
            'trainer_id': entry.trainer_id,
            'jockey_id': entry.jockey_id,
            'morning_line_odds': entry.morning_line_odds,
            'age_at_race': entry.age_at_race,
            'weight_carried': entry.weight_lbs,
        }

        # Horse form features
        horse_features = self.calculate_horse_features(
            entry.registration_number, race_date, race_context
        )
        features.update(horse_features)

        # Connection features
        connection_features = self.calculate_connection_features(
            entry.trainer_id, entry.jockey_id, race_date, race_context
        )
        features.update(connection_features)

        # Track features
        track_features = self.calculate_track_features(entry, race_context)
        features.update(track_features)

        # Equipment features
        equipment_features = self.calculate_equipment_features(entry)
        features.update(equipment_features)

        # Class features
        class_features = self.calculate_class_features(race_context, horse_features)
        features.update(class_features)

        return features

    def calculate_all_features(
        self,
        race_id: str,
        race_date: date
    ) -> List[Dict[str, Any]]:
        """
        Calculate features for all entries in a race.

        Args:
            race_id: Race identifier
            race_date: Race date

        Returns:
            List of feature dicts, one per entry
        """
        entries = self.get_race_entries(race_id)

        if not entries:
            logger.warning(f"No entries found for race: {race_id}")
            return []

        features_list = []

        for entry in entries:
            features = self.calculate_entry_features(race_id, entry.entry_id, race_date)
            if features:
                features_list.append(features)

        # Add field-relative features
        features_list = self.add_field_relative_features(features_list)

        return features_list

    def add_field_relative_features(
        self,
        features_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Add field-relative features (rankings within the field).

        Args:
            features_list: List of feature dicts for race entries

        Returns:
            Updated list with field-relative features added
        """
        if not features_list:
            return features_list

        field_size = len(features_list)

        # Features to rank
        rank_features = [
            ('best_speed_90_days', 'speed_rank_in_field', True),  # Higher is better
            ('avg_class_level', 'class_rank_in_field', True),     # Higher is better
            ('morning_line_odds', 'ml_rank_in_field', False),     # Lower is better (favorite)
        ]

        for feature_name, rank_name, higher_is_better in rank_features:
            # Get values with indices
            values = []
            for i, f in enumerate(features_list):
                val = f.get(feature_name)
                if val is not None:
                    values.append((i, val))

            # Sort and assign ranks
            values.sort(key=lambda x: x[1], reverse=higher_is_better)

            for rank, (idx, _) in enumerate(values, 1):
                features_list[idx][rank_name] = rank

            # Fill None for missing values
            for f in features_list:
                if rank_name not in f:
                    f[rank_name] = field_size  # Last

        # Add field quality score (average of top 3 speed figures)
        speeds = [f.get('best_speed_90_days', 0) for f in features_list if f.get('best_speed_90_days')]
        speeds.sort(reverse=True)
        field_quality = sum(speeds[:3]) / max(len(speeds[:3]), 1) if speeds else 0

        for f in features_list:
            f['field_quality_score'] = field_quality
            # Compare individual speed to field average
            individual_speed = f.get('avg_speed_90_days', 0)
            f['speed_vs_field_avg'] = individual_speed - field_quality if individual_speed else 0

        return features_list

    def calculate_features_for_date_range(
        self,
        start_date: date,
        end_date: date,
        progress_callback: Optional[callable] = None
    ) -> List[Dict[str, Any]]:
        """
        Calculate features for all races in a date range.

        Useful for building training datasets.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            progress_callback: Optional callback(race_id, current, total)

        Returns:
            List of all feature dicts
        """
        conn = self._get_connection()

        cursor = conn.execute("""
            SELECT race_id, race_date
            FROM races_standardized
            WHERE race_date >= ? AND race_date <= ?
            ORDER BY race_date
        """, (start_date.isoformat(), end_date.isoformat()))

        races = cursor.fetchall()
        total_races = len(races)

        logger.info(f"Calculating features for {total_races} races")

        all_features = []

        for i, row in enumerate(races):
            race_id = row['race_id']
            race_date = date.fromisoformat(row['race_date'])

            try:
                features = self.calculate_all_features(race_id, race_date)
                all_features.extend(features)
            except Exception as e:
                logger.warning(f"Error calculating features for {race_id}: {e}")

            if progress_callback:
                progress_callback(race_id, i + 1, total_races)

        logger.info(f"Generated {len(all_features)} feature rows")

        return all_features


# Convenience function
def generate_features(
    db_path: str,
    race_id: str,
    race_date: date
) -> List[Dict[str, Any]]:
    """
    Convenience function to generate features for a race.

    Args:
        db_path: Path to SQLite database
        race_id: Race identifier
        race_date: Race date

    Returns:
        List of feature dicts
    """
    engine = FeatureEngine(db_path=db_path)
    try:
        return engine.calculate_all_features(race_id, race_date)
    finally:
        engine.close()
