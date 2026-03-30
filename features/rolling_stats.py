"""
Rolling Statistics Calculator for Horse Racing Features

Calculates point-in-time rolling statistics for trainers, jockeys, and their
combinations across multiple time windows (14, 30, 60 days).

All calculations use STRICT point-in-time logic: only data from races that
occurred BEFORE the target date is used. This prevents data leakage.

Example:
    calculator = RollingStatsCalculator(db_session)
    trainer_stats = calculator.calculate_trainer_stats(
        trainer_id='T12345',
        as_of_date=date(2023, 9, 1),
        windows=[14, 30, 60]
    )
"""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple

import sqlite3

logger = logging.getLogger(__name__)


def parse_fractional_odds(odds_str) -> float:
    """Parse fractional odds string to decimal odds."""
    if odds_str is None or odds_str == '':
        return 0.0
    if isinstance(odds_str, (int, float)):
        return float(odds_str)
    odds_str = str(odds_str).strip().upper()
    if odds_str in ('EVEN', 'EVS', 'EV'):
        return 1.0
    if '/' in odds_str:
        try:
            parts = odds_str.split('/')
            if len(parts) == 2:
                num, denom = float(parts[0]), float(parts[1])
                if denom != 0:
                    return num / denom
        except (ValueError, ZeroDivisionError):
            pass
    try:
        return float(odds_str)
    except ValueError:
        return 0.0


@dataclass
class RollingStats:
    """Container for rolling statistics."""
    starts: int = 0
    wins: int = 0
    places: int = 0  # Top 2
    shows: int = 0   # Top 3

    # Computed rates
    win_rate: float = 0.0
    place_rate: float = 0.0
    show_rate: float = 0.0
    roi: float = 0.0

    # Advanced metrics
    avg_finish_position: float = 0.0
    avg_odds: float = 0.0
    avg_morning_line: float = 0.0

    # Surface splits
    dirt_starts: int = 0
    dirt_wins: int = 0
    turf_starts: int = 0
    turf_wins: int = 0

    # Sample size flag
    sufficient_sample: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            'starts': self.starts,
            'wins': self.wins,
            'places': self.places,
            'shows': self.shows,
            'win_rate': round(self.win_rate, 4),
            'place_rate': round(self.place_rate, 4),
            'show_rate': round(self.show_rate, 4),
            'roi': round(self.roi, 4),
            'avg_finish_position': round(self.avg_finish_position, 2),
            'avg_odds': round(self.avg_odds, 2),
            'avg_morning_line': round(self.avg_morning_line, 2),
            'dirt_starts': self.dirt_starts,
            'dirt_wins': self.dirt_wins,
            'turf_starts': self.turf_starts,
            'turf_wins': self.turf_wins,
            'sufficient_sample': self.sufficient_sample,
        }


@dataclass
class HorseForm:
    """Container for horse rolling form statistics."""
    days_since_last_race: Optional[int] = None
    last_3_finishes: str = ''
    last_3_speed_avg: float = 0.0

    # Career stats by surface
    dirt_starts: int = 0
    dirt_wins: int = 0
    dirt_avg_finish: float = 0.0
    turf_starts: int = 0
    turf_wins: int = 0
    turf_avg_finish: float = 0.0

    # Distance preferences
    sprint_starts: int = 0
    sprint_wins: int = 0
    route_starts: int = 0
    route_wins: int = 0

    # Speed figures
    best_speed_90_days: Optional[int] = None
    avg_speed_90_days: float = 0.0
    speed_trend: float = 0.0  # Positive = improving

    # Class metrics
    last_class_level: Optional[int] = None
    avg_class_level: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            'days_since_last_race': self.days_since_last_race,
            'last_3_finishes': self.last_3_finishes,
            'last_3_speed_avg': round(self.last_3_speed_avg, 2),
            'dirt_starts': self.dirt_starts,
            'dirt_wins': self.dirt_wins,
            'dirt_avg_finish': round(self.dirt_avg_finish, 2),
            'turf_starts': self.turf_starts,
            'turf_wins': self.turf_wins,
            'turf_avg_finish': round(self.turf_avg_finish, 2),
            'sprint_starts': self.sprint_starts,
            'sprint_wins': self.sprint_wins,
            'route_starts': self.route_starts,
            'route_wins': self.route_wins,
            'best_speed_90_days': self.best_speed_90_days,
            'avg_speed_90_days': round(self.avg_speed_90_days, 2),
            'speed_trend': round(self.speed_trend, 2),
            'last_class_level': self.last_class_level,
            'avg_class_level': round(self.avg_class_level, 2),
        }


class RollingStatsCalculator:
    """
    Calculates point-in-time rolling statistics for connections and horses.

    All methods use strict point-in-time logic: only data from races that
    occurred BEFORE the target date is used.

    Attributes:
        db_path: Path to SQLite database
        windows: List of rolling window sizes in days
        sample_thresholds: Dict of minimum sample sizes by entity type
    """

    # Default configuration
    DEFAULT_WINDOWS = [14, 30, 60]
    DEFAULT_SAMPLE_THRESHOLDS = {
        'trainer': 20,
        'jockey': 20,
        'combo': 5,
        'horse': 3,
    }

    # Distance bucket boundaries (yards)
    SPRINT_MAX = 1540    # < 7 furlongs
    ROUTE_MAX = 2200     # 7f to < 1.25 miles

    def __init__(
        self,
        db_path: str = 'racing_data.db',
        windows: Optional[List[int]] = None,
        sample_thresholds: Optional[Dict[str, int]] = None
    ):
        """
        Initialize the rolling stats calculator.

        Args:
            db_path: Path to SQLite database
            windows: List of rolling window sizes in days
            sample_thresholds: Minimum sample sizes by entity type
        """
        self.db_path = db_path
        self.windows = windows or self.DEFAULT_WINDOWS
        self.sample_thresholds = sample_thresholds or self.DEFAULT_SAMPLE_THRESHOLDS
        self._conn: Optional[sqlite3.Connection] = None

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection with performance optimizations."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA mmap_size = 268435456")
            self._conn.execute("PRAGMA cache_size = -64000")
            self._conn.execute("PRAGMA journal_mode = WAL")
        return self._conn

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def _calculate_stats_from_rows(
        self,
        rows: List[sqlite3.Row],
        min_sample: int
    ) -> RollingStats:
        """
        Calculate statistics from a list of race result rows.

        Args:
            rows: List of race entry rows
            min_sample: Minimum sample size for sufficient_sample flag

        Returns:
            RollingStats object with calculated metrics
        """
        stats = RollingStats()

        if not rows:
            return stats

        stats.starts = len(rows)

        finish_positions = []
        odds_list = []
        ml_list = []
        total_payout = 0.0
        total_bet = 0.0

        for row in rows:
            finish = row['official_finish_position']
            if finish is not None:
                finish_positions.append(finish)
                if finish == 1:
                    stats.wins += 1
                if finish <= 2:
                    stats.places += 1
                if finish <= 3:
                    stats.shows += 1

            # Odds
            odds = row['actual_odds']
            if odds is not None and odds > 0:
                odds_list.append(odds)
                total_bet += 2.0  # Assume $2 bet
                if finish == 1:
                    total_payout += 2.0 * (odds + 1)  # Decimal odds payout

            ml = parse_fractional_odds(row['morning_line_odds'])
            if ml > 0:
                ml_list.append(ml)

            # Surface splits
            try:
                surface = row['course_type_code'] or 'UNKNOWN'
            except (KeyError, IndexError):
                surface = 'UNKNOWN'
            if surface == 'DIRT':
                stats.dirt_starts += 1
                if finish == 1:
                    stats.dirt_wins += 1
            elif surface == 'TURF':
                stats.turf_starts += 1
                if finish == 1:
                    stats.turf_wins += 1

        # Calculate rates
        if stats.starts > 0:
            stats.win_rate = stats.wins / stats.starts
            stats.place_rate = stats.places / stats.starts
            stats.show_rate = stats.shows / stats.starts

        if total_bet > 0:
            stats.roi = (total_payout - total_bet) / total_bet

        if finish_positions:
            stats.avg_finish_position = sum(finish_positions) / len(finish_positions)

        if odds_list:
            stats.avg_odds = sum(odds_list) / len(odds_list)

        if ml_list:
            stats.avg_morning_line = sum(ml_list) / len(ml_list)

        stats.sufficient_sample = stats.starts >= min_sample

        return stats

    def calculate_trainer_stats(
        self,
        trainer_id: str,
        as_of_date: date,
        windows: Optional[List[int]] = None
    ) -> Dict[int, RollingStats]:
        """
        Calculate trainer rolling statistics for multiple windows.

        POINT-IN-TIME: Only uses races BEFORE as_of_date.

        Args:
            trainer_id: Trainer external party ID
            as_of_date: Target date (exclusive - data up to but not including)
            windows: List of window sizes in days (default: [14, 30, 60])

        Returns:
            Dict mapping window days to RollingStats
        """
        if windows is None:
            windows = self.windows

        conn = self._get_connection()
        results = {}

        for window in windows:
            start_date = as_of_date - timedelta(days=window)

            query = """
                SELECT
                    re.official_finish_position,
                    re.actual_odds,
                    re.morning_line_odds,
                    rs.course_type_code
                FROM race_entries_standardized re
                JOIN races_standardized rs ON re.race_id = rs.race_id
                WHERE re.trainer_id = ?
                    AND rs.race_date >= ?
                    AND rs.race_date < ?
                    AND re.scratched = 0
                ORDER BY rs.race_date DESC
            """

            cursor = conn.execute(query, (trainer_id, start_date.isoformat(), as_of_date.isoformat()))
            rows = cursor.fetchall()

            results[window] = self._calculate_stats_from_rows(
                rows,
                self.sample_thresholds['trainer']
            )

        return results

    def calculate_jockey_stats(
        self,
        jockey_id: str,
        as_of_date: date,
        windows: Optional[List[int]] = None
    ) -> Dict[int, RollingStats]:
        """
        Calculate jockey rolling statistics for multiple windows.

        POINT-IN-TIME: Only uses races BEFORE as_of_date.

        Args:
            jockey_id: Jockey external party ID
            as_of_date: Target date (exclusive)
            windows: List of window sizes in days

        Returns:
            Dict mapping window days to RollingStats
        """
        if windows is None:
            windows = self.windows

        conn = self._get_connection()
        results = {}

        for window in windows:
            start_date = as_of_date - timedelta(days=window)

            query = """
                SELECT
                    re.official_finish_position,
                    re.actual_odds,
                    re.morning_line_odds,
                    rs.course_type_code
                FROM race_entries_standardized re
                JOIN races_standardized rs ON re.race_id = rs.race_id
                WHERE re.jockey_id = ?
                    AND rs.race_date >= ?
                    AND rs.race_date < ?
                    AND re.scratched = 0
                ORDER BY rs.race_date DESC
            """

            cursor = conn.execute(query, (jockey_id, start_date.isoformat(), as_of_date.isoformat()))
            rows = cursor.fetchall()

            results[window] = self._calculate_stats_from_rows(
                rows,
                self.sample_thresholds['jockey']
            )

        return results

    def calculate_combo_stats(
        self,
        trainer_id: str,
        jockey_id: str,
        as_of_date: date,
        window: int = 365
    ) -> Dict[str, Any]:
        """
        Calculate trainer-jockey combination statistics.

        POINT-IN-TIME: Only uses races BEFORE as_of_date.

        Args:
            trainer_id: Trainer external party ID
            jockey_id: Jockey external party ID
            as_of_date: Target date (exclusive)
            window: Window size in days (default: 365 for combo)

        Returns:
            Dict with combo statistics and synergy score
        """
        conn = self._get_connection()
        start_date = as_of_date - timedelta(days=window)

        query = """
            SELECT
                re.official_finish_position,
                re.actual_odds,
                rs.course_type_code
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE re.trainer_id = ?
                AND re.jockey_id = ?
                AND rs.race_date >= ?
                AND rs.race_date < ?
                AND re.scratched = 0
            ORDER BY rs.race_date DESC
        """

        cursor = conn.execute(query, (trainer_id, jockey_id, start_date.isoformat(), as_of_date.isoformat()))
        rows = cursor.fetchall()

        stats = self._calculate_stats_from_rows(rows, self.sample_thresholds['combo'])

        # Calculate synergy score by comparing combo rate to individual rates
        trainer_stats = self.calculate_trainer_stats(trainer_id, as_of_date, windows=[window])
        jockey_stats = self.calculate_jockey_stats(jockey_id, as_of_date, windows=[window])

        expected_rate = (
            trainer_stats.get(window, RollingStats()).win_rate +
            jockey_stats.get(window, RollingStats()).win_rate
        ) / 2

        synergy_score = 0.0
        if expected_rate > 0 and stats.starts >= self.sample_thresholds['combo']:
            synergy_score = (stats.win_rate - expected_rate) / expected_rate

        return {
            'starts': stats.starts,
            'wins': stats.wins,
            'win_rate': round(stats.win_rate, 4),
            'roi': round(stats.roi, 4),
            'synergy_score': round(synergy_score, 4),
            'sufficient_sample': stats.sufficient_sample,
        }

    def calculate_horse_form(
        self,
        registration_number: str,
        as_of_date: date
    ) -> HorseForm:
        """
        Calculate horse rolling form statistics.

        POINT-IN-TIME: Only uses races BEFORE as_of_date.

        Args:
            registration_number: Horse registration number
            as_of_date: Target date (exclusive)

        Returns:
            HorseForm object with form statistics
        """
        conn = self._get_connection()
        form = HorseForm()

        # Get all prior races for this horse
        query = """
            SELECT
                rs.race_date,
                rs.course_type_code,
                rs.distance_yards,
                rs.class_level,
                re.official_finish_position,
                re.speed_rating
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE re.registration_number = ?
                AND rs.race_date < ?
                AND re.scratched = 0
            ORDER BY rs.race_date DESC
        """

        cursor = conn.execute(query, (registration_number, as_of_date.isoformat()))
        rows = cursor.fetchall()

        if not rows:
            return form

        # Days since last race
        last_race_date = date.fromisoformat(rows[0]['race_date'])
        form.days_since_last_race = (as_of_date - last_race_date).days

        # Last 3 finishes
        last_3 = []
        for i, row in enumerate(rows[:3]):
            if row['official_finish_position']:
                last_3.append(str(row['official_finish_position']))
        form.last_3_finishes = '-'.join(last_3)

        # Speed figures for last 90 days
        speed_90_days = []
        cutoff_90 = as_of_date - timedelta(days=90)

        dirt_finishes = []
        turf_finishes = []
        sprint_count = 0
        route_count = 0
        sprint_wins = 0
        route_wins = 0
        class_levels = []

        for row in rows:
            race_date = date.fromisoformat(row['race_date'])
            finish = row['official_finish_position']
            speed = row['speed_rating']
            surface = row['course_type_code']
            distance = row['distance_yards'] or 0
            class_level = row['class_level']

            # Speed figures within 90 days
            if race_date >= cutoff_90 and speed is not None:
                speed_90_days.append(speed)

            # Surface stats
            if surface == 'DIRT':
                form.dirt_starts += 1
                if finish == 1:
                    form.dirt_wins += 1
                if finish:
                    dirt_finishes.append(finish)
            elif surface == 'TURF':
                form.turf_starts += 1
                if finish == 1:
                    form.turf_wins += 1
                if finish:
                    turf_finishes.append(finish)

            # Distance stats
            if distance < self.SPRINT_MAX:
                sprint_count += 1
                if finish == 1:
                    sprint_wins += 1
            else:
                route_count += 1
                if finish == 1:
                    route_wins += 1

            # Class levels
            if class_level is not None:
                class_levels.append(class_level)

        form.sprint_starts = sprint_count
        form.sprint_wins = sprint_wins
        form.route_starts = route_count
        form.route_wins = route_wins

        # Calculate averages
        if dirt_finishes:
            form.dirt_avg_finish = sum(dirt_finishes) / len(dirt_finishes)
        if turf_finishes:
            form.turf_avg_finish = sum(turf_finishes) / len(turf_finishes)

        # Speed figures
        if speed_90_days:
            form.best_speed_90_days = max(speed_90_days)
            form.avg_speed_90_days = sum(speed_90_days) / len(speed_90_days)

            # Speed trend (compare first half to second half)
            if len(speed_90_days) >= 4:
                mid = len(speed_90_days) // 2
                recent = sum(speed_90_days[:mid]) / mid
                older = sum(speed_90_days[mid:]) / (len(speed_90_days) - mid)
                form.speed_trend = recent - older

        # Class metrics
        if class_levels:
            form.last_class_level = class_levels[0]
            form.avg_class_level = sum(class_levels) / len(class_levels)

        return form

    def calculate_surface_preference(
        self,
        registration_number: str,
        as_of_date: date,
        target_surface: str
    ) -> float:
        """
        Calculate horse's preference/performance on a specific surface.

        Returns a score from 0 to 1 indicating relative performance
        on the target surface compared to other surfaces.

        Args:
            registration_number: Horse registration number
            as_of_date: Target date (exclusive)
            target_surface: 'DIRT', 'TURF', or 'SYNTHETIC'

        Returns:
            Surface preference score (higher = better on this surface)
        """
        form = self.calculate_horse_form(registration_number, as_of_date)

        if target_surface == 'DIRT':
            starts = form.dirt_starts
            avg_finish = form.dirt_avg_finish
            other_finishes = []
            if form.turf_starts > 0:
                other_finishes.append(form.turf_avg_finish)
        elif target_surface == 'TURF':
            starts = form.turf_starts
            avg_finish = form.turf_avg_finish
            other_finishes = []
            if form.dirt_starts > 0:
                other_finishes.append(form.dirt_avg_finish)
        else:
            return 0.5  # Neutral for synthetic (limited data)

        if starts == 0:
            return 0.5  # Neutral if no experience

        if not other_finishes:
            # Only raced on target surface
            # Use finish position as proxy (lower is better)
            if avg_finish <= 2:
                return 0.8
            elif avg_finish <= 4:
                return 0.6
            else:
                return 0.4

        # Compare to other surface performance
        other_avg = sum(other_finishes) / len(other_finishes)

        # If better on target surface (lower avg finish), score > 0.5
        if other_avg > 0:
            relative = (other_avg - avg_finish) / other_avg
            # Clamp to 0-1 range
            return max(0, min(1, 0.5 + relative * 0.5))

        return 0.5

    def calculate_distance_preference(
        self,
        registration_number: str,
        as_of_date: date,
        target_distance: int
    ) -> float:
        """
        Calculate horse's preference for a specific distance.

        Args:
            registration_number: Horse registration number
            as_of_date: Target date (exclusive)
            target_distance: Target distance in yards

        Returns:
            Distance preference score (higher = better at this distance)
        """
        form = self.calculate_horse_form(registration_number, as_of_date)

        is_sprint = target_distance < self.SPRINT_MAX

        if is_sprint:
            starts = form.sprint_starts
            wins = form.sprint_wins
            other_starts = form.route_starts
            other_wins = form.route_wins
        else:
            starts = form.route_starts
            wins = form.route_wins
            other_starts = form.sprint_starts
            other_wins = form.sprint_wins

        if starts == 0:
            return 0.5  # Neutral if no experience

        target_win_rate = wins / starts if starts > 0 else 0
        other_win_rate = other_wins / other_starts if other_starts > 0 else 0

        if other_starts == 0:
            # Only raced at target distance
            if target_win_rate >= 0.25:
                return 0.8
            elif target_win_rate >= 0.15:
                return 0.6
            else:
                return 0.4

        # Compare win rates
        if target_win_rate + other_win_rate > 0:
            relative = target_win_rate / (target_win_rate + other_win_rate)
            return relative

        return 0.5

    def get_hot_streak_indicator(
        self,
        entity_type: str,
        entity_id: str,
        as_of_date: date,
        min_wins: int = 3,
        window: int = 14
    ) -> bool:
        """
        Determine if trainer/jockey is on a hot streak.

        A hot streak is defined as min_wins or more wins in the window period.

        Args:
            entity_type: 'trainer' or 'jockey'
            entity_id: Entity external party ID
            as_of_date: Target date (exclusive)
            min_wins: Minimum wins to qualify as hot streak
            window: Window size in days

        Returns:
            True if on hot streak
        """
        if entity_type == 'trainer':
            stats = self.calculate_trainer_stats(entity_id, as_of_date, windows=[window])
        elif entity_type == 'jockey':
            stats = self.calculate_jockey_stats(entity_id, as_of_date, windows=[window])
        else:
            return False

        return stats.get(window, RollingStats()).wins >= min_wins


# Convenience function for quick stats lookup
def get_connection_stats(
    db_path: str,
    trainer_id: str,
    jockey_id: str,
    race_date: date,
    windows: List[int] = [14, 30, 60]
) -> Dict[str, Any]:
    """
    Convenience function to get all connection stats for a race entry.

    Args:
        db_path: Path to SQLite database
        trainer_id: Trainer external party ID
        jockey_id: Jockey external party ID
        race_date: Target race date
        windows: List of window sizes

    Returns:
        Dict with trainer, jockey, and combo stats
    """
    calc = RollingStatsCalculator(db_path=db_path, windows=windows)

    try:
        trainer_stats = calc.calculate_trainer_stats(trainer_id, race_date)
        jockey_stats = calc.calculate_jockey_stats(jockey_id, race_date)
        combo_stats = calc.calculate_combo_stats(trainer_id, jockey_id, race_date)

        return {
            'trainer': {w: s.to_dict() for w, s in trainer_stats.items()},
            'jockey': {w: s.to_dict() for w, s in jockey_stats.items()},
            'combo': combo_stats,
        }
    finally:
        calc.close()
