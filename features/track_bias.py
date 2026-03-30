"""
Track Bias Calculator for Horse Racing Features

Calculates track-specific biases including post position advantages,
speed/pace bias, and rail bias across different surfaces and distances.

All calculations use STRICT point-in-time logic: only data from races that
occurred BEFORE the target date is used.

Example:
    calculator = TrackBiasCalculator(db_session)
    bias = calculator.calculate_post_position_bias(
        track_code='SAR',
        surface='DIRT',
        distance_bucket='sprint',
        as_of_date=date(2023, 9, 1)
    )
"""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Any, Tuple

import sqlite3

logger = logging.getLogger(__name__)


@dataclass
class PostPositionBias:
    """Container for post position bias statistics."""
    track_code: str = ''
    surface: str = ''
    distance_bucket: str = ''

    # Win rates by post position (1-indexed)
    post_win_rates: Dict[int, float] = field(default_factory=dict)

    # Sample sizes by post
    post_starts: Dict[int, int] = field(default_factory=dict)

    # Overall bias metrics
    inside_bias_score: float = 0.0  # Positive = inside advantage
    outside_bias_score: float = 0.0  # Positive = outside advantage

    # Sample size
    total_races: int = 0
    sufficient_sample: bool = False

    def get_post_win_rate(self, post: int) -> float:
        """Get win rate for a specific post position."""
        if post in self.post_win_rates:
            return self.post_win_rates[post]
        # For posts beyond our data, use the "outside" aggregate
        if post >= 9:
            return self.post_win_rates.get(9, 0.0)
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            'track_code': self.track_code,
            'surface': self.surface,
            'distance_bucket': self.distance_bucket,
            'post_1_win_rate': self.post_win_rates.get(1, 0.0),
            'post_2_win_rate': self.post_win_rates.get(2, 0.0),
            'post_3_win_rate': self.post_win_rates.get(3, 0.0),
            'post_4_win_rate': self.post_win_rates.get(4, 0.0),
            'post_5_win_rate': self.post_win_rates.get(5, 0.0),
            'post_6_win_rate': self.post_win_rates.get(6, 0.0),
            'post_7_win_rate': self.post_win_rates.get(7, 0.0),
            'post_8_win_rate': self.post_win_rates.get(8, 0.0),
            'post_outside_win_rate': self.post_win_rates.get(9, 0.0),
            'inside_bias_score': round(self.inside_bias_score, 4),
            'total_races': self.total_races,
            'sufficient_sample': self.sufficient_sample,
        }


@dataclass
class SpeedBias:
    """Container for pace/speed bias statistics."""
    track_code: str = ''
    surface: str = ''

    # Early speed advantage metrics
    front_runner_win_rate: float = 0.0  # Win rate of horses leading at first call
    stalker_win_rate: float = 0.0       # 2nd-4th at first call
    closer_win_rate: float = 0.0        # 5th or worse at first call

    # Speed holding rate (% of front runners that win)
    speed_holding_rate: float = 0.0

    # Overall bias
    speed_bias_score: float = 0.0  # Positive = speed advantage

    total_races: int = 0
    sufficient_sample: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            'track_code': self.track_code,
            'surface': self.surface,
            'front_runner_win_rate': round(self.front_runner_win_rate, 4),
            'stalker_win_rate': round(self.stalker_win_rate, 4),
            'closer_win_rate': round(self.closer_win_rate, 4),
            'speed_holding_rate': round(self.speed_holding_rate, 4),
            'speed_bias_score': round(self.speed_bias_score, 4),
            'total_races': self.total_races,
            'sufficient_sample': self.sufficient_sample,
        }


class TrackBiasCalculator:
    """
    Calculates track-specific biases for post positions and pace.

    All methods use strict point-in-time logic: only data from races that
    occurred BEFORE the target date is used.

    Attributes:
        db_path: Path to SQLite database
        min_sample_size: Minimum races for reliable bias calculation
        window_days: Rolling window size in days
    """

    # Distance bucket boundaries (yards)
    DISTANCE_BUCKETS = {
        'sprint': (0, 1540),        # < 7 furlongs
        'route': (1540, 2200),      # 7f to < 1.25 miles
        'marathon': (2200, 99999),  # >= 1.25 miles
    }

    DEFAULT_MIN_SAMPLE = 50
    DEFAULT_WINDOW_DAYS = 365

    def __init__(
        self,
        db_path: str = 'racing_data.db',
        min_sample_size: int = DEFAULT_MIN_SAMPLE,
        window_days: int = DEFAULT_WINDOW_DAYS
    ):
        """
        Initialize the track bias calculator.

        Args:
            db_path: Path to SQLite database
            min_sample_size: Minimum races for reliable calculation
            window_days: Rolling window size in days
        """
        self.db_path = db_path
        self.min_sample_size = min_sample_size
        self.window_days = window_days
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

    def get_distance_bucket(self, distance_yards: int) -> str:
        """
        Categorize distance into sprint/route/marathon.

        Args:
            distance_yards: Distance in yards

        Returns:
            Distance bucket name
        """
        for bucket, (min_dist, max_dist) in self.DISTANCE_BUCKETS.items():
            if min_dist <= distance_yards < max_dist:
                return bucket
        return 'route'  # Default

    def calculate_post_position_bias(
        self,
        track_code: str,
        surface: str,
        distance_bucket: str,
        as_of_date: date,
        window_days: Optional[int] = None
    ) -> PostPositionBias:
        """
        Calculate post position win rates for a track/surface/distance combo.

        POINT-IN-TIME: Only uses races BEFORE as_of_date.

        Args:
            track_code: Track code (e.g., 'SAR', 'CD')
            surface: Surface type ('DIRT', 'TURF', 'SYNTHETIC')
            distance_bucket: Distance category ('sprint', 'route', 'marathon')
            as_of_date: Target date (exclusive)
            window_days: Rolling window size (default: 365)

        Returns:
            PostPositionBias object with win rates by position
        """
        if window_days is None:
            window_days = self.window_days

        conn = self._get_connection()
        start_date = as_of_date - timedelta(days=window_days)

        # Get distance range for bucket
        dist_min, dist_max = self.DISTANCE_BUCKETS.get(distance_bucket, (0, 99999))

        query = """
            SELECT
                re.post_position,
                re.official_finish_position
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE rs.track_code = ?
                AND rs.course_type_code = ?
                AND rs.distance_yards >= ?
                AND rs.distance_yards < ?
                AND rs.race_date >= ?
                AND rs.race_date < ?
                AND re.scratched = 0
                AND re.post_position IS NOT NULL
                AND re.official_finish_position IS NOT NULL
            ORDER BY rs.race_date DESC
        """

        cursor = conn.execute(query, (
            track_code, surface, dist_min, dist_max,
            start_date.isoformat(), as_of_date.isoformat()
        ))
        rows = cursor.fetchall()

        bias = PostPositionBias(
            track_code=track_code,
            surface=surface,
            distance_bucket=distance_bucket
        )

        if not rows:
            return bias

        # Count wins and starts by post position
        post_wins: Dict[int, int] = {}
        post_starts: Dict[int, int] = {}

        for row in rows:
            post = row['post_position']
            finish = row['official_finish_position']

            # Group posts 9+ into "outside" bucket
            post_key = post if post <= 8 else 9

            post_starts[post_key] = post_starts.get(post_key, 0) + 1
            if finish == 1:
                post_wins[post_key] = post_wins.get(post_key, 0) + 1

        # Calculate win rates
        for post_key in post_starts:
            wins = post_wins.get(post_key, 0)
            starts = post_starts[post_key]
            bias.post_win_rates[post_key] = wins / starts if starts > 0 else 0.0
            bias.post_starts[post_key] = starts

        # Calculate inside bias score
        # Compare inside posts (1-3) to outside posts (6-9)
        inside_wins = sum(post_wins.get(p, 0) for p in [1, 2, 3])
        inside_starts = sum(post_starts.get(p, 0) for p in [1, 2, 3])
        outside_wins = sum(post_wins.get(p, 0) for p in [6, 7, 8, 9])
        outside_starts = sum(post_starts.get(p, 0) for p in [6, 7, 8, 9])

        inside_rate = inside_wins / inside_starts if inside_starts > 0 else 0
        outside_rate = outside_wins / outside_starts if outside_starts > 0 else 0

        # Bias score: positive means inside advantage
        if inside_rate + outside_rate > 0:
            bias.inside_bias_score = (inside_rate - outside_rate) / (inside_rate + outside_rate)

        # Count unique races
        race_ids = set()
        cursor = conn.execute("""
            SELECT DISTINCT rs.race_id
            FROM races_standardized rs
            WHERE rs.track_code = ?
                AND rs.course_type_code = ?
                AND rs.distance_yards >= ?
                AND rs.distance_yards < ?
                AND rs.race_date >= ?
                AND rs.race_date < ?
        """, (track_code, surface, dist_min, dist_max,
              start_date.isoformat(), as_of_date.isoformat()))

        bias.total_races = len(cursor.fetchall())
        bias.sufficient_sample = bias.total_races >= self.min_sample_size

        return bias

    def calculate_speed_bias(
        self,
        track_code: str,
        surface: str,
        as_of_date: date,
        window_days: Optional[int] = None
    ) -> SpeedBias:
        """
        Calculate pace/speed bias for a track and surface.

        Analyzes how often early speed holds to determine if track
        favors front-runners or closers.

        POINT-IN-TIME: Only uses races BEFORE as_of_date.

        Args:
            track_code: Track code
            surface: Surface type
            as_of_date: Target date (exclusive)
            window_days: Rolling window size

        Returns:
            SpeedBias object with speed/pace metrics
        """
        if window_days is None:
            window_days = self.window_days

        conn = self._get_connection()
        start_date = as_of_date - timedelta(days=window_days)

        # Get first call position and final position
        query = """
            SELECT
                re.first_call_position,
                re.official_finish_position
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE rs.track_code = ?
                AND rs.course_type_code = ?
                AND rs.race_date >= ?
                AND rs.race_date < ?
                AND re.scratched = 0
                AND re.first_call_position IS NOT NULL
                AND re.official_finish_position IS NOT NULL
            ORDER BY rs.race_date DESC
        """

        cursor = conn.execute(query, (
            track_code, surface,
            start_date.isoformat(), as_of_date.isoformat()
        ))
        rows = cursor.fetchall()

        bias = SpeedBias(track_code=track_code, surface=surface)

        if not rows:
            return bias

        # Categorize by running position at first call
        front_runners = []  # Leading at first call
        stalkers = []       # 2nd-4th at first call
        closers = []        # 5th or worse at first call

        for row in rows:
            first_call = row['first_call_position']
            finish = row['official_finish_position']

            if first_call == 1:
                front_runners.append(finish)
            elif first_call <= 4:
                stalkers.append(finish)
            else:
                closers.append(finish)

        # Calculate win rates
        if front_runners:
            wins = sum(1 for f in front_runners if f == 1)
            bias.front_runner_win_rate = wins / len(front_runners)
            bias.speed_holding_rate = bias.front_runner_win_rate

        if stalkers:
            wins = sum(1 for f in stalkers if f == 1)
            bias.stalker_win_rate = wins / len(stalkers)

        if closers:
            wins = sum(1 for f in closers if f == 1)
            bias.closer_win_rate = wins / len(closers)

        # Speed bias score: compare front runner rate to closer rate
        if bias.front_runner_win_rate + bias.closer_win_rate > 0:
            bias.speed_bias_score = (
                (bias.front_runner_win_rate - bias.closer_win_rate) /
                (bias.front_runner_win_rate + bias.closer_win_rate)
            )

        # Count races
        cursor = conn.execute("""
            SELECT COUNT(DISTINCT rs.race_id)
            FROM races_standardized rs
            WHERE rs.track_code = ?
                AND rs.course_type_code = ?
                AND rs.race_date >= ?
                AND rs.race_date < ?
        """, (track_code, surface, start_date.isoformat(), as_of_date.isoformat()))

        bias.total_races = cursor.fetchone()[0]
        bias.sufficient_sample = bias.total_races >= self.min_sample_size

        return bias

    def calculate_rail_adjustment(
        self,
        track_code: str,
        surface: str,
        post_position: int,
        distance_yards: int,
        as_of_date: date
    ) -> float:
        """
        Calculate an adjustment factor for a specific post position.

        Returns a multiplier (1.0 = neutral, >1 = advantage, <1 = disadvantage).

        Args:
            track_code: Track code
            surface: Surface type
            post_position: Horse's post position
            distance_yards: Race distance in yards
            as_of_date: Target date

        Returns:
            Adjustment factor (0.8 - 1.2 typical range)
        """
        distance_bucket = self.get_distance_bucket(distance_yards)

        bias = self.calculate_post_position_bias(
            track_code, surface, distance_bucket, as_of_date
        )

        if not bias.sufficient_sample:
            return 1.0  # No adjustment if insufficient data

        # Get expected win rate (average across all posts)
        total_wins = sum(
            bias.post_win_rates.get(p, 0) * bias.post_starts.get(p, 0)
            for p in bias.post_starts
        )
        total_starts = sum(bias.post_starts.values())

        if total_starts == 0:
            return 1.0

        expected_rate = total_wins / total_starts
        actual_rate = bias.get_post_win_rate(post_position)

        if expected_rate == 0:
            return 1.0

        # Adjustment is ratio of actual to expected, clamped to reasonable range
        adjustment = actual_rate / expected_rate
        return max(0.7, min(1.5, adjustment))

    def get_pace_style(
        self,
        first_call_positions: List[int],
        field_sizes: List[int]
    ) -> str:
        """
        Determine horse's typical pace style from historical races.

        Args:
            first_call_positions: List of first call positions (1-indexed)
            field_sizes: Corresponding field sizes

        Returns:
            Pace style: 'E' (early), 'EP' (early presser), 'P' (presser),
                       'S' (stalker), 'C' (closer)
        """
        if not first_call_positions or not field_sizes:
            return 'P'  # Default to presser

        # Calculate average relative position (0-1 scale)
        relative_positions = []
        for pos, field in zip(first_call_positions, field_sizes):
            if field > 1:
                relative = (pos - 1) / (field - 1)
                relative_positions.append(relative)

        if not relative_positions:
            return 'P'

        avg_relative = sum(relative_positions) / len(relative_positions)

        if avg_relative < 0.15:
            return 'E'   # Early (front runner)
        elif avg_relative < 0.30:
            return 'EP'  # Early presser
        elif avg_relative < 0.50:
            return 'P'   # Presser
        elif avg_relative < 0.70:
            return 'S'   # Stalker
        else:
            return 'C'   # Closer

    def calculate_pace_scenario(
        self,
        pace_styles: List[str]
    ) -> str:
        """
        Determine likely pace scenario for a race based on entries.

        Args:
            pace_styles: List of pace styles for each entry ('E', 'EP', etc.)

        Returns:
            Pace scenario: 'FAST', 'CONTESTED', 'AVERAGE', 'SLOW', 'LONE_SPEED'
        """
        if not pace_styles:
            return 'AVERAGE'

        early_count = sum(1 for s in pace_styles if s in ('E', 'EP'))
        presser_count = sum(1 for s in pace_styles if s == 'P')
        closer_count = sum(1 for s in pace_styles if s in ('S', 'C'))

        if early_count == 1 and closer_count >= 3:
            return 'LONE_SPEED'  # Single early speed, advantage
        elif early_count >= 3:
            return 'CONTESTED'   # Multiple speed horses, fast pace
        elif early_count >= 2:
            return 'FAST'        # Likely hot early pace
        elif closer_count >= len(pace_styles) * 0.6:
            return 'SLOW'        # Mostly closers, slow pace
        else:
            return 'AVERAGE'

    def get_all_track_biases(
        self,
        as_of_date: date,
        surfaces: List[str] = ['DIRT', 'TURF'],
        distance_buckets: List[str] = ['sprint', 'route']
    ) -> Dict[str, Dict[str, PostPositionBias]]:
        """
        Calculate biases for all tracks in the database.

        Args:
            as_of_date: Target date
            surfaces: List of surfaces to analyze
            distance_buckets: List of distance buckets to analyze

        Returns:
            Nested dict: {track_code: {surface_distance: PostPositionBias}}
        """
        conn = self._get_connection()

        # Get list of tracks
        cursor = conn.execute("""
            SELECT DISTINCT track_code FROM races_standardized
        """)
        tracks = [row[0] for row in cursor.fetchall()]

        results = {}

        for track in tracks:
            results[track] = {}
            for surface in surfaces:
                for bucket in distance_buckets:
                    key = f"{surface}_{bucket}"
                    results[track][key] = self.calculate_post_position_bias(
                        track, surface, bucket, as_of_date
                    )

        return results


# Convenience function
def get_track_bias(
    db_path: str,
    track_code: str,
    surface: str,
    distance_yards: int,
    race_date: date
) -> Dict[str, Any]:
    """
    Convenience function to get all track bias info for a race.

    Args:
        db_path: Path to SQLite database
        track_code: Track code
        surface: Surface type
        distance_yards: Distance in yards
        race_date: Target race date

    Returns:
        Dict with post position and speed bias information
    """
    calc = TrackBiasCalculator(db_path=db_path)

    try:
        distance_bucket = calc.get_distance_bucket(distance_yards)

        post_bias = calc.calculate_post_position_bias(
            track_code, surface, distance_bucket, race_date
        )
        speed_bias = calc.calculate_speed_bias(track_code, surface, race_date)

        return {
            'post_position': post_bias.to_dict(),
            'speed': speed_bias.to_dict(),
            'distance_bucket': distance_bucket,
        }
    finally:
        calc.close()
