"""
Feature Definitions for Horse Racing Model

This module provides the canonical list of features used in model training,
along with their types, expected ranges, and handling of missing values.
"""

from typing import List, Dict, Any
from dataclasses import dataclass
from enum import Enum


class FeatureType(Enum):
    """Enumeration of feature types for model training."""
    NUMERIC = "numeric"
    BINARY = "binary"
    CATEGORICAL = "categorical"


@dataclass
class FeatureDefinition:
    """
    Definition of a single feature including its type, defaults, and constraints.

    Attributes:
        name: Feature name matching column in DataFrame
        feature_type: Type of feature (numeric, binary, categorical)
        default_value: Value to use when feature is missing
        description: Human-readable description of the feature
        min_value: Minimum expected value (for numeric features)
        max_value: Maximum expected value (for numeric features)
    """
    name: str
    feature_type: FeatureType
    default_value: Any
    description: str
    min_value: float = None
    max_value: float = None


# Core feature columns for training (43 features total)
FEATURE_COLUMNS: List[str] = [
    # Horse form (14 features)
    'days_since_last', 'layoff_indicator', 'first_time_starter',
    'total_starts', 'total_wins', 'career_win_rate',
    'surface_win_rate', 'surface_preference', 'distance_preference',
    'best_speed_90_days', 'avg_speed_90_days', 'speed_trend',
    'last_class_level', 'class_change',

    # Connections (12 features)
    'trainer_win_rate_14d', 'trainer_win_rate_30d', 'trainer_win_rate_60d',
    'trainer_hot_streak', 'trainer_sample_flag',
    'jockey_win_rate_14d', 'jockey_win_rate_30d', 'jockey_win_rate_60d',
    'jockey_hot_streak', 'jockey_sample_flag',
    'combo_win_rate', 'combo_synergy_score',

    # Track/Position (6 features)
    'post_position', 'post_position_win_rate', 'inside_bias_score',
    'rail_bias_adjustment', 'speed_bias_score', 'field_size',

    # Equipment (4 features)
    'blinkers_on', 'blinkers_first_time', 'lasix_on', 'equipment_change',

    # Field-relative (4 features)
    'speed_rank_in_field', 'class_rank_in_field',
    'field_quality_score', 'speed_vs_field_avg',

    # Base (3 features)
    'morning_line_odds', 'age_at_race', 'class_level',

    # Pace features (8 features)
    'horse_pace_early', 'horse_pace_mid', 'horse_pace_late',
    'horse_pace_style', 'race_pace_scenario', 'horse_pace_fit_score',
    'field_early_speed_count', 'horse_is_lone_speed',

    # Speed adjustments (4 features)
    'horse_speed_track_adjusted', 'horse_speed_surface_adjusted',
    'horse_speed_class_adjusted', 'daily_track_variant',
]

# Target column for supervised learning
TARGET_COLUMN = 'is_winner'

# ID columns needed for grouping and tracking (not used in training)
ID_COLUMNS = ['race_id', 'entry_id', 'registration_number']


# Feature definitions with metadata
FEATURE_DEFINITIONS: Dict[str, FeatureDefinition] = {
    # =============================================================================
    # HORSE FORM FEATURES (14 features)
    # =============================================================================
    'days_since_last': FeatureDefinition(
        name='days_since_last',
        feature_type=FeatureType.NUMERIC,
        default_value=365,
        description='Days since last race (365 for first-time starters)',
        min_value=0,
        max_value=1000
    ),
    'layoff_indicator': FeatureDefinition(
        name='layoff_indicator',
        feature_type=FeatureType.BINARY,
        default_value=0,
        description='Binary indicator: 1 if layoff > 60 days, 0 otherwise',
        min_value=0,
        max_value=1
    ),
    'first_time_starter': FeatureDefinition(
        name='first_time_starter',
        feature_type=FeatureType.BINARY,
        default_value=0,
        description='Binary indicator: 1 if horse has never raced before',
        min_value=0,
        max_value=1
    ),
    'total_starts': FeatureDefinition(
        name='total_starts',
        feature_type=FeatureType.NUMERIC,
        default_value=0,
        description='Total number of career starts',
        min_value=0,
        max_value=500
    ),
    'total_wins': FeatureDefinition(
        name='total_wins',
        feature_type=FeatureType.NUMERIC,
        default_value=0,
        description='Total number of career wins',
        min_value=0,
        max_value=100
    ),
    'career_win_rate': FeatureDefinition(
        name='career_win_rate',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Career win percentage (total_wins / total_starts)',
        min_value=0.0,
        max_value=1.0
    ),
    'surface_win_rate': FeatureDefinition(
        name='surface_win_rate',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Win rate on current surface (dirt/turf)',
        min_value=0.0,
        max_value=1.0
    ),
    'surface_preference': FeatureDefinition(
        name='surface_preference',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Difference between surface win rate and career win rate',
        min_value=-1.0,
        max_value=1.0
    ),
    'distance_preference': FeatureDefinition(
        name='distance_preference',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Win rate at similar distances vs career average',
        min_value=-1.0,
        max_value=1.0
    ),
    'best_speed_90_days': FeatureDefinition(
        name='best_speed_90_days',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Best speed figure achieved in last 90 days',
        min_value=0.0,
        max_value=150.0
    ),
    'avg_speed_90_days': FeatureDefinition(
        name='avg_speed_90_days',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Average speed figure in last 90 days',
        min_value=0.0,
        max_value=150.0
    ),
    'speed_trend': FeatureDefinition(
        name='speed_trend',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Trend in speed figures (positive = improving form)',
        min_value=-50.0,
        max_value=50.0
    ),
    'last_class_level': FeatureDefinition(
        name='last_class_level',
        feature_type=FeatureType.NUMERIC,
        default_value=5,
        description='Class level of last race (1=highest, 10=lowest)',
        min_value=1,
        max_value=10
    ),
    'class_change': FeatureDefinition(
        name='class_change',
        feature_type=FeatureType.NUMERIC,
        default_value=0,
        description='Change in class level (negative = moving up)',
        min_value=-9,
        max_value=9
    ),

    # =============================================================================
    # CONNECTIONS FEATURES (12 features)
    # =============================================================================
    'trainer_win_rate_14d': FeatureDefinition(
        name='trainer_win_rate_14d',
        feature_type=FeatureType.NUMERIC,
        default_value=0.15,
        description='Trainer win rate in last 14 days',
        min_value=0.0,
        max_value=1.0
    ),
    'trainer_win_rate_30d': FeatureDefinition(
        name='trainer_win_rate_30d',
        feature_type=FeatureType.NUMERIC,
        default_value=0.15,
        description='Trainer win rate in last 30 days',
        min_value=0.0,
        max_value=1.0
    ),
    'trainer_win_rate_60d': FeatureDefinition(
        name='trainer_win_rate_60d',
        feature_type=FeatureType.NUMERIC,
        default_value=0.15,
        description='Trainer win rate in last 60 days',
        min_value=0.0,
        max_value=1.0
    ),
    'trainer_hot_streak': FeatureDefinition(
        name='trainer_hot_streak',
        feature_type=FeatureType.BINARY,
        default_value=0,
        description='Binary indicator: 1 if trainer is on hot streak (>25% wins recently)',
        min_value=0,
        max_value=1
    ),
    'trainer_sample_flag': FeatureDefinition(
        name='trainer_sample_flag',
        feature_type=FeatureType.BINARY,
        default_value=1,
        description='Binary indicator: 1 if trainer has small sample size (<20 starts)',
        min_value=0,
        max_value=1
    ),
    'jockey_win_rate_14d': FeatureDefinition(
        name='jockey_win_rate_14d',
        feature_type=FeatureType.NUMERIC,
        default_value=0.15,
        description='Jockey win rate in last 14 days',
        min_value=0.0,
        max_value=1.0
    ),
    'jockey_win_rate_30d': FeatureDefinition(
        name='jockey_win_rate_30d',
        feature_type=FeatureType.NUMERIC,
        default_value=0.15,
        description='Jockey win rate in last 30 days',
        min_value=0.0,
        max_value=1.0
    ),
    'jockey_win_rate_60d': FeatureDefinition(
        name='jockey_win_rate_60d',
        feature_type=FeatureType.NUMERIC,
        default_value=0.15,
        description='Jockey win rate in last 60 days',
        min_value=0.0,
        max_value=1.0
    ),
    'jockey_hot_streak': FeatureDefinition(
        name='jockey_hot_streak',
        feature_type=FeatureType.BINARY,
        default_value=0,
        description='Binary indicator: 1 if jockey is on hot streak (>25% wins recently)',
        min_value=0,
        max_value=1
    ),
    'jockey_sample_flag': FeatureDefinition(
        name='jockey_sample_flag',
        feature_type=FeatureType.BINARY,
        default_value=1,
        description='Binary indicator: 1 if jockey has small sample size (<30 rides)',
        min_value=0,
        max_value=1
    ),
    'combo_win_rate': FeatureDefinition(
        name='combo_win_rate',
        feature_type=FeatureType.NUMERIC,
        default_value=0.15,
        description='Historical win rate for this trainer-jockey combination',
        min_value=0.0,
        max_value=1.0
    ),
    'combo_synergy_score': FeatureDefinition(
        name='combo_synergy_score',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='How much better/worse combo performs vs individual rates',
        min_value=-0.5,
        max_value=0.5
    ),

    # =============================================================================
    # TRACK/POSITION FEATURES (6 features)
    # =============================================================================
    'post_position': FeatureDefinition(
        name='post_position',
        feature_type=FeatureType.NUMERIC,
        default_value=5,
        description='Starting gate position (1 = rail)',
        min_value=1,
        max_value=14
    ),
    'post_position_win_rate': FeatureDefinition(
        name='post_position_win_rate',
        feature_type=FeatureType.NUMERIC,
        default_value=0.10,
        description='Historical win rate from this post position at this track',
        min_value=0.0,
        max_value=1.0
    ),
    'inside_bias_score': FeatureDefinition(
        name='inside_bias_score',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Track bias toward inside posts (positive = inside advantage)',
        min_value=-0.5,
        max_value=0.5
    ),
    'rail_bias_adjustment': FeatureDefinition(
        name='rail_bias_adjustment',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Adjustment for rail position bias on this track/distance',
        min_value=-0.3,
        max_value=0.3
    ),
    'speed_bias_score': FeatureDefinition(
        name='speed_bias_score',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Track bias toward speed vs closers (positive = favors speed)',
        min_value=-0.5,
        max_value=0.5
    ),
    'field_size': FeatureDefinition(
        name='field_size',
        feature_type=FeatureType.NUMERIC,
        default_value=8,
        description='Number of horses in the race',
        min_value=2,
        max_value=20
    ),

    # =============================================================================
    # EQUIPMENT FEATURES (4 features)
    # =============================================================================
    'blinkers_on': FeatureDefinition(
        name='blinkers_on',
        feature_type=FeatureType.BINARY,
        default_value=0,
        description='Binary indicator: 1 if horse is wearing blinkers',
        min_value=0,
        max_value=1
    ),
    'blinkers_first_time': FeatureDefinition(
        name='blinkers_first_time',
        feature_type=FeatureType.BINARY,
        default_value=0,
        description='Binary indicator: 1 if first time using blinkers',
        min_value=0,
        max_value=1
    ),
    'lasix_on': FeatureDefinition(
        name='lasix_on',
        feature_type=FeatureType.BINARY,
        default_value=0,
        description='Binary indicator: 1 if horse is on Lasix/Salix',
        min_value=0,
        max_value=1
    ),
    'equipment_change': FeatureDefinition(
        name='equipment_change',
        feature_type=FeatureType.BINARY,
        default_value=0,
        description='Binary indicator: 1 if any equipment change from last race',
        min_value=0,
        max_value=1
    ),

    # =============================================================================
    # FIELD-RELATIVE FEATURES (4 features)
    # =============================================================================
    'speed_rank_in_field': FeatureDefinition(
        name='speed_rank_in_field',
        feature_type=FeatureType.NUMERIC,
        default_value=5,
        description='Rank of horse by speed figures in this field (1 = fastest)',
        min_value=1,
        max_value=20
    ),
    'class_rank_in_field': FeatureDefinition(
        name='class_rank_in_field',
        feature_type=FeatureType.NUMERIC,
        default_value=5,
        description='Rank of horse by class in this field (1 = highest class)',
        min_value=1,
        max_value=20
    ),
    'field_quality_score': FeatureDefinition(
        name='field_quality_score',
        feature_type=FeatureType.NUMERIC,
        default_value=50.0,
        description='Overall quality of the field (higher = stronger competition)',
        min_value=0.0,
        max_value=150.0
    ),
    'speed_vs_field_avg': FeatureDefinition(
        name='speed_vs_field_avg',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Horse speed figure vs field average (positive = faster)',
        min_value=-50.0,
        max_value=50.0
    ),

    # =============================================================================
    # BASE FEATURES (3 features)
    # =============================================================================
    'morning_line_odds': FeatureDefinition(
        name='morning_line_odds',
        feature_type=FeatureType.NUMERIC,
        default_value=10.0,
        description='Morning line odds (e.g., 5.0 = 5-1)',
        min_value=0.5,
        max_value=99.0
    ),
    'age_at_race': FeatureDefinition(
        name='age_at_race',
        feature_type=FeatureType.NUMERIC,
        default_value=4,
        description='Age of horse at time of race (in years)',
        min_value=2,
        max_value=12
    ),
    'class_level': FeatureDefinition(
        name='class_level',
        feature_type=FeatureType.NUMERIC,
        default_value=5,
        description='Numeric class level of race (1=highest, 10=lowest)',
        min_value=1,
        max_value=10
    ),

    # =============================================================================
    # PACE FEATURES (8 features)
    # =============================================================================
    'horse_pace_early': FeatureDefinition(
        name='horse_pace_early',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Average early pace figure (first call position, 0-10 scale) over last 5 races',
        min_value=0.0,
        max_value=10.0
    ),
    'horse_pace_mid': FeatureDefinition(
        name='horse_pace_mid',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Average mid pace figure (second call position, 0-10 scale) over last 5 races',
        min_value=0.0,
        max_value=10.0
    ),
    'horse_pace_late': FeatureDefinition(
        name='horse_pace_late',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Average late pace gain/loss (positive = closes ground, negative = fades)',
        min_value=-5.0,
        max_value=5.0
    ),
    'horse_pace_style': FeatureDefinition(
        name='horse_pace_style',
        feature_type=FeatureType.NUMERIC,
        default_value=2,
        description='Running style classification (1=E early, 2=EP presser, 3=PS stalker, 4=S closer)',
        min_value=1,
        max_value=4
    ),
    'race_pace_scenario': FeatureDefinition(
        name='race_pace_scenario',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Predicted pace scenario (positive = hot/contested, negative = slow/lone speed)',
        min_value=-3.0,
        max_value=3.0
    ),
    'horse_pace_fit_score': FeatureDefinition(
        name='horse_pace_fit_score',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='How well running style fits pace scenario (positive = favorable)',
        min_value=-3.0,
        max_value=3.0
    ),
    'field_early_speed_count': FeatureDefinition(
        name='field_early_speed_count',
        feature_type=FeatureType.NUMERIC,
        default_value=1,
        description='Number of E-type (early speed) horses in the field',
        min_value=0,
        max_value=14
    ),
    'horse_is_lone_speed': FeatureDefinition(
        name='horse_is_lone_speed',
        feature_type=FeatureType.BINARY,
        default_value=0,
        description='Binary: 1 if this horse is the only E-type in the field',
        min_value=0,
        max_value=1
    ),

    # =============================================================================
    # SPEED ADJUSTMENT FEATURES (4 features)
    # =============================================================================
    'horse_speed_track_adjusted': FeatureDefinition(
        name='horse_speed_track_adjusted',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Best speed figure (90 days) adjusted for daily track variant',
        min_value=0.0,
        max_value=150.0
    ),
    'horse_speed_surface_adjusted': FeatureDefinition(
        name='horse_speed_surface_adjusted',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Track-adjusted speed with surface conversion penalty applied',
        min_value=0.0,
        max_value=150.0
    ),
    'horse_speed_class_adjusted': FeatureDefinition(
        name='horse_speed_class_adjusted',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Surface-adjusted speed with class level differential penalty',
        min_value=0.0,
        max_value=150.0
    ),
    'daily_track_variant': FeatureDefinition(
        name='daily_track_variant',
        feature_type=FeatureType.NUMERIC,
        default_value=0.0,
        description='Track variant for current race (previous day or 7-day trailing avg)',
        min_value=-15.0,
        max_value=15.0
    ),
}


def get_feature_columns() -> List[str]:
    """
    Return the list of feature columns for training.

    Returns:
        List of feature column names
    """
    return FEATURE_COLUMNS.copy()


def get_target_column() -> str:
    """
    Return the target column name.

    Returns:
        Target column name
    """
    return TARGET_COLUMN


def get_id_columns() -> List[str]:
    """
    Return the list of ID columns.

    Returns:
        List of ID column names
    """
    return ID_COLUMNS.copy()


def get_default_values() -> Dict[str, Any]:
    """
    Return default values for missing features.

    Returns:
        Dictionary mapping feature names to their default values
    """
    return {name: defn.default_value for name, defn in FEATURE_DEFINITIONS.items()}


def get_numeric_features() -> List[str]:
    """
    Return list of numeric feature names.

    Returns:
        List of numeric feature names
    """
    return [
        name for name, defn in FEATURE_DEFINITIONS.items()
        if defn.feature_type == FeatureType.NUMERIC and name in FEATURE_COLUMNS
    ]


def get_binary_features() -> List[str]:
    """
    Return list of binary feature names.

    Returns:
        List of binary feature names
    """
    return [
        name for name, defn in FEATURE_DEFINITIONS.items()
        if defn.feature_type == FeatureType.BINARY and name in FEATURE_COLUMNS
    ]


def get_categorical_features() -> List[str]:
    """
    Return list of categorical feature names.

    Returns:
        List of categorical feature names
    """
    return [
        name for name, defn in FEATURE_DEFINITIONS.items()
        if defn.feature_type == FeatureType.CATEGORICAL and name in FEATURE_COLUMNS
    ]


def validate_features(df) -> List[str]:
    """
    Validate that a DataFrame has all required features.

    Args:
        df: pandas DataFrame to validate

    Returns:
        List of missing feature names (empty if all features present)
    """
    missing = [col for col in FEATURE_COLUMNS if col not in df.columns]
    return missing


def get_feature_info(feature_name: str) -> FeatureDefinition:
    """
    Get detailed information about a specific feature.

    Args:
        feature_name: Name of the feature

    Returns:
        FeatureDefinition object with feature metadata

    Raises:
        KeyError: If feature name not found in definitions
    """
    if feature_name not in FEATURE_DEFINITIONS:
        raise KeyError(f"Feature '{feature_name}' not found in feature definitions")
    return FEATURE_DEFINITIONS[feature_name]


def get_feature_ranges() -> Dict[str, tuple]:
    """
    Get expected min/max ranges for numeric features.

    Returns:
        Dictionary mapping feature names to (min_value, max_value) tuples
    """
    return {
        name: (defn.min_value, defn.max_value)
        for name, defn in FEATURE_DEFINITIONS.items()
        if defn.min_value is not None and defn.max_value is not None
    }


def get_all_columns() -> List[str]:
    """
    Get all columns including features, target, and IDs.

    Returns:
        List of all column names used in the modeling pipeline
    """
    return FEATURE_COLUMNS + [TARGET_COLUMN] + ID_COLUMNS


# Summary statistics for documentation
FEATURE_SUMMARY = {
    'total_features': len(FEATURE_COLUMNS),
    'horse_form': 14,
    'connections': 12,
    'track_position': 6,
    'equipment': 4,
    'field_relative': 4,
    'base': 3,
    'pace': 8,
    'speed_adjustments': 4,
    'numeric': len(get_numeric_features()),
    'binary': len(get_binary_features()),
    'categorical': len(get_categorical_features()),
}
