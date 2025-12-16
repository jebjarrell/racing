"""
Feature Engineering Layer for Horse Racing Quantitative Betting System

This package provides point-in-time feature calculation for ML model training
and inference. All features are calculated using only historical data available
before the target race to prevent data leakage.

Modules:
    rolling_stats: Trainer/jockey/combo rolling statistics (14/30/60 day windows)
    track_bias: Post position and track surface bias calculations
    validation: Leakage prevention and validation framework
    feature_engine: Main orchestration for feature calculation

Usage:
    from features import FeatureEngine

    engine = FeatureEngine()
    features_df = engine.calculate_all_features(race_id='SAR-2023-09-01-5',
                                                 race_date=date(2023, 9, 1))
"""

from .rolling_stats import RollingStatsCalculator
from .track_bias import TrackBiasCalculator
from .validation import LeakageValidator
from .feature_engine import FeatureEngine

__all__ = [
    'RollingStatsCalculator',
    'TrackBiasCalculator',
    'LeakageValidator',
    'FeatureEngine',
]

__version__ = '1.0.0'
