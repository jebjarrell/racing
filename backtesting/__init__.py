"""
Backtesting Module for Horse Racing Predictions

Provides tools to simulate betting strategies on historical data
and measure ROI, profit, and other performance metrics.
"""

from .backtester import Backtester
from .strategies import (
    BettingStrategy,
    FlatBetStrategy,
    KellyCriterionStrategy,
    ValueBettingStrategy,
    TopPickStrategy,
    MorningFavoriteStrategy,
    MomentumStrategy
)

__all__ = [
    'Backtester',
    'BettingStrategy',
    'FlatBetStrategy',
    'KellyCriterionStrategy',
    'ValueBettingStrategy',
    'TopPickStrategy',
    'MorningFavoriteStrategy',
    'MomentumStrategy'
]
