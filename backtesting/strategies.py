"""
Betting Strategies for Backtesting

Implements various betting strategies:
- Flat Bet: Fixed amount on each bet
- Kelly Criterion: Optimal bet sizing based on edge
- Value Betting: Only bet when model probability > implied odds probability
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional
import numpy as np


class BettingStrategy(ABC):
    """Abstract base class for betting strategies."""

    @abstractmethod
    def calculate_bet(
        self,
        model_prob: float,
        odds: float,
        bankroll: float,
        **kwargs
    ) -> float:
        """
        Calculate bet amount.

        Args:
            model_prob: Model's predicted win probability (0-1)
            odds: Decimal odds (e.g., 5.0 means 4:1)
            bankroll: Current bankroll
            **kwargs: Additional strategy-specific parameters

        Returns:
            Bet amount (0 if no bet)
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Strategy name for reporting."""
        pass


class FlatBetStrategy(BettingStrategy):
    """
    Flat betting: Fixed amount on each qualifying bet.

    Simple strategy that bets a fixed amount regardless of edge.
    Good baseline for comparison.
    """

    def __init__(
        self,
        bet_amount: float = 2.0,
        min_prob: float = 0.0,
        max_odds: float = float('inf')
    ):
        """
        Args:
            bet_amount: Fixed bet amount
            min_prob: Minimum model probability to place bet
            max_odds: Maximum odds to bet on
        """
        self.bet_amount = bet_amount
        self.min_prob = min_prob
        self.max_odds = max_odds

    @property
    def name(self) -> str:
        return f"FlatBet(${self.bet_amount})"

    def calculate_bet(
        self,
        model_prob: float,
        odds: float,
        bankroll: float,
        **kwargs
    ) -> float:
        # Check minimum probability threshold
        if model_prob < self.min_prob:
            return 0.0

        # Check maximum odds threshold
        if odds > self.max_odds:
            return 0.0

        # Check bankroll
        if bankroll < self.bet_amount:
            return 0.0

        return self.bet_amount


class KellyCriterionStrategy(BettingStrategy):
    """
    Kelly Criterion: Optimal bet sizing based on edge.

    Maximizes long-term growth rate by betting proportionally to edge.
    Uses fractional Kelly (e.g., 0.25) to reduce variance.

    Kelly formula: f* = (bp - q) / b
    where:
        b = decimal odds - 1 (net odds)
        p = probability of winning
        q = probability of losing (1 - p)
    """

    def __init__(
        self,
        fraction: float = 0.25,
        min_edge: float = 0.05,
        max_bet_fraction: float = 0.10
    ):
        """
        Args:
            fraction: Fraction of Kelly to bet (0.25 = quarter Kelly)
            min_edge: Minimum edge required to bet (model_prob - implied_prob)
            max_bet_fraction: Maximum fraction of bankroll per bet
        """
        self.fraction = fraction
        self.min_edge = min_edge
        self.max_bet_fraction = max_bet_fraction

    @property
    def name(self) -> str:
        return f"Kelly({self.fraction:.0%})"

    def calculate_bet(
        self,
        model_prob: float,
        odds: float,
        bankroll: float,
        **kwargs
    ) -> float:
        # Calculate implied probability from odds
        implied_prob = 1.0 / odds

        # Calculate edge
        edge = model_prob - implied_prob

        # Check minimum edge
        if edge < self.min_edge:
            return 0.0

        # Kelly formula
        b = odds - 1  # Net odds
        p = model_prob
        q = 1 - p

        kelly_fraction = (b * p - q) / b

        # Apply fractional Kelly
        bet_fraction = kelly_fraction * self.fraction

        # Cap at maximum bet fraction
        bet_fraction = min(bet_fraction, self.max_bet_fraction)

        # No negative bets
        if bet_fraction <= 0:
            return 0.0

        return bankroll * bet_fraction


class ValueBettingStrategy(BettingStrategy):
    """
    Value Betting: Only bet when model finds positive expected value.

    Bets a percentage of bankroll when:
    model_probability > (1 / odds) * (1 + min_edge)
    """

    def __init__(
        self,
        bet_fraction: float = 0.02,
        min_edge: float = 0.10,
        min_prob: float = 0.05,
        max_prob: float = 0.50
    ):
        """
        Args:
            bet_fraction: Fraction of bankroll to bet
            min_edge: Minimum edge over implied probability
            min_prob: Minimum model probability (avoid extreme longshots)
            max_prob: Maximum model probability (avoid heavy favorites)
        """
        self.bet_fraction = bet_fraction
        self.min_edge = min_edge
        self.min_prob = min_prob
        self.max_prob = max_prob

    @property
    def name(self) -> str:
        return f"Value({self.min_edge:.0%}edge)"

    def calculate_bet(
        self,
        model_prob: float,
        odds: float,
        bankroll: float,
        **kwargs
    ) -> float:
        # Check probability bounds
        if model_prob < self.min_prob or model_prob > self.max_prob:
            return 0.0

        # Calculate implied probability
        implied_prob = 1.0 / odds

        # Check edge
        edge = model_prob - implied_prob
        if edge < self.min_edge:
            return 0.0

        # Calculate expected value
        ev = (model_prob * (odds - 1)) - (1 - model_prob)
        if ev <= 0:
            return 0.0

        return bankroll * self.bet_fraction


class TopPickStrategy(BettingStrategy):
    """
    Top Pick: Bet on the horse with highest model probability in each race.

    Only bets if the top pick meets minimum probability threshold.
    """

    def __init__(
        self,
        bet_amount: float = 2.0,
        min_prob: float = 0.15,
        min_edge: float = 0.0
    ):
        """
        Args:
            bet_amount: Fixed bet amount
            min_prob: Minimum probability for top pick
            min_edge: Minimum edge over implied probability
        """
        self.bet_amount = bet_amount
        self.min_prob = min_prob
        self.min_edge = min_edge

    @property
    def name(self) -> str:
        return f"TopPick(${self.bet_amount})"

    def calculate_bet(
        self,
        model_prob: float,
        odds: float,
        bankroll: float,
        is_top_pick: bool = False,
        **kwargs
    ) -> float:
        # Only bet on top pick
        if not is_top_pick:
            return 0.0

        # Check minimum probability
        if model_prob < self.min_prob:
            return 0.0

        # Check edge if specified
        if self.min_edge > 0:
            implied_prob = 1.0 / odds
            if model_prob - implied_prob < self.min_edge:
                return 0.0

        # Check bankroll
        if bankroll < self.bet_amount:
            return 0.0

        return self.bet_amount
        
        
class MomentumStrategy(BettingStrategy):
      """
      Bet more on horses with improving form.
      Combines model probability with recent performance trend.
      """

      def __init__(self, base_fraction=0.02, momentum_multiplier=2.0):
          self.base_fraction = base_fraction
          self.momentum_multiplier = momentum_multiplier

      @property
      def name(self) -> str:
          return f"Momentum(x{self.momentum_multiplier})"

      def calculate_bet(
          self,
          model_prob: float,
          odds: float,
          bankroll: float,
          recent_form: str = None,  # e.g., "112" (1st, 1st, 2nd)
          **kwargs
      ) -> float:
          # Base check
          if model_prob < 0.10:  # Minimum 10% win probability
              return 0.0

          # Calculate momentum factor from recent form
          momentum_factor = 1.0
          if recent_form:
              # Count wins in last 3 races
              wins = recent_form.count('1')
              if wins >= 2:  # At least 2 wins
                  momentum_factor = self.momentum_multiplier

          # Calculate bet
          bet_amount = bankroll * self.base_fraction * momentum_factor

          # Apply Kelly-like edge adjustment
          implied_prob = 1.0 / odds
          edge = model_prob - implied_prob
          if edge > 0:
              bet_amount *= (1 + edge)  # Increase bet with edge

          return min(bet_amount, bankroll * 0.05)  # Cap at 5% of bankroll

class MorningFavoriteStrategy(BettingStrategy):
      """Only bet on morning line favorites with model edge."""

      def __init__(self, min_edge=0.10, bet_fraction=0.03):
          self.min_edge = min_edge
          self.bet_fraction = bet_fraction

      @property
      def name(self) -> str:
          return f"MorningFav({self.min_edge:.0%}edge)"

      def calculate_bet(self, model_prob, odds, bankroll,
                       morning_line_rank=None, **kwargs):
          # Only bet on morning line favorites
          if morning_line_rank != 1:
              return 0.0

          # Check edge
          implied_prob = 1.0 / odds
          if model_prob - implied_prob < self.min_edge:
              return 0.0

          return bankroll * self.bet_fraction
