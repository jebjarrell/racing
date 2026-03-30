"""
Backtester for Horse Racing Betting Strategies

Simulates betting on historical races using model predictions
and calculates performance metrics.
"""

import logging
import sys
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Any
import numpy as np
import pandas as pd
import sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.utils.betting import to_decimal_odds

from .strategies import BettingStrategy, FlatBetStrategy

logger = logging.getLogger(__name__)


@dataclass
class BetRecord:
    """Record of a single bet."""
    race_id: str
    race_date: date
    entry_id: str
    horse_name: str
    model_prob: float
    odds: float
    bet_amount: float
    won: bool
    payout: float
    profit: float


@dataclass
class BacktestResults:
    """Results from a backtest run."""
    strategy_name: str
    start_date: date
    end_date: date
    initial_bankroll: float
    final_bankroll: float

    # Bet statistics
    total_bets: int = 0
    winning_bets: int = 0
    total_wagered: float = 0.0
    total_returned: float = 0.0

    # Performance metrics
    roi: float = 0.0
    profit: float = 0.0
    win_rate: float = 0.0
    avg_odds_bet: float = 0.0
    avg_odds_won: float = 0.0

    # Risk metrics
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0

    # Detailed records
    bets: List[BetRecord] = field(default_factory=list)
    bankroll_history: List[float] = field(default_factory=list)

    def summary(self) -> str:
        """Return formatted summary string."""
        return f"""
Backtest Results: {self.strategy_name}
{'='*50}
Period: {self.start_date} to {self.end_date}

PERFORMANCE:
  ROI:           {self.roi:+.2%}
  Profit:        ${self.profit:+,.2f}
  Final Bankroll: ${self.final_bankroll:,.2f}

BETTING STATS:
  Total Bets:    {self.total_bets:,}
  Winning Bets:  {self.winning_bets:,} ({self.win_rate:.1%})
  Total Wagered: ${self.total_wagered:,.2f}
  Total Returned: ${self.total_returned:,.2f}

ODDS:
  Avg Odds Bet:  {self.avg_odds_bet:.2f}
  Avg Odds Won:  {self.avg_odds_won:.2f}

RISK:
  Max Drawdown:  {self.max_drawdown:.2%}
{'='*50}
"""


class Backtester:
    """
    Backtester for horse racing betting strategies.

    Uses trained model predictions and historical odds to simulate
    betting performance over a specified time period.
    """

    def __init__(
        self,
        model,
        calibrator,
        feature_columns: List[str],
        db_path: str = 'racing_data.db'
    ):
        """
        Initialize backtester.

        Args:
            model: Trained RacingLightGBM model
            calibrator: Fitted FieldSizeCalibrator
            feature_columns: List of feature column names
            db_path: Path to SQLite database
        """
        self.model = model
        self.calibrator = calibrator
        self.feature_columns = feature_columns
        self.db_path = db_path

    def run(
        self,
        strategy: BettingStrategy,
        start_date: date,
        end_date: date,
        initial_bankroll: float = 1000.0
    ) -> BacktestResults:
        """
        Run backtest simulation.

        Args:
            strategy: Betting strategy to use
            start_date: Start date for simulation
            end_date: End date for simulation
            initial_bankroll: Starting bankroll

        Returns:
            BacktestResults with performance metrics
        """
        logger.info(f"Running backtest: {strategy.name}")
        logger.info(f"Period: {start_date} to {end_date}")
        logger.info(f"Initial bankroll: ${initial_bankroll:,.2f}")

        # Load race data
        races_df = self._load_race_data(start_date, end_date)
        if races_df.empty:
            logger.warning("No race data found for backtest period")
            return BacktestResults(
                strategy_name=strategy.name,
                start_date=start_date,
                end_date=end_date,
                initial_bankroll=initial_bankroll,
                final_bankroll=initial_bankroll
            )

        logger.info(f"Loaded {len(races_df)} entries from {races_df['race_id'].nunique()} races")

        # Initialize results
        bankroll = initial_bankroll
        bets = []
        bankroll_history = [bankroll]

        # Process races chronologically
        race_dates = sorted(races_df['race_date'].unique())

        for race_date_val in race_dates:
            date_races = races_df[races_df['race_date'] == race_date_val]

            for race_id in date_races['race_id'].unique():
                race_entries = date_races[date_races['race_id'] == race_id].copy()

                # Skip if missing required data
                if race_entries['actual_odds'].isna().all():
                    continue
                if race_entries['official_finish_position'].isna().all():
                    continue

                # Generate predictions for this race
                race_bets = self._process_race(
                    race_entries, strategy, bankroll
                )

                # Process bets
                for bet in race_bets:
                    bets.append(bet)
                    bankroll += bet.profit
                    bankroll_history.append(bankroll)

                    # Stop if bankrupt
                    if bankroll <= 0:
                        logger.warning("Bankrupt! Stopping simulation.")
                        break

                if bankroll <= 0:
                    break

            if bankroll <= 0:
                break

        # Calculate metrics
        results = self._calculate_metrics(
            strategy_name=strategy.name,
            start_date=start_date,
            end_date=end_date,
            initial_bankroll=initial_bankroll,
            final_bankroll=bankroll,
            bets=bets,
            bankroll_history=bankroll_history
        )

        logger.info(f"Backtest complete: ROI = {results.roi:+.2%}")
        return results

    def _load_race_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Load race data with features for backtest period."""
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT
                e.entry_id,
                e.race_id,
                r.race_date,
                e.registration_number,
                h.horse_name,
                e.program_number,
                e.morning_line_odds,
                e.actual_odds,
                e.official_finish_position,
                e.win_payoff,
                e.jockey_id,
                e.trainer_id
            FROM race_entries_standardized e
            JOIN races_standardized r ON e.race_id = r.race_id
            LEFT JOIN horses_master h ON e.registration_number = h.registration_number
            WHERE r.race_date BETWEEN ? AND ?
              AND e.official_finish_position IS NOT NULL
              AND e.actual_odds IS NOT NULL
              AND e.actual_odds > 0
            ORDER BY r.race_date, r.race_number, e.program_number
        """

        df = pd.read_sql_query(
            query, conn,
            params=[start_date.isoformat(), end_date.isoformat()]
        )
        conn.close()

        return df

    def _process_race(
        self,
        race_entries: pd.DataFrame,
        strategy: BettingStrategy,
        bankroll: float
    ) -> List[BetRecord]:
        """Process a single race and return bets."""
        bets = []

        # Calculate features for this race (simplified - uses available columns)
        # In production, would use FeatureEngine
        X = self._prepare_features(race_entries)
        if X is None or X.empty:
            return bets

        # Get model predictions
        try:
            raw_probs = self.model.predict_raw(X)
            field_sizes = np.full(len(X), len(race_entries))
            probs = self.calibrator.calibrate(raw_probs, field_sizes)
        except Exception as e:
            logger.debug(f"Prediction error: {e}")
            return bets

        # Add probabilities to dataframe
        race_entries = race_entries.copy()
        race_entries['model_prob'] = probs

        # Find top pick
        top_pick_idx = race_entries['model_prob'].idxmax()

        # Compute morning line rank for MorningFavoriteStrategy
        race_entries = race_entries.copy()
        if 'morning_line_odds' in race_entries.columns:
            valid_ml = race_entries['morning_line_odds'].notna() & (race_entries['morning_line_odds'] > 0)
            race_entries.loc[valid_ml, 'morning_line_rank'] = (
                race_entries.loc[valid_ml, 'morning_line_odds'].rank(method='min', ascending=True)
            )

        # Process each entry
        for idx, row in race_entries.iterrows():
            odds = row['actual_odds']
            if pd.isna(odds) or odds <= 1:
                continue

            # Convert American odds to decimal if needed
            decimal_odds = to_decimal_odds(odds)

            # Calculate bet
            is_top_pick = (idx == top_pick_idx)
            bet_amount = strategy.calculate_bet(
                model_prob=row['model_prob'],
                odds=decimal_odds,
                bankroll=bankroll,
                is_top_pick=is_top_pick,
                morning_line_rank=row.get('morning_line_rank'),
                recent_form=row.get('recent_form'),
            )

            if bet_amount > 0:
                # Determine outcome
                won = row['official_finish_position'] == 1
                payout = 0.0

                if won and row['win_payoff'] and row['win_payoff'] > 0:
                    # Win payoff is typically per $2 bet
                    payout = bet_amount * (row['win_payoff'] / 2.0)

                profit = payout - bet_amount

                bet_record = BetRecord(
                    race_id=row['race_id'],
                    race_date=row['race_date'],
                    entry_id=row['entry_id'],
                    horse_name=row.get('horse_name', 'Unknown'),
                    model_prob=row['model_prob'],
                    odds=decimal_odds,
                    bet_amount=bet_amount,
                    won=won,
                    payout=payout,
                    profit=profit
                )
                bets.append(bet_record)
                bankroll += profit

        return bets

    def _prepare_features(self, race_entries: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Prepare feature matrix for prediction."""
        # Create feature dataframe with available columns
        # This is simplified - full version would use FeatureEngine

        # Check if we have the required feature columns
        available_cols = [c for c in self.feature_columns if c in race_entries.columns]

        if len(available_cols) < len(self.feature_columns) * 0.5:
            # Not enough features - need to calculate them
            # For now, return None to skip this race
            return None

        X = race_entries[available_cols].copy()
        X = X.fillna(0)

        # Ensure all required columns exist
        for col in self.feature_columns:
            if col not in X.columns:
                X[col] = 0

        return X[self.feature_columns]

    def _calculate_metrics(
        self,
        strategy_name: str,
        start_date: date,
        end_date: date,
        initial_bankroll: float,
        final_bankroll: float,
        bets: List[BetRecord],
        bankroll_history: List[float]
    ) -> BacktestResults:
        """Calculate performance metrics from bet records."""
        total_bets = len(bets)

        if total_bets == 0:
            return BacktestResults(
                strategy_name=strategy_name,
                start_date=start_date,
                end_date=end_date,
                initial_bankroll=initial_bankroll,
                final_bankroll=final_bankroll,
                bets=bets,
                bankroll_history=bankroll_history
            )

        winning_bets = sum(1 for b in bets if b.won)
        total_wagered = sum(b.bet_amount for b in bets)
        total_returned = sum(b.payout for b in bets)
        profit = final_bankroll - initial_bankroll

        # ROI
        roi = profit / total_wagered if total_wagered > 0 else 0

        # Win rate
        win_rate = winning_bets / total_bets if total_bets > 0 else 0

        # Average odds
        avg_odds_bet = np.mean([b.odds for b in bets]) if bets else 0
        winning_odds = [b.odds for b in bets if b.won]
        avg_odds_won = np.mean(winning_odds) if winning_odds else 0

        # Max drawdown
        peak = initial_bankroll
        max_drawdown = 0
        for bankroll in bankroll_history:
            if bankroll > peak:
                peak = bankroll
            drawdown = (peak - bankroll) / peak if peak > 0 else 0
            max_drawdown = max(max_drawdown, drawdown)

        return BacktestResults(
            strategy_name=strategy_name,
            start_date=start_date,
            end_date=end_date,
            initial_bankroll=initial_bankroll,
            final_bankroll=final_bankroll,
            total_bets=total_bets,
            winning_bets=winning_bets,
            total_wagered=total_wagered,
            total_returned=total_returned,
            roi=roi,
            profit=profit,
            win_rate=win_rate,
            avg_odds_bet=avg_odds_bet,
            avg_odds_won=avg_odds_won,
            max_drawdown=max_drawdown,
            bets=bets,
            bankroll_history=bankroll_history
        )

    def compare_strategies(
        self,
        strategies: List[BettingStrategy],
        start_date: date,
        end_date: date,
        initial_bankroll: float = 1000.0
    ) -> pd.DataFrame:
        """
        Compare multiple strategies on the same data.

        Args:
            strategies: List of strategies to compare
            start_date: Start date
            end_date: End date
            initial_bankroll: Starting bankroll for each

        Returns:
            DataFrame comparing strategy performance
        """
        results = []

        for strategy in strategies:
            result = self.run(strategy, start_date, end_date, initial_bankroll)
            results.append({
                'Strategy': result.strategy_name,
                'ROI': result.roi,
                'Profit': result.profit,
                'Total Bets': result.total_bets,
                'Win Rate': result.win_rate,
                'Avg Odds': result.avg_odds_bet,
                'Max Drawdown': result.max_drawdown,
                'Final Bankroll': result.final_bankroll
            })

        return pd.DataFrame(results)
