"""
Run Backtest Script

Usage:
    python run_backtest.py --model artifacts/models/v1.0
    python run_backtest.py --model artifacts/models/v1.0 --strategy kelly
    python run_backtest.py --model artifacts/models/v1.0 --compare
"""

import argparse
import json
import logging
import pickle
import sys
from datetime import date, timedelta
from pathlib import Path

from models import RacingLightGBM, FieldSizeCalibrator
from backtesting import (
    Backtester,
    FlatBetStrategy,
    KellyCriterionStrategy,
    ValueBettingStrategy,
    TopPickStrategy,
    MorningFavoriteStrategy,
    MomentumStrategy
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_model(model_dir: str):
    """Load model, calibrator, and metadata from artifacts directory."""
    model_path = Path(model_dir)

    # Load metadata
    metadata_file = model_path / 'metadata.json'
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)

    # Load model
    model = RacingLightGBM.load(str(model_path / 'model.pkl'))

    # Load calibrator
    with open(model_path / 'calibrator.pkl', 'rb') as f:
        calibrator = pickle.load(f)

    feature_columns = metadata.get('feature_columns', [])

    return model, calibrator, feature_columns, metadata


def main():
    parser = argparse.ArgumentParser(description='Run betting strategy backtest')
    parser.add_argument(
        '--model', '-m',
        default='artifacts/models/v1.0',
        help='Path to model artifacts directory'
    )
    parser.add_argument(
        '--strategy', '-s',
        choices=['flat', 'kelly', 'value', 'toppick', 'morning_favorite', 'momentum'],
        default='flat',
        help='Betting strategy to use'
    )
    parser.add_argument(
        '--compare', '-c',
        action='store_true',
        help='Compare all strategies'
    )
    parser.add_argument(
        '--start-date',
        default=None,
        help='Start date (YYYY-MM-DD), defaults to 30 days before end'
    )
    parser.add_argument(
        '--end-date',
        default=None,
        help='End date (YYYY-MM-DD), defaults to config test end date'
    )
    parser.add_argument(
        '--bankroll',
        type=float,
        default=1000.0,
        help='Initial bankroll (default: 1000)'
    )
    parser.add_argument(
        '--db',
        default='racing_data.db',
        help='Path to database'
    )

    args = parser.parse_args()

    # Load model
    logger.info(f"Loading model from {args.model}")
    try:
        model, calibrator, feature_columns, metadata = load_model(args.model)
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        sys.exit(1)

    logger.info(f"Model loaded: {len(feature_columns)} features")

    # Set date range
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)
    else:
        # Use test period from config
        config = metadata.get('config', {})
        test_end = config.get('model', {}).get('splits', {}).get('test', {}).get('end')
        end_date = date.fromisoformat(test_end) if test_end else date.today()

    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    else:
        start_date = end_date - timedelta(days=30)

    logger.info(f"Backtest period: {start_date} to {end_date}")

    # Initialize backtester
    backtester = Backtester(
        model=model,
        calibrator=calibrator,
        feature_columns=feature_columns,
        db_path=args.db
    )

    if args.compare:
        # Compare all strategies
        strategies = [
            FlatBetStrategy(bet_amount=2.0),
            FlatBetStrategy(bet_amount=2.0, min_prob=0.15),
            KellyCriterionStrategy(fraction=0.25, min_edge=0.05),
            KellyCriterionStrategy(fraction=0.10, min_edge=0.10),
            ValueBettingStrategy(bet_fraction=0.02, min_edge=0.10),
            TopPickStrategy(bet_amount=2.0, min_prob=0.15),
        ]

        comparison = backtester.compare_strategies(
            strategies=strategies,
            start_date=start_date,
            end_date=end_date,
            initial_bankroll=args.bankroll
        )

        print("\n" + "="*80)
        print("STRATEGY COMPARISON")
        print("="*80)
        print(comparison.to_string(index=False))
        print("="*80)

    else:
        # Run single strategy
        strategy_map = {
            'flat': FlatBetStrategy(bet_amount=2.0),
            'kelly': KellyCriterionStrategy(fraction=0.25, min_edge=0.05),
            'value': ValueBettingStrategy(bet_fraction=0.02, min_edge=0.10),
            'toppick': TopPickStrategy(bet_amount=2.0, min_prob=0.15),
            'morning_favorite': MorningFavoriteStrategy(min_edge=0.10, bet_fraction=0.03),
            'momentum': MomentumStrategy(base_fraction=0.02, momentum_multiplier=2.0),
        }

        strategy = strategy_map[args.strategy]

        results = backtester.run(
            strategy=strategy,
            start_date=start_date,
            end_date=end_date,
            initial_bankroll=args.bankroll
        )

        print(results.summary())

        # Show sample bets
        if results.bets:
            print("\nSAMPLE BETS (last 10):")
            print("-" * 80)
            for bet in results.bets[-10:]:
                outcome = "WIN" if bet.won else "LOSS"
                print(f"  {bet.race_date} | {bet.horse_name[:20]:<20} | "
                      f"Prob: {bet.model_prob:.1%} | Odds: {bet.odds:.1f} | "
                      f"Bet: ${bet.bet_amount:.2f} | {outcome} | P/L: ${bet.profit:+.2f}")


if __name__ == '__main__':
    main()
