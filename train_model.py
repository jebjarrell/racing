"""
Training Script for Horse Racing Probability Model - Phase 3

This script orchestrates the complete model training pipeline:
1. Data preparation from SQLite database
2. Feature engineering using point-in-time calculations
3. Model training with LightGBM (race-grouped softmax)
4. Probability calibration (field-size stratified isotonic)
5. Model evaluation and artifact saving

Usage:
    python train_model.py                    # Full training with defaults
    python train_model.py --version v1.0     # Specify version
    python train_model.py --config custom.yaml  # Custom config
    python train_model.py --quick            # Quick mode (subset of data)
    python train_model.py --dry-run          # Validate setup without training

Requirements:
    - racing_data.db populated with historical race data
    - config/config.yaml with model parameters
    - features/ package for feature engineering
    - models/ package for ML components

Author: Racing Pipeline Team
Version: 1.0.0
"""

import argparse
import logging
import sqlite3
import sys
import traceback
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd
import yaml

# Suppress warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

# Import feature engine
try:
    from features import FeatureEngine
except ImportError:
    print("ERROR: Cannot import FeatureEngine. Ensure features/ package exists.")
    sys.exit(1)

# Import model components (these will be implemented in Phase 3)
try:
    from models import (
        ModelTrainingPipeline,
        RacingLightGBM,
        FieldSizeCalibrator,
        ModelEvaluator
    )
    MODELS_AVAILABLE = True
except ImportError:
    MODELS_AVAILABLE = False
    print("WARNING: Models package not fully implemented yet.")
    print("This script will prepare data but cannot train models.")


# Configure logging
def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Setup logging configuration."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/train_model.log', mode='a')
        ]
    )
    return logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to config.yaml

    Returns:
        Configuration dictionary
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    return config


def validate_database(db_path: str, logger: logging.Logger) -> bool:
    """
    Validate that database exists and has required tables.

    Args:
        db_path: Path to SQLite database
        logger: Logger instance

    Returns:
        True if valid, False otherwise
    """
    db_file = Path(db_path)
    if not db_file.exists():
        logger.error(f"Database not found: {db_path}")
        return False

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check required tables
        required_tables = [
            'races_standardized',
            'race_entries_standardized',
            'horses_master',
            'trainers'
        ]

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = {row[0] for row in cursor.fetchall()}

        missing = set(required_tables) - existing_tables
        if missing:
            logger.error(f"Missing required tables: {missing}")
            conn.close()
            return False

        # Check data availability
        cursor.execute("SELECT COUNT(*) FROM races_standardized")
        race_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM race_entries_standardized")
        entry_count = cursor.fetchone()[0]

        logger.info(f"Database validation: {race_count:,} races, {entry_count:,} entries")

        conn.close()
        return True

    except Exception as e:
        logger.error(f"Database validation error: {e}")
        return False


def get_training_date_ranges(
    config: Dict[str, Any],
    quick_mode: bool = False
) -> Dict[str, Tuple[date, date]]:
    """
    Get training/validation/test date ranges from config.

    Args:
        config: Configuration dictionary
        quick_mode: If True, use reduced date ranges for testing

    Returns:
        Dictionary with train/val/test date ranges
    """
    splits = config['model']['splits']

    if quick_mode:
        # Quick mode: use just 30 days of data
        end = date.fromisoformat(splits['train']['end'])
        start = end - timedelta(days=30)
        return {
            'train': (start, end),
            'validation': (end, end),  # Same as train in quick mode
            'test': (end, end)
        }

    return {
        'train': (
            date.fromisoformat(splits['train']['start']),
            date.fromisoformat(splits['train']['end'])
        ),
        'validation': (
            date.fromisoformat(splits['validation']['start']),
            date.fromisoformat(splits['validation']['end'])
        ),
        'test': (
            date.fromisoformat(splits['test']['start']),
            date.fromisoformat(splits['test']['end'])
        )
    }


def prepare_training_data(
    db_path: str,
    date_ranges: Dict[str, Tuple[date, date]],
    logger: logging.Logger,
    quick_mode: bool = False
) -> Dict[str, pd.DataFrame]:
    """
    Prepare training data with features for all splits.

    Args:
        db_path: Path to SQLite database
        date_ranges: Date ranges for train/val/test
        logger: Logger instance
        quick_mode: If True, limit data for faster processing

    Returns:
        Dictionary with train/val/test DataFrames
    """
    logger.info("="*70)
    logger.info("STEP 1/5: PREPARING TRAINING DATA")
    logger.info("="*70)

    # Initialize feature engine
    engine = FeatureEngine(db_path=db_path)

    datasets = {}

    try:
        for split_name, (start_date, end_date) in date_ranges.items():
            logger.info(f"\nProcessing {split_name} split: {start_date} to {end_date}")

            # Progress callback
            def progress_callback(race_id: str, current: int, total: int):
                if current % 50 == 0 or current == total:
                    logger.info(f"  Progress: {current}/{total} races ({100*current/total:.1f}%)")

            # Calculate features for date range
            features_list = engine.calculate_features_for_date_range(
                start_date=start_date,
                end_date=end_date,
                progress_callback=progress_callback
            )

            if not features_list:
                logger.warning(f"No features generated for {split_name} split")
                continue

            # Convert to DataFrame
            df = pd.DataFrame(features_list)

            # Add target variable
            df = add_target_variable(df, db_path, logger)

            # Remove entries without target
            initial_rows = len(df)
            df = df.dropna(subset=['won'])
            logger.info(f"  Rows with target: {len(df)}/{initial_rows}")

            datasets[split_name] = df
            logger.info(f"  {split_name.capitalize()} set: {len(df):,} rows")

            if quick_mode and len(df) > 1000:
                logger.info(f"  Quick mode: limiting to 1000 rows")
                datasets[split_name] = df.sample(n=1000, random_state=42)

    finally:
        engine.close()

    # Log summary
    logger.info("\n" + "="*70)
    logger.info("DATA PREPARATION SUMMARY")
    logger.info("="*70)
    for split_name, df in datasets.items():
        if df is not None:
            wins = df['won'].sum()
            win_rate = wins / len(df) if len(df) > 0 else 0
            logger.info(f"{split_name.capitalize():12s}: {len(df):6,} rows, "
                       f"{wins:5,} wins ({win_rate:.1%} win rate)")

    return datasets


def add_target_variable(
    df: pd.DataFrame,
    db_path: str,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Add target variable (won) to feature DataFrame.

    Args:
        df: DataFrame with features
        db_path: Path to database
        logger: Logger instance

    Returns:
        DataFrame with 'won' column added
    """
    conn = sqlite3.connect(db_path)

    # Get finish positions for all entries
    entry_ids = df['entry_id'].unique()

    # Query in batches to avoid SQLite parameter limits
    batch_size = 999
    results = []

    for i in range(0, len(entry_ids), batch_size):
        batch = entry_ids[i:i + batch_size]
        placeholders = ','.join('?' * len(batch))

        query = f"""
            SELECT entry_id, official_finish_position
            FROM race_entries_standardized
            WHERE entry_id IN ({placeholders})
        """

        batch_results = pd.read_sql_query(query, conn, params=batch)
        results.append(batch_results)

    conn.close()

    # Combine results
    if results:
        finish_positions = pd.concat(results, ignore_index=True)

        # Create won flag (1 if official_finish_position == 1, else 0)
        finish_positions['won'] = (finish_positions['official_finish_position'] == 1).astype(int)

        # Merge with features
        df = df.merge(finish_positions[['entry_id', 'won']], on='entry_id', how='left')

    return df


def get_feature_columns(df: pd.DataFrame) -> List[str]:
    """
    Get list of feature columns (exclude metadata and target).

    Args:
        df: Feature DataFrame

    Returns:
        List of feature column names
    """
    exclude_cols = {
        'race_id', 'entry_id', 'registration_number',
        'trainer_id', 'jockey_id', 'won',
        'last_3_finishes'  # String column - exclude from features
    }

    return [col for col in df.columns if col not in exclude_cols]


def train_model_actual(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    config: Dict[str, Any],
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Train LightGBM model for horse racing predictions.

    Args:
        train_df: Training data with features and 'won' target
        val_df: Validation data
        config: Configuration with model hyperparameters
        logger: Logger

    Returns:
        Dictionary with model and training info
    """
    logger.info("="*70)
    logger.info("STEP 3/5: TRAINING MODEL")
    logger.info("="*70)

    feature_cols = get_feature_columns(train_df)
    logger.info(f"Features: {len(feature_cols)} columns")

    # Prepare training data
    X_train = train_df[feature_cols].copy()
    y_train = train_df['won'].copy()

    X_val = val_df[feature_cols].copy()
    y_val = val_df['won'].copy()

    # Handle missing values
    X_train = X_train.fillna(0)
    X_val = X_val.fillna(0)

    logger.info(f"Training samples: {len(X_train)}, Validation samples: {len(X_val)}")
    logger.info(f"Training positives: {y_train.sum()} ({100*y_train.mean():.2f}%)")

    # Get model parameters from config
    model_config = config.get('model', {}).get('lightgbm', {})
    params = {
        'objective': 'binary',
        'metric': 'binary_logloss',
        'boosting_type': 'gbdt',
        'num_leaves': model_config.get('num_leaves', 31),
        'learning_rate': model_config.get('learning_rate', 0.05),
        'feature_fraction': model_config.get('feature_fraction', 0.8),
        'bagging_fraction': model_config.get('bagging_fraction', 0.8),
        'bagging_freq': model_config.get('bagging_freq', 5),
        'verbose': -1,
        'n_estimators': model_config.get('n_estimators', 500),
        'early_stopping_rounds': model_config.get('early_stopping_rounds', 50),
    }

    # Create and train model
    model = RacingLightGBM(params)
    model.fit(X_train, y_train, eval_set=(X_val, y_val))

    logger.info(f"Model training complete. Best iteration: {model.model.best_iteration}")

    return {
        'model': model,
        'feature_columns': feature_cols,
        'training_complete': True
    }


def calibrate_model_actual(
    model: RacingLightGBM,
    val_df: pd.DataFrame,
    config: Dict[str, Any],
    logger: logging.Logger
) -> FieldSizeCalibrator:
    """
    Calibrate model probabilities using field-size stratified isotonic regression.

    Args:
        model: Trained RacingLightGBM model
        val_df: Validation data
        config: Configuration
        logger: Logger

    Returns:
        Fitted FieldSizeCalibrator
    """
    logger.info("="*70)
    logger.info("STEP 4/5: CALIBRATING PROBABILITIES")
    logger.info("="*70)

    feature_cols = get_feature_columns(val_df)
    X_val = val_df[feature_cols].fillna(0)
    y_val = val_df['won'].values

    # Get raw predictions
    y_pred_raw = model.predict_raw(X_val)
    logger.info(f"Raw predictions range: [{y_pred_raw.min():.4f}, {y_pred_raw.max():.4f}]")

    # Calculate field sizes from race_id
    field_sizes = val_df.groupby('race_id').size().reindex(val_df['race_id']).values

    # Fit calibrator
    calibrator = FieldSizeCalibrator()
    calibrator.fit(y_pred_raw, y_val, field_sizes)

    logger.info("Calibration complete")
    return calibrator


def evaluate_model_actual(
    model: RacingLightGBM,
    calibrator: FieldSizeCalibrator,
    test_df: pd.DataFrame,
    config: Dict[str, Any],
    logger: logging.Logger,
    save_dir: Optional[str] = None
) -> Dict[str, float]:
    """
    Evaluate model using ModelEvaluator.

    Args:
        model: Trained RacingLightGBM model
        calibrator: Fitted FieldSizeCalibrator
        test_df: Test data with features and 'won' target
        config: Configuration dictionary
        logger: Logger
        save_dir: Optional directory to save evaluation plots

    Returns:
        Dictionary of evaluation metrics
    """
    logger.info("="*70)
    logger.info("STEP 5/5: EVALUATING MODEL")
    logger.info("="*70)

    feature_cols = get_feature_columns(test_df)
    X_test = test_df[feature_cols].fillna(0)
    y_test = test_df['won']
    race_ids = test_df['race_id']

    # Calculate field sizes for each entry
    field_sizes = test_df.groupby('race_id').size().reindex(test_df['race_id']).values

    logger.info(f"Test samples: {len(X_test)}")
    logger.info(f"Test positives: {y_test.sum()} ({100*y_test.mean():.2f}%)")

    # Get raw predictions
    y_pred_raw = model.predict_raw(X_test)

    # Apply calibration
    y_pred_calibrated = calibrator.calibrate(y_pred_raw, field_sizes)

    logger.info(f"Calibrated predictions range: [{y_pred_calibrated.min():.4f}, {y_pred_calibrated.max():.4f}]")

    # Initialize evaluator
    evaluator = ModelEvaluator(n_calibration_bins=10)

    # Calculate metrics
    brier = evaluator.calculate_brier_score(y_pred_calibrated, y_test.values)
    log_loss_val = evaluator.calculate_log_loss(y_pred_calibrated, y_test.values)
    roc_auc = evaluator.calculate_roc_auc(y_pred_calibrated, y_test.values)
    cal_metrics = evaluator.calculate_calibration_error(y_pred_calibrated, y_test.values)

    metrics = {
        'brier_score': brier,
        'ece': cal_metrics['ece'],
        'mce': cal_metrics['mce'],
        'roc_auc': roc_auc,
        'log_loss': log_loss_val
    }

    # Log results
    logger.info("\nEVALUATION RESULTS:")
    logger.info(f"  Brier Score: {brier:.4f} (target < 0.20)")
    logger.info(f"  ECE:         {cal_metrics['ece']:.4f} (target < 0.03)")
    logger.info(f"  ROC-AUC:     {roc_auc:.4f}")
    logger.info(f"  Log Loss:    {log_loss_val:.4f}")

    # Generate and save plots if directory provided
    if save_dir:
        save_path = Path(save_dir)
        save_path.mkdir(parents=True, exist_ok=True)

        try:
            evaluator.generate_calibration_plot(
                y_pred_calibrated, y_test.values,
                save_path=str(save_path / 'calibration_plot.png')
            )
            logger.info(f"  Saved: calibration_plot.png")

            evaluator.generate_roc_curve(
                y_pred_calibrated, y_test.values,
                save_path=str(save_path / 'roc_curve.png')
            )
            logger.info(f"  Saved: roc_curve.png")

            # Feature importance
            importance = model.get_feature_importance(importance_type='gain')
            evaluator.generate_feature_importance_plot(
                importance, top_n=20,
                save_path=str(save_path / 'feature_importance.png')
            )
            logger.info(f"  Saved: feature_importance.png")
        except Exception as e:
            logger.warning(f"Error generating plots: {e}")

    return metrics


def save_model_artifacts(
    model: Any,
    calibrator: Any,
    metrics: Dict[str, float],
    feature_columns: List[str],
    config: Dict[str, Any],
    version: str,
    logger: logging.Logger
) -> None:
    """
    Save model artifacts to disk.

    Args:
        model: Trained model
        calibrator: Calibrator object
        metrics: Evaluation metrics
        feature_columns: List of feature names
        config: Configuration used
        version: Model version string
        logger: Logger
    """
    # Create artifacts directory
    artifacts_dir = Path('artifacts/models') / version
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"\nSaving artifacts to: {artifacts_dir}")

    # Save metadata
    metadata = {
        'version': version,
        'timestamp': datetime.now().isoformat(),
        'config': config,
        'feature_columns': feature_columns,
        'metrics': metrics,
        'model_type': 'RacingLightGBM',
        'calibration_method': config['model']['calibration']['method']
    }

    import json
    metadata_path = artifacts_dir / 'metadata.json'
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"  Saved: metadata.json")

    # Save model and calibrator objects
    if model is not None and hasattr(model, 'save'):
        model_path = artifacts_dir / 'model.pkl'
        model.save(str(model_path))
        logger.info(f"  Saved: model.pkl")

    if calibrator is not None:
        import pickle
        calibrator_path = artifacts_dir / 'calibrator.pkl'
        with open(calibrator_path, 'wb') as f:
            pickle.dump(calibrator, f, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(f"  Saved: calibrator.pkl")

    logger.info(f"  Model artifacts saved to: {artifacts_dir}")


def print_training_summary(
    metrics: Dict[str, float],
    datasets: Dict[str, pd.DataFrame],
    elapsed_time: float,
    logger: logging.Logger
) -> None:
    """
    Print final training summary.

    Args:
        metrics: Evaluation metrics
        datasets: Training datasets
        elapsed_time: Total elapsed time in seconds
        logger: Logger
    """
    print("\n" + "="*70)
    print("TRAINING COMPLETE")
    print("="*70)

    # Metrics
    print("\nMODEL PERFORMANCE:")
    print(f"  Brier Score:  {metrics.get('brier_score', 0):.4f}  (target: < 0.20)")
    print(f"  ECE:          {metrics.get('ece', 0):.4f}  (target: < 0.03)")
    print(f"  ROC AUC:      {metrics.get('roc_auc', 0):.4f}  (target: > 0.70)")
    print(f"  Log Loss:     {metrics.get('log_loss', 0):.4f}")

    # Data summary
    print("\nDATA SUMMARY:")
    for split_name, df in datasets.items():
        if df is not None:
            print(f"  {split_name.capitalize():12s}: {len(df):6,} rows")

    # Timing
    print(f"\nTOTAL TIME: {elapsed_time/60:.1f} minutes")
    print("="*70)

    # Recommendations
    if metrics.get('brier_score', 1.0) > 0.20:
        print("\n⚠ WARNING: Brier score is above target. Consider:")
        print("  - Collecting more training data")
        print("  - Adding more features")
        print("  - Tuning hyperparameters")

    if metrics.get('ece', 1.0) > 0.03:
        print("\n⚠ WARNING: ECE is above target. Consider:")
        print("  - Adjusting calibration method")
        print("  - Using more calibration bins")
        print("  - Checking for distribution drift")


def main():
    """Main training script entry point."""
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Train horse racing probability model',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python train_model.py                           # Full training
  python train_model.py --quick                   # Quick test run
  python train_model.py --version v1.1            # Custom version
  python train_model.py --config custom.yaml      # Custom config
  python train_model.py --dry-run                 # Validate without training
        """
    )
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Path to config file (default: config/config.yaml)'
    )
    parser.add_argument(
        '--version',
        default='v1.0',
        help='Model version string (default: v1.0)'
    )
    parser.add_argument(
        '--db',
        default='racing_data.db',
        help='Path to SQLite database (default: racing_data.db)'
    )
    parser.add_argument(
        '--quick',
        action='store_true',
        help='Quick mode: use subset of data for testing'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate setup without training'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    args = parser.parse_args()

    # Setup
    start_time = datetime.now()

    # Create logs directory
    Path('logs').mkdir(exist_ok=True)

    # Setup logging
    logger = setup_logging(args.log_level)

    logger.info("="*70)
    logger.info("HORSE RACING MODEL TRAINING PIPELINE")
    logger.info("="*70)
    logger.info(f"Version:    {args.version}")
    logger.info(f"Database:   {args.db}")
    logger.info(f"Config:     {args.config}")
    logger.info(f"Quick mode: {args.quick}")
    logger.info(f"Dry run:    {args.dry_run}")
    logger.info("="*70)

    try:
        # Load configuration
        logger.info("\nLoading configuration...")
        config = load_config(args.config)
        logger.info(f"  Model: {config['model']['algorithm']}")
        logger.info(f"  Calibration: {config['model']['calibration']['method']}")

        # Validate database
        logger.info("\nValidating database...")
        if not validate_database(args.db, logger):
            logger.error("Database validation failed. Exiting.")
            sys.exit(1)
        logger.info("  Database validation passed")

        # Get date ranges
        date_ranges = get_training_date_ranges(config, quick_mode=args.quick)
        logger.info("\nDate ranges:")
        for split_name, (start, end) in date_ranges.items():
            logger.info(f"  {split_name.capitalize():12s}: {start} to {end}")

        if args.dry_run:
            logger.info("\nDry run mode - validation complete. Exiting.")
            return

        # Step 1: Prepare training data
        datasets = prepare_training_data(
            db_path=args.db,
            date_ranges=date_ranges,
            logger=logger,
            quick_mode=args.quick
        )

        if datasets.get('train') is None or datasets['train'].empty:
            logger.error("No training data available. Exiting.")
            sys.exit(1)

        # Step 2: Split data (already done in prepare_training_data)
        logger.info("\n" + "="*70)
        logger.info("STEP 2/5: DATA SPLITS")
        logger.info("="*70)
        logger.info("Data already split by date ranges")

        train_df = datasets.get('train')
        val_df = datasets.get('validation', train_df)
        test_df = datasets.get('test', val_df)

        # Get feature columns
        feature_columns = get_feature_columns(train_df)
        logger.info(f"Feature columns: {len(feature_columns)}")

        # Steps 3-5: Model training, calibration, evaluation
        if MODELS_AVAILABLE:
            # Train model
            model_result = train_model_actual(train_df, val_df, config, logger)

            # Calibrate
            calibrator = calibrate_model_actual(
                model_result['model'], val_df, config, logger
            )

            # Evaluate with plots saved to artifacts directory
            artifacts_dir = Path('artifacts/models') / args.version
            metrics = evaluate_model_actual(
                model_result['model'], calibrator, test_df, config, logger,
                save_dir=str(artifacts_dir)
            )
        else:
            logger.warning("Models package not available - skipping training steps")
            model_result = {'model': None, 'feature_columns': feature_columns}
            calibrator = None
            metrics = {
                'brier_score': 0.0,
                'ece': 0.0,
                'roc_auc': 0.0,
                'log_loss': 0.0
            }

        # Save artifacts
        save_model_artifacts(
            model=model_result.get('model'),
            calibrator=calibrator,
            metrics=metrics,
            feature_columns=feature_columns,
            config=config,
            version=args.version,
            logger=logger
        )

        # Print summary
        elapsed_time = (datetime.now() - start_time).total_seconds()
        print_training_summary(metrics, datasets, elapsed_time, logger)

        logger.info("\n✓ Training pipeline completed successfully")

    except KeyboardInterrupt:
        logger.warning("\nTraining interrupted by user")
        sys.exit(1)

    except Exception as e:
        logger.error(f"\n✗ Training failed with error: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()
