"""
Model Training Pipeline - Main orchestration for training horse racing models.

This module provides the ModelTrainingPipeline class that orchestrates the complete
end-to-end training process including:
- Data preparation and feature engineering
- Target variable creation
- Train/validation/test splitting
- Model training with early stopping
- Model evaluation and metrics calculation
- Model persistence and versioning

Example:
    pipeline = ModelTrainingPipeline()
    model, metrics = pipeline.run_full_pipeline()
    print(f"Test ROC-AUC: {metrics['test_roc_auc']:.4f}")
"""

import logging
import os
import sqlite3
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd
import yaml

from features.feature_engine import FeatureEngine

logger = logging.getLogger(__name__)


# Feature columns to use for training (from Phase 3 spec)
FEATURE_COLUMNS = [
    # Horse form features
    'days_since_last', 'layoff_indicator', 'first_time_starter',
    'total_starts', 'total_wins', 'career_win_rate',
    'surface_win_rate', 'surface_preference', 'distance_preference',
    'best_speed_90_days', 'avg_speed_90_days', 'speed_trend',
    'last_class_level', 'class_change',

    # Trainer features
    'trainer_win_rate_14d', 'trainer_win_rate_30d', 'trainer_win_rate_60d',
    'trainer_hot_streak', 'trainer_sample_flag',

    # Jockey features
    'jockey_win_rate_14d', 'jockey_win_rate_30d', 'jockey_win_rate_60d',
    'jockey_hot_streak', 'jockey_sample_flag',

    # Combo features
    'combo_win_rate', 'combo_synergy_score',

    # Track and position features
    'post_position', 'post_position_win_rate', 'inside_bias_score',
    'rail_bias_adjustment', 'speed_bias_score', 'field_size',

    # Equipment features
    'blinkers_on', 'blinkers_first_time', 'lasix_on', 'equipment_change',

    # Field-relative features
    'speed_rank_in_field', 'class_rank_in_field',
    'field_quality_score', 'speed_vs_field_avg',

    # Race context features
    'morning_line_odds', 'age_at_race', 'class_level',
]

TARGET_COLUMN = 'is_winner'


class ModelTrainingPipeline:
    """
    Main orchestrator for model training pipeline.

    Handles data preparation, feature engineering, model training,
    evaluation, and persistence.

    Attributes:
        db_path: Path to SQLite database
        config_path: Path to YAML configuration file
        config: Loaded configuration dict
        feature_engine: FeatureEngine instance for feature calculation
        artifacts_dir: Directory for saving model artifacts
    """

    def __init__(
        self,
        db_path: str = 'racing_data.db',
        config_path: str = 'config/config.yaml'
    ):
        """
        Initialize the training pipeline.

        Args:
            db_path: Path to SQLite database with standardized tables
            config_path: Path to YAML configuration file
        """
        self.db_path = db_path
        self.config_path = config_path

        # Load configuration
        self.config = self._load_config()

        # Initialize feature engine
        self.feature_engine = FeatureEngine(
            db_path=db_path,
            rolling_windows=self.config.get('features', {}).get('rolling_windows', [14, 30, 60]),
            sample_thresholds=self.config.get('features', {}).get('sample_size_thresholds', {})
        )

        # Setup artifacts directory
        self.artifacts_dir = Path('artifacts')
        self.artifacts_dir.mkdir(exist_ok=True)

        logger.info(f"ModelTrainingPipeline initialized")
        logger.info(f"Database: {db_path}")
        logger.info(f"Config: {config_path}")

    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.

        Returns:
            Configuration dictionary
        """
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config from {self.config_path}: {e}")
            raise

    def prepare_training_data(
        self,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """
        Prepare training data by calculating features for a date range.

        This method:
        1. Queries all races in the date range
        2. Calculates features for each race entry
        3. Returns a DataFrame with features and identifiers

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            DataFrame with features for all entries in date range
        """
        logger.info(f"Preparing training data from {start_date} to {end_date}")

        # Calculate features using feature engine
        features_list = self.feature_engine.calculate_features_for_date_range(
            start_date=start_date,
            end_date=end_date,
            progress_callback=self._log_progress
        )

        if not features_list:
            raise ValueError(f"No features calculated for date range {start_date} to {end_date}")

        # Convert to DataFrame
        df = pd.DataFrame(features_list)

        logger.info(f"Prepared {len(df)} entries across {df['race_id'].nunique()} races")
        logger.info(f"Features calculated: {len([c for c in df.columns if c in FEATURE_COLUMNS])}")

        return df

    def _log_progress(self, race_id: str, current: int, total: int) -> None:
        """Log progress callback for feature calculation."""
        if current % 100 == 0 or current == total:
            logger.info(f"Progress: {current}/{total} races processed ({100*current/total:.1f}%)")

    def add_target_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add target column (is_winner) to features DataFrame.

        Queries race_entries_standardized to get official_finish_position
        and creates binary is_winner column (1 if position == 1, else 0).

        Args:
            df: DataFrame with features and entry_id column

        Returns:
            DataFrame with is_winner column added
        """
        logger.info("Adding target column (is_winner) to dataset")

        if 'entry_id' not in df.columns:
            raise ValueError("DataFrame must have 'entry_id' column")

        # Connect to database
        conn = sqlite3.connect(self.db_path)

        try:
            # Query finish positions for all entries
            entry_ids = df['entry_id'].tolist()
            placeholders = ','.join(['?'] * len(entry_ids))

            query = f"""
                SELECT
                    entry_id,
                    official_finish_position,
                    scratched
                FROM race_entries_standardized
                WHERE entry_id IN ({placeholders})
            """

            results_df = pd.read_sql_query(query, conn, params=entry_ids)

            # Create is_winner column (1 if finished first, 0 otherwise)
            # Exclude scratched horses from dataset
            results_df['is_winner'] = (results_df['official_finish_position'] == 1).astype(int)

            # Merge with features
            df = df.merge(
                results_df[['entry_id', 'is_winner', 'official_finish_position', 'scratched']],
                on='entry_id',
                how='left'
            )

            # Filter out scratched horses
            initial_count = len(df)
            df = df[df['scratched'] == 0].copy()
            scratched_count = initial_count - len(df)

            if scratched_count > 0:
                logger.info(f"Filtered out {scratched_count} scratched horses")

            # Filter out entries with missing finish position
            missing_finish = df['official_finish_position'].isna().sum()
            if missing_finish > 0:
                logger.warning(f"Found {missing_finish} entries with missing finish position - removing")
                df = df[df['official_finish_position'].notna()].copy()

            # Drop temporary columns
            df = df.drop(columns=['scratched', 'official_finish_position'])

            # Log class distribution
            winner_count = df['is_winner'].sum()
            winner_pct = 100 * winner_count / len(df)
            logger.info(f"Target distribution: {winner_count} winners ({winner_pct:.2f}%), "
                       f"{len(df) - winner_count} non-winners ({100 - winner_pct:.2f}%)")

            # Log winners per race stats
            winners_per_race = df.groupby('race_id')['is_winner'].sum()
            logger.info(f"Winners per race - Mean: {winners_per_race.mean():.2f}, "
                       f"Min: {winners_per_race.min()}, Max: {winners_per_race.max()}")

            return df

        finally:
            conn.close()

    def split_data(
        self,
        df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Split data into train, validation, and test sets using time-based splits.

        Uses date ranges from config:
        - Train: 2023-01-01 to 2023-06-30
        - Validation: 2023-07-01 to 2023-09-30
        - Test: 2023-10-01 to 2023-12-31

        Args:
            df: DataFrame with features and race_id

        Returns:
            Tuple of (train_df, val_df, test_df)
        """
        logger.info("Splitting data into train/validation/test sets")

        # Get split dates from config
        splits_config = self.config.get('model', {}).get('splits', {})

        train_start = date.fromisoformat(splits_config['train']['start'])
        train_end = date.fromisoformat(splits_config['train']['end'])

        val_start = date.fromisoformat(splits_config['validation']['start'])
        val_end = date.fromisoformat(splits_config['validation']['end'])

        test_start = date.fromisoformat(splits_config['test']['start'])
        test_end = date.fromisoformat(splits_config['test']['end'])

        # Extract race_date from race_id (format: TRACK-YYYY-MM-DD-RACE_NUM)
        df['race_date'] = pd.to_datetime(
            df['race_id'].str.extract(r'(\d{4}-\d{2}-\d{2})')[0]
        ).dt.date

        # Split by date ranges
        train_df = df[
            (df['race_date'] >= train_start) &
            (df['race_date'] <= train_end)
        ].copy()

        val_df = df[
            (df['race_date'] >= val_start) &
            (df['race_date'] <= val_end)
        ].copy()

        test_df = df[
            (df['race_date'] >= test_start) &
            (df['race_date'] <= test_end)
        ].copy()

        # Drop race_date column (not a feature)
        for split_df in [train_df, val_df, test_df]:
            if 'race_date' in split_df.columns:
                split_df.drop(columns=['race_date'], inplace=True)

        # Log split statistics
        logger.info(f"Train set: {len(train_df)} entries, "
                   f"{train_df['race_id'].nunique()} races, "
                   f"{train_df['is_winner'].sum()} winners")
        logger.info(f"Validation set: {len(val_df)} entries, "
                   f"{val_df['race_id'].nunique()} races, "
                   f"{val_df['is_winner'].sum()} winners")
        logger.info(f"Test set: {len(test_df)} entries, "
                   f"{test_df['race_id'].nunique()} races, "
                   f"{test_df['is_winner'].sum()} winners")

        # Verify no data leakage
        train_races = set(train_df['race_id'].unique())
        val_races = set(val_df['race_id'].unique())
        test_races = set(test_df['race_id'].unique())

        if train_races & val_races:
            raise ValueError("Data leakage: train and validation overlap")
        if train_races & test_races:
            raise ValueError("Data leakage: train and test overlap")
        if val_races & test_races:
            raise ValueError("Data leakage: validation and test overlap")

        logger.info("Data split verification passed - no leakage detected")

        return train_df, val_df, test_df

    def get_feature_columns(self) -> List[str]:
        """
        Get list of feature columns to use for training.

        Returns:
            List of feature column names
        """
        return FEATURE_COLUMNS.copy()

    def train_model(
        self,
        train_df: pd.DataFrame,
        val_df: pd.DataFrame
    ) -> 'RacingLightGBM':
        """
        Train a RacingLightGBM model with early stopping.

        Args:
            train_df: Training DataFrame with features and target
            val_df: Validation DataFrame with features and target

        Returns:
            Trained RacingLightGBM model
        """
        from .lightgbm_model import RacingLightGBM

        logger.info("Training RacingLightGBM model")

        # Get feature columns
        feature_cols = self.get_feature_columns()

        # Verify all feature columns exist
        missing_cols = [col for col in feature_cols if col not in train_df.columns]
        if missing_cols:
            logger.warning(f"Missing feature columns: {missing_cols}")
            feature_cols = [col for col in feature_cols if col in train_df.columns]
            logger.info(f"Using {len(feature_cols)} available features")

        # Prepare training data
        X_train = train_df[feature_cols].copy()
        y_train = train_df[TARGET_COLUMN].copy()

        # Prepare validation data
        X_val = val_df[feature_cols].copy()
        y_val = val_df[TARGET_COLUMN].copy()

        # Log feature info
        logger.info(f"Training with {len(feature_cols)} features")
        logger.info(f"Training samples: {len(X_train)}, Validation samples: {len(X_val)}")

        # Check for missing values
        train_missing = X_train.isna().sum().sum()
        val_missing = X_val.isna().sum().sum()

        if train_missing > 0:
            logger.warning(f"Training data has {train_missing} missing values")
            # Fill with appropriate defaults
            X_train = self._handle_missing_values(X_train)

        if val_missing > 0:
            logger.warning(f"Validation data has {val_missing} missing values")
            X_val = self._handle_missing_values(X_val)

        # Get hyperparameters from config
        hyperparams = self.config.get('model', {}).get('hyperparameters', {})
        training_config = self.config.get('model', {}).get('training', {})

        # Initialize model — RacingLightGBM accepts a single params dict
        params = {
            'n_estimators': hyperparams.get('n_estimators', 500),
            'max_depth': hyperparams.get('max_depth', 6),
            'learning_rate': hyperparams.get('learning_rate', 0.05),
            'bagging_fraction': hyperparams.get('subsample', 0.8),
            'feature_fraction': hyperparams.get('colsample_bytree', 0.8),
            'reg_alpha': hyperparams.get('reg_alpha', 0.1),
            'reg_lambda': hyperparams.get('reg_lambda', 0.1),
            'min_child_samples': hyperparams.get('min_child_samples', 20),
            'num_leaves': hyperparams.get('num_leaves', 31),
            'random_state': training_config.get('random_state', 42),
            'n_jobs': training_config.get('n_jobs', -1),
            'verbose': training_config.get('verbose', -1),
            'early_stopping_rounds': training_config.get('early_stopping_rounds', 50),
        }
        model = RacingLightGBM(params=params)

        # Train model with early stopping
        logger.info("Starting model training...")
        model.fit(
            X_train, y_train,
            eval_set=(X_val, y_val)
        )

        logger.info(f"Model training completed")
        logger.info(f"Best iteration: {model.best_iteration_}")

        return model

    def _handle_missing_values(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in feature matrix.

        Strategy:
        - Boolean features: fill with 0 (False)
        - Rate/probability features: fill with 0
        - Count features: fill with 0
        - Indicator features: fill with 0
        - Other numeric: fill with median

        Args:
            X: Feature DataFrame

        Returns:
            DataFrame with missing values filled
        """
        X = X.copy()

        for col in X.columns:
            if X[col].isna().any():
                # Boolean columns
                if col in ['layoff_indicator', 'first_time_starter', 'trainer_hot_streak',
                          'jockey_hot_streak', 'trainer_sample_flag', 'jockey_sample_flag',
                          'blinkers_on', 'blinkers_first_time', 'lasix_on', 'equipment_change']:
                    X[col] = X[col].fillna(0)

                # Rate/probability columns (fill with 0 for missing)
                elif 'win_rate' in col or 'preference' in col or 'synergy' in col:
                    X[col] = X[col].fillna(0)

                # Count/starts columns
                elif 'starts' in col or 'wins' in col:
                    X[col] = X[col].fillna(0)

                # Other numeric: fill with median
                else:
                    X[col] = X[col].fillna(X[col].median())

        return X

    def evaluate_model(
        self,
        model: 'RacingLightGBM',
        test_df: pd.DataFrame
    ) -> Dict[str, float]:
        """
        Evaluate model on test set and return comprehensive metrics.

        Metrics calculated:
        - ROC-AUC: Area under ROC curve
        - Log Loss: Logarithmic loss
        - Brier Score: Mean squared error of probabilities
        - Accuracy at various probability thresholds
        - Top-1 accuracy (model's top pick per race)
        - Top-3 accuracy (winner in model's top 3)

        Args:
            model: Trained RacingLightGBM model
            test_df: Test DataFrame with features and target

        Returns:
            Dictionary of evaluation metrics
        """
        from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss, accuracy_score

        logger.info("Evaluating model on test set")

        # Get feature columns
        feature_cols = self.get_feature_columns()
        feature_cols = [col for col in feature_cols if col in test_df.columns]

        # Prepare test data
        X_test = test_df[feature_cols].copy()
        y_test = test_df[TARGET_COLUMN].copy()
        race_ids_test = test_df['race_id'].values

        # Handle missing values
        if X_test.isna().sum().sum() > 0:
            X_test = self._handle_missing_values(X_test)

        # Get predictions
        y_pred_proba = model.predict_proba(X_test, race_ids_test)

        # Calculate metrics
        metrics = {}

        # ROC-AUC
        metrics['test_roc_auc'] = roc_auc_score(y_test, y_pred_proba)

        # Log Loss
        metrics['test_log_loss'] = log_loss(y_test, y_pred_proba)

        # Brier Score
        metrics['test_brier_score'] = brier_score_loss(y_test, y_pred_proba)

        # Top-1 accuracy per race (model's top pick)
        test_df_with_preds = test_df.copy()
        test_df_with_preds['pred_proba'] = y_pred_proba

        top_picks = test_df_with_preds.loc[
            test_df_with_preds.groupby('race_id')['pred_proba'].idxmax()
        ]
        metrics['test_top1_accuracy'] = top_picks['is_winner'].mean()

        # Top-3 accuracy per race
        test_df_with_preds['rank'] = test_df_with_preds.groupby('race_id')['pred_proba'].rank(
            ascending=False, method='first'
        )
        top3_races = test_df_with_preds[test_df_with_preds['rank'] <= 3]
        metrics['test_top3_accuracy'] = top3_races.groupby('race_id')['is_winner'].max().mean()

        # Accuracy at various thresholds
        for threshold in [0.1, 0.15, 0.2, 0.25, 0.3]:
            y_pred_binary = (y_pred_proba >= threshold).astype(int)
            metrics[f'test_accuracy_at_{threshold}'] = accuracy_score(y_test, y_pred_binary)

        # Expected Calibration Error (ECE)
        metrics['test_ece'] = self._calculate_ece(y_test.values, y_pred_proba, n_bins=10)

        # Log metrics
        logger.info("Test Set Metrics:")
        logger.info(f"  ROC-AUC: {metrics['test_roc_auc']:.4f}")
        logger.info(f"  Log Loss: {metrics['test_log_loss']:.4f}")
        logger.info(f"  Brier Score: {metrics['test_brier_score']:.4f}")
        logger.info(f"  Top-1 Accuracy: {metrics['test_top1_accuracy']:.4f}")
        logger.info(f"  Top-3 Accuracy: {metrics['test_top3_accuracy']:.4f}")
        logger.info(f"  ECE: {metrics['test_ece']:.4f}")

        return metrics

    def _calculate_ece(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        n_bins: int = 10
    ) -> float:
        """
        Calculate Expected Calibration Error (ECE).

        Args:
            y_true: True binary labels
            y_pred: Predicted probabilities
            n_bins: Number of bins for calibration

        Returns:
            ECE value
        """
        bin_boundaries = np.linspace(0, 1, n_bins + 1)
        bin_lowers = bin_boundaries[:-1]
        bin_uppers = bin_boundaries[1:]

        ece = 0.0
        for bin_lower, bin_upper in zip(bin_lowers, bin_uppers):
            in_bin = (y_pred >= bin_lower) & (y_pred < bin_upper)
            prop_in_bin = in_bin.mean()

            if prop_in_bin > 0:
                accuracy_in_bin = y_true[in_bin].mean()
                avg_confidence_in_bin = y_pred[in_bin].mean()
                ece += np.abs(avg_confidence_in_bin - accuracy_in_bin) * prop_in_bin

        return ece

    def save_model(
        self,
        model: 'RacingLightGBM',
        metrics: Dict[str, float],
        version: str
    ) -> str:
        """
        Save trained model and metadata to artifacts directory.

        Saves:
        - Model pickle file
        - Metrics JSON file
        - Feature list
        - Config snapshot

        Args:
            model: Trained model
            metrics: Evaluation metrics
            version: Model version string (e.g., 'v1.0')

        Returns:
            Path to saved model directory
        """
        import json
        import pickle
        from datetime import datetime

        logger.info(f"Saving model version {version}")

        # Create versioned directory
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        model_dir = self.artifacts_dir / f"{version}_{timestamp}"
        model_dir.mkdir(exist_ok=True)

        # Save model
        model_path = model_dir / 'model.pkl'
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        logger.info(f"Model saved to {model_path}")

        # Save metrics
        metrics_path = model_dir / 'metrics.json'
        with open(metrics_path, 'w') as f:
            json.dump(metrics, f, indent=2)
        logger.info(f"Metrics saved to {metrics_path}")

        # Save feature list
        features_path = model_dir / 'features.json'
        with open(features_path, 'w') as f:
            json.dump({'features': self.get_feature_columns()}, f, indent=2)
        logger.info(f"Feature list saved to {features_path}")

        # Save config snapshot
        config_path = model_dir / 'config.yaml'
        with open(config_path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)
        logger.info(f"Config saved to {config_path}")

        # Save metadata
        metadata = {
            'version': version,
            'timestamp': timestamp,
            'feature_count': len(self.get_feature_columns()),
            'model_type': 'RacingLightGBM',
            'database': self.db_path,
            'config': self.config_path,
        }
        metadata_path = model_dir / 'metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Metadata saved to {metadata_path}")

        logger.info(f"Model artifacts saved to {model_dir}")

        return str(model_dir)

    def run_full_pipeline(self, version: str = 'v1.0') -> Tuple['RacingLightGBM', Dict[str, float]]:
        """
        Run the complete training pipeline end-to-end.

        Steps:
        1. Prepare training data (calculate features for all splits)
        2. Add target column
        3. Split into train/val/test
        4. Train model with early stopping
        5. Evaluate on test set
        6. Save model and artifacts

        Args:
            version: Model version string

        Returns:
            Tuple of (trained_model, test_metrics)
        """
        logger.info("=" * 80)
        logger.info("STARTING FULL TRAINING PIPELINE")
        logger.info("=" * 80)

        try:
            # Step 1: Prepare data for all splits
            logger.info("\n[1/6] Preparing training data...")

            splits_config = self.config.get('model', {}).get('splits', {})

            # Get date range covering all splits
            train_start = date.fromisoformat(splits_config['train']['start'])
            test_end = date.fromisoformat(splits_config['test']['end'])

            df = self.prepare_training_data(train_start, test_end)

            # Step 2: Add target column
            logger.info("\n[2/6] Adding target column...")
            df = self.add_target_column(df)

            # Step 3: Split data
            logger.info("\n[3/6] Splitting data...")
            train_df, val_df, test_df = self.split_data(df)

            # Step 4: Train model
            logger.info("\n[4/6] Training model...")
            model = self.train_model(train_df, val_df)

            # Step 5: Evaluate model
            logger.info("\n[5/6] Evaluating model...")
            metrics = self.evaluate_model(model, test_df)

            # Step 6: Save model
            logger.info("\n[6/6] Saving model...")
            model_path = self.save_model(model, metrics, version)

            logger.info("\n" + "=" * 80)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(f"Model saved to: {model_path}")
            logger.info(f"Test ROC-AUC: {metrics['test_roc_auc']:.4f}")
            logger.info(f"Test Top-1 Accuracy: {metrics['test_top1_accuracy']:.4f}")
            logger.info("=" * 80)

            return model, metrics

        except Exception as e:
            logger.error(f"Pipeline failed with error: {e}", exc_info=True)
            raise
        finally:
            # Cleanup
            self.feature_engine.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.feature_engine.close()


# Convenience function for quick pipeline execution
def train_model(
    db_path: str = 'racing_data.db',
    config_path: str = 'config/config.yaml',
    version: str = 'v1.0'
) -> Tuple['RacingLightGBM', Dict[str, float]]:
    """
    Convenience function to run full training pipeline.

    Args:
        db_path: Path to SQLite database
        config_path: Path to YAML config
        version: Model version string

    Returns:
        Tuple of (trained_model, test_metrics)
    """
    with ModelTrainingPipeline(db_path, config_path) as pipeline:
        return pipeline.run_full_pipeline(version)


if __name__ == '__main__':
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Run pipeline
    model, metrics = train_model()

    print("\n" + "=" * 80)
    print("TRAINING COMPLETE")
    print("=" * 80)
    print(f"Test ROC-AUC: {metrics['test_roc_auc']:.4f}")
    print(f"Test Log Loss: {metrics['test_log_loss']:.4f}")
    print(f"Test Top-1 Accuracy: {metrics['test_top1_accuracy']:.4f}")
    print(f"Test Top-3 Accuracy: {metrics['test_top3_accuracy']:.4f}")
    print("=" * 80)
