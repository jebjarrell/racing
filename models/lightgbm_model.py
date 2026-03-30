"""
LightGBM Model Wrapper with Race-Grouped Softmax Normalization

This module provides a wrapper around LightGBM for horse racing predictions,
with specialized softmax normalization that ensures probabilities sum to 1.0
within each race.
"""

import logging
from typing import Dict, Optional, Tuple
import numpy as np
import pandas as pd
import lightgbm as lgb
import pickle
from pathlib import Path

logger = logging.getLogger(__name__)


# Default hyperparameters optimized for racing predictions
DEFAULT_PARAMS = {
    'objective': 'binary',
    'metric': ['binary_logloss', 'auc'],
    'boosting_type': 'gbdt',
    'num_leaves': 31,
    'learning_rate': 0.05,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': -1,
    'n_estimators': 500,
    'early_stopping_rounds': 50,
    'reg_alpha': 0.1,
    'reg_lambda': 0.1,
    'min_child_samples': 20,
}


def softmax_by_race(raw_probs: np.ndarray, race_ids: np.ndarray) -> np.ndarray:
    """
    Apply softmax normalization grouped by race.

    This ensures that probabilities sum to exactly 1.0 within each race,
    which is critical for racing predictions where exactly one horse wins.

    Args:
        raw_probs: Raw probability predictions from the model
        race_ids: Race identifiers for grouping

    Returns:
        Softmax-normalized probabilities grouped by race

    Notes:
        - Uses numerical stability technique: exp(x - max(x))
        - Handles edge cases where races have single entries
        - Preserves array shape and ordering
    """
    if len(raw_probs) != len(race_ids):
        raise ValueError(
            f"Length mismatch: raw_probs ({len(raw_probs)}) != race_ids ({len(race_ids)})"
        )

    result = np.zeros_like(raw_probs, dtype=np.float64)
    unique_races = np.unique(race_ids)

    logger.debug(f"Applying softmax normalization across {len(unique_races)} races")

    for race_id in unique_races:
        mask = race_ids == race_id
        race_probs = raw_probs[mask]

        # Numerical stability: subtract max before exp
        max_prob = np.max(race_probs)
        exp_probs = np.exp(race_probs - max_prob)

        # Normalize to sum to 1.0
        sum_exp = exp_probs.sum()
        if sum_exp > 0:
            result[mask] = exp_probs / sum_exp
        else:
            # Fallback for edge case: uniform distribution
            result[mask] = 1.0 / len(race_probs)
            logger.warning(f"Race {race_id}: All probabilities were equal, using uniform distribution")

    return result


class RacingLightGBM:
    """
    LightGBM wrapper for horse racing predictions with race-grouped softmax.

    This class provides a specialized interface for racing predictions that:
    - Wraps LightGBM's gradient boosting classifier
    - Applies race-grouped softmax normalization for proper probability distributions
    - Provides feature importance analysis
    - Supports model persistence

    Attributes:
        params: Model hyperparameters
        model: Trained LightGBM booster (None until fit is called)
        feature_names: List of feature names from training data
    """

    def __init__(self, params: Optional[Dict] = None):
        """
        Initialize the RacingLightGBM model.

        Args:
            params: Model hyperparameters. If None, uses DEFAULT_PARAMS.
                   Provided params will be merged with defaults.
        """
        self.params = DEFAULT_PARAMS.copy()
        if params:
            self.params.update(params)
            logger.info(f"Initialized RacingLightGBM with custom parameters: {params}")
        else:
            logger.info("Initialized RacingLightGBM with default parameters")

        self.model: Optional[lgb.Booster] = None
        self.feature_names: Optional[list] = None

    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        eval_set: Optional[Tuple[pd.DataFrame, pd.Series]] = None
    ) -> 'RacingLightGBM':
        """
        Fit the LightGBM model on training data.

        Args:
            X: Training features (DataFrame)
            y: Training labels (Series)
            eval_set: Optional validation set as (X_val, y_val) tuple

        Returns:
            Self for method chaining

        Raises:
            ValueError: If X and y have mismatched lengths
        """
        if len(X) != len(y):
            raise ValueError(f"X and y length mismatch: {len(X)} != {len(y)}")

        logger.info(f"Fitting LightGBM model on {len(X)} samples with {X.shape[1]} features")

        # Store feature names for later use
        self.feature_names = list(X.columns)

        # Create LightGBM dataset
        train_data = lgb.Dataset(X, label=y, feature_name=self.feature_names)

        # Prepare validation data if provided
        valid_sets = [train_data]
        valid_names = ['train']

        if eval_set is not None:
            X_val, y_val = eval_set
            if len(X_val) != len(y_val):
                raise ValueError(f"Validation X and y length mismatch: {len(X_val)} != {len(y_val)}")

            val_data = lgb.Dataset(X_val, label=y_val, feature_name=self.feature_names, reference=train_data)
            valid_sets.append(val_data)
            valid_names.append('valid')
            logger.info(f"Using validation set with {len(X_val)} samples")

        # Extract training parameters (use .get to avoid mutating self.params)
        n_estimators = self.params.get('n_estimators', 500)
        early_stopping_rounds = self.params.get('early_stopping_rounds', 50)

        # Train the model
        logger.info("Starting model training...")
        callbacks = []
        if eval_set is not None and early_stopping_rounds:
            callbacks.append(lgb.early_stopping(stopping_rounds=early_stopping_rounds))

        # Filter out sklearn-style keys that lgb.train doesn't recognize
        _non_lgb_keys = {'n_estimators', 'early_stopping_rounds', 'random_state', 'n_jobs'}
        lgb_params = {k: v for k, v in self.params.items() if k not in _non_lgb_keys}

        self.model = lgb.train(
            lgb_params,
            train_data,
            num_boost_round=n_estimators,
            valid_sets=valid_sets,
            valid_names=valid_names,
            callbacks=callbacks
        )

        logger.info(f"Model training completed. Best iteration: {self.model.best_iteration}")

        return self

    def predict_raw(self, X: pd.DataFrame) -> np.ndarray:
        """
        Get raw probability predictions from the model.

        These are uncalibrated probabilities that do NOT sum to 1.0 within races.
        Use predict_proba() for race-normalized probabilities.

        Args:
            X: Features for prediction (DataFrame)

        Returns:
            Raw probability predictions

        Raises:
            ValueError: If model is not fitted or feature mismatch
        """
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() before predict_raw()")

        if set(X.columns) != set(self.feature_names):
            missing = set(self.feature_names) - set(X.columns)
            extra = set(X.columns) - set(self.feature_names)
            raise ValueError(
                f"Feature mismatch. Expected {len(self.feature_names)} features, got {len(X.columns)}. "
                f"Missing: {missing or 'none'}. Extra: {extra or 'none'}"
            )
        X = X[self.feature_names]

        logger.debug(f"Generating raw predictions for {len(X)} samples")
        num_iter = self.model.best_iteration if self.model.best_iteration > 0 else self.model.num_trees()
        raw_probs = self.model.predict(X, num_iteration=num_iter)

        return raw_probs

    def predict_proba(self, X: pd.DataFrame, race_ids: pd.Series) -> np.ndarray:
        """
        Get race-grouped softmax-normalized probability predictions.

        This applies softmax normalization within each race, ensuring that
        probabilities sum to exactly 1.0 for each race. This is the recommended
        prediction method for racing applications.

        Args:
            X: Features for prediction (DataFrame)
            race_ids: Race identifiers for grouping (Series)

        Returns:
            Softmax-normalized probabilities grouped by race

        Raises:
            ValueError: If X and race_ids have mismatched lengths
        """
        if len(X) != len(race_ids):
            raise ValueError(
                f"X and race_ids length mismatch: {len(X)} != {len(race_ids)}"
            )

        logger.debug(f"Generating softmax-normalized predictions for {len(X)} samples")

        # Get raw predictions
        raw_probs = self.predict_raw(X)

        # Apply race-grouped softmax
        normalized_probs = softmax_by_race(raw_probs, race_ids.values)

        return normalized_probs

    def get_feature_importance(self, importance_type: str = 'gain') -> Dict[str, float]:
        """
        Get feature importance scores.

        Args:
            importance_type: Type of importance to compute:
                - 'gain': Total gain of splits using the feature
                - 'split': Number of times the feature is used

        Returns:
            Dictionary mapping feature names to importance scores

        Raises:
            ValueError: If model is not fitted or invalid importance_type
        """
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() before get_feature_importance()")

        valid_types = ['gain', 'split']
        if importance_type not in valid_types:
            raise ValueError(f"Invalid importance_type '{importance_type}'. Must be one of {valid_types}")

        logger.debug(f"Computing feature importance using '{importance_type}' method")

        # Get importance scores
        importance = self.model.feature_importance(importance_type=importance_type)

        # Create dictionary mapping feature names to scores
        feature_importance = dict(zip(self.feature_names, importance))

        # Sort by importance (descending)
        feature_importance = dict(
            sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
        )

        return feature_importance

    def save(self, path: str) -> None:
        """
        Save the model to disk.

        Saves both the LightGBM booster and model metadata (params, feature names).

        Args:
            path: File path to save the model (should end in .pkl)

        Raises:
            ValueError: If model is not fitted
        """
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() before save()")

        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Saving model to {path}")

        # Create save dictionary
        save_dict = {
            'model': self.model,
            'params': self.params,
            'feature_names': self.feature_names,
        }

        # Save using pickle
        with open(path, 'wb') as f:
            pickle.dump(save_dict, f, protocol=pickle.HIGHEST_PROTOCOL)

        logger.info(f"Model saved successfully to {path}")

    @classmethod
    def load(cls, path: str) -> 'RacingLightGBM':
        """
        Load a saved model from disk.

        Args:
            path: File path to load the model from

        Returns:
            Loaded RacingLightGBM instance

        Raises:
            FileNotFoundError: If model file doesn't exist
        """
        path_obj = Path(path)
        if not path_obj.exists():
            raise FileNotFoundError(f"Model file not found: {path}")

        logger.info(f"Loading model from {path}")

        # Load from pickle
        with open(path, 'rb') as f:
            save_dict = pickle.load(f)

        # Create new instance
        instance = cls(params=save_dict['params'])
        instance.model = save_dict['model']
        instance.feature_names = save_dict['feature_names']

        logger.info(f"Model loaded successfully from {path}")

        return instance

    def __repr__(self) -> str:
        """String representation of the model."""
        fitted = "fitted" if self.model is not None else "not fitted"
        n_features = len(self.feature_names) if self.feature_names else 0
        return f"RacingLightGBM({fitted}, {n_features} features)"
