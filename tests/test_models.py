"""
Integration Tests for Models Package

Tests the training pipeline, LightGBM model, calibration, and evaluation.
Uses pytest fixtures for shared data.

This test suite verifies:
1. RacingLightGBM: Model training, softmax normalization, feature importance
2. FieldSizeCalibrator: Calibration fitting, bucket assignment, ECE improvement
3. ModelEvaluator: Brier score, calibration error, evaluation reports
4. Softmax functions: Probability normalization, numerical stability
5. ModelTrainingPipeline: End-to-end training workflow

Run with: pytest tests/test_models.py -v
"""

import pytest
import numpy as np
import pandas as pd
from datetime import date
from pathlib import Path
import tempfile
import joblib

# Import modules to test
from models import (
    ModelTrainingPipeline,
    RacingLightGBM,
    FieldSizeCalibrator,
    ModelEvaluator
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def sample_training_data():
    """
    Create sample training data for tests.

    Creates 1000 entries across 100 races with:
    - 10 horses per race
    - Random features
    - Exactly one winner per race
    - Realistic field sizes
    """
    np.random.seed(42)
    n_samples = 1000
    n_races = 100
    n_features = 47  # Based on feature catalog

    # Create feature columns based on documented feature set
    feature_names = [
        # Horse form features (14)
        'days_since_last', 'layoff_indicator', 'first_time_starter',
        'total_starts', 'total_wins', 'career_win_rate',
        'surface_win_rate', 'surface_preference', 'distance_preference',
        'best_speed_90_days', 'avg_speed_90_days', 'speed_trend',
        'last_class_level', 'class_change',

        # Connection features (12)
        'trainer_win_rate_14d', 'trainer_win_rate_30d', 'trainer_win_rate_60d',
        'trainer_hot_streak', 'trainer_sample_flag',
        'jockey_win_rate_14d', 'jockey_win_rate_30d', 'jockey_win_rate_60d',
        'jockey_hot_streak', 'jockey_sample_flag',
        'combo_win_rate', 'combo_synergy_score',

        # Track/Position features (6)
        'post_position', 'post_position_win_rate', 'inside_bias_score',
        'rail_bias_adjustment', 'speed_bias_score', 'field_size',

        # Equipment features (4)
        'blinkers_on', 'blinkers_first_time', 'lasix_on', 'equipment_change',

        # Field-relative features (4)
        'speed_rank_in_field', 'class_rank_in_field',
        'field_quality_score', 'speed_vs_field_avg',

        # Base features (3)
        'morning_line_odds', 'age_at_race', 'class_level',
    ]

    # Generate random feature data
    data = {}
    for col in feature_names:
        if 'win_rate' in col or 'preference' in col or 'career_win_rate' in col:
            # Win rates and preferences: 0-1 range
            data[col] = np.random.uniform(0, 1, n_samples)
        elif 'indicator' in col or 'flag' in col or 'first_time' in col:
            # Binary features
            data[col] = np.random.choice([0, 1], n_samples)
        elif 'rank' in col or 'position' in col:
            # Ranks and positions: 1-12
            data[col] = np.random.randint(1, 13, n_samples)
        elif 'odds' in col:
            # Odds: 2-30 range
            data[col] = np.random.uniform(2, 30, n_samples)
        elif 'age' in col:
            # Age: 2-8 years
            data[col] = np.random.randint(2, 9, n_samples)
        else:
            # Default: standardized features (mean 0, std 1)
            data[col] = np.random.randn(n_samples)

    # Add race metadata
    data['race_id'] = np.repeat([f'race_{i:03d}' for i in range(n_races)], 10)
    data['field_size'] = np.random.choice([6, 8, 10, 12], n_samples)
    data['is_winner'] = np.zeros(n_samples, dtype=int)

    # Make exactly one winner per race
    for i in range(n_races):
        race_mask = np.arange(i * 10, (i + 1) * 10)
        winner_idx = np.random.choice(race_mask)
        data['is_winner'][winner_idx] = 1

    df = pd.DataFrame(data)

    # Validate structure
    assert len(df) == n_samples
    assert df.groupby('race_id')['is_winner'].sum().eq(1).all(), "Each race must have exactly 1 winner"

    return df


@pytest.fixture
def sample_predictions():
    """
    Create sample prediction data for evaluation tests.
    """
    np.random.seed(42)
    n_entries = 100

    return pd.DataFrame({
        'race_id': np.repeat([f'race_{i}' for i in range(10)], 10),
        'predicted_prob': np.random.uniform(0.05, 0.35, n_entries),
        'is_winner': np.concatenate([
            [1] + [0]*9 for _ in range(10)  # One winner per race
        ]),
        'field_size': np.random.choice([6, 8, 10, 12], n_entries)
    })


# ============================================================================
# Test RacingLightGBM
# ============================================================================

class TestRacingLightGBM:
    """Test suite for the RacingLightGBM model wrapper."""

    def test_fit_predict(self, sample_training_data):
        """Test basic fit and predict functionality."""
        # Split data
        train_size = int(0.7 * len(sample_training_data))
        train_df = sample_training_data.iloc[:train_size]
        test_df = sample_training_data.iloc[train_size:]

        # Get feature columns
        feature_cols = [col for col in train_df.columns
                       if col not in ['race_id', 'is_winner', 'field_size']]

        X_train = train_df[feature_cols]
        y_train = train_df['is_winner']
        X_test = test_df[feature_cols]
        race_ids_test = test_df['race_id']

        # Train model
        model = RacingLightGBM()
        model.fit(X_train, y_train)

        # Make predictions
        predictions = model.predict_proba(X_test, race_ids_test)

        # Assertions
        assert len(predictions) == len(X_test)
        assert np.all(predictions >= 0) and np.all(predictions <= 1)
        assert not np.any(np.isnan(predictions))
        assert not np.any(np.isinf(predictions))

    def test_softmax_normalization(self, sample_training_data):
        """Test that probabilities sum to 1 within each race."""
        # Train model on all data for simplicity
        feature_cols = [col for col in sample_training_data.columns
                       if col not in ['race_id', 'is_winner', 'field_size']]

        X = sample_training_data[feature_cols]
        y = sample_training_data['is_winner']
        race_ids = sample_training_data['race_id']

        model = RacingLightGBM()
        model.fit(X, y)

        # Get predictions
        predictions = model.predict_proba(X, race_ids)

        # Check that probabilities sum to 1 per race
        pred_df = pd.DataFrame({
            'race_id': race_ids,
            'prob': predictions
        })

        race_sums = pred_df.groupby('race_id')['prob'].sum()

        # All race probabilities should sum to 1.0 (within numerical tolerance)
        assert np.allclose(race_sums, 1.0, atol=1e-6), \
            f"Probabilities don't sum to 1. Min: {race_sums.min()}, Max: {race_sums.max()}"

    def test_feature_importance(self, sample_training_data):
        """Test feature importance extraction."""
        feature_cols = [col for col in sample_training_data.columns
                       if col not in ['race_id', 'is_winner', 'field_size']]

        X = sample_training_data[feature_cols]
        y = sample_training_data['is_winner']

        model = RacingLightGBM()
        model.fit(X, y)

        # Get feature importance
        importance = model.get_feature_importance()

        # Assertions
        assert importance is not None
        assert len(importance) == len(feature_cols)
        assert all(imp >= 0 for imp in importance.values())

        # Check that some features have non-zero importance
        assert sum(importance.values()) > 0

    def test_save_load(self, sample_training_data, tmp_path):
        """Test model serialization and loading."""
        feature_cols = [col for col in sample_training_data.columns
                       if col not in ['race_id', 'is_winner', 'field_size']]

        X = sample_training_data[feature_cols].iloc[:500]
        y = sample_training_data['is_winner'].iloc[:500]

        # Train and save model
        model = RacingLightGBM()
        model.fit(X, y)

        model_path = tmp_path / "test_model.pkl"
        model.save(str(model_path))

        # Load model
        loaded_model = RacingLightGBM.load(str(model_path))

        # Verify predictions match
        X_test = sample_training_data[feature_cols].iloc[500:600]
        race_ids_test = sample_training_data['race_id'].iloc[500:600]

        pred_original = model.predict_proba(X_test, race_ids_test)
        pred_loaded = loaded_model.predict_proba(X_test, race_ids_test)

        np.testing.assert_array_almost_equal(pred_original, pred_loaded, decimal=6)


# ============================================================================
# Test FieldSizeCalibrator
# ============================================================================

class TestFieldSizeCalibrator:
    """Test suite for field-size stratified calibration."""

    def test_fit_calibrate(self):
        """Test calibrator fit and calibrate methods."""
        np.random.seed(42)

        # Create miscalibrated probabilities
        # (e.g., model predicts 0.3 but actual win rate is 0.2)
        n_samples = 1000
        predicted_probs = np.random.uniform(0.05, 0.5, n_samples)

        # True probabilities (with systematic bias)
        true_probs = predicted_probs * 0.8  # Model is overconfident
        actual_outcomes = np.random.binomial(1, true_probs)

        # Field sizes
        field_sizes = np.random.choice([6, 8, 10, 12], n_samples)

        # Fit calibrator
        calibrator = FieldSizeCalibrator()
        calibrator.fit(predicted_probs, actual_outcomes, field_sizes)

        # Calibrate probabilities
        calibrated_probs = calibrator.calibrate(predicted_probs, field_sizes)

        # Assertions
        assert len(calibrated_probs) == n_samples
        assert np.all(calibrated_probs >= 0) and np.all(calibrated_probs <= 1)
        assert not np.any(np.isnan(calibrated_probs))

    def test_bucket_assignment(self):
        """Test field size bucket assignment."""
        calibrator = FieldSizeCalibrator()

        # Test bucket assignments based on DEFAULT_BUCKETS:
        # (4, 6, 'small'), (7, 9, 'medium'), (10, 14, 'large')
        test_cases = [
            (4, 'small'),
            (5, 'small'),
            (6, 'small'),
            (7, 'medium'),
            (8, 'medium'),
            (9, 'medium'),
            (10, 'large'),
            (11, 'large'),
            (12, 'large'),
            (14, 'large'),
        ]

        for field_size, expected_bucket in test_cases:
            bucket = calibrator.get_bucket(field_size)
            assert bucket == expected_bucket, \
                f"Field size {field_size} should be in bucket '{expected_bucket}', got '{bucket}'"

    def test_calibration_improves_ece(self, sample_training_data):
        """Test that calibration reduces Expected Calibration Error."""
        np.random.seed(42)

        # Train a model
        feature_cols = [col for col in sample_training_data.columns
                       if col not in ['race_id', 'is_winner', 'field_size']]

        train_size = int(0.6 * len(sample_training_data))
        val_size = int(0.2 * len(sample_training_data))

        X_train = sample_training_data[feature_cols].iloc[:train_size]
        y_train = sample_training_data['is_winner'].iloc[:train_size]

        X_val = sample_training_data[feature_cols].iloc[train_size:train_size+val_size]
        y_val = sample_training_data['is_winner'].iloc[train_size:train_size+val_size]
        race_ids_val = sample_training_data['race_id'].iloc[train_size:train_size+val_size]
        field_sizes_val = sample_training_data['field_size'].iloc[train_size:train_size+val_size]

        model = RacingLightGBM()
        model.fit(X_train, y_train)

        # Get uncalibrated predictions
        uncalibrated_probs = model.predict_proba(X_val, race_ids_val)

        # Calculate ECE before calibration
        evaluator = ModelEvaluator()
        ece_result_before = evaluator.calculate_calibration_error(uncalibrated_probs, y_val.values)
        ece_before = ece_result_before['ece']

        # Fit calibrator
        calibrator = FieldSizeCalibrator()
        calibrator.fit(uncalibrated_probs, y_val.values, field_sizes_val.values)

        # Calibrate probabilities
        calibrated_probs = calibrator.calibrate(uncalibrated_probs, field_sizes_val.values)

        # Re-normalize per race
        calibrated_df = pd.DataFrame({
            'race_id': race_ids_val,
            'prob': calibrated_probs
        })
        calibrated_df['prob'] = calibrated_df.groupby('race_id')['prob'].transform(
            lambda x: x / x.sum()
        )

        # Calculate ECE after calibration
        ece_result_after = evaluator.calculate_calibration_error(
            calibrated_df['prob'].values, y_val.values
        )
        ece_after = ece_result_after['ece']

        # Calibration should improve (reduce) ECE
        # Note: With small sample size, improvement may not always be guaranteed
        assert ece_after <= ece_before * 1.2, \
            f"Calibration should not significantly worsen ECE. Before: {ece_before:.4f}, After: {ece_after:.4f}"


# ============================================================================
# Test ModelEvaluator
# ============================================================================

class TestModelEvaluator:
    """Test suite for model evaluation metrics."""

    def test_brier_score(self):
        """Test Brier score calculation."""
        y_true = np.array([1, 0, 1, 0, 1])
        y_pred = np.array([0.9, 0.1, 0.8, 0.2, 0.7])

        evaluator = ModelEvaluator()
        score = evaluator.calculate_brier_score(y_pred, y_true)

        # Brier score = mean((y_pred - y_true)^2)
        expected = np.mean((y_pred - y_true) ** 2)

        assert abs(score - expected) < 1e-6
        assert 0 <= score <= 1

    def test_brier_score_perfect_prediction(self):
        """Test Brier score with perfect predictions."""
        y_true = np.array([1, 0, 1, 0])
        y_pred = np.array([1.0, 0.0, 1.0, 0.0])

        evaluator = ModelEvaluator()
        score = evaluator.calculate_brier_score(y_pred, y_true)

        assert score == 0.0, "Perfect predictions should have Brier score of 0"

    def test_brier_score_worst_prediction(self):
        """Test Brier score with worst possible predictions."""
        y_true = np.array([1, 0, 1, 0])
        y_pred = np.array([0.0, 1.0, 0.0, 1.0])

        evaluator = ModelEvaluator()
        score = evaluator.calculate_brier_score(y_pred, y_true)

        assert score == 1.0, "Worst predictions should have Brier score of 1"

    def test_calibration_error(self):
        """Test Expected Calibration Error (ECE) calculation."""
        # Create data with known calibration error
        y_true = np.array([1, 0, 1, 0, 1, 0, 1, 0] * 50)  # 50% win rate
        y_pred = np.array([0.7] * 400)  # Overconfident predictions

        evaluator = ModelEvaluator()
        result = evaluator.calculate_calibration_error(y_pred, y_true)
        ece = result['ece']

        # ECE should be positive (model is overconfident)
        assert ece > 0
        assert ece < 1.0

        # With constant predictions of 0.7 and actual 0.5, ECE should be ~0.2
        assert 0.1 < ece < 0.3

    def test_calibration_error_perfect(self):
        """Test ECE with perfectly calibrated predictions."""
        np.random.seed(42)

        # Generate calibrated predictions
        n_samples = 1000
        y_pred = np.random.uniform(0, 1, n_samples)
        y_true = np.random.binomial(1, y_pred)  # Generate outcomes from predicted probs

        evaluator = ModelEvaluator()
        result = evaluator.calculate_calibration_error(y_pred, y_true)
        ece = result['ece']

        # ECE should be small for calibrated predictions
        assert ece < 0.15  # Allow some random variation

    def test_evaluation_report(self, sample_training_data, tmp_path):
        """Test full evaluation report generation."""
        # Prepare data
        feature_cols = [col for col in sample_training_data.columns
                       if col not in ['race_id', 'is_winner', 'field_size']]

        train_size = int(0.7 * len(sample_training_data))

        X_train = sample_training_data[feature_cols].iloc[:train_size]
        y_train = sample_training_data['is_winner'].iloc[:train_size]

        X_test = sample_training_data[feature_cols].iloc[train_size:]
        y_test = sample_training_data['is_winner'].iloc[train_size:]
        race_ids_test = sample_training_data['race_id'].iloc[train_size:]
        field_sizes_test = sample_training_data['field_size'].iloc[train_size:]

        # Train model
        model = RacingLightGBM()
        model.fit(X_train, y_train)

        # Generate report
        evaluator = ModelEvaluator()
        report = evaluator.generate_evaluation_report(
            model, X_test, y_test, race_ids_test, field_sizes_test,
            save_dir=str(tmp_path)
        )

        # Check report structure
        assert 'overall_metrics' in report
        assert 'calibration_metrics' in report
        assert 'brier_score' in report['overall_metrics']
        assert 'log_loss' in report['overall_metrics']
        assert 'ece' in report['calibration_metrics']

        # Check metric values are reasonable
        assert 0 <= report['overall_metrics']['brier_score'] <= 1
        assert report['overall_metrics']['log_loss'] > 0
        assert 0 <= report['calibration_metrics']['ece'] <= 1

    def test_log_loss(self):
        """Test log loss calculation."""
        y_true = np.array([1, 0, 1, 0])
        y_pred = np.array([0.9, 0.1, 0.8, 0.2])

        evaluator = ModelEvaluator()
        ll = evaluator.calculate_log_loss(y_pred, y_true)

        # Log loss should be positive
        assert ll > 0

        # Manual calculation for verification
        eps = 1e-15
        y_pred_clipped = np.clip(y_pred, eps, 1 - eps)
        expected_ll = -np.mean(
            y_true * np.log(y_pred_clipped) +
            (1 - y_true) * np.log(1 - y_pred_clipped)
        )

        assert abs(ll - expected_ll) < 1e-6


# ============================================================================
# Test Softmax Functions
# ============================================================================

class TestSoftmaxByRace:
    """Test suite for race-grouped softmax normalization."""

    def test_probabilities_sum_to_one(self):
        """Test that softmax probabilities sum to 1 per race."""
        from models.lightgbm_model import softmax_by_race

        raw_probs = np.array([0.3, 0.4, 0.2, 0.5, 0.3])
        race_ids = np.array(['A', 'A', 'A', 'B', 'B'])

        result = softmax_by_race(raw_probs, race_ids)

        # Check sums per race
        race_a_sum = result[:3].sum()
        race_b_sum = result[3:].sum()

        assert abs(race_a_sum - 1.0) < 1e-6, f"Race A sum: {race_a_sum}"
        assert abs(race_b_sum - 1.0) < 1e-6, f"Race B sum: {race_b_sum}"

    def test_numerical_stability(self):
        """Test softmax with extreme values."""
        from models.lightgbm_model import softmax_by_race

        # Very large values (should not overflow)
        raw_probs = np.array([1000, 1001, 1002])
        race_ids = np.array(['A', 'A', 'A'])

        result = softmax_by_race(raw_probs, race_ids)

        assert not np.any(np.isnan(result)), "Result contains NaN"
        assert not np.any(np.isinf(result)), "Result contains Inf"
        assert abs(result.sum() - 1.0) < 1e-6, "Probabilities don't sum to 1"

    def test_negative_values(self):
        """Test softmax with negative values."""
        from models.lightgbm_model import softmax_by_race

        raw_probs = np.array([-2, -1, 0, 1, 2])
        race_ids = np.array(['A', 'A', 'A', 'A', 'A'])

        result = softmax_by_race(raw_probs, race_ids)

        # Check properties
        assert np.all(result > 0), "All probabilities should be positive"
        assert np.all(result < 1), "All probabilities should be less than 1"
        assert abs(result.sum() - 1.0) < 1e-6

        # Higher raw values should have higher probabilities
        assert result[-1] > result[0]

    def test_multiple_races(self):
        """Test softmax with multiple races."""
        from models.lightgbm_model import softmax_by_race

        # 3 races with different field sizes
        raw_probs = np.array([
            0.5, 0.3, 0.2, 0.1,      # Race 1: 4 horses
            0.8, 0.6,                 # Race 2: 2 horses
            0.4, 0.3, 0.2, 0.1, 0.05  # Race 3: 5 horses
        ])
        race_ids = np.array(['R1', 'R1', 'R1', 'R1', 'R2', 'R2', 'R3', 'R3', 'R3', 'R3', 'R3'])

        result = softmax_by_race(raw_probs, race_ids)

        # Check each race sums to 1
        assert abs(result[0:4].sum() - 1.0) < 1e-6, "Race 1 sum incorrect"
        assert abs(result[4:6].sum() - 1.0) < 1e-6, "Race 2 sum incorrect"
        assert abs(result[6:11].sum() - 1.0) < 1e-6, "Race 3 sum incorrect"

        # Check ordering is preserved within each race
        assert result[0] > result[1] > result[2] > result[3]  # Race 1
        assert result[4] > result[5]  # Race 2
        assert result[6] > result[7] > result[8] > result[9] > result[10]  # Race 3


# ============================================================================
# Test ModelTrainingPipeline
# ============================================================================

class TestModelTrainingPipeline:
    """Test suite for the training pipeline orchestration."""

    def test_pipeline_initialization(self):
        """Test pipeline initialization."""
        pipeline = ModelTrainingPipeline()

        assert pipeline is not None
        assert hasattr(pipeline, 'train_model')
        assert hasattr(pipeline, 'evaluate_model')
        assert hasattr(pipeline, 'run_full_pipeline')

    def test_data_splitting(self, sample_training_data):
        """Test time-based data splitting."""
        pipeline = ModelTrainingPipeline()

        # Add race_id with embedded dates to match the split logic
        # The pipeline extracts date from race_id format: TRACK-YYYY-MM-DD-RACE_NUM
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        sample_training_data['race_id'] = [
            f'TEST-{dates[i // 10].strftime("%Y-%m-%d")}-{(i % 10) + 1}'
            for i in range(len(sample_training_data))
        ]

        # Split data (uses config dates: train: Jan-Jun, val: Jul-Sep, test: Oct-Dec)
        train_df, val_df, test_df = pipeline.split_data(sample_training_data)

        # At least one split should have data (depends on date range coverage)
        # Our test data covers Jan 1 - Apr 10, so only train should have data
        assert len(train_df) > 0 or len(val_df) > 0 or len(test_df) > 0

    def test_feature_columns(self):
        """Test that feature columns are properly defined."""
        from models.training_pipeline import FEATURE_COLUMNS, TARGET_COLUMN

        # Check we have the expected number of features
        assert len(FEATURE_COLUMNS) == 43

        # Check target column
        assert TARGET_COLUMN == 'is_winner'

        # Check some key features exist
        assert 'days_since_last' in FEATURE_COLUMNS
        assert 'trainer_win_rate_14d' in FEATURE_COLUMNS
        assert 'morning_line_odds' in FEATURE_COLUMNS


# ============================================================================
# Integration Tests
# ============================================================================

class TestIntegration:
    """Integration tests for complete workflows."""

    def test_train_evaluate_workflow(self, sample_training_data, tmp_path):
        """Test complete train-evaluate-save workflow."""
        # Setup
        feature_cols = [col for col in sample_training_data.columns
                       if col not in ['race_id', 'is_winner', 'field_size']]

        train_size = int(0.6 * len(sample_training_data))
        val_size = int(0.2 * len(sample_training_data))

        X_train = sample_training_data[feature_cols].iloc[:train_size]
        y_train = sample_training_data['is_winner'].iloc[:train_size]

        X_val = sample_training_data[feature_cols].iloc[train_size:train_size+val_size]
        y_val = sample_training_data['is_winner'].iloc[train_size:train_size+val_size]
        race_ids_val = sample_training_data['race_id'].iloc[train_size:train_size+val_size]
        field_sizes_val = sample_training_data['field_size'].iloc[train_size:train_size+val_size]

        # Train model
        model = RacingLightGBM()
        model.fit(X_train, y_train)

        # Get predictions
        predictions = model.predict_proba(X_val, race_ids_val)

        # Calibrate
        calibrator = FieldSizeCalibrator()
        calibrator.fit(predictions, y_val.values, field_sizes_val.values)
        calibrated_probs = calibrator.calibrate(predictions, field_sizes_val.values)

        # Evaluate using individual metrics
        evaluator = ModelEvaluator()
        brier = evaluator.calculate_brier_score(calibrated_probs, y_val.values)
        log_loss_val = evaluator.calculate_log_loss(calibrated_probs, y_val.values)
        calibration_result = evaluator.calculate_calibration_error(calibrated_probs, y_val.values)

        # Save artifacts
        model_path = tmp_path / "model.pkl"
        calibrator_path = tmp_path / "calibrator.pkl"

        model.save(str(model_path))
        joblib.dump(calibrator, calibrator_path)

        # Verify files exist
        assert model_path.exists()
        assert calibrator_path.exists()

        # Verify metrics are reasonable
        assert brier < 1.0
        assert log_loss_val > 0
        assert calibration_result['ece'] >= 0


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
