# Test Suite Documentation

## Overview

This directory contains comprehensive integration tests for the Horse Racing Quantitative Betting System. The test suite validates all Phase 3 components: model training, calibration, and evaluation.

## Test Statistics

- **Test Classes:** 6
- **Test Methods:** 22
- **Fixtures:** 2
- **Coverage:** Models package (Phase 3)

## Test Structure

### Test Files

```
tests/
├── __init__.py           # Package initialization
├── test_models.py        # Phase 3 models package tests
└── README.md            # This file
```

### Test Classes

1. **TestRacingLightGBM** (4 tests)
   - `test_fit_predict`: Basic model training and prediction
   - `test_softmax_normalization`: Per-race probability normalization
   - `test_feature_importance`: Feature importance extraction
   - `test_save_load`: Model serialization and loading

2. **TestFieldSizeCalibrator** (3 tests)
   - `test_fit_calibrate`: Calibrator fitting and transformation
   - `test_bucket_assignment`: Field size bucket assignment logic
   - `test_calibration_improves_ece`: ECE reduction validation

3. **TestModelEvaluator** (6 tests)
   - `test_brier_score`: Brier score calculation
   - `test_brier_score_perfect_prediction`: Perfect prediction edge case
   - `test_brier_score_worst_prediction`: Worst prediction edge case
   - `test_calibration_error`: ECE calculation
   - `test_calibration_error_perfect`: Calibrated prediction validation
   - `test_evaluation_report`: Full evaluation report generation
   - `test_log_loss`: Log loss calculation

4. **TestSoftmaxByRace** (4 tests)
   - `test_probabilities_sum_to_one`: Per-race probability sum validation
   - `test_numerical_stability`: Extreme value handling
   - `test_negative_values`: Negative input handling
   - `test_multiple_races`: Multi-race normalization

5. **TestModelTrainingPipeline** (3 tests)
   - `test_pipeline_initialization`: Pipeline setup
   - `test_data_splitting`: Time-based data splitting
   - `test_end_to_end_training`: Complete training workflow

6. **TestIntegration** (1 test)
   - `test_train_evaluate_workflow`: End-to-end integration test

## Running Tests

### Prerequisites

Install test dependencies:

```bash
pip install pytest pytest-cov numpy pandas lightgbm scikit-learn joblib
```

### Run All Tests

```bash
# From the racing directory
pytest tests/test_models.py -v
```

### Run Specific Test Class

```bash
pytest tests/test_models.py::TestRacingLightGBM -v
```

### Run Specific Test Method

```bash
pytest tests/test_models.py::TestRacingLightGBM::test_fit_predict -v
```

### Run with Coverage

```bash
pytest tests/test_models.py --cov=models --cov-report=html --cov-report=term
```

Coverage report will be in `htmlcov/index.html`.

### Run with Verbose Output

```bash
pytest tests/test_models.py -vv --tb=short
```

## Fixtures

### `sample_training_data`

Creates 1000 synthetic training samples across 100 races:
- 10 horses per race
- 47 engineered features (matching feature catalog)
- Exactly one winner per race
- Realistic field sizes (6, 8, 10, 12 horses)
- Random but reproducible (seed=42)

**Feature Categories:**
- Horse form features: 14
- Connection features (trainer/jockey): 12
- Track/position features: 6
- Equipment features: 4
- Field-relative features: 4
- Base features: 3

### `sample_predictions`

Creates 100 synthetic prediction samples across 10 races:
- Predicted probabilities
- Actual outcomes (is_winner)
- Field sizes
- Race IDs

## Test Details

### RacingLightGBM Tests

**Key Validations:**
- Model can fit and predict without errors
- Predictions are valid probabilities [0, 1]
- Race-grouped softmax ensures probabilities sum to 1.0 per race
- Feature importance can be extracted
- Model can be saved and loaded with consistent predictions

### FieldSizeCalibrator Tests

**Key Validations:**
- Calibrator handles all field size buckets (small: 5-7, medium: 8-10, large: 11+)
- Calibration reduces Expected Calibration Error (ECE)
- Calibrated probabilities remain in [0, 1] range
- Re-normalization maintains per-race probability sums

### ModelEvaluator Tests

**Key Validations:**
- Brier score: mean squared error of probabilities
  - Perfect predictions: Brier = 0.0
  - Worst predictions: Brier = 1.0
- Expected Calibration Error (ECE): calibration quality metric
- Log loss: negative log-likelihood
- Evaluation reports include all required metrics

### Softmax Tests

**Key Validations:**
- Numerical stability with extreme values (prevents overflow/underflow)
- Handles negative inputs correctly
- Multi-race normalization maintains independence
- Probability ordering preserved within each race

### Integration Tests

**Complete Workflow:**
1. Train RacingLightGBM model
2. Generate predictions on validation set
3. Fit FieldSizeCalibrator
4. Calibrate predictions
5. Evaluate with ModelEvaluator
6. Save model artifacts
7. Verify all steps complete successfully

## Expected Test Results

All tests should pass when the models package is properly implemented. Expected runtime: < 30 seconds.

### Success Criteria

- ✅ All 22 tests pass
- ✅ No warnings about numerical instability
- ✅ Calibration reduces ECE
- ✅ Softmax probabilities sum to 1.0 per race (within 1e-6 tolerance)
- ✅ Model artifacts can be saved and loaded

### Common Issues

1. **Import errors**: Ensure models package modules exist
   - `models/training_pipeline.py`
   - `models/lightgbm_model.py`
   - `models/calibration.py`
   - `models/evaluation.py`

2. **Numerical issues**: Softmax overflow/underflow
   - Solution: Use `exp(x - max(x))` for numerical stability

3. **Calibration not improving ECE**: Small sample size
   - Expected with synthetic data
   - Real data should show clear improvement

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov
      - name: Run tests
        run: pytest tests/test_models.py -v --cov=models
```

## Future Test Additions

Planned test expansions:
- Database integration tests
- Feature engineering tests
- End-to-end system tests
- Performance/benchmark tests
- Regression tests for model predictions

## Maintenance

**Review Frequency:** Before each major release

**Update Triggers:**
- New model components added
- Feature set changes
- Calibration methodology changes
- Evaluation metrics added

## Contact

For test-related questions or issues, refer to the project documentation in `docs/`.

---

**Last Updated:** 2025-12-17
**Test Suite Version:** 1.0.0
