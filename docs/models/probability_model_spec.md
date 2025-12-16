# Win Probability Model Specification

**Version:** 1.0
**Last Updated:** 2025-12-16
**Model Type:** Gradient Boosted Trees (LightGBM)

---

## Table of Contents

1. [Overview](#1-overview)
2. [Problem Formulation](#2-problem-formulation)
3. [Model Architecture](#3-model-architecture)
4. [Training Configuration](#4-training-configuration)
5. [Data Splitting Strategy](#5-data-splitting-strategy)
6. [Calibration Methodology](#6-calibration-methodology)
7. [Evaluation Metrics](#7-evaluation-metrics)
8. [Feature Selection](#8-feature-selection)
9. [Hyperparameter Tuning](#9-hyperparameter-tuning)
10. [Model Versioning](#10-model-versioning)
11. [Inference Pipeline](#11-inference-pipeline)

---

## 1. Overview

### 1.1 Objective

Predict the probability that each horse in a race will win, producing calibrated probabilities that:

1. Sum to 1.0 within each race
2. Are well-calibrated across probability bins
3. Outperform market-implied probabilities (morning line)

### 1.2 Key Requirements

| Requirement | Target | Rationale |
|-------------|--------|-----------|
| Calibration | ECE < 0.03 | Accurate probabilities for Kelly sizing |
| Discrimination | AUC > 0.70 | Identify winners better than random |
| Prediction Speed | < 1 second/race | Enable scratch re-computation |
| Robustness | Stable across tracks | Generalize to new tracks |

### 1.3 Model Selection Rationale

**Why Gradient Boosted Trees (LightGBM)?**

| Factor | LightGBM | Deep Learning | Logistic Regression |
|--------|----------|---------------|---------------------|
| Tabular data performance | Excellent | Good | Moderate |
| Training speed | Fast | Slow | Fast |
| Calibration | Good | Requires post-hoc | Naturally calibrated |
| Interpretability | Moderate | Low | High |
| Feature interactions | Automatic | Manual | Manual |
| Sample size requirements | Moderate | High | Low |

Given ~50,000 historical races and 115 features, LightGBM provides the best balance.

---

## 2. Problem Formulation

### 2.1 Task Definition

For each race with N horses, predict:

```
P(horse_i wins | features_i, race_context) for i ∈ {1, ..., N}
```

**Constraint:** Σ P_i = 1 (enforced via softmax normalization)

### 2.2 Label Definition

```python
# Binary label per horse
y_i = 1 if horse_i finished 1st else 0

# Per race, exactly one horse has y = 1
sum(y_race) == 1
```

### 2.3 Model Output

Raw model output: score `s_i` for each horse

Transformed to probability:
```
P_i = exp(s_i) / Σ_j∈race exp(s_j)
```

---

## 3. Model Architecture

### 3.1 Base Model: LightGBM

```python
import lightgbm as lgb

model = lgb.LGBMClassifier(
    objective='binary',
    n_estimators=500,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_weight=10,
    reg_alpha=0.1,
    reg_lambda=0.1,
    random_state=42,
    n_jobs=-1
)
```

### 3.2 Softmax Normalization Layer

```python
def race_softmax(scores: np.ndarray, race_ids: np.ndarray) -> np.ndarray:
    """
    Apply softmax within each race to ensure probabilities sum to 1.

    Args:
        scores: Raw model scores, shape (n_entries,)
        race_ids: Race ID for each entry, shape (n_entries,)

    Returns:
        Calibrated probabilities, shape (n_entries,)
    """
    probabilities = np.zeros_like(scores)

    for race_id in np.unique(race_ids):
        mask = race_ids == race_id
        race_scores = scores[mask]

        # Softmax
        exp_scores = np.exp(race_scores - np.max(race_scores))  # Numerical stability
        probabilities[mask] = exp_scores / exp_scores.sum()

    return probabilities
```

### 3.3 Calibration Layer

After softmax, apply field-size-stratified isotonic regression:

```python
from sklearn.isotonic import IsotonicRegression

class FieldSizeCalibrator:
    def __init__(self):
        self.calibrators = {}  # {field_size_bucket: IsotonicRegression}

    def fit(self, probabilities, outcomes, field_sizes):
        for bucket in ['small', 'medium', 'large']:
            mask = self._get_bucket_mask(field_sizes, bucket)
            if mask.sum() > 100:  # Minimum sample
                calibrator = IsotonicRegression(out_of_bounds='clip')
                calibrator.fit(probabilities[mask], outcomes[mask])
                self.calibrators[bucket] = calibrator

    def transform(self, probabilities, field_sizes):
        calibrated = probabilities.copy()
        for bucket, calibrator in self.calibrators.items():
            mask = self._get_bucket_mask(field_sizes, bucket)
            if mask.sum() > 0:
                calibrated[mask] = calibrator.transform(probabilities[mask])
        return calibrated

    def _get_bucket_mask(self, field_sizes, bucket):
        if bucket == 'small':
            return (field_sizes >= 5) & (field_sizes <= 7)
        elif bucket == 'medium':
            return (field_sizes >= 8) & (field_sizes <= 10)
        else:  # large
            return field_sizes >= 11
```

### 3.4 Re-Normalization

After calibration, re-normalize within race to ensure sum = 1:

```python
def renormalize_race(probabilities: np.ndarray, race_ids: np.ndarray) -> np.ndarray:
    """
    Re-normalize probabilities to sum to 1 within each race.
    """
    renormalized = np.zeros_like(probabilities)

    for race_id in np.unique(race_ids):
        mask = race_ids == race_id
        race_probs = probabilities[mask]
        renormalized[mask] = race_probs / race_probs.sum()

    return renormalized
```

---

## 4. Training Configuration

### 4.1 LightGBM Parameters

```python
GBM_CONFIG = {
    # Core parameters
    'objective': 'binary',
    'metric': 'binary_logloss',
    'boosting_type': 'gbdt',

    # Tree structure
    'n_estimators': 500,
    'max_depth': 6,
    'num_leaves': 31,
    'min_child_samples': 20,
    'min_child_weight': 0.001,

    # Learning
    'learning_rate': 0.05,
    'subsample': 0.8,
    'subsample_freq': 1,
    'colsample_bytree': 0.8,

    # Regularization
    'reg_alpha': 0.1,  # L1
    'reg_lambda': 0.1,  # L2
    'min_split_gain': 0.0,

    # Practical
    'random_state': 42,
    'n_jobs': -1,
    'verbose': -1,

    # Early stopping
    'early_stopping_rounds': 50
}
```

### 4.2 Training Procedure

```python
def train_model(X_train, y_train, X_val, y_val, config):
    """
    Train LightGBM model with early stopping.
    """
    model = lgb.LGBMClassifier(**config)

    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        eval_metric='logloss',
        callbacks=[
            lgb.early_stopping(config['early_stopping_rounds']),
            lgb.log_evaluation(period=50)
        ]
    )

    return model
```

### 4.3 Cross-Validation Strategy

```python
def time_series_cv(data, n_splits=5):
    """
    Time-series cross-validation with expanding window.

    Split 1: Train [0:20%], Val [20:30%]
    Split 2: Train [0:30%], Val [30:40%]
    Split 3: Train [0:40%], Val [40:50%]
    Split 4: Train [0:50%], Val [50:60%]
    Split 5: Train [0:60%], Val [60:70%]
    """
    n = len(data)
    splits = []

    for i in range(n_splits):
        train_end = int(n * (0.2 + i * 0.1))
        val_end = int(n * (0.3 + i * 0.1))

        train_idx = np.arange(0, train_end)
        val_idx = np.arange(train_end, val_end)

        splits.append((train_idx, val_idx))

    return splits
```

---

## 5. Data Splitting Strategy

### 5.1 Time-Based Splits

**Critical:** All splits must be time-based to prevent look-ahead bias.

```
2023 Data Timeline:
├── January ─────────────┐
├── February             │
├── March                │ Training Set (6 months)
├── April                │ ~30,000 races
├── May                  │
├── June ────────────────┘
├── July ────────────────┐
├── August               │ Validation Set (3 months)
├── September ───────────┘ ~15,000 races
├── October ─────────────┐
├── November             │ Test Set (3 months)
├── December ────────────┘ ~15,000 races
```

### 5.2 Split Implementation

```python
SPLIT_CONFIG = {
    'train': {
        'start': '2023-01-01',
        'end': '2023-06-30'
    },
    'validation': {
        'start': '2023-07-01',
        'end': '2023-09-30'
    },
    'test': {
        'start': '2023-10-01',
        'end': '2023-12-31'
    }
}

def create_splits(data: pd.DataFrame, config: dict) -> tuple:
    """
    Create train/validation/test splits.
    """
    train_mask = (
        (data['race_date'] >= config['train']['start']) &
        (data['race_date'] <= config['train']['end'])
    )
    val_mask = (
        (data['race_date'] >= config['validation']['start']) &
        (data['race_date'] <= config['validation']['end'])
    )
    test_mask = (
        (data['race_date'] >= config['test']['start']) &
        (data['race_date'] <= config['test']['end'])
    )

    return data[train_mask], data[val_mask], data[test_mask]
```

### 5.3 Stratification Analysis

After splitting, verify stratification balance:

| Metric | Train | Validation | Test |
|--------|-------|------------|------|
| Total races | ~10,000 | ~5,000 | ~5,000 |
| Avg field size | 8.5 | 8.4 | 8.6 |
| Win rate (expected) | 11.8% | 11.9% | 11.7% |
| Track distribution | Similar | Similar | Similar |

---

## 6. Calibration Methodology

### 6.1 Why Calibration Matters

For Kelly criterion betting, calibration is critical:

- If P(win) = 20% and odds = 5-1, EV = (0.20 × 6) - 1 = +20%
- If actual win rate at P(win) = 20% is only 15%, EV = (0.15 × 6) - 1 = -10%

Miscalibration directly translates to losing bets.

### 6.2 Calibration Challenges

| Challenge | Cause | Solution |
|-----------|-------|----------|
| Field size variation | 5-horse vs 14-horse races | Stratified calibration |
| Low sample favorites | Few races with >40% favorites | Isotonic regression |
| Boundary effects | Extreme probabilities | Clipping |

### 6.3 Field-Size Stratification

**Rationale:** A 20% predicted probability means different things:
- In 5-horse field: Expected = 20%, Above random (20% > 20%)
- In 14-horse field: Expected = 7%, Well above random (20% >> 7%)

```python
FIELD_SIZE_BUCKETS = {
    'small': (5, 7),    # 5-7 horses
    'medium': (8, 10),  # 8-10 horses
    'large': (11, 20)   # 11+ horses
}
```

### 6.4 Calibration Procedure

```python
def calibrate_probabilities(raw_probs, outcomes, field_sizes, race_ids):
    """
    Full calibration pipeline.

    1. Apply softmax per race
    2. Apply field-size-stratified isotonic regression
    3. Re-normalize per race
    """
    # Step 1: Softmax normalization
    softmax_probs = race_softmax(raw_probs, race_ids)

    # Step 2: Fit calibrator on validation set
    calibrator = FieldSizeCalibrator()
    calibrator.fit(softmax_probs, outcomes, field_sizes)

    # Step 3: Transform probabilities
    calibrated_probs = calibrator.transform(softmax_probs, field_sizes)

    # Step 4: Re-normalize per race
    final_probs = renormalize_race(calibrated_probs, race_ids)

    return final_probs, calibrator
```

### 6.5 Calibration Validation

```python
def validate_calibration(probabilities, outcomes, n_bins=10):
    """
    Compute calibration metrics.
    """
    bins = np.linspace(0, 1, n_bins + 1)
    bin_indices = np.digitize(probabilities, bins) - 1

    calibration_data = []
    for i in range(n_bins):
        mask = bin_indices == i
        if mask.sum() > 0:
            predicted = probabilities[mask].mean()
            actual = outcomes[mask].mean()
            count = mask.sum()
            calibration_data.append({
                'bin': i,
                'predicted': predicted,
                'actual': actual,
                'count': count,
                'error': abs(predicted - actual)
            })

    # Expected Calibration Error
    ece = sum(d['count'] * d['error'] for d in calibration_data) / len(probabilities)

    return calibration_data, ece
```

---

## 7. Evaluation Metrics

### 7.1 Primary Metrics

| Metric | Target | Formula |
|--------|--------|---------|
| **Brier Score** | < 0.20 | `mean((p - y)^2)` |
| **Log Loss** | Minimize | `-mean(y*log(p) + (1-y)*log(1-p))` |
| **ECE** | < 0.03 | Expected Calibration Error |

### 7.2 Secondary Metrics

| Metric | Target | Notes |
|--------|--------|-------|
| AUC-ROC | > 0.70 | Discrimination ability |
| Top-1 Accuracy | > 15% | Picking winners |
| Top-3 Accuracy | > 50% | Picking in-the-money |

### 7.3 Baseline Comparison

| Metric | Model | Morning Line | Improvement |
|--------|-------|--------------|-------------|
| Brier Score | 0.185 | 0.210 | -12% |
| Log Loss | 2.10 | 2.35 | -11% |
| AUC | 0.72 | 0.68 | +6% |

### 7.4 Stratified Analysis

Evaluate metrics by:
- Track type (high-volume vs regional)
- Race class (maiden, claiming, stakes)
- Field size (small, medium, large)
- Odds bucket (favorites, mid-range, longshots)

```python
def stratified_evaluation(predictions, outcomes, stratification_column):
    """
    Evaluate model performance by strata.
    """
    results = {}
    for stratum in predictions[stratification_column].unique():
        mask = predictions[stratification_column] == stratum
        results[stratum] = {
            'brier': brier_score_loss(outcomes[mask], predictions['prob'][mask]),
            'log_loss': log_loss(outcomes[mask], predictions['prob'][mask]),
            'count': mask.sum()
        }
    return results
```

---

## 8. Feature Selection

### 8.1 Initial Feature Set

Start with all 115 features from the [Feature Catalog](../features/feature_catalog.md).

### 8.2 Feature Importance Analysis

```python
def analyze_feature_importance(model, feature_names):
    """
    Extract and analyze feature importance.
    """
    importance = pd.DataFrame({
        'feature': feature_names,
        'importance': model.feature_importances_,
        'importance_gain': model.booster_.feature_importance(importance_type='gain'),
        'importance_split': model.booster_.feature_importance(importance_type='split')
    })

    return importance.sort_values('importance_gain', ascending=False)
```

### 8.3 Feature Selection Criteria

| Criterion | Threshold | Action |
|-----------|-----------|--------|
| Zero importance | importance = 0 | Remove |
| Low importance | importance < 0.1% | Consider removing |
| Leakage suspect | Very high importance unexpectedly | Investigate |
| Highly correlated | correlation > 0.95 | Keep one |

### 8.4 Recursive Feature Elimination

```python
def recursive_feature_elimination(X, y, model, n_features_to_select):
    """
    RFE with cross-validation.
    """
    from sklearn.feature_selection import RFECV

    selector = RFECV(
        estimator=model,
        step=1,
        cv=TimeSeriesSplit(n_splits=5),
        scoring='neg_log_loss',
        min_features_to_select=n_features_to_select
    )

    selector.fit(X, y)
    return selector.support_, selector.ranking_
```

---

## 9. Hyperparameter Tuning

### 9.1 Tuning Strategy

Use Optuna for Bayesian optimization:

```python
import optuna

def objective(trial):
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
        'max_depth': trial.suggest_int('max_depth', 3, 10),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
        'subsample': trial.suggest_float('subsample', 0.5, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
        'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 10.0, log=True),
        'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 10.0, log=True),
        'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
    }

    model = lgb.LGBMClassifier(**params)
    model.fit(X_train, y_train)
    preds = model.predict_proba(X_val)[:, 1]

    # Apply softmax and calibration
    calibrated = calibrate_probabilities(preds, y_val, field_sizes_val, race_ids_val)

    # Compute Brier score
    score = brier_score_loss(y_val, calibrated)

    return score  # Minimize
```

### 9.2 Search Space

| Parameter | Range | Scale |
|-----------|-------|-------|
| n_estimators | 100-1000 | Linear |
| max_depth | 3-10 | Linear |
| learning_rate | 0.01-0.3 | Log |
| subsample | 0.5-1.0 | Linear |
| colsample_bytree | 0.5-1.0 | Linear |
| reg_alpha | 1e-8 to 10 | Log |
| reg_lambda | 1e-8 to 10 | Log |

### 9.3 Tuning Protocol

1. Run 100 trials of Optuna optimization
2. Select top 5 parameter sets
3. Evaluate each on test set
4. Choose parameters with best test performance
5. Document final parameters

---

## 10. Model Versioning

### 10.1 Version Format

```
v{major}.{minor}.{patch}_{date}_{hash}

Example: v1.0.0_20231215_a1b2c3d
```

### 10.2 MLflow Tracking

```python
import mlflow

def train_and_log_model(X_train, y_train, X_val, y_val, config):
    with mlflow.start_run():
        # Log parameters
        mlflow.log_params(config)

        # Train model
        model = train_model(X_train, y_train, X_val, y_val, config)

        # Compute metrics
        preds = model.predict_proba(X_val)[:, 1]
        brier = brier_score_loss(y_val, preds)
        logloss = log_loss(y_val, preds)

        # Log metrics
        mlflow.log_metrics({
            'val_brier': brier,
            'val_logloss': logloss
        })

        # Log model
        mlflow.lightgbm.log_model(model, 'model')

        # Log artifacts
        mlflow.log_artifact('feature_importance.csv')
        mlflow.log_artifact('calibration_curve.png')

        return model
```

### 10.3 Model Registry

| Field | Description |
|-------|-------------|
| model_id | Unique identifier |
| version | Semantic version |
| train_date | Training date |
| train_data_range | Date range of training data |
| val_brier | Validation Brier score |
| test_brier | Test Brier score |
| status | staging/production/archived |

---

## 11. Inference Pipeline

### 11.1 Prediction Flow

```python
class WinProbabilityPredictor:
    def __init__(self, model_path: str, calibrator_path: str):
        self.model = lgb.Booster(model_file=model_path)
        self.calibrator = joblib.load(calibrator_path)

    def predict_race(self, race_id: str) -> Dict[str, float]:
        """
        Generate win probabilities for a single race.

        Returns: {horse_registration: probability, ...}
        """
        # 1. Load race features
        features = self.feature_engine.calculate_all_features(race_id)
        X = self._features_to_matrix(features)

        # 2. Generate raw scores
        raw_scores = self.model.predict(X)

        # 3. Apply softmax
        softmax_probs = np.exp(raw_scores) / np.exp(raw_scores).sum()

        # 4. Apply calibration
        field_size = len(features)
        calibrated = self.calibrator.transform(
            softmax_probs,
            np.array([field_size] * len(softmax_probs))
        )

        # 5. Re-normalize
        final_probs = calibrated / calibrated.sum()

        # 6. Return as dict
        return {reg: prob for reg, prob in zip(features.keys(), final_probs)}

    def predict_races_batch(self, race_ids: List[str]) -> List[Dict[str, float]]:
        """
        Generate predictions for multiple races.
        """
        return [self.predict_race(race_id) for race_id in race_ids]
```

### 11.2 Performance Requirements

| Metric | Target | Notes |
|--------|--------|-------|
| Single race prediction | < 100ms | Enables scratch recomputation |
| Batch prediction (100 races) | < 5s | Morning predictions |
| Memory usage | < 1GB | Model + features in memory |

### 11.3 Error Handling

```python
def predict_race_safe(self, race_id: str) -> Optional[Dict[str, float]]:
    """
    Prediction with error handling.
    """
    try:
        return self.predict_race(race_id)
    except MissingFeaturesError as e:
        logger.warning(f"Missing features for race {race_id}: {e}")
        return None
    except ModelLoadError as e:
        logger.error(f"Model load failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error predicting {race_id}: {e}")
        return None
```

---

## Appendix A: Model Card

### Model Details

| Field | Value |
|-------|-------|
| Model Name | Win Probability Model v1.0 |
| Model Type | LightGBM Classifier |
| Training Data | 2023 Equibase Historical |
| Features | 115 engineered features |
| Output | Calibrated win probabilities |

### Intended Use

- Win bet recommendation for US thoroughbred racing
- Manual execution only
- Educational/research purposes

### Limitations

- Trained on 2023 data only
- May not generalize to new tracks without retraining
- Does not account for real-time odds movement
- Requires scratch recomputation when scratches occur

### Performance Summary

| Split | Brier Score | Log Loss | AUC |
|-------|-------------|----------|-----|
| Validation | 0.185 | 2.10 | 0.72 |
| Test | 0.188 | 2.12 | 0.71 |

---

*Document maintained by: ML Engineering Team*
*Review cycle: Before each model update*
