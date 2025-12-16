# Phase 3 Continuation Prompt

**Project**: Horse Racing Quantitative Betting System
**Date**: 2025-12-16
**Status**: Phases 0-2 Complete, Ready for Phase 3

---

## Context for Next Conversation

Use this prompt to continue development in a new conversation:

```
I'm continuing development of a horse racing quantitative betting system. Phases 0-2 are complete.

Please review the implementation plan at: docs/PHASE3_CONTINUATION_PROMPT.md

Then continue with Phase 3: Probability Model Training, which includes:
1. Create models/training_pipeline.py - model training orchestration
2. Create models/calibration.py - field-size stratified calibration
3. Create models/evaluation.py - Brier score, calibration metrics
4. Create models/lightgbm_model.py - LightGBM wrapper with softmax
5. Create training scripts and notebooks

The existing codebase has:
- Complete feature engineering layer in features/
- Database schema and migrations in database/
- Configuration in config/config.yaml
- SQLite database with 2023 Equibase data (racing_data.db, 248MB)
```

---

## What Has Been Completed

### Phase 0: Documentation Foundation ✅
All documentation files in docs/ directory.

### Phase 1: Database Migration ✅
PostgreSQL schema and migration scripts in database/ directory.

### Phase 2: Feature Engineering Layer ✅

| File | Lines | Description |
|------|-------|-------------|
| `features/__init__.py` | 25 | Package exports |
| `features/rolling_stats.py` | 450 | Trainer/jockey/combo rolling statistics |
| `features/track_bias.py` | 380 | Post position and speed bias calculations |
| `features/validation.py` | 420 | Leakage prevention and validation framework |
| `features/feature_engine.py` | 480 | Main orchestration for feature calculation |
| `standardization.py` (extended) | 712 | Added speed/pace/class calculation methods |

**Key Classes:**
- `RollingStatsCalculator`: 14/30/60-day windows for trainer, jockey, combo stats
- `TrackBiasCalculator`: Post position bias by track×surface×distance
- `LeakageValidator`: Point-in-time validation framework
- `FeatureEngine`: Main orchestrator combining all feature categories

**Features Implemented:**
- Horse form (20): days_since_last, speed figures, surface/distance preferences
- Connections (20): trainer/jockey win rates, hot streaks, combo synergy
- Track (15): post position bias, rail adjustment, speed bias
- Equipment (10): blinkers, lasix, first-time changes
- Class (10): class change, purse comparisons
- Field-relative (5): speed rank, class rank, field quality

---

## Phase 3: Probability Model Training (Next)

### 3.1 Training Pipeline (`models/training_pipeline.py`)

Main training orchestration:

```python
class ModelTrainingPipeline:
    def prepare_training_data(self, start_date: date, end_date: date) -> DataFrame
    def split_data(self, df: DataFrame) -> Tuple[train, val, test]
    def train_model(self, train_df: DataFrame, val_df: DataFrame) -> Model
    def evaluate_model(self, model: Model, test_df: DataFrame) -> Metrics
    def save_model(self, model: Model, version: str) -> str
```

**Data Split Strategy (Time-based):**
- Training: Jan-Jun (6 months)
- Validation: Jul-Sep (3 months)
- Test: Oct-Dec (3 months)

### 3.2 LightGBM Model (`models/lightgbm_model.py`)

Model wrapper with race-grouped softmax:

```python
class RacingLightGBM:
    def __init__(self, params: Dict)
    def fit(self, X: DataFrame, y: Series, groups: Series)
    def predict_proba(self, X: DataFrame, race_ids: Series) -> DataFrame
    def apply_softmax_by_race(self, raw_probs: array, race_ids: array) -> array
```

**LightGBM Configuration:**
```python
params = {
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
}
```

### 3.3 Calibration (`models/calibration.py`)

Field-size stratified isotonic regression:

```python
class FieldSizeCalibrator:
    def __init__(self, field_size_buckets: List[Tuple[int, int]])
    def fit(self, y_pred: array, y_true: array, field_sizes: array)
    def calibrate(self, y_pred: array, field_sizes: array) -> array
    def get_calibration_curve(self, bucket: str) -> Tuple[x, y]
```

**Field Size Buckets:**
- Small: 4-6 horses
- Medium: 7-9 horses
- Large: 10-14 horses

### 3.4 Evaluation (`models/evaluation.py`)

Comprehensive model evaluation:

```python
class ModelEvaluator:
    def calculate_brier_score(self, y_pred: array, y_true: array) -> float
    def calculate_log_loss(self, y_pred: array, y_true: array) -> float
    def calculate_calibration_error(self, y_pred: array, y_true: array) -> Dict
    def calculate_roc_auc(self, y_pred: array, y_true: array) -> float
    def generate_calibration_plot(self, y_pred: array, y_true: array) -> Figure
    def generate_evaluation_report(self, model: Model, test_df: DataFrame) -> Report
```

**Target Metrics:**
- Brier Score: < 0.20
- Expected Calibration Error (ECE): < 0.03
- Log Loss: Reasonable for class imbalance

---

## Key Technical Requirements

### Race-Grouped Softmax Normalization

Probabilities must sum to 1.0 within each race:

```python
def softmax_by_race(raw_probs: np.array, race_ids: np.array) -> np.array:
    """
    Apply softmax normalization grouped by race.

    Args:
        raw_probs: Raw predicted probabilities (N,)
        race_ids: Race identifiers for grouping (N,)

    Returns:
        Normalized probabilities summing to 1.0 per race
    """
    result = np.zeros_like(raw_probs)

    for race_id in np.unique(race_ids):
        mask = race_ids == race_id
        race_probs = raw_probs[mask]

        # Apply softmax
        exp_probs = np.exp(race_probs - np.max(race_probs))  # Numerical stability
        result[mask] = exp_probs / exp_probs.sum()

    return result
```

### Calibration by Field Size

Different field sizes have different probability distributions:

```python
# 6-horse field: winner probability ~16.7% baseline
# 10-horse field: winner probability ~10% baseline
# Calibrators must account for this

field_size_buckets = [
    (4, 6, 'small'),
    (7, 9, 'medium'),
    (10, 14, 'large'),
]
```

### Feature Importance Tracking

Track and store feature importance:

```python
importance_dict = {
    'feature_name': model.feature_importance(importance_type='gain'),
    'feature_name': model.feature_importance(importance_type='split'),
}

# Store in models.model_registry.feature_importance (JSONB)
```

---

## Directory Structure After Phase 3

```
k:\racing-pipeline\racing\
├── models/                      # NEW: Model training
│   ├── __init__.py
│   ├── training_pipeline.py    # Training orchestration
│   ├── lightgbm_model.py       # LightGBM wrapper
│   ├── calibration.py          # Field-size calibration
│   └── evaluation.py           # Metrics and evaluation
├── features/                    # Phase 2: Feature engineering
├── config/
├── database/
├── docs/
└── [existing extraction scripts]
```

---

## Database Tables to Use

From Phase 1 schema:

```sql
-- Store trained models
models.model_registry (
    model_id, model_name, model_version,
    brier_score, log_loss, calibration_error,
    model_artifact_path, feature_importance,
    is_active, deployed_at
)

-- Store predictions for monitoring
models.prediction_log (
    prediction_id, model_id, race_id, entry_id,
    raw_probability, calibrated_probability,
    field_normalized_probability,
    field_size, field_size_bucket,
    actual_finish_position, is_winner
)
```

---

## Success Criteria for Phase 3

- [ ] `models/training_pipeline.py` with data preparation and training
- [ ] `models/lightgbm_model.py` with race-grouped softmax
- [ ] `models/calibration.py` with field-size stratified calibration
- [ ] `models/evaluation.py` with Brier score, ECE, calibration plots
- [ ] Model achieves Brier score < 0.20 on test set
- [ ] Calibration error (ECE) < 0.03
- [ ] Model artifacts saved to artifacts/ directory
- [ ] Feature importance analysis completed

---

## Feature Set for Training

Features from `FeatureEngine.calculate_entry_features()`:

```python
FEATURE_COLUMNS = [
    # Horse form (14)
    'days_since_last', 'layoff_indicator', 'first_time_starter',
    'total_starts', 'total_wins', 'career_win_rate',
    'surface_win_rate', 'surface_preference', 'distance_preference',
    'best_speed_90_days', 'avg_speed_90_days', 'speed_trend',
    'last_class_level', 'class_change',

    # Connections (12)
    'trainer_win_rate_14d', 'trainer_win_rate_30d', 'trainer_win_rate_60d',
    'trainer_hot_streak', 'trainer_sample_flag',
    'jockey_win_rate_14d', 'jockey_win_rate_30d', 'jockey_win_rate_60d',
    'jockey_hot_streak', 'jockey_sample_flag',
    'combo_win_rate', 'combo_synergy_score',

    # Track/Position (6)
    'post_position', 'post_position_win_rate', 'inside_bias_score',
    'rail_bias_adjustment', 'speed_bias_score', 'field_size',

    # Equipment (4)
    'blinkers_on', 'blinkers_first_time', 'lasix_on', 'equipment_change',

    # Field-relative (4)
    'speed_rank_in_field', 'class_rank_in_field',
    'field_quality_score', 'speed_vs_field_avg',

    # Base (3)
    'morning_line_odds', 'age_at_race', 'class_level',
]

TARGET_COLUMN = 'is_winner'  # Binary: 1 if official_finish_position == 1
```

---

## Notes

- Use `features.FeatureEngine` to generate training data
- Validate features with `features.LeakageValidator` before training
- Time-based splits prevent leakage between train/val/test
- All model artifacts should be versioned and stored

---

*Document created: 2025-12-16*
*For use in continuing development in a new conversation*
