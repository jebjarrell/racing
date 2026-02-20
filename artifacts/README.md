# Racing Model Artifacts

This directory contains all model artifacts generated during training, calibration, and evaluation of the racing prediction models.

## Directory Structure

### `models/`
Contains saved model files from training runs.

**File formats:**
- `.pkl` - Serialized scikit-learn compatible models (e.g., LightGBM, CatBoost)
- `.txt` - LightGBM text format models
- `.cbm` - CatBoost native format models

**Contents:**
- Trained race outcome prediction models
- Feature importance data
- Model hyperparameters and configuration

### `calibrators/`
Contains probability calibration models used to improve model predictions.

**Contents:**
- Isotonic regression calibrators
- Platt scaling calibrators
- Calibration mappings and transformations

### `reports/`
Contains evaluation reports, performance metrics, and visualization plots.

**Contents:**
- Model evaluation metrics (accuracy, ROC-AUC, log loss, etc.)
- Feature importance plots
- Calibration curves
- ROC curves and precision-recall curves
- Confusion matrices
- Performance comparison reports

## Naming Convention

Model files should follow this naming convention:

```
{model_type}_v{version}_{date}.{extension}
```

**Examples:**
- `lightgbm_v1.0_20231215.pkl`
- `lightgbm_v1.0_20231215.txt`
- `catboost_v2.1_20240115.cbm`
- `isotonic_calibrator_v1.0_20231215.pkl`

**Components:**
- `model_type`: Type of model (lightgbm, catboost, xgboost, etc.)
- `version`: Semantic version number (major.minor)
- `date`: Training date in YYYYMMDD format
- `extension`: Appropriate file extension for the format

## Git Ignore

All artifacts in this directory are ignored by Git (except for this README.md) to prevent large binary files from being committed to version control. Model artifacts should be managed separately through:
- Model registry systems
- Cloud storage (S3, Azure Blob, etc.)
- DVC (Data Version Control)
- MLflow artifact storage

Only the directory structure and this documentation are tracked in Git.
