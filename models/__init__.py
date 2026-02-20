"""
Horse Racing ML Model Package.

This package provides a complete machine learning pipeline for horse racing predictions,
including model training, calibration, and evaluation components.

Main Components:
    ModelTrainingPipeline: Orchestrates the end-to-end model training process, including
                          data preparation, feature engineering, model training, and
                          evaluation.

    RacingLightGBM: A specialized LightGBM wrapper that implements race-grouped softmax
                   for proper probability predictions across competing horses in each race.

    FieldSizeCalibrator: Implements field-size stratified isotonic calibration to ensure
                        well-calibrated probabilities across different race field sizes.

    ModelEvaluator: Comprehensive model evaluation toolkit providing metrics such as
                   Brier score, Expected Calibration Error (ECE), and calibration plots.

Usage Example:
    >>> from models import ModelTrainingPipeline, RacingLightGBM
    >>> pipeline = ModelTrainingPipeline()
    >>> model = pipeline.train()
    >>> predictions = model.predict(race_data)
"""

from .training_pipeline import ModelTrainingPipeline
from .lightgbm_model import RacingLightGBM
from .calibration import FieldSizeCalibrator
from .evaluation import ModelEvaluator

__all__ = [
    'ModelTrainingPipeline',
    'RacingLightGBM',
    'FieldSizeCalibrator',
    'ModelEvaluator',
]

__version__ = '1.0.0'
