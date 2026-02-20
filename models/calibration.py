"""
Field-size stratified isotonic regression calibration for horse racing predictions.

This module provides calibration capabilities that account for the varying baseline
probabilities across different field sizes. A 10% probability in a 5-horse race
(baseline 20%) is very different from 10% in a 14-horse race (baseline 7%).
"""

import numpy as np
import joblib
from typing import List, Tuple, Optional, Dict
from sklearn.isotonic import IsotonicRegression
import warnings


# Default field size buckets based on typical race configurations
DEFAULT_BUCKETS = [
    (4, 6, 'small'),    # 4-6 horses: ~16.7% baseline win prob
    (7, 9, 'medium'),   # 7-9 horses: ~11-14% baseline
    (10, 14, 'large'),  # 10-14 horses: ~7-10% baseline
]


class FieldSizeCalibrator:
    """
    Calibrates probabilities using isotonic regression, stratified by field size.

    Different field sizes have different baseline win probabilities, so we learn
    separate calibration functions for small, medium, and large fields.

    Attributes:
        field_size_buckets: List of (min_size, max_size, bucket_name) tuples
        calibrators: Dict mapping bucket names to IsotonicRegression models
        bucket_stats: Dict with training statistics per bucket
    """

    def __init__(self, field_size_buckets: Optional[List[Tuple[int, int, str]]] = None):
        """
        Initialize the field size calibrator.

        Args:
            field_size_buckets: Optional list of (min_size, max_size, name) tuples.
                               If None, uses DEFAULT_BUCKETS.
        """
        self.field_size_buckets = field_size_buckets or DEFAULT_BUCKETS
        self.calibrators: Dict[str, IsotonicRegression] = {}
        self.bucket_stats: Dict[str, Dict] = {}
        self._validate_buckets()

    def _validate_buckets(self):
        """Validate that buckets are well-formed and non-overlapping."""
        if not self.field_size_buckets:
            raise ValueError("Field size buckets cannot be empty")

        bucket_names = set()
        for min_size, max_size, name in self.field_size_buckets:
            if min_size > max_size:
                raise ValueError(f"Invalid bucket: min_size ({min_size}) > max_size ({max_size})")
            if name in bucket_names:
                raise ValueError(f"Duplicate bucket name: {name}")
            bucket_names.add(name)

    def get_bucket(self, field_size: int) -> str:
        """
        Get the bucket name for a given field size.

        Args:
            field_size: Number of horses in the race

        Returns:
            Bucket name (e.g., 'small', 'medium', 'large')

        Note:
            If field_size doesn't match any bucket exactly, returns the closest bucket.
        """
        # Try exact match first
        for min_size, max_size, name in self.field_size_buckets:
            if min_size <= field_size <= max_size:
                return name

        # Find closest bucket if no exact match
        closest_bucket = None
        min_distance = float('inf')

        for min_size, max_size, name in self.field_size_buckets:
            if field_size < min_size:
                distance = min_size - field_size
            elif field_size > max_size:
                distance = field_size - max_size
            else:
                distance = 0

            if distance < min_distance:
                min_distance = distance
                closest_bucket = name

        if closest_bucket is None:
            # Fallback to first bucket
            closest_bucket = self.field_size_buckets[0][2]

        return closest_bucket

    def fit(self, y_pred: np.ndarray, y_true: np.ndarray, field_sizes: np.ndarray) -> 'FieldSizeCalibrator':
        """
        Fit isotonic regression calibrators for each field size bucket.

        Args:
            y_pred: Predicted probabilities, shape (n_samples,)
            y_true: True binary labels (1 for winner, 0 for non-winner), shape (n_samples,)
            field_sizes: Number of horses in each race, shape (n_samples,)

        Returns:
            self (for method chaining)

        Raises:
            ValueError: If input arrays have mismatched shapes or invalid values
        """
        # Validate inputs
        y_pred = np.asarray(y_pred)
        y_true = np.asarray(y_true)
        field_sizes = np.asarray(field_sizes)

        if not (y_pred.shape[0] == y_true.shape[0] == field_sizes.shape[0]):
            raise ValueError(f"Shape mismatch: y_pred={y_pred.shape}, y_true={y_true.shape}, field_sizes={field_sizes.shape}")

        if not np.all((y_true == 0) | (y_true == 1)):
            raise ValueError("y_true must contain only 0s and 1s")

        if not np.all(field_sizes > 0):
            raise ValueError("field_sizes must be positive")

        # Group data by bucket
        bucket_data: Dict[str, Dict] = {}
        for i in range(len(y_pred)):
            bucket = self.get_bucket(field_sizes[i])
            if bucket not in bucket_data:
                bucket_data[bucket] = {'y_pred': [], 'y_true': []}
            bucket_data[bucket]['y_pred'].append(y_pred[i])
            bucket_data[bucket]['y_true'].append(y_true[i])

        # Fit calibrator for each bucket
        self.calibrators = {}
        self.bucket_stats = {}

        for bucket_name in [name for _, _, name in self.field_size_buckets]:
            if bucket_name not in bucket_data:
                warnings.warn(f"No data for bucket '{bucket_name}', skipping calibration")
                continue

            bucket_y_pred = np.array(bucket_data[bucket_name]['y_pred'])
            bucket_y_true = np.array(bucket_data[bucket_name]['y_true'])

            # Check for minimum samples
            if len(bucket_y_pred) < 10:
                warnings.warn(f"Insufficient samples for bucket '{bucket_name}' ({len(bucket_y_pred)} < 10), skipping")
                continue

            # Check for variation in predictions and outcomes
            if len(np.unique(bucket_y_pred)) < 2:
                warnings.warn(f"No variation in predictions for bucket '{bucket_name}', skipping")
                continue

            if len(np.unique(bucket_y_true)) < 2:
                warnings.warn(f"No variation in outcomes for bucket '{bucket_name}', using mean calibration")
                # For no variation, we can still fit but it will be a constant

            # Fit isotonic regression
            ir = IsotonicRegression(out_of_bounds='clip', y_min=0.0, y_max=1.0)
            try:
                ir.fit(bucket_y_pred, bucket_y_true)
                self.calibrators[bucket_name] = ir

                # Store statistics
                self.bucket_stats[bucket_name] = {
                    'n_samples': len(bucket_y_pred),
                    'mean_pred': float(np.mean(bucket_y_pred)),
                    'mean_true': float(np.mean(bucket_y_true)),
                    'std_pred': float(np.std(bucket_y_pred)),
                    'pred_range': (float(np.min(bucket_y_pred)), float(np.max(bucket_y_pred)))
                }
            except Exception as e:
                warnings.warn(f"Failed to fit calibrator for bucket '{bucket_name}': {e}")
                continue

        if not self.calibrators:
            raise ValueError("Failed to fit any calibrators. Check your data.")

        return self

    def calibrate(self, y_pred: np.ndarray, field_sizes: np.ndarray) -> np.ndarray:
        """
        Calibrate predicted probabilities based on field sizes.

        Args:
            y_pred: Predicted probabilities, shape (n_samples,)
            field_sizes: Number of horses in each race, shape (n_samples,)

        Returns:
            Calibrated probabilities, shape (n_samples,)

        Raises:
            ValueError: If calibrator hasn't been fitted yet
        """
        if not self.calibrators:
            raise ValueError("Calibrator must be fitted before calling calibrate()")

        y_pred = np.asarray(y_pred)
        field_sizes = np.asarray(field_sizes)

        if y_pred.shape[0] != field_sizes.shape[0]:
            raise ValueError(f"Shape mismatch: y_pred={y_pred.shape}, field_sizes={field_sizes.shape}")

        calibrated = np.zeros_like(y_pred)

        for i in range(len(y_pred)):
            bucket = self.get_bucket(field_sizes[i])

            if bucket in self.calibrators:
                # Use fitted calibrator for this bucket
                calibrated[i] = self.calibrators[bucket].predict([y_pred[i]])[0]
            else:
                # No calibrator for this bucket, find closest available
                available_buckets = list(self.calibrators.keys())
                if available_buckets:
                    # Use first available calibrator as fallback
                    fallback_bucket = available_buckets[0]
                    calibrated[i] = self.calibrators[fallback_bucket].predict([y_pred[i]])[0]
                else:
                    # No calibrators at all, return original prediction
                    calibrated[i] = y_pred[i]

        return calibrated

    def get_calibration_curve(self, bucket: str) -> Tuple[np.ndarray, np.ndarray]:
        """
        Get the calibration curve for a specific bucket.

        Args:
            bucket: Bucket name (e.g., 'small', 'medium', 'large')

        Returns:
            Tuple of (x_values, y_values) where:
                x_values: Original predicted probabilities
                y_values: Calibrated probabilities

        Raises:
            ValueError: If bucket doesn't exist or hasn't been fitted
        """
        if bucket not in self.calibrators:
            raise ValueError(f"No calibrator fitted for bucket '{bucket}'")

        ir = self.calibrators[bucket]

        # Get the calibration function points
        # IsotonicRegression stores X_ and y_ attributes after fitting
        if hasattr(ir, 'X_thresholds_') and hasattr(ir, 'y_thresholds_'):
            # sklearn >= 1.3
            x_values = ir.X_thresholds_
            y_values = ir.y_thresholds_
        elif hasattr(ir, 'X_') and hasattr(ir, 'y_'):
            # sklearn < 1.3
            x_values = ir.X_
            y_values = ir.y_
        else:
            # Fallback: create curve by sampling
            x_values = np.linspace(0, 1, 100)
            y_values = ir.predict(x_values)

        return np.asarray(x_values), np.asarray(y_values)

    def save(self, path: str) -> None:
        """
        Save the calibrator to disk.

        Args:
            path: File path to save to (e.g., 'calibrator.pkl')
        """
        data = {
            'field_size_buckets': self.field_size_buckets,
            'calibrators': self.calibrators,
            'bucket_stats': self.bucket_stats
        }
        joblib.dump(data, path)

    @classmethod
    def load(cls, path: str) -> 'FieldSizeCalibrator':
        """
        Load a calibrator from disk.

        Args:
            path: File path to load from

        Returns:
            Loaded FieldSizeCalibrator instance
        """
        data = joblib.load(path)
        calibrator = cls(field_size_buckets=data['field_size_buckets'])
        calibrator.calibrators = data['calibrators']
        calibrator.bucket_stats = data['bucket_stats']
        return calibrator

    def __repr__(self) -> str:
        """String representation of the calibrator."""
        bucket_info = ", ".join([f"{name}({min_}-{max_})"
                                 for min_, max_, name in self.field_size_buckets])
        fitted = len(self.calibrators) > 0
        return f"FieldSizeCalibrator(buckets=[{bucket_info}], fitted={fitted})"


def compute_calibration_stats(y_pred: np.ndarray, y_true: np.ndarray, n_bins: int = 10) -> Dict:
    """
    Compute calibration statistics including Expected Calibration Error (ECE).

    Calibration measures how well predicted probabilities match observed frequencies.
    For example, among all predictions of 70%, we expect ~70% to be actual winners.

    Args:
        y_pred: Predicted probabilities, shape (n_samples,)
        y_true: True binary labels (1 for winner, 0 for non-winner), shape (n_samples,)
        n_bins: Number of bins to use for calibration curve (default: 10)

    Returns:
        Dictionary containing:
            - bin_edges: Bin boundaries, shape (n_bins + 1,)
            - bin_counts: Number of samples per bin, shape (n_bins,)
            - predicted_means: Mean predicted probability per bin, shape (n_bins,)
            - observed_means: Mean observed frequency per bin, shape (n_bins,)
            - ece: Expected Calibration Error (scalar)
            - mce: Maximum Calibration Error (scalar)

    Example:
        >>> y_pred = np.array([0.1, 0.3, 0.5, 0.7, 0.9])
        >>> y_true = np.array([0, 0, 1, 1, 1])
        >>> stats = compute_calibration_stats(y_pred, y_true, n_bins=5)
        >>> print(f"ECE: {stats['ece']:.4f}")
    """
    y_pred = np.asarray(y_pred)
    y_true = np.asarray(y_true)

    if y_pred.shape != y_true.shape:
        raise ValueError(f"Shape mismatch: y_pred={y_pred.shape}, y_true={y_true.shape}")

    if not np.all((y_true == 0) | (y_true == 1)):
        raise ValueError("y_true must contain only 0s and 1s")

    if n_bins < 2:
        raise ValueError("n_bins must be at least 2")

    # Create bins
    bin_edges = np.linspace(0, 1, n_bins + 1)
    bin_counts = np.zeros(n_bins)
    predicted_means = np.zeros(n_bins)
    observed_means = np.zeros(n_bins)

    # Assign samples to bins
    bin_indices = np.digitize(y_pred, bin_edges[1:-1])

    # Compute statistics per bin
    for i in range(n_bins):
        mask = bin_indices == i
        bin_counts[i] = np.sum(mask)

        if bin_counts[i] > 0:
            predicted_means[i] = np.mean(y_pred[mask])
            observed_means[i] = np.mean(y_true[mask])
        else:
            # Empty bin: use bin center as predicted mean, NaN for observed
            predicted_means[i] = (bin_edges[i] + bin_edges[i + 1]) / 2
            observed_means[i] = np.nan

    # Compute Expected Calibration Error (ECE)
    # ECE = sum over bins of: (bin_count / total) * |predicted_mean - observed_mean|
    total_samples = len(y_pred)
    ece = 0.0
    mce = 0.0  # Maximum Calibration Error

    for i in range(n_bins):
        if bin_counts[i] > 0 and not np.isnan(observed_means[i]):
            weight = bin_counts[i] / total_samples
            calibration_error = abs(predicted_means[i] - observed_means[i])
            ece += weight * calibration_error
            mce = max(mce, calibration_error)

    return {
        'bin_edges': bin_edges,
        'bin_counts': bin_counts,
        'predicted_means': predicted_means,
        'observed_means': observed_means,
        'ece': float(ece),
        'mce': float(mce),
        'n_bins': n_bins,
        'n_samples': len(y_pred)
    }
