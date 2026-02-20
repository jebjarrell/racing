"""
Model Evaluation Metrics Module

Provides comprehensive evaluation tools for racing prediction models including:
- Core classification metrics (Brier Score, Log Loss, ROC-AUC)
- Calibration metrics (ECE, MCE)
- Visualization tools (calibration plots, ROC curves, feature importance)
- Full evaluation reports

Target Metrics (Phase 3):
- Brier Score: < 0.20
- Expected Calibration Error (ECE): < 0.03
- Log Loss: reasonable for class imbalance
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Optional, Tuple, Any
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    roc_auc_score,
    roc_curve,
    auc
)
from pathlib import Path
import warnings


class ModelEvaluator:
    """
    Comprehensive model evaluation for racing prediction models.

    Calculates core metrics, calibration metrics, and generates visualizations
    to assess model performance and reliability.
    """

    def __init__(self, n_calibration_bins: int = 10):
        """
        Initialize the ModelEvaluator.

        Parameters
        ----------
        n_calibration_bins : int, default=10
            Number of bins to use for calibration error calculations
        """
        self.n_calibration_bins = n_calibration_bins

    def calculate_brier_score(self, y_pred: np.ndarray, y_true: np.ndarray) -> float:
        """
        Calculate Brier Score (lower is better, target < 0.20).

        Brier Score measures the mean squared difference between predicted
        probabilities and actual outcomes.

        Parameters
        ----------
        y_pred : np.ndarray
            Predicted probabilities (0-1)
        y_true : np.ndarray
            True binary labels (0 or 1)

        Returns
        -------
        float
            Brier score value
        """
        try:
            # Ensure inputs are numpy arrays
            y_pred = np.asarray(y_pred)
            y_true = np.asarray(y_true)

            # Validate inputs
            if len(y_pred) != len(y_true):
                raise ValueError("y_pred and y_true must have the same length")

            if len(y_pred) == 0:
                warnings.warn("Empty arrays provided, returning NaN")
                return np.nan

            # Check for valid probability range
            if np.any((y_pred < 0) | (y_pred > 1)):
                warnings.warn("Predictions outside [0,1] range detected")

            return brier_score_loss(y_true, y_pred)

        except Exception as e:
            warnings.warn(f"Error calculating Brier score: {str(e)}")
            return np.nan

    def calculate_log_loss(self, y_pred: np.ndarray, y_true: np.ndarray) -> float:
        """
        Calculate Log Loss (lower is better).

        Log Loss measures the accuracy of probabilistic predictions by
        penalizing false classifications.

        Parameters
        ----------
        y_pred : np.ndarray
            Predicted probabilities (0-1)
        y_true : np.ndarray
            True binary labels (0 or 1)

        Returns
        -------
        float
            Log loss value
        """
        try:
            # Ensure inputs are numpy arrays
            y_pred = np.asarray(y_pred)
            y_true = np.asarray(y_true)

            # Validate inputs
            if len(y_pred) != len(y_true):
                raise ValueError("y_pred and y_true must have the same length")

            if len(y_pred) == 0:
                warnings.warn("Empty arrays provided, returning NaN")
                return np.nan

            # Clip predictions to avoid log(0)
            y_pred_clipped = np.clip(y_pred, 1e-15, 1 - 1e-15)

            return log_loss(y_true, y_pred_clipped)

        except Exception as e:
            warnings.warn(f"Error calculating log loss: {str(e)}")
            return np.nan

    def calculate_roc_auc(self, y_pred: np.ndarray, y_true: np.ndarray) -> float:
        """
        Calculate ROC-AUC score (higher is better, range 0-1).

        ROC-AUC measures the model's ability to distinguish between classes
        across all classification thresholds.

        Parameters
        ----------
        y_pred : np.ndarray
            Predicted probabilities (0-1)
        y_true : np.ndarray
            True binary labels (0 or 1)

        Returns
        -------
        float
            ROC-AUC score
        """
        try:
            # Ensure inputs are numpy arrays
            y_pred = np.asarray(y_pred)
            y_true = np.asarray(y_true)

            # Validate inputs
            if len(y_pred) != len(y_true):
                raise ValueError("y_pred and y_true must have the same length")

            if len(y_pred) == 0:
                warnings.warn("Empty arrays provided, returning NaN")
                return np.nan

            # Check if we have both classes
            if len(np.unique(y_true)) < 2:
                warnings.warn("Only one class present in y_true, cannot calculate ROC-AUC")
                return np.nan

            return roc_auc_score(y_true, y_pred)

        except Exception as e:
            warnings.warn(f"Error calculating ROC-AUC: {str(e)}")
            return np.nan

    def calculate_calibration_error(
        self,
        y_pred: np.ndarray,
        y_true: np.ndarray
    ) -> Dict[str, Any]:
        """
        Calculate calibration metrics including ECE and MCE.

        Expected Calibration Error (ECE): weighted average of bin-wise
        calibration errors (target < 0.03).

        Maximum Calibration Error (MCE): maximum bin-wise calibration error.

        Parameters
        ----------
        y_pred : np.ndarray
            Predicted probabilities (0-1)
        y_true : np.ndarray
            True binary labels (0 or 1)

        Returns
        -------
        Dict[str, Any]
            Dictionary containing:
            - 'ece': Expected Calibration Error
            - 'mce': Maximum Calibration Error
            - 'reliability_diagram_data': Dict with bin data for plotting
        """
        try:
            # Ensure inputs are numpy arrays
            y_pred = np.asarray(y_pred)
            y_true = np.asarray(y_true)

            # Validate inputs
            if len(y_pred) != len(y_true):
                raise ValueError("y_pred and y_true must have the same length")

            if len(y_pred) == 0:
                warnings.warn("Empty arrays provided")
                return {
                    'ece': np.nan,
                    'mce': np.nan,
                    'reliability_diagram_data': {}
                }

            # Create bins
            bin_edges = np.linspace(0, 1, self.n_calibration_bins + 1)
            bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

            # Initialize containers for bin statistics
            bin_confidences = []
            bin_accuracies = []
            bin_counts = []
            bin_calibration_errors = []

            ece = 0.0
            mce = 0.0
            n_total = len(y_pred)

            # Calculate calibration error for each bin
            for i in range(self.n_calibration_bins):
                # Find predictions in this bin
                mask = (y_pred >= bin_edges[i]) & (y_pred < bin_edges[i + 1])

                # Handle last bin inclusively
                if i == self.n_calibration_bins - 1:
                    mask = (y_pred >= bin_edges[i]) & (y_pred <= bin_edges[i + 1])

                bin_count = mask.sum()

                if bin_count > 0:
                    # Calculate bin confidence (average predicted probability)
                    bin_confidence = y_pred[mask].mean()

                    # Calculate bin accuracy (fraction of positives)
                    bin_accuracy = y_true[mask].mean()

                    # Calibration error for this bin
                    bin_error = abs(bin_accuracy - bin_confidence)

                    # Update ECE (weighted by bin size)
                    ece += (bin_count / n_total) * bin_error

                    # Update MCE (maximum error across bins)
                    mce = max(mce, bin_error)

                    # Store for reliability diagram
                    bin_confidences.append(bin_confidence)
                    bin_accuracies.append(bin_accuracy)
                    bin_counts.append(bin_count)
                    bin_calibration_errors.append(bin_error)
                else:
                    # Empty bin
                    bin_confidences.append(bin_centers[i])
                    bin_accuracies.append(np.nan)
                    bin_counts.append(0)
                    bin_calibration_errors.append(0.0)

            # Prepare reliability diagram data
            reliability_diagram_data = {
                'bin_edges': bin_edges,
                'bin_centers': bin_centers,
                'bin_confidences': np.array(bin_confidences),
                'bin_accuracies': np.array(bin_accuracies),
                'bin_counts': np.array(bin_counts),
                'bin_errors': np.array(bin_calibration_errors)
            }

            return {
                'ece': ece,
                'mce': mce,
                'reliability_diagram_data': reliability_diagram_data
            }

        except Exception as e:
            warnings.warn(f"Error calculating calibration metrics: {str(e)}")
            return {
                'ece': np.nan,
                'mce': np.nan,
                'reliability_diagram_data': {}
            }

    def generate_calibration_plot(
        self,
        y_pred: np.ndarray,
        y_true: np.ndarray,
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Generate calibration (reliability) diagram.

        The calibration plot shows how well predicted probabilities match
        actual outcomes. A perfectly calibrated model would follow the
        diagonal line.

        Parameters
        ----------
        y_pred : np.ndarray
            Predicted probabilities (0-1)
        y_true : np.ndarray
            True binary labels (0 or 1)
        save_path : Optional[str]
            If provided, save the plot to this path

        Returns
        -------
        matplotlib.figure.Figure
            The generated figure
        """
        # Calculate calibration metrics
        cal_metrics = self.calculate_calibration_error(y_pred, y_true)
        reliability_data = cal_metrics['reliability_diagram_data']

        # Create figure
        fig, ax = plt.subplots(figsize=(10, 8))

        if reliability_data:
            bin_centers = reliability_data['bin_centers']
            bin_accuracies = reliability_data['bin_accuracies']
            bin_counts = reliability_data['bin_counts']

            # Plot reliability diagram (bar chart)
            valid_mask = ~np.isnan(bin_accuracies)
            ax.bar(
                bin_centers[valid_mask],
                bin_accuracies[valid_mask],
                width=1.0 / self.n_calibration_bins,
                alpha=0.7,
                edgecolor='black',
                label='Actual Accuracy'
            )

            # Plot perfect calibration line
            ax.plot([0, 1], [0, 1], 'r--', linewidth=2, label='Perfect Calibration')

            # Add bin confidence markers
            ax.scatter(
                bin_centers[valid_mask],
                bin_accuracies[valid_mask],
                s=100,
                c='darkblue',
                marker='o',
                zorder=5,
                label='Bin Accuracy'
            )

            # Formatting
            ax.set_xlabel('Predicted Probability (Confidence)', fontsize=12)
            ax.set_ylabel('Actual Probability (Accuracy)', fontsize=12)
            ax.set_title(
                f'Calibration Plot\n'
                f'ECE: {cal_metrics["ece"]:.4f} | MCE: {cal_metrics["mce"]:.4f}',
                fontsize=14,
                fontweight='bold'
            )
            ax.set_xlim([0, 1])
            ax.set_ylim([0, 1])
            ax.legend(loc='upper left', fontsize=10)
            ax.grid(True, alpha=0.3)

            # Add sample count text
            total_samples = len(y_pred)
            ax.text(
                0.95, 0.05,
                f'Total Samples: {total_samples:,}',
                transform=ax.transAxes,
                fontsize=10,
                verticalalignment='bottom',
                horizontalalignment='right',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5)
            )
        else:
            ax.text(
                0.5, 0.5,
                'Insufficient data for calibration plot',
                transform=ax.transAxes,
                fontsize=14,
                ha='center',
                va='center'
            )

        plt.tight_layout()

        # Save if path provided
        if save_path:
            Path(save_path).parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(save_path, dpi=300, bbox_inches='tight')

        return fig

    def generate_roc_curve(
        self,
        y_pred: np.ndarray,
        y_true: np.ndarray,
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Generate ROC curve visualization.

        The ROC curve shows the trade-off between true positive rate and
        false positive rate across different classification thresholds.

        Parameters
        ----------
        y_pred : np.ndarray
            Predicted probabilities (0-1)
        y_true : np.ndarray
            True binary labels (0 or 1)
        save_path : Optional[str]
            If provided, save the plot to this path

        Returns
        -------
        matplotlib.figure.Figure
            The generated figure
        """
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 8))

        try:
            # Ensure inputs are numpy arrays
            y_pred = np.asarray(y_pred)
            y_true = np.asarray(y_true)

            # Check if we have both classes
            if len(np.unique(y_true)) < 2:
                ax.text(
                    0.5, 0.5,
                    'Only one class present - cannot generate ROC curve',
                    transform=ax.transAxes,
                    fontsize=14,
                    ha='center',
                    va='center'
                )
            else:
                # Calculate ROC curve
                fpr, tpr, thresholds = roc_curve(y_true, y_pred)
                roc_auc = auc(fpr, tpr)

                # Plot ROC curve
                ax.plot(
                    fpr, tpr,
                    color='darkorange',
                    lw=2,
                    label=f'ROC curve (AUC = {roc_auc:.4f})'
                )

                # Plot random classifier line
                ax.plot([0, 1], [0, 1], 'r--', lw=2, label='Random Classifier')

                # Formatting
                ax.set_xlabel('False Positive Rate', fontsize=12)
                ax.set_ylabel('True Positive Rate', fontsize=12)
                ax.set_title('Receiver Operating Characteristic (ROC) Curve', fontsize=14, fontweight='bold')
                ax.set_xlim([0.0, 1.0])
                ax.set_ylim([0.0, 1.05])
                ax.legend(loc='lower right', fontsize=11)
                ax.grid(True, alpha=0.3)

                # Add sample count and positive rate
                n_positive = y_true.sum()
                n_total = len(y_true)
                positive_rate = n_positive / n_total if n_total > 0 else 0

                ax.text(
                    0.95, 0.05,
                    f'Total Samples: {n_total:,}\n'
                    f'Positive Rate: {positive_rate:.2%}',
                    transform=ax.transAxes,
                    fontsize=10,
                    verticalalignment='bottom',
                    horizontalalignment='right',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5)
                )

        except Exception as e:
            ax.text(
                0.5, 0.5,
                f'Error generating ROC curve:\n{str(e)}',
                transform=ax.transAxes,
                fontsize=12,
                ha='center',
                va='center'
            )

        plt.tight_layout()

        # Save if path provided
        if save_path:
            Path(save_path).parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(save_path, dpi=300, bbox_inches='tight')

        return fig

    def generate_feature_importance_plot(
        self,
        importance_dict: Dict[str, float],
        top_n: int = 20,
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Generate feature importance visualization.

        Parameters
        ----------
        importance_dict : Dict[str, float]
            Dictionary mapping feature names to importance scores
        top_n : int, default=20
            Number of top features to display
        save_path : Optional[str]
            If provided, save the plot to this path

        Returns
        -------
        matplotlib.figure.Figure
            The generated figure
        """
        # Create figure
        fig, ax = plt.subplots(figsize=(12, max(8, top_n * 0.4)))

        if not importance_dict:
            ax.text(
                0.5, 0.5,
                'No feature importance data provided',
                transform=ax.transAxes,
                fontsize=14,
                ha='center',
                va='center'
            )
        else:
            # Sort features by importance
            sorted_features = sorted(
                importance_dict.items(),
                key=lambda x: x[1],
                reverse=True
            )[:top_n]

            features, importances = zip(*sorted_features)

            # Create horizontal bar chart
            y_pos = np.arange(len(features))
            ax.barh(y_pos, importances, alpha=0.8, color='steelblue', edgecolor='black')

            # Formatting
            ax.set_yticks(y_pos)
            ax.set_yticklabels(features, fontsize=10)
            ax.set_xlabel('Importance Score', fontsize=12)
            ax.set_title(
                f'Top {len(features)} Feature Importances',
                fontsize=14,
                fontweight='bold'
            )
            ax.invert_yaxis()  # Highest importance at top
            ax.grid(True, axis='x', alpha=0.3)

            # Add value labels on bars
            for i, v in enumerate(importances):
                ax.text(
                    v, i,
                    f' {v:.4f}',
                    va='center',
                    fontsize=9
                )

        plt.tight_layout()

        # Save if path provided
        if save_path:
            Path(save_path).parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(save_path, dpi=300, bbox_inches='tight')

        return fig

    def generate_evaluation_report(
        self,
        model,
        X_test: pd.DataFrame,
        y_test: pd.Series,
        race_ids: pd.Series,
        field_sizes: pd.Series,
        save_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate comprehensive evaluation report for a model.

        Calculates all metrics, generates all plots, and produces a detailed
        evaluation report including race-level and field-size-stratified metrics.

        Parameters
        ----------
        model : object
            Trained model with predict_proba method
        X_test : pd.DataFrame
            Test features
        y_test : pd.Series
            Test labels
        race_ids : pd.Series
            Race IDs corresponding to test samples
        field_sizes : pd.Series
            Field sizes corresponding to test samples
        save_dir : Optional[str]
            If provided, save plots and report to this directory

        Returns
        -------
        Dict[str, Any]
            Comprehensive evaluation report containing:
            - Overall metrics
            - Calibration metrics
            - Race-level metrics
            - Field-size-stratified metrics
            - Plot paths (if saved)
        """
        try:
            # Get predictions
            if hasattr(model, 'predict_proba'):
                # Check if model is RacingLightGBM (requires race_ids for softmax)
                import inspect
                sig = inspect.signature(model.predict_proba)
                if 'race_ids' in sig.parameters:
                    # RacingLightGBM model - pass race_ids for softmax normalization
                    y_pred = model.predict_proba(X_test, race_ids)
                else:
                    # Standard sklearn-style model
                    y_pred_proba = model.predict_proba(X_test)
                    # Handle both binary and multi-class output
                    if y_pred_proba.ndim == 2 and y_pred_proba.shape[1] == 2:
                        y_pred = y_pred_proba[:, 1]  # Probability of positive class
                    else:
                        y_pred = y_pred_proba.ravel()
            else:
                raise ValueError("Model must have predict_proba method")

            # Convert to numpy arrays
            y_pred = np.asarray(y_pred)
            y_true = np.asarray(y_test)
            race_ids = np.asarray(race_ids)
            field_sizes = np.asarray(field_sizes)

            # Calculate core metrics
            brier = self.calculate_brier_score(y_pred, y_true)
            logloss = self.calculate_log_loss(y_pred, y_true)
            roc_auc = self.calculate_roc_auc(y_pred, y_true)

            # Calculate calibration metrics
            cal_metrics = self.calculate_calibration_error(y_pred, y_true)

            # Calculate race-level metrics
            race_level_metrics = self._calculate_race_level_metrics(
                y_pred, y_true, race_ids
            )

            # Calculate field-size-stratified metrics
            field_size_metrics = self._calculate_field_size_metrics(
                y_pred, y_true, field_sizes
            )

            # Prepare report
            report = {
                'overall_metrics': {
                    'brier_score': brier,
                    'log_loss': logloss,
                    'roc_auc': roc_auc,
                    'n_samples': len(y_test),
                    'n_positive': y_true.sum(),
                    'positive_rate': y_true.mean()
                },
                'calibration_metrics': {
                    'ece': cal_metrics['ece'],
                    'mce': cal_metrics['mce'],
                    'n_bins': self.n_calibration_bins
                },
                'race_level_metrics': race_level_metrics,
                'field_size_metrics': field_size_metrics,
                'target_metrics': {
                    'brier_score_target': 0.20,
                    'brier_score_achieved': brier < 0.20,
                    'ece_target': 0.03,
                    'ece_achieved': cal_metrics['ece'] < 0.03
                }
            }

            # Generate and save plots if directory provided
            if save_dir:
                save_dir_path = Path(save_dir)
                save_dir_path.mkdir(parents=True, exist_ok=True)

                # Generate calibration plot
                cal_plot_path = save_dir_path / 'calibration_plot.png'
                self.generate_calibration_plot(y_pred, y_true, str(cal_plot_path))

                # Generate ROC curve
                roc_plot_path = save_dir_path / 'roc_curve.png'
                self.generate_roc_curve(y_pred, y_true, str(roc_plot_path))

                # Generate feature importance plot if available
                if hasattr(model, 'feature_importances_'):
                    importance_dict = dict(zip(X_test.columns, model.feature_importances_))
                    fi_plot_path = save_dir_path / 'feature_importance.png'
                    self.generate_feature_importance_plot(
                        importance_dict,
                        top_n=20,
                        save_path=str(fi_plot_path)
                    )
                    report['plot_paths'] = {
                        'calibration_plot': str(cal_plot_path),
                        'roc_curve': str(roc_plot_path),
                        'feature_importance': str(fi_plot_path)
                    }
                else:
                    report['plot_paths'] = {
                        'calibration_plot': str(cal_plot_path),
                        'roc_curve': str(roc_plot_path)
                    }

                # Save text report
                self._save_text_report(report, save_dir_path / 'evaluation_report.txt')

            return report

        except Exception as e:
            warnings.warn(f"Error generating evaluation report: {str(e)}")
            return {
                'error': str(e),
                'overall_metrics': {},
                'calibration_metrics': {},
                'race_level_metrics': {},
                'field_size_metrics': {}
            }

    def _calculate_race_level_metrics(
        self,
        y_pred: np.ndarray,
        y_true: np.ndarray,
        race_ids: np.ndarray
    ) -> Dict[str, Any]:
        """
        Calculate metrics aggregated at the race level.

        This helps understand model performance on a per-race basis.
        """
        try:
            unique_races = np.unique(race_ids)
            race_briers = []
            race_logloss = []

            for race_id in unique_races:
                mask = race_ids == race_id
                if mask.sum() > 1:  # Need at least 2 samples per race
                    race_y_pred = y_pred[mask]
                    race_y_true = y_true[mask]

                    brier = self.calculate_brier_score(race_y_pred, race_y_true)
                    logloss = self.calculate_log_loss(race_y_pred, race_y_true)

                    if not np.isnan(brier):
                        race_briers.append(brier)
                    if not np.isnan(logloss):
                        race_logloss.append(logloss)

            return {
                'n_races': len(unique_races),
                'mean_race_brier': np.mean(race_briers) if race_briers else np.nan,
                'std_race_brier': np.std(race_briers) if race_briers else np.nan,
                'mean_race_logloss': np.mean(race_logloss) if race_logloss else np.nan,
                'std_race_logloss': np.std(race_logloss) if race_logloss else np.nan
            }

        except Exception as e:
            warnings.warn(f"Error calculating race-level metrics: {str(e)}")
            return {}

    def _calculate_field_size_metrics(
        self,
        y_pred: np.ndarray,
        y_true: np.ndarray,
        field_sizes: np.ndarray
    ) -> Dict[str, Any]:
        """
        Calculate metrics stratified by field size.

        This helps understand how model performance varies with race competitiveness.
        """
        try:
            # Define field size bins
            field_size_bins = [(2, 6), (7, 10), (11, 14), (15, float('inf'))]
            bin_names = ['Small (2-6)', 'Medium (7-10)', 'Large (11-14)', 'Very Large (15+)']

            metrics_by_size = {}

            for (min_size, max_size), bin_name in zip(field_size_bins, bin_names):
                mask = (field_sizes >= min_size) & (field_sizes <= max_size)

                if mask.sum() > 0:
                    bin_y_pred = y_pred[mask]
                    bin_y_true = y_true[mask]

                    brier = self.calculate_brier_score(bin_y_pred, bin_y_true)
                    logloss = self.calculate_log_loss(bin_y_pred, bin_y_true)
                    roc_auc = self.calculate_roc_auc(bin_y_pred, bin_y_true)
                    cal_metrics = self.calculate_calibration_error(bin_y_pred, bin_y_true)

                    metrics_by_size[bin_name] = {
                        'n_samples': mask.sum(),
                        'brier_score': brier,
                        'log_loss': logloss,
                        'roc_auc': roc_auc,
                        'ece': cal_metrics['ece'],
                        'positive_rate': bin_y_true.mean()
                    }

            return metrics_by_size

        except Exception as e:
            warnings.warn(f"Error calculating field-size metrics: {str(e)}")
            return {}

    def _save_text_report(self, report: Dict[str, Any], file_path: Path) -> None:
        """
        Save evaluation report as formatted text file.
        """
        try:
            with open(file_path, 'w') as f:
                f.write("=" * 80 + "\n")
                f.write("MODEL EVALUATION REPORT\n")
                f.write("=" * 80 + "\n\n")

                # Overall metrics
                f.write("OVERALL METRICS\n")
                f.write("-" * 80 + "\n")
                overall = report.get('overall_metrics', {})
                f.write(f"Brier Score:      {overall.get('brier_score', np.nan):.4f}\n")
                f.write(f"Log Loss:         {overall.get('log_loss', np.nan):.4f}\n")
                f.write(f"ROC-AUC:          {overall.get('roc_auc', np.nan):.4f}\n")
                f.write(f"Total Samples:    {overall.get('n_samples', 0):,}\n")
                f.write(f"Positive Samples: {overall.get('n_positive', 0):,}\n")
                f.write(f"Positive Rate:    {overall.get('positive_rate', 0):.4f}\n\n")

                # Calibration metrics
                f.write("CALIBRATION METRICS\n")
                f.write("-" * 80 + "\n")
                cal = report.get('calibration_metrics', {})
                f.write(f"ECE (Expected Calibration Error): {cal.get('ece', np.nan):.4f}\n")
                f.write(f"MCE (Maximum Calibration Error):  {cal.get('mce', np.nan):.4f}\n")
                f.write(f"Number of Bins:                   {cal.get('n_bins', 0)}\n\n")

                # Target achievement
                f.write("TARGET METRICS ACHIEVEMENT\n")
                f.write("-" * 80 + "\n")
                targets = report.get('target_metrics', {})
                brier_achieved = "YES" if targets.get('brier_score_achieved', False) else "NO"
                ece_achieved = "YES" if targets.get('ece_achieved', False) else "NO"
                f.write(f"Brier Score < 0.20:  {brier_achieved}\n")
                f.write(f"ECE < 0.03:          {ece_achieved}\n\n")

                # Race-level metrics
                f.write("RACE-LEVEL METRICS\n")
                f.write("-" * 80 + "\n")
                race = report.get('race_level_metrics', {})
                f.write(f"Number of Races:       {race.get('n_races', 0):,}\n")
                f.write(f"Mean Race Brier:       {race.get('mean_race_brier', np.nan):.4f} "
                       f"(+/- {race.get('std_race_brier', np.nan):.4f})\n")
                f.write(f"Mean Race Log Loss:    {race.get('mean_race_logloss', np.nan):.4f} "
                       f"(+/- {race.get('std_race_logloss', np.nan):.4f})\n\n")

                # Field-size metrics
                f.write("FIELD-SIZE STRATIFIED METRICS\n")
                f.write("-" * 80 + "\n")
                field = report.get('field_size_metrics', {})
                for bin_name, metrics in field.items():
                    f.write(f"\n{bin_name}:\n")
                    f.write(f"  Samples:      {metrics.get('n_samples', 0):,}\n")
                    f.write(f"  Brier Score:  {metrics.get('brier_score', np.nan):.4f}\n")
                    f.write(f"  Log Loss:     {metrics.get('log_loss', np.nan):.4f}\n")
                    f.write(f"  ROC-AUC:      {metrics.get('roc_auc', np.nan):.4f}\n")
                    f.write(f"  ECE:          {metrics.get('ece', np.nan):.4f}\n")
                    f.write(f"  Pos. Rate:    {metrics.get('positive_rate', np.nan):.4f}\n")

                f.write("\n" + "=" * 80 + "\n")

        except Exception as e:
            warnings.warn(f"Error saving text report: {str(e)}")
