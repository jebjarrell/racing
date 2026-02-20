"""Model serialization and version management utilities."""

import json
import joblib
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
import hashlib
import pickle


class ModelRegistry:
    """
    Manages model versions and artifacts.

    Artifacts are stored in:
        artifacts/models/{model_name}_{version}_{timestamp}/
            - model.pkl (or model.txt for LightGBM)
            - calibrator.pkl
            - metadata.json
            - feature_importance.json
            - metrics.json
    """

    def __init__(self, artifacts_dir: str = 'artifacts'):
        """
        Initialize the model registry.

        Args:
            artifacts_dir: Base directory for storing artifacts
        """
        self.artifacts_dir = Path(artifacts_dir)
        self.models_dir = self.artifacts_dir / 'models'
        self.models_dir.mkdir(parents=True, exist_ok=True)

    def save_model_artifacts(
        self,
        model,  # RacingLightGBM
        calibrator,  # FieldSizeCalibrator
        metrics: Dict[str, float],
        feature_importance: Dict[str, float],
        version: str,
        model_name: str = 'lightgbm'
    ) -> str:
        """
        Save all model artifacts and return the artifact path.

        Args:
            model: Trained model instance (RacingLightGBM)
            calibrator: Field size calibrator instance
            metrics: Dictionary of model performance metrics
            feature_importance: Dictionary of feature importance scores
            version: Version string for the model
            model_name: Name of the model type

        Returns:
            str: Path to the saved artifact directory
        """
        # Create artifact directory with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        artifact_name = f"{model_name}_{version}_{timestamp}"
        artifact_path = self.models_dir / artifact_name
        artifact_path.mkdir(parents=True, exist_ok=True)

        # Save model (use LightGBM's native format if available)
        model_path = artifact_path / 'model.pkl'
        try:
            # Try to use LightGBM's native save if model has the attribute
            if hasattr(model, 'model') and hasattr(model.model, 'save_model'):
                model.model.save_model(str(artifact_path / 'model.txt'))
                # Also save the wrapper for complete reconstruction
                joblib.dump(model, model_path)
            else:
                joblib.dump(model, model_path)
        except Exception as e:
            # Fallback to joblib
            joblib.dump(model, model_path)

        # Save calibrator
        calibrator_path = artifact_path / 'calibrator.pkl'
        joblib.dump(calibrator, calibrator_path)

        # Save metrics
        metrics_path = artifact_path / 'metrics.json'
        with open(metrics_path, 'w') as f:
            json.dump(metrics, f, indent=2)

        # Save feature importance
        importance_path = artifact_path / 'feature_importance.json'
        with open(importance_path, 'w') as f:
            json.dump(feature_importance, f, indent=2)

        # Create and save metadata
        feature_columns = getattr(model, 'feature_columns', [])
        config = getattr(model, 'config', {})

        metadata = create_metadata(
            model_name=model_name,
            version=version,
            metrics=metrics,
            config=config,
            feature_columns=feature_columns
        )

        # Add model hash for tracking
        metadata['model_hash'] = self.compute_model_hash(model)
        metadata['artifact_path'] = str(artifact_path)

        metadata_path = artifact_path / 'metadata.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        return str(artifact_path)

    def load_model_artifacts(self, artifact_path: str) -> Dict[str, Any]:
        """
        Load all artifacts from a saved model directory.

        Args:
            artifact_path: Path to the artifact directory

        Returns:
            Dictionary containing:
                - model: Loaded model instance
                - calibrator: Loaded calibrator instance
                - metrics: Performance metrics
                - feature_importance: Feature importance scores
                - metadata: Model metadata
        """
        artifact_path = Path(artifact_path)

        if not artifact_path.exists():
            raise ValueError(f"Artifact path does not exist: {artifact_path}")

        # Load model
        model_path = artifact_path / 'model.pkl'
        if not model_path.exists():
            raise ValueError(f"Model file not found: {model_path}")
        model = joblib.load(model_path)

        # Load calibrator
        calibrator_path = artifact_path / 'calibrator.pkl'
        calibrator = None
        if calibrator_path.exists():
            calibrator = joblib.load(calibrator_path)

        # Load metrics
        metrics_path = artifact_path / 'metrics.json'
        metrics = {}
        if metrics_path.exists():
            with open(metrics_path, 'r') as f:
                metrics = json.load(f)

        # Load feature importance
        importance_path = artifact_path / 'feature_importance.json'
        feature_importance = {}
        if importance_path.exists():
            with open(importance_path, 'r') as f:
                feature_importance = json.load(f)

        # Load metadata
        metadata_path = artifact_path / 'metadata.json'
        metadata = {}
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

        return {
            'model': model,
            'calibrator': calibrator,
            'metrics': metrics,
            'feature_importance': feature_importance,
            'metadata': metadata
        }

    def get_latest_model(self, model_name: str = 'lightgbm') -> Optional[str]:
        """
        Get path to the latest model version.

        Args:
            model_name: Name of the model type

        Returns:
            Path to the latest model artifact directory, or None if no models found
        """
        # Find all model directories matching the model name
        pattern = f"{model_name}_*"
        model_dirs = list(self.models_dir.glob(pattern))

        if not model_dirs:
            return None

        # Sort by modification time (most recent first)
        model_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)

        return str(model_dirs[0])

    def list_models(self) -> List[Dict[str, Any]]:
        """
        List all saved models with their metadata.

        Returns:
            List of dictionaries containing model information
        """
        models = []

        for model_dir in self.models_dir.iterdir():
            if not model_dir.is_dir():
                continue

            metadata_path = model_dir / 'metadata.json'
            if not metadata_path.exists():
                continue

            try:
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)

                # Add directory info
                metadata['artifact_path'] = str(model_dir)
                metadata['created_timestamp'] = model_dir.stat().st_mtime

                models.append(metadata)
            except Exception as e:
                # Skip models with corrupted metadata
                continue

        # Sort by creation time (most recent first)
        models.sort(key=lambda x: x.get('created_timestamp', 0), reverse=True)

        return models

    def compute_model_hash(self, model) -> str:
        """
        Compute hash of model for version tracking.

        Args:
            model: Model instance to hash

        Returns:
            SHA256 hash string of the model
        """
        try:
            # Serialize model to bytes
            model_bytes = pickle.dumps(model)

            # Compute SHA256 hash
            hash_obj = hashlib.sha256(model_bytes)
            return hash_obj.hexdigest()
        except Exception as e:
            # If hashing fails, return a placeholder
            return f"hash_error_{datetime.now().timestamp()}"


def create_metadata(
    model_name: str,
    version: str,
    metrics: Dict[str, float],
    config: Dict[str, Any],
    feature_columns: List[str]
) -> Dict[str, Any]:
    """
    Create metadata dict for model artifact.

    Args:
        model_name: Name of the model type
        version: Version string
        metrics: Performance metrics dictionary
        config: Model configuration dictionary
        feature_columns: List of feature column names

    Returns:
        Dictionary containing all metadata
    """
    return {
        'model_name': model_name,
        'version': version,
        'created_at': datetime.now().isoformat(),
        'metrics': metrics,
        'config': config,
        'feature_columns': feature_columns,
        'python_version': sys.version,
        'num_features': len(feature_columns)
    }


def save_model(
    model,
    path: str,
    use_native_format: bool = True
) -> None:
    """
    Save a model to disk.

    Args:
        model: Model instance to save
        path: Path to save the model
        use_native_format: If True, try to use model's native save format
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if use_native_format and hasattr(model, 'model') and hasattr(model.model, 'save_model'):
        # Use LightGBM's native format
        model.model.save_model(str(path))
    else:
        # Use joblib for general serialization
        joblib.dump(model, path)


def load_model(path: str):
    """
    Load a model from disk.

    Args:
        path: Path to the saved model

    Returns:
        Loaded model instance
    """
    path = Path(path)

    if not path.exists():
        raise ValueError(f"Model file not found: {path}")

    # Try joblib first
    try:
        return joblib.load(path)
    except Exception as e:
        raise ValueError(f"Failed to load model from {path}: {e}")


def export_model_info(
    artifact_path: str,
    output_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Export model information in a human-readable format.

    Args:
        artifact_path: Path to the model artifact directory
        output_path: Optional path to save the info as JSON

    Returns:
        Dictionary containing model information
    """
    artifact_path = Path(artifact_path)

    # Load metadata
    metadata_path = artifact_path / 'metadata.json'
    if not metadata_path.exists():
        raise ValueError(f"Metadata not found at {artifact_path}")

    with open(metadata_path, 'r') as f:
        metadata = json.load(f)

    # Load metrics
    metrics_path = artifact_path / 'metrics.json'
    metrics = {}
    if metrics_path.exists():
        with open(metrics_path, 'r') as f:
            metrics = json.load(f)

    # Load feature importance (top 20)
    importance_path = artifact_path / 'feature_importance.json'
    feature_importance = {}
    if importance_path.exists():
        with open(importance_path, 'r') as f:
            importance = json.load(f)
            # Get top 20 features
            sorted_features = sorted(
                importance.items(),
                key=lambda x: x[1],
                reverse=True
            )[:20]
            feature_importance = dict(sorted_features)

    info = {
        'model_name': metadata.get('model_name'),
        'version': metadata.get('version'),
        'created_at': metadata.get('created_at'),
        'num_features': metadata.get('num_features'),
        'metrics': metrics,
        'top_features': feature_importance,
        'config': metadata.get('config', {}),
        'python_version': metadata.get('python_version')
    }

    # Save if output path provided
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(info, f, indent=2)

    return info
