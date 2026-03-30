"""Centralized feature matrix preparation."""

import numpy as np
import pandas as pd


def prepare_feature_matrix(df: pd.DataFrame, feature_columns: list) -> pd.DataFrame:
    """Select available columns, add missing as 0, reorder to match training order."""
    available = [c for c in feature_columns if c in df.columns]
    X = df[available].copy()
    for c in feature_columns:
        if c not in X.columns:
            X[c] = 0
    return X[feature_columns].fillna(0)


def get_field_sizes(df: pd.DataFrame, default: int = 8) -> np.ndarray:
    """Extract field_size column from df with fallback to default."""
    if "field_size" in df.columns:
        return df["field_size"].fillna(default).values
    return np.full(len(df), default)
