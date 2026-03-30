"""
Model Validation Script — Honest metric assessment with baselines and leakage tests.

Trains a fresh model on the current DB, then evaluates at three stages:
  1. Raw scores (pre-softmax, pre-calibration)
  2. Softmax-normalized (no calibration)
  3. Calibrated (isotonic + softmax)

Compares against naive (1/field_size) and morning line baselines.
Runs a leakage smoke test on 100 random test races.

Usage:
    python scripts/validate_model.py
"""

import logging
import sys
import random
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    roc_auc_score, brier_score_loss, log_loss as sklearn_log_loss,
)
from sklearn.linear_model import LogisticRegression

# Project imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.training_pipeline import ModelTrainingPipeline, FEATURE_COLUMNS
from models.lightgbm_model import RacingLightGBM, softmax_by_race
from models.calibration import FieldSizeCalibrator
from app.utils.features import prepare_feature_matrix, get_field_sizes

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

DB_PATH = str(Path(__file__).resolve().parent.parent / "racing_data.db")
CONFIG_PATH = str(Path(__file__).resolve().parent.parent / "config" / "config.yaml")


# ─── Metric Helpers ────────────────────────────────────────────────────────────

def compute_ece(y_true, y_pred, n_bins=5, strategy="uniform"):
    """Compute Expected Calibration Error.

    strategy: 'uniform' (equal-width) or 'quantile' (equal-count)
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    if strategy == "quantile":
        quantiles = np.linspace(0, 100, n_bins + 1)
        bin_edges = np.percentile(y_pred, quantiles)
        bin_edges = np.unique(bin_edges)  # remove duplicates
    else:
        bin_edges = np.linspace(0, 1, n_bins + 1)

    ece = 0.0
    for i in range(len(bin_edges) - 1):
        lo, hi = bin_edges[i], bin_edges[i + 1]
        if i == len(bin_edges) - 2:
            mask = (y_pred >= lo) & (y_pred <= hi)
        else:
            mask = (y_pred >= lo) & (y_pred < hi)

        n = mask.sum()
        if n == 0:
            continue
        avg_pred = y_pred[mask].mean()
        avg_true = y_true[mask].mean()
        ece += (n / len(y_true)) * abs(avg_pred - avg_true)

    return ece


def compute_per_race_auc(y_true, y_pred, race_ids, exclude_short_fav=False,
                         morning_line_odds=None, min_entries=5):
    """Compute mean AUC across individual races.

    exclude_short_fav: if True, exclude races where favorite (lowest ML odds) won
                       and had ML odds <= 0.2 (i.e., 1-5 or shorter)
    """
    df = pd.DataFrame({
        "y_true": y_true,
        "y_pred": y_pred,
        "race_id": race_ids,
    })
    if morning_line_odds is not None:
        df["ml_odds"] = morning_line_odds

    aucs = []
    for race_id, group in df.groupby("race_id"):
        if len(group) < min_entries:
            continue
        if group["y_true"].nunique() < 2:
            continue  # skip races with no winner or all winners

        if exclude_short_fav and morning_line_odds is not None:
            winner = group[group["y_true"] == 1]
            if not winner.empty:
                winner_ml = winner["ml_odds"].iloc[0]
                if pd.notna(winner_ml) and winner_ml <= 0.2:
                    continue  # skip short-price favorite winner

        try:
            auc = roc_auc_score(group["y_true"], group["y_pred"])
            aucs.append(auc)
        except ValueError:
            continue

    return np.mean(aucs) if aucs else float("nan"), len(aucs)


def compute_topk_accuracy(y_true, y_pred, race_ids, k=1):
    """Compute top-k accuracy: fraction of races where winner is in model's top k picks."""
    df = pd.DataFrame({"y_true": y_true, "y_pred": y_pred, "race_id": race_ids})
    correct = 0
    total = 0
    for _, group in df.groupby("race_id"):
        if group["y_true"].sum() == 0:
            continue
        total += 1
        topk = group.nlargest(k, "y_pred")
        if topk["y_true"].sum() > 0:
            correct += 1
    return correct / total if total > 0 else float("nan")


def compute_platt_slope(y_true, y_pred):
    """Fit logistic regression on log-odds of predictions. Returns slope, intercept."""
    y_pred = np.clip(y_pred, 1e-7, 1 - 1e-7)
    log_odds = np.log(y_pred / (1 - y_pred)).reshape(-1, 1)
    lr = LogisticRegression(penalty=None, solver="lbfgs", max_iter=1000)
    lr.fit(log_odds, y_true)
    return lr.coef_[0][0], lr.intercept_[0]


# ─── Main Validation ───────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("MODEL VALIDATION REPORT")
    print("=" * 70)

    # ── Step 1: Prepare data ──────────────────────────────────────────────
    print("\n[1/6] Preparing training data (parallel feature computation)...")
    pipeline = ModelTrainingPipeline(db_path=DB_PATH, config_path=CONFIG_PATH)

    data = pipeline.prepare_training_data(date(2023, 1, 1), date(2023, 12, 31))
    data = pipeline.add_target_column(data)

    pipeline.config["model"]["splits"]["train"] = {"start": "2023-01-01", "end": "2023-06-30"}
    pipeline.config["model"]["splits"]["validation"] = {"start": "2023-07-01", "end": "2023-09-30"}
    pipeline.config["model"]["splits"]["test"] = {"start": "2023-10-01", "end": "2023-12-31"}
    train_df, val_df, test_df = pipeline.split_data(data)

    print(f"      Train: {len(train_df):,} entries, {train_df['race_id'].nunique():,} races")
    print(f"      Val:   {len(val_df):,} entries, {val_df['race_id'].nunique():,} races")
    print(f"      Test:  {len(test_df):,} entries, {test_df['race_id'].nunique():,} races")
    print(f"      Win rate: {100 * test_df['is_winner'].mean():.1f}%")

    # ── Step 2: Train model ───────────────────────────────────────────────
    print("\n[2/6] Training model...")
    feature_cols = [c for c in FEATURE_COLUMNS if c in train_df.columns]
    X_train = prepare_feature_matrix(train_df, feature_cols)
    X_val = prepare_feature_matrix(val_df, feature_cols)
    X_test = prepare_feature_matrix(test_df, feature_cols)
    y_train = train_df["is_winner"].values
    y_val = val_df["is_winner"].values
    y_test = test_df["is_winner"].values

    model = RacingLightGBM(params={
        "n_estimators": 500, "learning_rate": 0.05, "max_depth": 6,
        "feature_fraction": 0.8, "bagging_fraction": 0.8,
        "reg_alpha": 0.1, "reg_lambda": 0.1,
    })
    model.fit(X_train, y_train, eval_set=(X_val, y_val))
    print(f"      Best iteration: {model.model.best_iteration}")
    print(f"      Features: {len(feature_cols)}")

    # ── Step 3: Generate predictions at each stage ────────────────────────
    print("\n[3/6] Generating predictions...")

    # Race IDs and field sizes for test set
    test_race_ids = test_df["race_id"].values
    test_field_sizes = get_field_sizes(test_df)

    # Stage 1: Raw predictions
    raw_preds = model.predict_raw(X_test)

    # Stage 2: Softmax-normalized (no calibration)
    softmax_preds = softmax_by_race(raw_preds, test_race_ids)

    # Stage 3: Calibrated + softmax
    calibrator = FieldSizeCalibrator()
    val_raw = model.predict_raw(X_val)
    val_field_sizes = get_field_sizes(val_df)
    calibrator.fit(val_raw, y_val, val_field_sizes)

    calibrated_raw = calibrator.calibrate(raw_preds, test_field_sizes)
    calibrated_preds = softmax_by_race(calibrated_raw, test_race_ids)

    # ── Step 4: Baselines ─────────────────────────────────────────────────
    print("\n[4/6] Computing baselines...")

    # Naive: 1/field_size
    naive_preds = np.zeros(len(y_test))
    for race_id in np.unique(test_race_ids):
        mask = test_race_ids == race_id
        field_size = mask.sum()
        naive_preds[mask] = 1.0 / field_size

    # Morning line baseline
    ml_odds_col = "morning_line_odds"
    if ml_odds_col in test_df.columns:
        ml_raw = test_df[ml_odds_col].fillna(0).values.astype(float)
        # Convert to-one odds to implied probability: 1/(odds+1)
        ml_implied = np.where(ml_raw > 0, 1.0 / (ml_raw + 1), 0.0)

        # Normalize within race
        ml_preds = np.zeros_like(ml_implied)
        for race_id in np.unique(test_race_ids):
            mask = test_race_ids == race_id
            total = ml_implied[mask].sum()
            if total > 0:
                ml_preds[mask] = ml_implied[mask] / total
            else:
                ml_preds[mask] = 1.0 / mask.sum()
        has_ml = True
    else:
        ml_preds = naive_preds.copy()
        ml_raw = np.zeros(len(y_test))
        has_ml = False
        print("      WARNING: morning_line_odds not found, using naive as ML baseline")

    # ── Step 5: Compute all metrics ───────────────────────────────────────
    print("\n[5/6] Computing metrics...")

    n_test = len(y_test)
    n_races = len(np.unique(test_race_ids))
    win_rate = y_test.mean()

    print(f"\n{'=' * 70}")
    print(f"Test set: {n_test:,} entries across {n_races:,} races (Oct-Dec 2023)")
    print(f"Win rate: {100 * win_rate:.1f}%")
    print(f"{'=' * 70}")

    # --- DISCRIMINATION ---
    print(f"\n--- DISCRIMINATION ---")
    header = f"{'':30s} {'Model(raw)':>14s} {'Model(smx)':>14s} {'Model(cal)':>14s} {'MorningLine':>14s}"
    print(header)
    print("-" * len(header))

    # Entry-wise AUC
    raw_auc = roc_auc_score(y_test, raw_preds)
    ml_auc = roc_auc_score(y_test, ml_preds) if has_ml else float("nan")
    print(f"{'Entry-wise ROC-AUC':30s} {raw_auc:14.4f} {'—':>14s} {'—':>14s} {ml_auc:14.4f}")

    # Per-race AUC (all)
    pr_raw, pr_raw_n = compute_per_race_auc(y_test, raw_preds, test_race_ids)
    pr_smx, _ = compute_per_race_auc(y_test, softmax_preds, test_race_ids)
    pr_cal, _ = compute_per_race_auc(y_test, calibrated_preds, test_race_ids)
    pr_ml, _ = compute_per_race_auc(y_test, ml_preds, test_race_ids)
    print(f"{'Per-race AUC (all, n=' + str(pr_raw_n) + ')':30s} {pr_raw:14.4f} {pr_smx:14.4f} {pr_cal:14.4f} {pr_ml:14.4f}")

    # Per-race AUC (excluding short-price favorites)
    pr_raw_nf, pr_raw_nf_n = compute_per_race_auc(
        y_test, raw_preds, test_race_ids,
        exclude_short_fav=True, morning_line_odds=ml_raw)
    pr_smx_nf, _ = compute_per_race_auc(
        y_test, softmax_preds, test_race_ids,
        exclude_short_fav=True, morning_line_odds=ml_raw)
    pr_cal_nf, _ = compute_per_race_auc(
        y_test, calibrated_preds, test_race_ids,
        exclude_short_fav=True, morning_line_odds=ml_raw)
    pr_ml_nf, _ = compute_per_race_auc(
        y_test, ml_preds, test_race_ids,
        exclude_short_fav=True, morning_line_odds=ml_raw)
    print(f"{'Per-race AUC (no fav, n=' + str(pr_raw_nf_n) + ')':30s} {pr_raw_nf:14.4f} {pr_smx_nf:14.4f} {pr_cal_nf:14.4f} {pr_ml_nf:14.4f}")

    # --- CALIBRATION ---
    print(f"\n--- CALIBRATION ---")
    header2 = f"{'':30s} {'Model(smx)':>14s} {'Model(cal)':>14s} {'MorningLine':>14s} {'Naive':>14s}"
    print(header2)
    print("-" * len(header2))

    # Brier
    b_smx = brier_score_loss(y_test, softmax_preds)
    b_cal = brier_score_loss(y_test, calibrated_preds)
    b_ml = brier_score_loss(y_test, ml_preds)
    b_naive = brier_score_loss(y_test, naive_preds)
    print(f"{'Brier Score':30s} {b_smx:14.4f} {b_cal:14.4f} {b_ml:14.4f} {b_naive:14.4f}")

    # Log loss
    eps = 1e-7
    ll_smx = sklearn_log_loss(y_test, np.clip(softmax_preds, eps, 1 - eps))
    ll_cal = sklearn_log_loss(y_test, np.clip(calibrated_preds, eps, 1 - eps))
    ll_ml = sklearn_log_loss(y_test, np.clip(ml_preds, eps, 1 - eps))
    ll_naive = sklearn_log_loss(y_test, np.clip(naive_preds, eps, 1 - eps))
    print(f"{'Log Loss':30s} {ll_smx:14.4f} {ll_cal:14.4f} {ll_ml:14.4f} {ll_naive:14.4f}")

    # ECE (equal-width)
    ece_smx_ew = compute_ece(y_test, softmax_preds, n_bins=5, strategy="uniform")
    ece_cal_ew = compute_ece(y_test, calibrated_preds, n_bins=5, strategy="uniform")
    ece_ml_ew = compute_ece(y_test, ml_preds, n_bins=5, strategy="uniform")
    ece_naive_ew = compute_ece(y_test, naive_preds, n_bins=5, strategy="uniform")
    print(f"{'ECE (5 equal-width)':30s} {ece_smx_ew:14.4f} {ece_cal_ew:14.4f} {ece_ml_ew:14.4f} {ece_naive_ew:14.4f}")

    # ECE (quantile)
    ece_smx_q = compute_ece(y_test, softmax_preds, n_bins=5, strategy="quantile")
    ece_cal_q = compute_ece(y_test, calibrated_preds, n_bins=5, strategy="quantile")
    ece_ml_q = compute_ece(y_test, ml_preds, n_bins=5, strategy="quantile")
    ece_naive_q = compute_ece(y_test, naive_preds, n_bins=5, strategy="quantile")
    print(f"{'ECE (5 quantile)':30s} {ece_smx_q:14.4f} {ece_cal_q:14.4f} {ece_ml_q:14.4f} {ece_naive_q:14.4f}")

    # Top-k
    t1_smx = compute_topk_accuracy(y_test, softmax_preds, test_race_ids, k=1)
    t1_cal = compute_topk_accuracy(y_test, calibrated_preds, test_race_ids, k=1)
    t1_ml = compute_topk_accuracy(y_test, ml_preds, test_race_ids, k=1)
    print(f"{'Top-1 Accuracy':30s} {t1_smx:13.1%} {t1_cal:13.1%} {t1_ml:13.1%} {'—':>14s}")

    t3_smx = compute_topk_accuracy(y_test, softmax_preds, test_race_ids, k=3)
    t3_cal = compute_topk_accuracy(y_test, calibrated_preds, test_race_ids, k=3)
    t3_ml = compute_topk_accuracy(y_test, ml_preds, test_race_ids, k=3)
    print(f"{'Top-3 Accuracy':30s} {t3_smx:13.1%} {t3_cal:13.1%} {t3_ml:13.1%} {'—':>14s}")

    # --- CALIBRATION SLOPE ---
    print(f"\n--- CALIBRATION SLOPE (Platt) ---")
    slope_smx, intercept_smx = compute_platt_slope(y_test, softmax_preds)
    slope_cal, intercept_cal = compute_platt_slope(y_test, calibrated_preds)

    print(f"Softmax only:  slope={slope_smx:.3f}  intercept={intercept_smx:.3f}")
    print(f"Calibrated:    slope={slope_cal:.3f}  intercept={intercept_cal:.3f}")
    print(f"(Perfect calibration = slope 1.000, intercept 0.000)")

    if slope_cal > 1.05:
        print("Interpretation: Model is UNDERCONFIDENT (predictions too conservative)")
    elif slope_cal < 0.95:
        print("Interpretation: Model is OVERCONFIDENT (predictions too extreme)")
    else:
        print("Interpretation: Model is reasonably well-calibrated")

    # ── Step 6: Leakage smoke test ────────────────────────────────────────
    print(f"\n--- LEAKAGE SMOKE TEST ---")
    print("[6/6] Testing 100 random test races for feature leakage...")

    from features.feature_engine import FeatureEngine

    test_races = test_df["race_id"].unique()
    sample_races = random.sample(list(test_races), min(100, len(test_races)))

    engine = FeatureEngine(db_path=DB_PATH)
    passed = 0
    failed = 0
    failure_details = []

    # Get the features we computed during training for comparison
    # These are already in test_df
    check_features = ["career_win_rate", "best_speed_90_days", "jockey_win_rate_30d",
                      "trainer_win_rate_30d", "avg_speed_90_days"]
    available_checks = [f for f in check_features if f in test_df.columns]

    for race_id in sample_races:
        race_date_str = race_id.split("_")[1]  # e.g., "USA_2023-10-15_3" -> "2023-10-15"
        try:
            race_dt = date.fromisoformat(race_date_str)
        except ValueError:
            continue

        # Recompute features fresh
        fresh_features = engine.calculate_all_features(race_id, race_dt)
        if not fresh_features:
            continue

        fresh_df = pd.DataFrame(fresh_features)

        # Compare against training-time features
        training_entries = test_df[test_df["race_id"] == race_id]

        race_ok = True
        for entry_id in training_entries["entry_id"].values:
            fresh_entry = fresh_df[fresh_df["entry_id"] == entry_id]
            train_entry = training_entries[training_entries["entry_id"] == entry_id]

            if fresh_entry.empty or train_entry.empty:
                continue

            for feat in available_checks:
                if feat not in fresh_entry.columns:
                    continue
                fresh_val = fresh_entry[feat].iloc[0]
                train_val = train_entry[feat].iloc[0]

                if pd.isna(fresh_val) and pd.isna(train_val):
                    continue
                if pd.isna(fresh_val) != pd.isna(train_val):
                    race_ok = False
                    failure_details.append(f"  {race_id}/{entry_id}: {feat} NaN mismatch")
                    break
                if abs(fresh_val - train_val) > 1e-6:
                    race_ok = False
                    failure_details.append(
                        f"  {race_id}/{entry_id}: {feat} changed "
                        f"({train_val:.6f} -> {fresh_val:.6f})"
                    )
                    break

        if race_ok:
            passed += 1
        else:
            failed += 1

    engine.close()

    total_tested = passed + failed
    print(f"{total_tested} races tested: {passed}/{total_tested} passed")
    if failure_details:
        print(f"FAILURES ({failed}):")
        for detail in failure_details[:10]:
            print(detail)
        if len(failure_details) > 10:
            print(f"  ... and {len(failure_details) - 10} more")
    else:
        print("No leakage detected.")

    print(f"\n{'=' * 70}")


if __name__ == "__main__":
    main()
