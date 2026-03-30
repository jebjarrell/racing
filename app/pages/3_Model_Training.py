"""Model Training - Train/retrain LightGBM model via ModelTrainingPipeline."""

import json
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from app.components.sidebar import render_sidebar, get_available_models, load_config, PROJECT_ROOT
from app.components.charts import feature_importance_chart, calibration_plot, roc_curve_chart
from app.components.tooltips import METRICS, HYPERPARAMS, SPLITS
from app.utils.features import prepare_feature_matrix, get_field_sizes
from app.utils.db import streamlit_error_boundary, db_path_default
from app.components.metrics_display import display_model_metrics

render_sidebar()

st.title("Model Training")
st.markdown("---")

config = load_config()
model_config = config.get("model", {})
splits_config = model_config.get("splits", {})
hyper_config = model_config.get("hyperparameters", {})


# --- Existing Models ---
st.subheader("Existing Models")

models = get_available_models()
if models:
    for m in models:
        metrics = m.get("metrics", {})
        st.markdown(
            f"**{m['version']}** -- "
            f"AUC: {metrics.get('roc_auc', 0):.4f} | "
            f"Brier: {metrics.get('brier_score', 0):.4f} | "
            f"ECE: {metrics.get('ece', 0):.4f} | "
            f"Features: {len(m.get('feature_columns', []))}"
        )
else:
    st.info("No trained models found.")

st.markdown("---")


# --- Training Configuration ---
st.subheader("Train New Model")

col_left, col_right = st.columns(2)

with col_left:
    st.markdown("**Data Splits**")
    train_start = st.date_input(
        "Train start",
        value=date.fromisoformat(splits_config.get("train", {}).get("start", "2023-01-01")),
        help=SPLITS["train"],
    )
    train_end = st.date_input(
        "Train end",
        value=date.fromisoformat(splits_config.get("train", {}).get("end", "2023-06-30")),
    )
    val_start = st.date_input(
        "Validation start",
        value=date.fromisoformat(splits_config.get("validation", {}).get("start", "2023-07-01")),
        help=SPLITS["validation"],
    )
    val_end = st.date_input(
        "Validation end",
        value=date.fromisoformat(splits_config.get("validation", {}).get("end", "2023-09-30")),
    )
    test_start = st.date_input(
        "Test start",
        value=date.fromisoformat(splits_config.get("test", {}).get("start", "2023-10-01")),
        help=SPLITS["test"],
    )
    test_end = st.date_input(
        "Test end",
        value=date.fromisoformat(splits_config.get("test", {}).get("end", "2023-12-31")),
    )

with col_right:
    st.markdown("**Hyperparameters**")
    n_estimators = st.number_input("n_estimators", value=hyper_config.get("n_estimators", 500), min_value=50, max_value=5000, step=50, help=HYPERPARAMS["n_estimators"])
    max_depth = st.number_input("max_depth", value=hyper_config.get("max_depth", 6), min_value=2, max_value=15, help=HYPERPARAMS["max_depth"])
    learning_rate = st.number_input("learning_rate", value=hyper_config.get("learning_rate", 0.05), min_value=0.001, max_value=0.5, step=0.005, format="%.3f", help=HYPERPARAMS["learning_rate"])
    subsample = st.slider("subsample (row)", 0.1, 1.0, hyper_config.get("subsample", 0.8), 0.05, help=HYPERPARAMS["subsample"])
    colsample = st.slider("colsample (column)", 0.1, 1.0, hyper_config.get("colsample_bytree", 0.8), 0.05, help="Fraction of features sampled per tree. Controls column subsampling.")
    reg_alpha = st.number_input("reg_alpha (L1)", value=hyper_config.get("reg_alpha", 0.1), min_value=0.0, max_value=10.0, step=0.1, help=HYPERPARAMS["reg_alpha"])
    reg_lambda = st.number_input("reg_lambda (L2)", value=hyper_config.get("reg_lambda", 0.1), min_value=0.0, max_value=10.0, step=0.1, help=HYPERPARAMS["reg_lambda"])

    version_name = st.text_input("Model version", value="v1.2")

st.markdown("---")

# --- Validation ---
valid_version = bool(version_name and re.match(r'^[a-zA-Z0-9._-]+$', version_name))
if version_name and not valid_version:
    st.warning("Version name must contain only letters, numbers, dots, hyphens, and underscores.")

# --- Run Training ---
if st.button("Start Training", type="primary", disabled=not valid_version):
    with streamlit_error_boundary("Training"):
        from models.training_pipeline import ModelTrainingPipeline, FEATURE_COLUMNS
        from models.lightgbm_model import RacingLightGBM
        from models.calibration import FieldSizeCalibrator
        from models.evaluation import ModelEvaluator
        import numpy as np

        save_dir = PROJECT_ROOT / "artifacts" / "models" / version_name
        if save_dir.exists():
            st.warning(f"Version `{version_name}` already exists and will be overwritten.")

        pipeline = ModelTrainingPipeline(
            db_path=db_path_default(),
            config_path=str(PROJECT_ROOT / "config" / "config.yaml"),
        )

        # Step 1: Prepare data
        with st.status("Step 1: Generating features...", expanded=True) as status:
            full_start = min(train_start, val_start, test_start)
            full_end = max(train_end, val_end, test_end)
            data = pipeline.prepare_training_data(full_start, full_end)
            status.update(label=f"Features generated: {len(data):,} entries", state="complete")

        # Step 2: Add targets
        with st.status("Step 2: Adding target labels...", expanded=True) as status:
            data = pipeline.add_target_column(data)
            winners = data["is_winner"].sum()
            status.update(label=f"Targets: {winners:,} winners / {len(data):,} entries", state="complete")

        # Step 3: Split (override pipeline config with user-selected dates)
        with st.status("Step 3: Splitting data...", expanded=True) as status:
            pipeline.config['model']['splits']['train'] = {'start': str(train_start), 'end': str(train_end)}
            pipeline.config['model']['splits']['validation'] = {'start': str(val_start), 'end': str(val_end)}
            pipeline.config['model']['splits']['test'] = {'start': str(test_start), 'end': str(test_end)}
            train_df, val_df, test_df = pipeline.split_data(data)
            status.update(
                label=f"Split: train={len(train_df):,}, val={len(val_df):,}, test={len(test_df):,}",
                state="complete",
            )

        # Step 4: Prepare features
        with st.status("Step 4: Preparing features...", expanded=True) as status:
            feature_cols = [c for c in FEATURE_COLUMNS if c in train_df.columns]
            X_train = prepare_feature_matrix(train_df, feature_cols)
            y_train = train_df["is_winner"]
            X_val = prepare_feature_matrix(val_df, feature_cols)
            y_val = val_df["is_winner"]
            X_test = prepare_feature_matrix(test_df, feature_cols)
            y_test = test_df["is_winner"]
            status.update(label=f"Prepared {len(feature_cols)} features", state="complete")

        # Step 5: Train model
        with st.status("Step 5: Training LightGBM model...", expanded=True) as status:
            params = {
                "n_estimators": n_estimators,
                "learning_rate": learning_rate,
                "max_depth": max_depth,
                "feature_fraction": colsample,
                "bagging_fraction": subsample,
                "reg_alpha": reg_alpha,
                "reg_lambda": reg_lambda,
            }
            model = RacingLightGBM(params=params)
            model.fit(X_train, y_train, eval_set=(X_val, y_val))
            status.update(label="Model trained", state="complete")

        # Step 6: Calibrate
        with st.status("Step 6: Calibrating probabilities...", expanded=True) as status:
            raw_val_probs = model.predict_raw(X_val)
            val_field_sizes = get_field_sizes(val_df)

            calibrator = FieldSizeCalibrator()
            calibrator.fit(raw_val_probs, y_val.values, val_field_sizes)
            status.update(label="Calibration fitted", state="complete")

        # Step 7: Evaluate
        with st.status("Step 7: Evaluating on test set...", expanded=True) as status:
            raw_test_probs = model.predict_raw(X_test)
            test_field_sizes = get_field_sizes(test_df)
            calibrated_probs = calibrator.calibrate(raw_test_probs, test_field_sizes)

            evaluator = ModelEvaluator()
            brier = evaluator.calculate_brier_score(calibrated_probs, y_test.values)
            logloss = evaluator.calculate_log_loss(calibrated_probs, y_test.values)
            roc_auc = evaluator.calculate_roc_auc(calibrated_probs, y_test.values)
            cal_metrics = evaluator.calculate_calibration_error(calibrated_probs, y_test.values)

            status.update(label=f"Evaluation: AUC={roc_auc:.4f}, Brier={brier:.4f}", state="complete")

        # Step 8: Save
        with st.status("Step 8: Saving model artifacts...", expanded=True) as status:
            save_dir.mkdir(parents=True, exist_ok=True)

            model.save(str(save_dir / "model.pkl"))
            calibrator.save(str(save_dir / "calibrator.pkl"))

            metadata = {
                "version": version_name,
                "timestamp": pd.Timestamp.now().isoformat(),
                "config": config,
                "feature_columns": feature_cols,
                "metrics": {
                    "brier_score": float(brier),
                    "ece": float(cal_metrics["ece"]),
                    "mce": float(cal_metrics["mce"]),
                    "roc_auc": float(roc_auc),
                    "log_loss": float(logloss),
                },
                "model_type": "RacingLightGBM",
                "calibration_method": "isotonic",
            }
            with open(save_dir / "metadata.json", "w") as f:
                json.dump(metadata, f, indent=2, default=str)

            # Save evaluation plots
            evaluator.generate_calibration_plot(calibrated_probs, y_test.values, str(save_dir / "calibration_plot.png"))
            evaluator.generate_roc_curve(calibrated_probs, y_test.values, str(save_dir / "roc_curve.png"))

            importance = model.get_feature_importance()
            evaluator.generate_feature_importance_plot(importance, save_path=str(save_dir / "feature_importance.png"))

            status.update(label=f"Saved to `artifacts/models/{version_name}/`", state="complete")

        # --- Display Results ---
        st.success(f"Training complete! Model saved as **{version_name}**")

        display_model_metrics(
            {"roc_auc": roc_auc, "brier_score": brier, "ece": cal_metrics["ece"], "log_loss": logloss},
            show_features=True, feature_count=len(feature_cols),
        )

        # Feature importance chart
        st.plotly_chart(
            feature_importance_chart(importance, top_n=25),
            use_container_width=True,
        )

        # Calibration plot
        rel_data = cal_metrics.get("reliability_diagram_data", {})
        if rel_data:
            st.plotly_chart(
                calibration_plot(
                    rel_data.get("bin_confidences", []),
                    rel_data.get("bin_accuracies", []),
                ),
                use_container_width=True,
            )

        # ROC curve
        from sklearn.metrics import roc_curve as sk_roc_curve
        fpr, tpr, _ = sk_roc_curve(y_test.values, calibrated_probs)
        st.plotly_chart(roc_curve_chart(fpr, tpr, roc_auc), use_container_width=True)

        st.cache_data.clear()
        st.cache_resource.clear()
