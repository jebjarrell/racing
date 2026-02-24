"""Model Training - Train/retrain LightGBM model via ModelTrainingPipeline."""

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from app.components.sidebar import render_sidebar, get_available_models, load_config
from app.components.charts import feature_importance_chart, calibration_plot, roc_curve_chart

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
    )
    train_end = st.date_input(
        "Train end",
        value=date.fromisoformat(splits_config.get("train", {}).get("end", "2023-06-30")),
    )
    val_start = st.date_input(
        "Validation start",
        value=date.fromisoformat(splits_config.get("validation", {}).get("start", "2023-07-01")),
    )
    val_end = st.date_input(
        "Validation end",
        value=date.fromisoformat(splits_config.get("validation", {}).get("end", "2023-09-30")),
    )
    test_start = st.date_input(
        "Test start",
        value=date.fromisoformat(splits_config.get("test", {}).get("start", "2023-10-01")),
    )
    test_end = st.date_input(
        "Test end",
        value=date.fromisoformat(splits_config.get("test", {}).get("end", "2023-12-31")),
    )

with col_right:
    st.markdown("**Hyperparameters**")
    n_estimators = st.number_input("n_estimators", value=hyper_config.get("n_estimators", 500), min_value=50, max_value=5000, step=50)
    max_depth = st.number_input("max_depth", value=hyper_config.get("max_depth", 6), min_value=2, max_value=15)
    learning_rate = st.number_input("learning_rate", value=hyper_config.get("learning_rate", 0.05), min_value=0.001, max_value=0.5, step=0.005, format="%.3f")
    subsample = st.slider("subsample", 0.1, 1.0, hyper_config.get("subsample", 0.8), 0.05)
    reg_alpha = st.number_input("reg_alpha (L1)", value=hyper_config.get("reg_alpha", 0.1), min_value=0.0, max_value=10.0, step=0.1)
    reg_lambda = st.number_input("reg_lambda (L2)", value=hyper_config.get("reg_lambda", 0.1), min_value=0.0, max_value=10.0, step=0.1)

    version_name = st.text_input("Model version", value="v1.2")

st.markdown("---")

# --- Run Training ---
if st.button("Start Training", type="primary"):
    try:
        from models.training_pipeline import ModelTrainingPipeline, FEATURE_COLUMNS
        from models.lightgbm_model import RacingLightGBM
        from models.calibration import FieldSizeCalibrator
        from models.evaluation import ModelEvaluator
        import numpy as np

        pipeline = ModelTrainingPipeline(db_path="racing_data.db", config_path="config/config.yaml")

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

        # Step 3: Split
        with st.status("Step 3: Splitting data...", expanded=True) as status:
            train_df, val_df, test_df = pipeline.split_data(data)
            status.update(
                label=f"Split: train={len(train_df):,}, val={len(val_df):,}, test={len(test_df):,}",
                state="complete",
            )

        # Step 4: Prepare features
        feature_cols = [c for c in FEATURE_COLUMNS if c in train_df.columns]

        X_train = train_df[feature_cols].fillna(0)
        y_train = train_df["is_winner"]
        X_val = val_df[feature_cols].fillna(0)
        y_val = val_df["is_winner"]
        X_test = test_df[feature_cols].fillna(0)
        y_test = test_df["is_winner"]

        # Step 5: Train model
        with st.status("Step 4: Training LightGBM model...", expanded=True) as status:
            params = {
                "n_estimators": n_estimators,
                "learning_rate": learning_rate,
                "max_depth": max_depth,
                "feature_fraction": subsample,
                "bagging_fraction": subsample,
                "reg_alpha": reg_alpha,
                "reg_lambda": reg_lambda,
            }
            model = RacingLightGBM(params=params)
            model.fit(X_train, y_train, eval_set=(X_val, y_val))
            status.update(label="Model trained", state="complete")

        # Step 6: Calibrate
        with st.status("Step 5: Calibrating probabilities...", expanded=True) as status:
            raw_val_probs = model.predict_raw(X_val)
            val_field_sizes = val_df["field_size"].fillna(8).values if "field_size" in val_df.columns else np.full(len(val_df), 8)

            calibrator = FieldSizeCalibrator()
            calibrator.fit(raw_val_probs, y_val.values, val_field_sizes)
            status.update(label="Calibration fitted", state="complete")

        # Step 7: Evaluate
        with st.status("Step 6: Evaluating on test set...", expanded=True) as status:
            raw_test_probs = model.predict_raw(X_test)
            test_field_sizes = test_df["field_size"].fillna(8).values if "field_size" in test_df.columns else np.full(len(test_df), 8)
            calibrated_probs = calibrator.calibrate(raw_test_probs, test_field_sizes)

            evaluator = ModelEvaluator()
            brier = evaluator.calculate_brier_score(calibrated_probs, y_test.values)
            logloss = evaluator.calculate_log_loss(calibrated_probs, y_test.values)
            roc_auc = evaluator.calculate_roc_auc(calibrated_probs, y_test.values)
            cal_metrics = evaluator.calculate_calibration_error(calibrated_probs, y_test.values)

            status.update(label=f"Evaluation: AUC={roc_auc:.4f}, Brier={brier:.4f}", state="complete")

        # Step 8: Save
        with st.status("Step 7: Saving model artifacts...", expanded=True) as status:
            save_dir = Path("artifacts/models") / version_name
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
                json.dump(metadata, f, indent=2)

            # Save evaluation plots
            evaluator.generate_calibration_plot(calibrated_probs, y_test.values, str(save_dir / "calibration_plot.png"))
            evaluator.generate_roc_curve(calibrated_probs, y_test.values, str(save_dir / "roc_curve.png"))

            importance = model.get_feature_importance()
            evaluator.generate_feature_importance_plot(importance, save_path=str(save_dir / "feature_importance.png"))

            status.update(label=f"Saved to `artifacts/models/{version_name}/`", state="complete")

        # --- Display Results ---
        st.success(f"Training complete! Model saved as **{version_name}**")

        mc1, mc2, mc3, mc4, mc5 = st.columns(5)
        mc1.metric("ROC-AUC", f"{roc_auc:.4f}")
        mc2.metric("Brier Score", f"{brier:.4f}")
        mc3.metric("ECE", f"{cal_metrics['ece']:.4f}")
        mc4.metric("Log Loss", f"{logloss:.4f}")
        mc5.metric("Features", len(feature_cols))

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
                    rel_data.get("bin_counts", []),
                ),
                use_container_width=True,
            )

        # ROC curve
        from sklearn.metrics import roc_curve as sk_roc_curve
        fpr, tpr, _ = sk_roc_curve(y_test.values, calibrated_probs)
        st.plotly_chart(roc_curve_chart(fpr, tpr, roc_auc), use_container_width=True)

        st.cache_data.clear()

    except Exception as e:
        st.error(f"Training failed: {e}")
        import traceback
        st.code(traceback.format_exc())
