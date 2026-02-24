# Web Interface Implementation Plan (Revised)

## Overview

Build a Streamlit multi-page application for the horse racing quantitative betting system. The app provides data management, model training, backtesting, single-race predictions, and performance monitoring — all from a local browser UI.

**Tech choice: Streamlit** — already specified in config (port 8501), Python-native (matches project), ideal for data dashboards, no frontend build step.

---

## What Already Exists (from main)

The entire backend is complete. No backend code needs to be written:

| Module | Status | Key APIs |
|--------|--------|----------|
| `models/training_pipeline.py` | Complete | `ModelTrainingPipeline.run_full_pipeline()`, `.prepare_training_data()`, `.train_model()`, `.evaluate_model()`, `.save_model()` |
| `models/lightgbm_model.py` | Complete | `RacingLightGBM.fit(X, y, eval_set)`, `.predict_proba(X, race_ids)`, `.predict_raw(X)`, `.get_feature_importance()`, `.save()/.load()` |
| `models/calibration.py` | Complete | `FieldSizeCalibrator.fit()`, `.calibrate(y_pred, field_sizes)`, `.save()/.load()` |
| `models/evaluation.py` | Complete | `ModelEvaluator.calculate_brier_score()`, `.calculate_calibration_error()`, `.generate_calibration_plot()`, `.generate_roc_curve()`, `.generate_feature_importance_plot()`, `.generate_evaluation_report()` |
| `models/serialization.py` | Complete | Model save/load utilities |
| `models/feature_definitions.py` | Complete | Feature column definitions |
| `backtesting/backtester.py` | Complete | `Backtester.run(strategy, start, end, bankroll)`, `.compare_strategies()` |
| `backtesting/strategies.py` | Complete | `FlatBetStrategy`, `KellyCriterionStrategy`, `ValueBettingStrategy`, `TopPickStrategy`, `MomentumStrategy`, `MorningFavoriteStrategy` |
| `features/feature_engine.py` | Complete | `FeatureEngine.calculate_all_features()`, `.calculate_features_for_date_range()` |
| `train_model.py` | Complete | CLI for training |
| `run_backtest.py` | Complete | CLI for backtesting |
| `requirements.txt` | Complete | All deps including streamlit |
| `artifacts/models/v1.0/` | Complete | Trained model (AUC 0.78, ECE 0.003), calibrator, metadata |

**Scope: Build only the Streamlit UI, wiring into existing backends.**

---

## Architecture

```
racing/
├── app/                           # NEW - Streamlit application
│   ├── app.py                     # Main entry point (multi-page setup)
│   ├── pages/
│   │   ├── 1_Dashboard.py         # System overview & DB stats
│   │   ├── 2_Data_Management.py   # Upload XML, trigger extraction, view DB
│   │   ├── 3_Model_Training.py    # Train/retrain via ModelTrainingPipeline
│   │   ├── 4_Backtesting.py       # Backtester with strategy selection
│   │   ├── 5_Race_Predictions.py  # Single-race prediction & recommendations
│   │   └── 6_Settings.py          # Edit config parameters
│   └── components/
│       ├── charts.py              # Reusable Plotly chart builders
│       └── sidebar.py             # Shared sidebar (model status, bankroll)
├── models/                        # EXISTS - ML pipeline
├── backtesting/                   # EXISTS - Backtesting engine
├── features/                      # EXISTS - Feature engineering
├── artifacts/                     # EXISTS - Trained models
├── config/config.yaml             # EXISTS - Configuration
└── requirements.txt               # EXISTS - Dependencies
```

---

## Implementation Steps

### Step 1: Project Setup

Create `app/`, `app/pages/`, `app/components/` directories. Add `plotly` to requirements.txt (not currently listed; needed for interactive Streamlit charts).

---

### Step 2: Shared Components

**`app/components/sidebar.py`** — Renders on every page via import:
- Scans `artifacts/models/` for available model versions
- Shows current model version, training date, key metrics (AUC, ECE) from `metadata.json`
- Shows bankroll from config
- Alert badges: calibration drift warning if ECE > 0.03

**`app/components/charts.py`** — Reusable Plotly chart builders:
- `bankroll_curve(bankroll_history)` → Plotly line chart
- `calibration_plot(predicted_means, observed_means, bin_counts)` → Reliability diagram
- `feature_importance_chart(importance_dict, top_n)` → Horizontal bar
- `daily_pnl_chart(bets)` → Bar chart grouped by date
- `roc_curve_chart(y_true, y_pred)` → Interactive ROC
- `bet_distribution_chart(bets)` → Histograms of bet sizes, odds, EV

---

### Step 3: Dashboard Page (`pages/1_Dashboard.py`)

System overview. Shows at a glance:

- **Database stats**: Total races, entries, horses, date range, DB file size
  - Query `races_standardized` for count + min/max `race_date`
  - Query `race_entries_standardized` for entry count
  - Query `horses_master` for horse count
  - `os.path.getsize('racing_data.db')`
- **Model status card**: version, date trained, AUC, ECE, Brier from `metadata.json`
- **Config summary**: Kelly fraction, EV threshold, bankroll, min bet
- **Recent races table**: Last 20 races from DB with track, date, #entries, purse
- **Top performers**: Top 10 trainers/jockeys by win rate (30-day rolling, min 20 starts)

---

### Step 4: Data Management Page (`pages/2_Data_Management.py`)

**Upload section:**
- `st.file_uploader()` accepting `.xml` files (multiple=True)
- Detect file type from filename: `SIMD*` = Past Performance, `TCHM*` = Result Chart
- Save to `data/uploads/` staging directory
- Show file list with detected types and sizes

**Extraction section:**
- "Run Extraction" button → subprocess call to existing scripts:
  - `python extract_horses.py`
  - `python extract_past_performance.py`
  - `python extract_result_charts.py`
- Stream stdout/stderr to `st.code()` container
- Progress indicator via `st.spinner()`

**Database browser:**
- Table selector dropdown
- Date range filter for race tables
- Track filter multiselect
- `st.dataframe()` with row counts, pagination

---

### Step 5: Model Training Page (`pages/3_Model_Training.py`)

Uses existing `ModelTrainingPipeline` directly.

**Training configuration panel:**
- Date range pickers for train/val/test (defaults from `config.yaml` splits)
- Hyperparameter overrides: n_estimators, max_depth, learning_rate, subsample, etc.
- Version string input (e.g., "v1.1")
- "Start Training" button

**Training execution:**
- Instantiate `ModelTrainingPipeline(db_path, config_path)`
- Call `prepare_training_data()` → `add_target_column()` → `split_data()` → `train_model()` → `evaluate_model()` → `save_model()`
- Show progress via `st.status()` expander per step
- Cache results in `st.session_state`

**Results display:**
- Metrics row: ROC-AUC, Log Loss, Brier, ECE, Top-1 Accuracy, Top-3 Accuracy
- Feature importance chart (Plotly, from `model.get_feature_importance()`)
- Calibration plot (Plotly, from evaluation data)
- ROC curve (Plotly)
- Data split summary table (rows, races, winners per split)

**Existing model viewer:**
- List all model versions in `artifacts/models/`
- Click to view any version's metrics + plots (from saved PNG artifacts)

---

### Step 6: Backtesting Page (`pages/4_Backtesting.py`)

Uses existing `Backtester` and `BettingStrategy` subclasses.

**Configuration panel (sidebar):**
- Model version selector (dropdown of available versions)
- Strategy selector: Flat, Kelly, Value, TopPick, Momentum, MorningFavorite
- Strategy-specific params (dynamic based on selection):
  - Kelly: fraction slider (0.05-0.50), min_edge slider (0-0.20), max_bet_fraction
  - Flat: bet_amount, min_prob, max_odds
  - Value: bet_fraction, min_edge, min_prob, max_prob
  - etc.
- Date range pickers (default: test period from config)
- Starting bankroll input
- "Run Backtest" button

**Results display:**
- Summary metrics row: ROI, Profit, Total Bets, Win Rate, Avg Odds, Max Drawdown
- **Bankroll curve** (Plotly line chart from `results.bankroll_history`)
- **Daily P&L** (Plotly bar chart, aggregate `results.bets` by date)
- **Bet log table**: `st.dataframe()` of all bets with date, horse, prob, odds, stake, outcome, P&L
- **Strategy comparison mode**: "Compare All" button → `backtester.compare_strategies()` → results table + overlaid bankroll curves

---

### Step 7: Race Predictions Page (`pages/5_Race_Predictions.py`)

**Race selection:**
- Date picker + track dropdown (populated from distinct tracks in DB) + race number dropdown
- "Load Race" button → queries `races_standardized` + `race_entries_standardized`
- Shows race context: track, distance, surface, conditions, class, purse, field size

**Predictions panel:**
- Loads model from `artifacts/models/{version}/model.pkl` and calibrator via `RacingLightGBM.load()` + `FieldSizeCalibrator.load()`
- Calculates features via `FeatureEngine.calculate_all_features(race_id, race_date)`
- Runs `model.predict_proba(X, race_ids)` → `calibrator.calibrate(raw_probs, field_sizes)`
- Results table per horse:
  - Program #, horse name, post position
  - Model probability (color-gradient)
  - Morning line odds, actual odds (if available)
  - Implied probability (1/decimal_odds)
  - EV = (model_prob × decimal_odds) - 1
  - Overlay = model_prob / implied_prob
  - Kelly fraction + recommended stake
  - Qualifying bet indicator (passes all config filters)
- Sorted by model probability descending
- Highlighted rows for +EV qualifying bets

**Feature inspection:**
- Expandable per horse showing computed features
- Speed figure comparison bar chart across field

---

### Step 8: Settings Page (`pages/6_Settings.py`)

Reads from and writes back to `config/config.yaml`.

Sections with input widgets:
- **Betting**: fractional_kelly, min_ev_threshold, min_probability, min_overlay, max_odds, max_per_race_pct, daily_loss_limit_pct, min_bet_amount
- **Bankroll**: initial, reduce_stakes_threshold, pause_threshold
- **Track classifications**: multiselect for high_volume / regional lists
- **Model hyperparameters**: n_estimators, max_depth, learning_rate, etc.

"Save Configuration" button → writes YAML, shows diff of changes.

---

## Key Design Decisions

1. **Streamlit session state** for model/calibrator caching — load once via `@st.cache_resource`, reuse across pages
2. **Subprocess-based extraction** — existing extraction scripts (23K+ LOC each) called via `subprocess.run()`
3. **SQLite only** — local development, all DB access via `sqlite3` (matches existing code)
4. **Model artifacts on disk** — `.pkl` files in `artifacts/models/{version}/` (existing convention)
5. **No auth** — single user, local only
6. **Plotly for interactive charts** — Streamlit's native `st.plotly_chart()` integration; existing `ModelEvaluator` matplotlib plots used for saved artifacts only
7. **Wire into existing APIs** — no new backend code. The UI is a pure frontend layer calling `models.*`, `backtesting.*`, and `features.*`

---

## Execution Order

| Step | What | Depends On |
|------|------|------------|
| 1 | Directory structure + plotly dep | Nothing |
| 2 | Shared components (sidebar, charts) | Step 1 |
| 3 | Dashboard page | Step 2 |
| 4 | Data Management page | Step 2 |
| 5 | Model Training page | Step 2 |
| 6 | Backtesting page | Step 2 |
| 7 | Race Predictions page | Step 2 |
| 8 | Settings page | Step 2 |

Steps 3-8 are independent of each other and can be built in any order after Step 2.

---

## Running the App

```bash
pip install -r requirements.txt
streamlit run app/app.py --server.port 8501
```
