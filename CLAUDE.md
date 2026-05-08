# CLAUDE.md — Horse Racing Quantitative Betting System

Reference for Claude (and humans) working in this repo. Covers architecture, pipeline, files, config, database, models, dependencies, and gotchas.

## 1. Project Overview

End-to-end ML system for betting on US thoroughbred horse racing using 2023 Equibase data.

Pipeline: **Raw XML (PPs + Result Charts) → extraction → standardization → SQLite DB → point-in-time feature engineering (41+ features) → LightGBM with race-grouped softmax → field-size-stratified isotonic calibration → backtesting + Streamlit dashboard**.

## 2. Quick Start Commands

All commands run from the project root (`F:/racing`).

| Task | Command |
|---|---|
| Launch dashboard | `streamlit run app/app.py --server.port 8501` → `http://localhost:8501` |
| Train model | `python train_model.py` |
| Train with version tag | `python train_model.py --version v1.4` |
| Quick-mode training (subset) | `python train_model.py --quick` |
| Validate training setup only | `python train_model.py --dry-run` |
| Run a single backtest strategy | `python run_backtest.py --model artifacts/models/v1.3 --strategy kelly` |
| Compare all 6 strategies | `python run_backtest.py --model artifacts/models/v1.3 --compare` |
| Extract everything from XML | `python run_full_extraction.py` |
| Run test suite | `pytest tests/` |

Strategies accepted by `--strategy`: `flat`, `kelly`, `value`, `toppick`, `morning_favorite`, `momentum`.

## 3. Directory Layout

```
F:/racing/
├── app/                     # Streamlit multi-page dashboard
│   ├── app.py               # Entry point — landing page + sidebar
│   ├── pages/               # Auto-routed pages (numbered)
│   ├── components/          # Sidebar, charts, metrics, tooltips
│   └── utils/               # db.py, betting.py, features.py helpers
├── models/                  # ML pipeline (training, model, calibration, eval)
├── features/                # Feature engineering (point-in-time)
├── backtesting/             # Backtester + 6 strategies
├── config/                  # config.yaml, database.py, postgres_schema.sql
├── database/                # Schema files & migrations
├── artifacts/               # Trained models and plots (versioned)
├── scripts/                 # Utility/maintenance scripts
├── tests/                   # pytest suite
├── docs/                    # Design docs, phase prompts, feature priorities
├── 2023 PPs/                # Raw Past Performance XML (~5,925 files, ~1.7 GB)
├── 2023 Result Charts/      # Raw Result Chart XML (~4,906 files)
├── racing_data.db           # SQLite database (~252 MB, populated)
├── enhanced_schema.sql      # Canonical DDL
├── train_model.py           # Training CLI
├── run_backtest.py          # Backtest CLI
├── run_full_extraction.py   # Extraction orchestrator
├── extract_horses.py        # Phase 1 extractor
├── extract_past_performance.py   # Phase 2 extractor
├── extract_result_charts.py      # Phase 3 extractor
├── standardization.py       # Distance / track / race-type normalization
├── requirements.txt         # Full deps
├── requirements-minimal.txt # Core deps only
├── plan.md                  # Original project plan
└── IMPLEMENTATION_REPORT.md # Bug-fix report (3 critical bugs fixed)
```

## 4. Data Pipeline Flow

1. **Raw XML**
   - `2023 PPs/SIMD*.xml[.zip]` — Past Performance files
   - `2023 Result Charts/TCHM*.xml[.zip]` — Result Charts

2. **Extraction** (each script can run standalone; `run_full_extraction.py` orchestrates all three)
   - `extract_horses.py` → `horses_master`, `trainers`, `owners`
   - `extract_past_performance.py` → `races_standardized`, `race_entries_standardized`, equipment, medication, fractions
   - `extract_result_charts.py` → `race_wagering`, `horse_position_calls`, final results

3. **Standardization** — `standardization.py` normalizes distances (yards → furlongs), track codes, race types, surfaces, equipment, conditions. Critical: three bugs here were fixed in `IMPLEMENTATION_REPORT.md` (distance off by 100–1600×, wrong track codes, misclassified maiden-claiming).

4. **Database** — SQLite `racing_data.db` is the default data store. PostgreSQL is supported via `config/database.py` and `database/postgres_schema.sql` but is **not** the default despite `config.yaml` setting `database.type: postgresql` — the app layer currently reads SQLite.

5. **Feature engineering** — `features/feature_engine.py` orchestrates:
   - `rolling_stats.py` — horse form over 14/30/60-day windows by surface/distance
   - `track_bias.py` — post-position and speed bias per track/surface/distance bucket
   - `pace_calculator.py` — per-horse pace metrics from fractional times
   - `speed_adjustments.py` — normalized speed figures
   - `validation.py` — sanity checks
   Produces 41+ columns per entry, enforcing **no data leakage** (as-of-race-date only).

6. **Model training** — `train_model.py` → `models/training_pipeline.py`:
   - Time-based splits per `config.yaml`: train 2023-01-01→06-30, val 2023-07-01→09-30, test 2023-10-01→12-31
   - `models/lightgbm_model.py` — `RacingLightGBM` wrapper with race-grouped softmax so per-race probabilities sum to 1
   - `models/calibration.py` — `FieldSizeCalibrator` fits separate isotonic curves for small/medium/large fields
   - `models/evaluation.py` — Brier, ECE, MCE, log loss, ROC-AUC, calibration/ROC/feature-importance plots
   - Saves to `artifacts/models/<version>/` with `model.pkl`, `calibrator.pkl`, `metadata.json`, plots

7. **Backtesting / Dashboard** — `backtesting/backtester.py` replays historical races through a chosen strategy; `app/` visualizes metrics, race predictions, and lets you retrain / re-backtest interactively.

## 5. Critical Files

| Purpose | Path |
|---|---|
| Dashboard entry | `app/app.py` |
| Dashboard pages | `app/pages/1_Dashboard.py … 6_Settings.py` |
| Shared sidebar | `app/components/sidebar.py` |
| Training CLI | `train_model.py` |
| Backtest CLI | `run_backtest.py` |
| Extraction orchestrator | `run_full_extraction.py` |
| Standardization | `standardization.py` |
| Main config | `config/config.yaml` |
| DB connection | `config/database.py` |
| Canonical schema | `enhanced_schema.sql` (also `database/postgres_schema.sql`) |
| SQLite DB | `racing_data.db` |
| Training pipeline | `models/training_pipeline.py` |
| Model wrapper | `models/lightgbm_model.py` |
| Calibration | `models/calibration.py` |
| Evaluation | `models/evaluation.py` |
| Feature list | `models/feature_definitions.py` |
| Feature engine | `features/feature_engine.py` |
| Backtester | `backtesting/backtester.py` |
| Strategies | `backtesting/strategies.py` |

## 6. Dashboard Pages

| Page | File | Purpose |
|---|---|---|
| Dashboard | `app/pages/1_Dashboard.py` | System stats, DB counts, model health |
| Data Management | `app/pages/2_Data_Management.py` | Upload XML, trigger extraction, browse tables |
| Model Training | `app/pages/3_Model_Training.py` | Train / retrain, view metrics, compare versions |
| Backtesting | `app/pages/4_Backtesting.py` | Run strategies, compare ROI, view bet logs |
| Race Predictions | `app/pages/5_Race_Predictions.py` | Per-race win probabilities, EV, bet recs |
| Settings | `app/pages/6_Settings.py` | Edit betting config, bankroll, hyperparameters |

## 7. Database

- **File:** `racing_data.db` (SQLite, ~252 MB, already populated with 2023 data).
- **Schema:** `enhanced_schema.sql` (canonical DDL).
- **Core tables:** `horses_master`, `races_standardized`, `race_entries_standardized`, `race_wagering`, `trainers`, `owners`.
- **Reference tables:** `course_types`, `race_types`, `equipment_types`, `track_conditions`.
- **Normalized child tables:** `horse_race_equipment`, `horse_race_medication`, `horse_position_calls`, `race_fractions`.
- **Views:** `vw_horse_performance_summary`, `vw_race_entries_complete`.
- **PostgreSQL alternative:** `database/postgres_schema.sql` (+ TimescaleDB hooks in `config.yaml`), not currently active.

Useful one-liners:
```
sqlite3 racing_data.db ".tables"
sqlite3 racing_data.db "SELECT COUNT(*) FROM races_standardized;"
python check_database.py
```

## 8. Models & Artifacts

`artifacts/models/vX.Y/` contains:
- `model.pkl` — trained LightGBM
- `calibrator.pkl` — field-size isotonic calibrator
- `metadata.json` — metrics, config snapshot, feature list, train timestamp
- `calibration_plot.png`, `roc_curve.png`, `feature_importance.png`

Current versions: `v1.2`, `v1.3` (latest — ROC-AUC 0.734, Brier 0.102, ECE 0.0023, log loss 0.345).

Load in code:
```python
from models import RacingLightGBM, FieldSizeCalibrator
model = RacingLightGBM.load("artifacts/models/v1.3/model.pkl")
cal = FieldSizeCalibrator.load("artifacts/models/v1.3/calibrator.pkl")
```

## 9. Configuration — `config/config.yaml`

Headline knobs (full file is annotated):

- **Betting:** `fractional_kelly: 0.25`, `min_ev_threshold: 0.08`, `min_probability: 0.08`, `min_overlay: 1.20`, `max_odds: 15.0`, `max_per_race_pct: 0.02`, `daily_loss_limit_pct: 0.10`, `min_bet_amount: 2.0`.
- **Bankroll:** `initial: 2000.0` USD, reduce stakes 50% at −20%, pause at −30%, scale up at +25%.
- **Tracks:** high-volume (CD, SAR, BEL, GP, SA, DMR, KEE, AQU) and regional (TP, CT, PEN, LRL, TAM, FG, OP, GG, PRM, IND). Split-testing 50/50 is enabled.
- **Model hyperparameters:** LightGBM `n_estimators=500`, `max_depth=6`, `learning_rate=0.05`, `num_leaves=31`, `min_child_samples=20`, `subsample=0.8`, `colsample_bytree=0.8`, `reg_alpha=0.1`, `reg_lambda=0.1`, `early_stopping_rounds=50`.
- **Calibration:** isotonic, field-size buckets 5–7 / 8–10 / 11–20.
- **Data splits:** train 2023-01→06, val 2023-07→09, test 2023-10→12.
- **Feature windows:** 14/30/60 days.
- **Dashboard:** port 8501, NY timezone.
- **Execution (future / live-bet):** TwinSpires primary, DraftKings backup. Not yet wired to a live platform.
- **MLflow:** tracking URI `http://localhost:5000`, experiment `racing_model`.

Note: `database.type` is set to `postgresql` in config but current code paths use SQLite (`racing_data.db`). Treat SQLite as authoritative until a migration is wired up end-to-end.

## 10. Betting Strategies (`backtesting/strategies.py`)

1. `FlatBetStrategy` — fixed unit per qualifying bet
2. `KellyCriterionStrategy` — fractional Kelly stake sizing
3. `ValueBettingStrategy` — EV / overlay-gated bets
4. `TopPickStrategy` — bet the model's #1 in each race
5. `MorningFavoriteStrategy` — bet the morning-line favorite (baseline)
6. `MomentumStrategy` — form/trend-weighted selection

## 11. Dependencies

`requirements.txt` key pins:
- **NumPy 1.24–1.x** — **NOT** 2.x. A compatibility shim in `models/__init__.py` protects against accidental upgrades.
- Pandas 2.0–2.2, LightGBM 4.0+, scikit-learn 1.3+, SciPy 1.11+, Optuna 3+
- lxml 4.9+ (XML parsing), PyYAML 6+ (config)
- Streamlit 1.28+, Plotly 5.18+, Matplotlib 3.7+, Seaborn 0.12+
- FastAPI 0.100+ / Uvicorn 0.23+ (future API)
- MLflow 2.8+ (experiment tracking)
- pytest 7.4+ (testing)

`requirements-minimal.txt` — core ML path only, no dev/test extras.

## 12. Testing

```
pytest tests/                    # full suite (models, features, integration)
pytest tests/test_models.py
pytest tests/test_pace_calculator.py
pytest tests/test_speed_adjustments.py
```

Root-level extraction tests: `test_extraction.py`, `test_extraction_scripts.py`, `test_standardization.py`, `test_integration.py`, `test_single_extraction.py`. Per `IMPLEMENTATION_REPORT.md`, the last full run was 22/22 passing after the distance / track-code / race-type fixes.

## 13. Known Issues & Gotchas

- **NumPy must stay on 1.x.** Upgrading breaks LightGBM and the shim in `models/__init__.py`.
- **Pre-fix DB rows may have corrupted distances** (100–1600× off). If anything looks wrong in older data, re-run `python run_full_extraction.py`. Details in `IMPLEMENTATION_REPORT.md`.
- **SQLite is authoritative** even though `config.yaml` declares PostgreSQL — the full Postgres path isn't wired end-to-end.
- **Shell cwd resets between tool calls** on this Windows setup. When scripting with Claude Code, use absolute paths (`F:/racing/...`) or prefix commands with `cd F:/racing && …`.
- **Live betting is not implemented.** `execution:` config is forward-looking; there is no broker integration today.
- **Dashboard is local-only.** No auth, no TLS — don't expose port 8501 beyond localhost.

## 14. Reference Documentation

Already in the repo — read these rather than duplicating:

- `plan.md` — original project plan and scope
- `IMPLEMENTATION_REPORT.md` — bug-fix report (3 critical fixes, test results)
- `artifacts/README.md` — artifact / versioning conventions
- `docs/PROJECT_STATE_AND_FEATURE_PRIORITIES.md` — feature inventory & priorities
- `docs/IMPLEMENTATION_PROMPT_PACE_AND_SPEED.md` — pace/speed feature design
- `docs/PHASE2_CONTINUATION_PROMPT.md`, `docs/PHASE3_CONTINUATION_PROMPT.md` — phase handoffs
- `userguide.md` — short human-facing launch guide
