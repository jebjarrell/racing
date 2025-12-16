# Horse Racing Quantitative Betting System - Architecture Overview

**Version:** 1.0
**Last Updated:** 2025-12-16
**Status:** Draft

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Goals](#2-system-goals)
3. [High-Level Architecture](#3-high-level-architecture)
4. [Component Responsibilities](#4-component-responsibilities)
5. [Data Flow](#5-data-flow)
6. [Technology Stack](#6-technology-stack)
7. [Database Architecture](#7-database-architecture)
8. [API Specifications](#8-api-specifications)
9. [Deployment Architecture](#9-deployment-architecture)
10. [Security Considerations](#10-security-considerations)

---

## 1. Executive Summary

This document describes the architecture of a quantitative horse racing betting system designed to:

- Generate win probability predictions for US thoroughbred races
- Calculate expected value (EV) against market odds
- Produce risk-managed bet recommendations using fractional Kelly criterion
- Track performance through comprehensive monitoring and calibration

**MVP Scope (v1):**
- Win bets only (no exotics)
- US tracks available via TwinSpires/DraftKings
- Manual bet execution based on system recommendations
- $2,000 starting bankroll

---

## 2. System Goals

### 2.1 Primary Objectives

| Objective | Target | Measurement |
|-----------|--------|-------------|
| Predictive accuracy | Brier Score < 0.20 | Out-of-time test set |
| Profitability | ROI > 3% (backtest) | Historical simulation |
| Risk management | Max drawdown < 30% | Rolling 30-day tracking |
| Calibration | ECE < 0.03 | Field-size stratified |

### 2.2 Design Principles

1. **Point-in-Time Integrity**: All features computed using only pre-race information
2. **Conservative Sizing**: Fractional Kelly (0.25x) to manage variance
3. **Transparency**: Full logging of predictions, recommendations, and outcomes
4. **Modularity**: Independent components with clear interfaces
5. **Reproducibility**: Version-controlled models with tracked experiments

---

## 3. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           EXTERNAL DATA SOURCES                              │
├─────────────────────────────────────────────────────────────────────────────┤
│   Equibase XML Files          │   Live Odds API (Future)                    │
│   - Past Performance (PP)      │   - TwinSpires / DraftKings                │
│   - Result Charts (RC)         │   - Real-time price feeds                  │
└──────────────┬─────────────────┴────────────────────┬───────────────────────┘
               │                                      │
               ▼                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          DATA INGESTION LAYER                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │ HorseExtractor  │  │ PPExtractor     │  │ RCExtractor     │              │
│  │ (horses,        │  │ (pre-race       │  │ (results,       │              │
│  │  trainers,      │  │  entries,       │  │  payoffs,       │              │
│  │  owners)        │  │  equipment)     │  │  fractions)     │              │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘              │
│           │                    │                    │                        │
│           └────────────────────┼────────────────────┘                        │
│                                ▼                                             │
│                    ┌───────────────────────┐                                 │
│                    │  RacingDataStandardizer│                                │
│                    │  (normalization,       │                                │
│                    │   type mapping)        │                                │
│                    └───────────┬───────────┘                                 │
└────────────────────────────────┼─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          DATA STORAGE LAYER                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                     PostgreSQL + TimescaleDB                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   racing    │  │  features   │  │   betting   │  │ monitoring  │         │
│  │   schema    │  │   schema    │  │   schema    │  │   schema    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘         │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       FEATURE ENGINEERING LAYER                               │
├──────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  RollingStats   │  │  TrackBias      │  │  FeatureEngine  │              │
│  │  (14/30/60 day  │  │  (post position │  │  (relative      │              │
│  │   windows)      │  │   by surface)   │  │   features)     │              │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘              │
│           │                    │                    │                        │
│           └────────────────────┼────────────────────┘                        │
│                                ▼                                             │
│                    ┌───────────────────────┐                                 │
│                    │  LeakageValidator     │                                 │
│                    │  (point-in-time       │                                 │
│                    │   validation)         │                                 │
│                    └───────────┬───────────┘                                 │
└────────────────────────────────┼─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          MODELING LAYER                                       │
├──────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  ModelTrainer   │  │  Calibrator     │  │  Predictor      │              │
│  │  (LightGBM      │  │  (isotonic      │  │  (inference,    │              │
│  │   training)     │  │   regression)   │  │   batch/single) │              │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘              │
│           │                    │                    │                        │
│           └────────────────────┼────────────────────┘                        │
│                                ▼                                             │
│                    ┌───────────────────────┐                                 │
│                    │  Race-Grouped         │                                 │
│                    │  Softmax Normalization│                                 │
│                    └───────────┬───────────┘                                 │
└────────────────────────────────┼─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         STRATEGY LAYER                                        │
├──────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  EVCalculator   │  │  PositionSizer  │  │  BankrollMgr    │              │
│  │  (overlay,      │  │  (Kelly 0.25x,  │  │  (limits,       │              │
│  │   filters)      │  │   constraints)  │  │   tracking)     │              │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘              │
│           │                    │                    │                        │
│           └────────────────────┼────────────────────┘                        │
│                                ▼                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  SplitTestMgr   │  │  Sensitivity    │  │  ScratchHandler │              │
│  │  (high-vol vs   │  │  Analyzer       │  │  (recompute on  │              │
│  │   regional)     │  │  (odds gap)     │  │   scratch)      │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                      RECOMMENDATION ENGINE                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │ RecommendEngine │  │  BetTicket      │  │  BetLogger      │              │
│  │  (generate      │  │  (format for    │  │  (discipline,   │              │
│  │   recs)         │  │   manual exec)  │  │   slippage)     │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       PRESENTATION LAYER                                      │
├──────────────────────────────────────────────────────────────────────────────┤
│                      Dashboard (FastAPI + Streamlit)                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │ Live Recs View  │  │ Daily Summary   │  │ Analytics View  │              │
│  │                 │  │                 │  │                 │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
│  ┌─────────────────┐                                                         │
│  │ Model Health    │                                                         │
│  │                 │                                                         │
│  └─────────────────┘                                                         │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       MONITORING LAYER                                        │
├──────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │ PerformanceTrack│  │ CalibrationMon  │  │ FeatureValidator│              │
│  │  (ROI, Sharpe,  │  │  (drift detect, │  │  (leakage       │              │
│  │   drawdown)     │  │   Brier score)  │  │   audits)       │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Component Responsibilities

### 4.1 Data Ingestion Layer

| Component | Responsibility | Input | Output |
|-----------|---------------|-------|--------|
| `HorseExtractor` | Extract horse, trainer, owner master data | Equibase XML | `horses_master`, `trainers`, `owners` |
| `PastPerformanceExtractor` | Extract pre-race entries and equipment | PP XML files | `races_standardized`, `race_entries_standardized` |
| `ResultChartExtractor` | Extract race results and payoffs | RC XML files | Updates to races with results |
| `RacingDataStandardizer` | Normalize categorical fields | Raw data | Standardized codes |

### 4.2 Feature Engineering Layer

| Component | Responsibility | Key Methods |
|-----------|---------------|-------------|
| `FeatureEngine` | Orchestrate feature computation | `calculate_all_features()` |
| `RollingStats` | Time-windowed trainer/jockey stats | `calculate_trainer_rolling_stats()` |
| `TrackBias` | Post position bias by track×surface | `calculate_post_position_bias()` |
| `LeakageValidator` | Verify point-in-time integrity | `validate_no_leakage()` |

### 4.3 Modeling Layer

| Component | Responsibility | Key Methods |
|-----------|---------------|-------------|
| `WinProbabilityTrainer` | Train GBM models | `train_gbm_model()` |
| `ProbabilityCalibrator` | Field-size stratified calibration | `apply_isotonic_regression()` |
| `WinProbabilityPredictor` | Generate predictions | `predict_race()`, `predict_races_batch()` |

### 4.4 Strategy Layer

| Component | Responsibility | Key Methods |
|-----------|---------------|-------------|
| `EVCalculator` | Calculate expected value | `calculate_ev()`, `apply_filters()` |
| `PositionSizer` | Kelly criterion sizing | `calculate_kelly_stake()` |
| `BankrollManager` | Track bankroll state | `get_current_bankroll()`, `check_daily_limit()` |
| `SplitTestManager` | Track high-vol vs regional | `track_segment_performance()` |
| `SensitivityAnalyzer` | Odds gap analysis | `generate_sensitivity_table()` |
| `ScratchHandler` | Recompute on scratches | `recompute_predictions()` |

### 4.5 Recommendation Layer

| Component | Responsibility | Key Methods |
|-----------|---------------|-------------|
| `BetRecommendationEngine` | Generate bet tickets | `generate_recommendations()` |
| `BetLogger` | Log execution and outcomes | `log_execution()`, `get_discipline_report()` |

### 4.6 Presentation Layer

| Component | Responsibility |
|-----------|---------------|
| Dashboard API (FastAPI) | Serve recommendation and performance data |
| Dashboard UI (Streamlit) | Display live recommendations and analytics |

### 4.7 Monitoring Layer

| Component | Responsibility | Key Methods |
|-----------|---------------|-------------|
| `PerformanceTracker` | ROI, Sharpe, drawdown metrics | `calculate_roi()`, `calculate_sharpe_ratio()` |
| `CalibrationMonitor` | Track calibration drift | `detect_calibration_drift()` |
| `FeatureValidator` | Audit feature pipeline | `check_temporal_leakage()` |

---

## 5. Data Flow

### 5.1 Historical Data Pipeline (Batch)

```
Equibase XML Files
       │
       ▼
┌──────────────────┐
│  Extractors      │  (parallel processing, 45 workers)
│  - Horses        │
│  - PPs           │
│  - Result Charts │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Standardization │  (normalize codes, parse restrictions)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  PostgreSQL      │  (racing schema)
│  - races         │
│  - entries       │
│  - results       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Feature Engine  │  (point-in-time strict)
│  - Rolling stats │
│  - Track bias    │
│  - Relative feats│
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Model Training  │  (time-based splits)
│  Jan-Jun: Train  │
│  Jul-Sep: Valid  │
│  Oct-Dec: Test   │
└──────────────────┘
```

### 5.2 Live Prediction Pipeline (Real-Time)

```
Race Card Available
       │
       ▼
┌──────────────────┐
│  Fetch Entries   │  (T-60 min before first post)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Feature Engine  │  (compute features for today's races)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Predictor       │  (generate win probabilities)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  EV Calculator   │  (compare to market odds)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Position Sizer  │  (Kelly sizing with constraints)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Recommendation  │  (generate bet tickets)
│  Engine          │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Dashboard       │  (display to user)
└──────────────────┘
```

### 5.3 Scratch Update Flow

```
Scratch Detected
       │
       ▼
┌──────────────────┐
│  ScratchHandler  │  (detect new scratches)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Recompute       │  (recalculate relative features)
│  Features        │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Regenerate      │  (new softmax probabilities)
│  Predictions     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Update          │  (new bet tickets)
│  Recommendations │
└──────────────────┘
```

---

## 6. Technology Stack

### 6.1 Core Technologies

| Category | Technology | Rationale |
|----------|------------|-----------|
| Language | Python 3.10+ | ML ecosystem, data processing |
| Database | PostgreSQL 15 + TimescaleDB | Time-series, ACID compliance |
| ML Framework | LightGBM | Fast training, good calibration |
| Data Processing | pandas, polars | Efficient dataframe operations |
| XML Parsing | lxml | High-performance XML processing |
| Web Framework | FastAPI | Async API, automatic docs |
| Dashboard | Streamlit | Rapid prototyping, interactive |
| Experiment Tracking | MLflow | Model versioning, metrics |

### 6.2 Development Tools

| Category | Technology |
|----------|------------|
| Testing | pytest |
| Linting | ruff |
| Formatting | black |
| Type Checking | mypy |
| CI/CD | GitHub Actions |
| Containerization | Docker |

### 6.3 Python Dependencies

```
# Core
python>=3.10
pandas>=2.0
polars>=0.19
numpy>=1.24
scipy>=1.11

# Database
psycopg2-binary>=2.9
sqlalchemy>=2.0
timescaledb

# ML
lightgbm>=4.0
scikit-learn>=1.3
optuna>=3.0

# API/Dashboard
fastapi>=0.100
uvicorn>=0.23
streamlit>=1.28

# Data Processing
lxml>=4.9
pyyaml>=6.0

# Experiment Tracking
mlflow>=2.8

# Development
pytest>=7.4
ruff>=0.1
black>=23.0
mypy>=1.5
```

---

## 7. Database Architecture

### 7.1 Schema Organization

```
PostgreSQL Database: racing_db
├── racing (schema)          # Core racing data
│   ├── horses_master
│   ├── trainers
│   ├── owners
│   ├── races
│   ├── race_entries
│   ├── race_fractions
│   ├── horse_position_calls
│   └── race_wagering
│
├── features (schema)        # Computed features
│   ├── trainer_rolling_stats
│   ├── jockey_rolling_stats
│   ├── track_bias_stats
│   └── daily_features (hypertable)
│
├── models (schema)          # Model artifacts
│   ├── model_versions
│   ├── model_metrics
│   └── feature_importance
│
├── betting (schema)         # Betting operations
│   ├── bet_recommendations
│   ├── bet_log
│   ├── bankroll_snapshots
│   └── split_test_results
│
└── monitoring (schema)      # Performance tracking
    ├── predictions
    ├── calibration_metrics
    └── validation_runs
```

### 7.2 Key Tables

#### racing.races
```sql
CREATE TABLE racing.races (
    race_id VARCHAR(100) PRIMARY KEY,
    track_code VARCHAR(10) NOT NULL,
    race_date DATE NOT NULL,
    race_number INTEGER NOT NULL,
    course_type_code VARCHAR(20),
    race_type_code VARCHAR(20),
    track_condition VARCHAR(20),
    distance_yards INTEGER,
    purse_usd DECIMAL(12,2),
    class_level INTEGER,
    winning_time DECIMAL(8,3),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### features.trainer_rolling_stats
```sql
CREATE TABLE features.trainer_rolling_stats (
    trainer_id VARCHAR(20),
    calculation_date DATE,
    window_days INTEGER,
    starts INTEGER,
    wins INTEGER,
    win_rate DECIMAL(5,4),
    roi DECIMAL(8,4),
    PRIMARY KEY (trainer_id, calculation_date, window_days)
);
```

#### betting.bet_log
```sql
CREATE TABLE betting.bet_log (
    log_id SERIAL PRIMARY KEY,
    recommendation_id VARCHAR(50),
    race_id VARCHAR(100),
    horse_registration VARCHAR(20),
    recommended_stake DECIMAL(10,2),
    recommended_odds DECIMAL(8,2),
    model_probability DECIMAL(5,4),
    expected_value DECIMAL(5,4),
    executed BOOLEAN,
    actual_stake DECIMAL(10,2),
    actual_odds DECIMAL(8,2),
    outcome VARCHAR(20),
    payout DECIMAL(10,2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 7.3 TimescaleDB Hypertables

```sql
-- Create hypertable for daily features
SELECT create_hypertable('features.daily_features', 'calculation_date');

-- Create hypertable for bet log
SELECT create_hypertable('betting.bet_log', 'created_at');

-- Create hypertable for predictions
SELECT create_hypertable('monitoring.predictions', 'prediction_time');
```

---

## 8. API Specifications

### 8.1 Dashboard API Endpoints

#### Recommendations

```
GET /api/recommendations/live
Response:
{
    "race": {
        "race_id": "CD_2025-12-16_5",
        "track": "Churchill Downs",
        "race_number": 5,
        "post_time": "2025-12-16T15:15:00-05:00",
        "minutes_to_post": 12
    },
    "recommendations": [
        {
            "horse_name": "Fast Mover",
            "program_number": "4",
            "model_probability": 0.223,
            "market_odds": 5.0,
            "expected_value": 0.12,
            "recommended_stake": 8.00
        }
    ],
    "race_total": 12.00,
    "daily_remaining": 188.00
}
```

#### Performance

```
GET /api/performance/metrics
Response:
{
    "roi": {
        "all_time": 0.035,
        "last_30_days": 0.042,
        "last_7_days": 0.028
    },
    "sharpe_ratio": 0.52,
    "max_drawdown": {
        "value": 0.18,
        "start_date": "2025-11-15",
        "end_date": "2025-11-28"
    },
    "total_bets": 347,
    "win_rate": 0.19
}
```

#### Calibration

```
GET /api/calibration/current
Response:
{
    "brier_score": 0.185,
    "expected_calibration_error": 0.024,
    "calibration_by_bucket": [
        {"bucket": "0-10%", "predicted": 0.05, "actual": 0.048},
        {"bucket": "10-20%", "predicted": 0.15, "actual": 0.158},
        ...
    ],
    "drift_detected": false
}
```

### 8.2 Internal Module Interfaces

#### FeatureEngine Interface

```python
class FeatureEngine:
    def calculate_all_features(
        self,
        race_id: str
    ) -> Dict[str, Dict[str, float]]:
        """
        Returns: {horse_reg: {feature_name: value, ...}, ...}
        """
        pass

    def validate_features(
        self,
        race_id: str,
        feature_matrix: Dict
    ) -> bool:
        """
        Validates point-in-time integrity.
        """
        pass
```

#### Predictor Interface

```python
class WinProbabilityPredictor:
    def predict_race(
        self,
        race_id: str
    ) -> Dict[str, float]:
        """
        Returns: {horse_reg: probability, ...}
        Sum of probabilities = 1.0
        """
        pass
```

#### BetRecommendationEngine Interface

```python
class BetRecommendationEngine:
    def generate_recommendations(
        self,
        race_id: str
    ) -> List[BetRecommendation]:
        """
        Returns list of qualified bets with stakes.
        """
        pass
```

---

## 9. Deployment Architecture

### 9.1 Development Environment

```
Local Development
├── Python virtual environment
├── PostgreSQL (local or Docker)
├── SQLite (for testing)
└── VS Code / PyCharm
```

### 9.2 Production Environment (MVP)

```
Single Server Deployment
├── Ubuntu 22.04 LTS
├── PostgreSQL 15 + TimescaleDB
├── Python 3.10 + application code
├── Cron jobs for scheduled tasks
├── Nginx (reverse proxy for dashboard)
└── Systemd services
```

### 9.3 Scheduled Jobs

| Job | Schedule | Description |
|-----|----------|-------------|
| `fetch_race_cards.py` | 6:00 AM ET | Fetch day's race cards |
| `generate_predictions.py` | 6:30 AM ET | Generate initial predictions |
| `refresh_odds.py` | Every 10 min | Update odds and recommendations |
| `record_results.py` | 11:00 PM ET | Record day's results |
| `daily_report.py` | 11:30 PM ET | Generate daily summary |

---

## 10. Security Considerations

### 10.1 Data Security

- Database credentials stored in environment variables
- No sensitive data (passwords, API keys) in code
- PostgreSQL SSL connections enabled

### 10.2 Application Security

- Input validation on all API endpoints
- Rate limiting on dashboard API
- No automated betting (manual execution only)

### 10.3 Operational Security

- Regular database backups
- Model versioning with rollback capability
- Logging of all predictions and recommendations

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| EV | Expected Value - (probability × odds) - 1 |
| Kelly | Kelly Criterion - optimal bet sizing formula |
| Brier Score | Mean squared error of probability predictions |
| ECE | Expected Calibration Error |
| Overlay | (model_prob - market_prob) / market_prob |
| Point-in-Time | Using only data available before race |
| Softmax | Normalization ensuring probabilities sum to 1 |

---

## Appendix B: Related Documents

- [Feature Catalog](../features/feature_catalog.md)
- [Model Specification](../models/probability_model_spec.md)
- [Betting Rules](../strategy/betting_rules.md)
- [Data Dictionary](../data/data_dictionary.md)
- [Configuration Guide](../config/configuration_guide.md)
- [Platform Verification](../platform/platform_verification.md)

---

*Document maintained by: Engineering Team*
*Review cycle: Monthly*
