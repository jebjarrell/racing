# Phase 2 Continuation Prompt

**Project**: Horse Racing Quantitative Betting System
**Date**: 2025-12-16
**Status**: Phase 0-1 Complete, Ready for Phase 2

---

## Context for Next Conversation

Use this prompt to continue development in a new conversation:

```
I'm continuing development of a horse racing quantitative betting system. Phases 0-1 are complete.

Please review the implementation plan at: docs/PHASE2_CONTINUATION_PROMPT.md

Then continue with Phase 2: Feature Engineering Layer, which includes:
1. Create features/feature_engine.py - main feature calculation engine
2. Create features/rolling_stats.py - trainer/jockey rolling statistics
3. Create features/track_bias.py - post position and track bias calculations
4. Create features/validation.py - leakage validation framework
5. Extend standardization.py with speed/pace/class calculation methods

The existing codebase has:
- Complete documentation suite in docs/
- PostgreSQL schema in database/postgres_schema.sql
- Migration scripts in database/migrations/
- Database connection module in config/database.py
- Configuration in config/config.yaml
- Existing data extraction pipeline (extract_*.py files)
- SQLite database with 2023 Equibase data (racing_data.db, 248MB)
```

---

## What Has Been Completed

### Phase 0: Documentation Foundation ✅

| File | Description |
|------|-------------|
| `docs/architecture/system_overview.md` | System architecture, data flow diagrams, component responsibilities |
| `docs/features/feature_catalog.md` | 115 features across 7 categories with formulas |
| `docs/models/probability_model_spec.md` | LightGBM model specification, calibration methodology |
| `docs/strategy/betting_rules.md` | EV calculation, Kelly criterion, risk management |
| `docs/data/data_dictionary.md` | Complete field definitions, Equibase mappings |
| `docs/config/configuration_guide.md` | All configurable parameters with tuning guidelines |
| `docs/platform/platform_verification.md` | TwinSpires/DraftKings verification checklist |
| `config/config.yaml` | Main configuration file with all parameters |

### Phase 1: Database Migration ✅

| File | Description |
|------|-------------|
| `database/postgres_schema.sql` | Complete PostgreSQL schema (5 schemas, 25+ tables) |
| `database/migrations/001_create_schemas.sql` | Schema creation and reference data |
| `database/migrations/002_create_core_tables.sql` | Core racing entity tables |
| `database/migrations/003_create_feature_tables.sql` | Feature engineering tables |
| `database/migrations/004_create_betting_tables.sql` | Betting and monitoring tables |
| `database/migrations/005_migrate_sqlite_data.py` | SQLite to PostgreSQL migration script |
| `config/database.py` | Database connection management module |

---

## Phase 2: Feature Engineering Layer (Next)

### 2.1 Feature Engine (`features/feature_engine.py`)

Main orchestration class for feature calculation:

```python
class FeatureEngine:
    def calculate_all_features(self, race_id: str, race_date: date) -> pd.DataFrame
    def calculate_horse_features(self, registration_number: str, race_date: date) -> dict
    def calculate_connection_features(self, trainer_id: str, jockey_id: str, race_date: date) -> dict
    def calculate_field_relative_features(self, race_id: str) -> pd.DataFrame
```

### 2.2 Rolling Stats (`features/rolling_stats.py`)

Point-in-time rolling statistics:

```python
class RollingStatsCalculator:
    def calculate_trainer_stats(self, trainer_id: str, as_of_date: date, windows: List[int]) -> dict
    def calculate_jockey_stats(self, jockey_id: str, as_of_date: date, windows: List[int]) -> dict
    def calculate_combo_stats(self, trainer_id: str, jockey_id: str, as_of_date: date) -> dict
    def calculate_horse_form(self, registration_number: str, as_of_date: date) -> dict
```

Windows: 14, 30, 60 days
Sample size thresholds: trainer=20, jockey=20, combo=5

### 2.3 Track Bias (`features/track_bias.py`)

Track and post position bias calculations:

```python
class TrackBiasCalculator:
    def calculate_post_position_bias(self, track_code: str, surface: str, distance_bucket: str) -> dict
    def calculate_speed_bias(self, track_code: str, surface: str) -> float
    def get_distance_bucket(self, distance_yards: int) -> str  # sprint/route/marathon
```

Distance buckets:
- Sprint: < 1540 yards (< 7 furlongs)
- Route: 1540-2200 yards (7f to < 1 mile)
- Marathon: >= 2200 yards (>= 1 mile)

Minimum sample size: 50 races

### 2.4 Validation (`features/validation.py`)

Leakage prevention and validation:

```python
class LeakageValidator:
    def validate_no_leakage(self, race_id: str, feature_row: dict) -> bool
    def run_validation_suite(self, sample_size: int = 100) -> ValidationReport
    def check_point_in_time(self, feature_date: date, data_dates: List[date]) -> bool
```

Must validate on 100+ random races before any model training.

### 2.5 Extend Standardization (`standardization.py`)

Add methods to existing file:

```python
def calculate_speed_figure(self, final_time: float, distance: int, track_variant: float) -> int
def calculate_pace_figure(self, fraction_times: List[float], distance: int) -> dict
def calculate_class_rating(self, purse: float, race_type: str, field_quality: float) -> float
```

---

## Key Technical Requirements

### Point-in-Time Integrity

All features must be calculated using only data available BEFORE the race:

```python
# CORRECT: Only use races before target date
historical_races = session.query(RaceEntry).filter(
    RaceEntry.trainer_id == trainer_id,
    Race.race_date < target_race_date  # Strict less than
).all()

# WRONG: Includes the target race
historical_races = session.query(RaceEntry).filter(
    RaceEntry.trainer_id == trainer_id,
    Race.race_date <= target_race_date  # Leakage!
).all()
```

### Feature Categories (from feature_catalog.md)

| Category | Count | Key Features |
|----------|-------|--------------|
| Horse Form | 20 | days_since_last, speed_figures, finish_trends |
| Connections | 20 | trainer/jockey win rates, combo synergy |
| Speed/Pace | 25 | early_pace, late_pace, pace_style |
| Class | 15 | class_change, earnings_per_start |
| Track/Conditions | 15 | post_position_bias, surface_preference |
| Equipment | 10 | blinkers_first_time, lasix_on |
| Meta | 10 | sample_size_flags, confidence_scores |

### Database Tables to Populate

Features should write to these tables (defined in Phase 1):

- `features.trainer_rolling_stats`
- `features.jockey_rolling_stats`
- `features.trainer_jockey_combo_stats`
- `features.track_bias_stats`
- `features.horse_rolling_form`
- `features.race_features`

---

## Existing Code to Reference

### standardization.py (405 lines)

Key existing methods:
- `standardize_race_type()` - Maps race types to hierarchy
- `standardize_distance()` - Converts to yards
- `standardize_surface()` - Normalizes surface types
- `parse_equipment()` - Extracts equipment flags
- `standardize_odds()` - Converts odds formats

### Database Schema

```sql
-- Rolling stats structure
CREATE TABLE features.trainer_rolling_stats (
    trainer_id VARCHAR(20),
    calculation_date DATE,
    window_days INTEGER,  -- 14, 30, 60
    starts INTEGER,
    wins INTEGER,
    win_rate DECIMAL(5,4),
    roi DECIMAL(8,4),
    sufficient_sample BOOLEAN
);
```

### Configuration (config/config.yaml)

```yaml
features:
  rolling_windows: [14, 30, 60]

  track_bias:
    min_sample_size: 50
    distance_buckets:
      sprint: [0, 1540]
      route: [1540, 2200]
      marathon: [2200, 99999]

  sample_size_thresholds:
    horse: 3
    trainer: 20
    jockey: 20
    combo: 5
    track_bias: 50
```

---

## Directory Structure After Phase 2

```
k:\racing-pipeline\racing\
├── features/                    # NEW: Feature engineering
│   ├── __init__.py
│   ├── feature_engine.py       # Main orchestration
│   ├── rolling_stats.py        # Rolling statistics
│   ├── track_bias.py           # Track bias calculations
│   └── validation.py           # Leakage validation
├── standardization.py          # MODIFY: Add speed/pace/class methods
├── config/
│   ├── config.yaml
│   └── database.py
├── database/
│   ├── postgres_schema.sql
│   └── migrations/
├── docs/
│   ├── architecture/
│   ├── features/
│   ├── models/
│   ├── strategy/
│   ├── data/
│   ├── config/
│   └── platform/
└── [existing extraction scripts]
```

---

## Success Criteria for Phase 2

- [ ] `features/feature_engine.py` created with main calculation methods
- [ ] `features/rolling_stats.py` calculates 14/30/60 day windows
- [ ] `features/track_bias.py` calculates post position bias by track×surface×distance
- [ ] `features/validation.py` includes leakage validation framework
- [ ] `standardization.py` extended with speed/pace/class methods
- [ ] All features pass point-in-time validation (100+ races tested)
- [ ] Features write correctly to PostgreSQL tables
- [ ] Unit tests created for critical calculations

---

## Notes

- The SQLite database `racing_data.db` contains 2023 Equibase data (248MB)
- PostgreSQL migration should be run before feature calculation at scale
- For development, features can be calculated directly from SQLite
- Use `config/database.py` for all database connections

---

*Document created: 2025-12-16*
*For use in continuing development in a new conversation*
