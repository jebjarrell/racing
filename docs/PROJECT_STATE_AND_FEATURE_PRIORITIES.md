# Racing Engine: Project State & Feature Priorities

**Date:** 2026-04-13
**Status:** Active Development — Core pipeline functional, major feature gaps remain

---

## Part 1: Current Project State

### Architecture Summary

The system is a quantitative horse racing betting engine designed to generate win probability predictions for US thoroughbred races, calculate expected value against market odds, and produce risk-managed bet recommendations using fractional Kelly criterion. It targets win bets only (no exotics), manual execution via TwinSpires/DraftKings, with a $2,000 starting bankroll.

**Stack:** Python 3.10+, SQLite (dev) / PostgreSQL + TimescaleDB (target), LightGBM, Streamlit, FastAPI.

### Data Pipeline (Functional)

The ingestion layer is the most mature part of the system. Three extractors parse Equibase XML files in parallel (up to 45 workers):

- **HorseExtractor** — parses horse/trainer/owner master data from PP files
- **PastPerformanceExtractor** — extracts race metadata and entry-level data (post, jockey, equipment, scratch flags)
- **ResultChartExtractor** — appends finishing positions, times, payouts

Data flows through `RacingDataStandardizer` for normalization of categorical fields (course types, race types, equipment codes). Three critical extraction bugs were fixed in November 2025 (distance conversion, track code parsing, maiden claiming classification) and validated with 22 passing tests. The existing SQLite database was flagged as corrupted and needing re-extraction.

**Data volume:** ~5,925 PP files, ~4,906 RC files covering 2023 US thoroughbred racing.

### Database Schema (Designed, partially implemented)

The target schema uses four PostgreSQL schemas:

- **racing** — core tables: `horses_master`, `trainers`, `owners`, `races`, `race_entries`, `race_fractions`, `horse_position_calls`, `race_wagering`
- **features** — computed features: `trainer_rolling_stats`, `jockey_rolling_stats`, `track_bias_stats`, `daily_features` (TimescaleDB hypertable)
- **betting** — operational tables: `bet_recommendations`, `bet_log`, `bankroll_snapshots`, `split_test_results`
- **monitoring** — `predictions`, `calibration_metrics`, `validation_runs`

A full PostgreSQL schema (`database/postgres_schema.sql`, 43KB) exists but the system currently runs against SQLite for development. The PostgreSQL migration (`database/migrations/005_migrate_sqlite_data.py`) exists but hasn't been validated end-to-end.

### Feature Engineering (Implemented — 43 features of 115 spec'd)

The `FeatureEngine` class orchestrates computation across three sub-modules:

- **`RollingStatsCalculator`** — trainer/jockey rolling win rates at 14/30/60-day windows, hot streak flags, combo synergy scores
- **`TrackBiasCalculator`** — post position bias, inside bias score, rail bias adjustment, speed bias score
- **`FeatureEngine` (main)** — horse form features (days since last, layoff, career stats, speed figures, class change), equipment flags, field-relative features (speed rank, class rank, field quality)

**What's actually implemented (43 features):**

| Category | Count | Features |
|----------|-------|----------|
| Horse Form | 14 | days_since_last, layoff_indicator, first_time_starter, total_starts, total_wins, career_win_rate, surface_win_rate, surface_preference, distance_preference, best_speed_90_days, avg_speed_90_days, speed_trend, last_class_level, class_change |
| Connections | 12 | trainer_win_rate_{14d,30d,60d}, trainer_hot_streak, trainer_sample_flag, jockey_win_rate_{14d,30d,60d}, jockey_hot_streak, jockey_sample_flag, combo_win_rate, combo_synergy_score |
| Track/Position | 6 | post_position, post_position_win_rate, inside_bias_score, rail_bias_adjustment, speed_bias_score, field_size |
| Equipment | 4 | blinkers_on, blinkers_first_time, lasix_on, equipment_change |
| Field-Relative | 4 | speed_rank_in_field, class_rank_in_field, field_quality_score, speed_vs_field_avg |
| Base | 3 | morning_line_odds, age_at_race, class_level |

**What's spec'd but NOT implemented (72 features):**

The feature catalog specifies 115 total features. The missing 72 include the full pace analysis suite (early/mid/late pace figures, pace style classification, pace shape compatibility, lone speed detection), speed adjustments (track variant, surface, class), detailed class performance metrics (win rate at class, ITM at class, class ceiling), track condition preferences, detailed distance stats, workout analysis, and the full meta/confidence scoring system.

### Model (Trained — v1.0, single model)

A single LightGBM binary classifier trained on 2023 H1 data:

- **Architecture:** LightGBM with 500 estimators, max_depth=6, learning_rate=0.05, subsample=0.8
- **Calibration:** Field-size-stratified isotonic regression (small 5-7, medium 8-10, large 11+) → softmax normalization per race
- **Reported metrics:** AUC 0.78, ECE 0.003 (per model metadata)
- **Training split:** Jan-Jun train, Jul-Sep validation, Oct-Dec test (time-based, no leakage)
- **Artifact location:** `artifacts/models/v1.0/` (model.pkl, calibrator, metadata.json)

The model pipeline (`models/training_pipeline.py`) handles data preparation, training, evaluation, and serialization. Evaluation includes Brier score, log loss, AUC-ROC, calibration curves, feature importance, and stratified analysis.

### Backtesting (Implemented — 6 strategies)

The backtester (`backtesting/backtester.py`) runs historical simulations with configurable strategies:

1. **FlatBetStrategy** — fixed $2 on every qualifying bet (baseline)
2. **KellyCriterionStrategy** — fractional Kelly (default 0.25x) with min edge filter
3. **ValueBettingStrategy** — bet when model prob > implied prob by threshold
4. **TopPickStrategy** — bet on model's top pick per race
5. **MomentumStrategy** — increase stakes during winning streaks
6. **MorningFavoriteStrategy** — bet morning line favorites (market baseline)

Strategy comparison mode overlays bankroll curves for head-to-head evaluation. The backtester tracks ROI, win rate, max drawdown, Sharpe ratio, and daily P&L.

### Streamlit UI (Implemented — 6 pages)

A multi-page Streamlit app wired into the backend:

- **Dashboard** — DB stats, model status, config summary, recent races, top performers
- **Data Management** — XML upload, extraction trigger, database browser
- **Model Training** — train/retrain via pipeline, view metrics/plots/feature importance
- **Backtesting** — strategy selection, date range, bankroll curve, bet log, strategy comparison
- **Race Predictions** — single-race prediction with probability table, EV calculation, Kelly sizing
- **Settings** — edit config.yaml parameters

### What's NOT Built Yet

- **Live data pipeline** — no real-time race card fetching, odds API integration, or scratch monitoring
- **PostgreSQL deployment** — still on SQLite
- **Exotic bet support** — win only, no place/show/exacta/trifecta
- **Odds movement tracking** — no historical odds time series
- **MLflow integration** — specified but not connected
- **Automated scheduling** — no cron jobs for daily workflows
- **Performance monitoring dashboard** — the monitoring schema exists but no live tracking

---

## Part 2: Reference Library in Knowledge Base

The following documents were found in the lmkb and Obsidian vaults. Each contributes specific handicapping concepts or quantitative methods that map to potential features.

### Handicapping Books

| Source | Key Concepts |
|--------|-------------|
| **Modern Pace Handicapping** (Brohamer) | Running style classification (E/P/S), turn-time velocity, pace segment analysis, daily track variants, par-time charts, the Decision Model for pace handicapping |
| **Betting Thoroughbreds for the 21st Century** (Davidowitz) | Pace and single-race bias, class handicapping at minor tracks, workout analysis, key race method, mid-race move detection |
| **Dave Litfin's Expert Handicapping** (Litfin) | Fractional time evaluation, race shape analysis, contender evaluation by running position, angle-based handicapping, "for whom was the race written" condition analysis |
| **Commonsense Betting** (Mitchell) | Claiming race economic logic, allowance/stakes class hierarchy, ability/form/angles/value framework, maiden race prediction, turf racing specifics |
| **150 Professional Horseracing Systems** | Systematic approach catalog — beaten favorites, form cycle analysis, progression betting, follow-up plays |

### Academic / Quantitative Papers

| Source | Key Concepts |
|--------|-------------|
| **Handbook of Sports and Lottery Markets** (Hausch, Ziemba) | Favorite-longshot bias (FLB) across markets, distance preference modeling (Hong Kong/Sydney case studies), place/show inefficiency (Hausch-Ziemba system), capital growth betting strategies |
| **Efficiency of Racetrack Markets** (Hausch, Lo, Ziemba / Dowie) | Weak vs. semi-strong efficiency in parimutuel markets, FP/SP ratio as information signal, starting price vs. forecast price divergence, over-round calculation, Tattersalls deduction methodology |
| **Managing Losses in Exotic Horse Race Wagering** (Deza, Huang, Metel) | Factor-based horse modeling (Post, Pre, Form, Class, Speed, Driver Points), probability estimation from factor scores, exotic bet loss management |
| **Ornstein-Uhlenbeck Process for Horse Race Betting** | Odds movement as stochastic process, herder vs. informed bettor dynamics, odds convergence timing, true probability estimation from odds series |
| **Optimal Speed in Thoroughbred Horse Racing** (Mercier, Aftalion) | Optimal pacing strategy from biomechanics, aerobic/anaerobic energy partition by race distance, speed regulation over race course |
| **Component Ratios of Independent and Herding Betters** (Mori, Hisakado) | Market microstructure of parimutuel betting, independent vs. herd bettor ratio estimation, odds ranking dynamics over betting period |
| **Efficient Market Dynamics: UK Horse Racing** (Tondapu) | Betfair time series analysis, short-tailed return distributions, rapidly decaying autocorrelations, informational efficiency in exchange markets |

### Obsidian Project Notes

| Source | Key Content |
|--------|------------|
| **projects/racing-pipeline.md** | Project status tracker: notes SQLite→PostgreSQL migration in progress, probability calibration by field size bucket in progress, post-position bias quantification in progress |
| **derived/horse-racing-model.md** | Comprehensive system design doc including ITM (in-the-money) probability output, split testing busy vs. regional tracks, 15-18 month timeline for statistical significance |

---

## Part 3: Feature Gap Analysis

Cross-referencing the 43 implemented features against the 115-feature spec and the reference library reveals features that are either (a) spec'd but not built, or (b) not spec'd but referenced heavily in the literature. Here they are grouped by theme.

### Gap A: Pace Analysis (Not Implemented — Referenced in Brohamer, Davidowitz, Litfin, Handbook)

This is the single largest feature gap. The spec calls for 25 speed/pace features; only 6 speed-related features are implemented and zero pace features.

**Missing from spec:**
- `horse_pace_early_avg`, `horse_pace_mid_avg`, `horse_pace_late_avg` — average pace figures by segment from last 3 races
- `horse_pace_style` — ordinal classification (1=Front, 2=Presser, 3=Closer) from position call data
- `race_pace_scenario` — predicted pace shape (Slow/Avg/Fast) from counting early speed horses
- `horse_pace_fit_score` — compatibility of running style with expected pace scenario (-1 to +1)
- `field_early_speed_count` — count of early speed horses in field
- `horse_is_lone_speed` — binary flag for sole front-runner

**Referenced in literature but NOT in spec:**
- **Turn-time velocity** (Brohamer) — speed through the second fraction, the most diagnostic pace segment
- **Mid-race move detection** (Davidowitz) — tracking horses who make moves equal to or faster than the opening quarter split
- **Pace segment energy distribution** (Mercier/Aftalion) — aerobic/anaerobic energy allocation by race distance informs optimal pace profiles
- **Par-time differential** (Brohamer) — comparing actual fractional times to par times for the class/track/distance combination
- **Single-race bias index** (Davidowitz/Quirin) — post-race analysis of whether pace shape advantaged a specific running style

### Gap B: Speed Figure Adjustments (Partially Implemented)

Only raw speed figures are used. The spec calls for three adjustment layers:

**Missing from spec:**
- `horse_speed_track_adjusted` — normalize speed figures by track variant
- `horse_speed_surface_adjusted` — dirt/turf conversion differential
- `horse_speed_class_adjusted` — normalize for class level context

**Referenced in literature but NOT in spec:**
- **Daily track variant calculation** (Brohamer) — computing the daily speed bias for each track, essential for making speed figures comparable across days
- **Track-to-track speed variant** (Brohamer) — normalization for comparing figures earned at different tracks

### Gap C: Class Analysis (Partially Implemented)

Only `class_level`, `last_class_level`, and `class_change` are implemented. The spec includes 15 class features.

**Missing from spec:**
- `horse_win_pct_at_class`, `horse_itm_pct_at_class` — performance at today's specific class level
- `horse_best_class_won`, `horse_class_ceiling` — highest class won and highest class attempted
- `horse_class_overqualified` — won at higher class (dropper with proven ability)
- `field_class_rank`, `field_class_vs_median` — relative class position within field

**Referenced in literature but NOT in spec:**
- **Economic class logic** (Mitchell) — claiming price as true ability indicator; if a horse is entered for less than its established value, the trainer may be signaling poor form
- **Race condition matching** (Litfin) — "for whom was the race written" analysis of eligibility conditions vs. horse record
- **Negative class drop detection** (Brohamer) — distinguishing a strategic drop from a "dumping" drop where the horse is compromised

### Gap D: Workout Analysis (Not Implemented — Referenced in Davidowitz)

No workout features exist in either the implementation or the 43-feature model. The full spec's appendix references workouts but doesn't include them as features.

**Referenced in literature:**
- **Workout times relative to par** (Davidowitz) — noteworthy workout table by distance, differentiating breezing vs. handily
- **First-time starter workout patterns** — critical for maiden races where no race history exists
- **Workout frequency and recency** — a horse working frequently and recently (bullet works, gate works) signals readiness

### Gap E: Favorite-Longshot Bias Exploitation (Not Implemented — Hausch/Ziemba, Dowie, Handbook)

The system has a 15-1 max odds cap but no explicit FLB modeling.

**Referenced in literature:**
- **FLB-adjusted probability recalibration** — the systematic overbet on longshots and underbet on favorites creates a known return structure by odds bucket that could refine calibration
- **Odds bucket-specific expected returns** — the Hausch data shows returns approaching break-even for 1-100 to 2-5 odds and severe losses beyond 18-1
- **Takeout-adjusted fair odds** — accounting for the ~17% win pool takeout differently by odds range

### Gap F: Odds Movement / Market Microstructure (Not Implemented — O-U Process paper, Mori/Hisakado)

The system uses morning line odds as a single static feature. No odds dynamics.

**Referenced in literature:**
- **FP/SP ratio** (Dowie) — divergence between forecast price and starting price as an information signal; high FP/SP ratio horses show positive expected returns
- **Odds convergence timing** (O-U process paper) — modeling when odds stabilize reveals informed bettor activity
- **Herder vs. fundamentalist ratio** (Mori/Hisakado) — the proportion of independent bettors affects market efficiency; lower independent-bettor ratios = more exploitable markets

### Gap G: Place/Show and Exotic Bet Opportunities (Not Implemented — Hausch/Ziemba)

Win-only is an explicit MVP constraint, but the literature identifies the largest market inefficiencies in place/show pools.

**Referenced in literature:**
- **Hausch-Ziemba place/show system** — the original documented profitable system exploits the fact that favorites are significantly underbet in place and show pools relative to the win pool
- **Cross-pool arbitrage** — comparing win probabilities to place/show implied probabilities to find the most inefficient pool
- **Exotic bet construction** (Deza et al.) — factor-based approaches to exacta/trifecta wagering with loss management

### Gap H: Track Condition / Weather Features (Partially Implemented)

Only `track_condition` as an ordinal and `horse_off_track_win_pct` are in the spec. Neither is in the 43-feature model.

**Missing from spec:**
- `horse_prefers_off_track` — computed flag for horses that improve on wet surfaces
- Off-track adjustment to speed figures

**Referenced in literature:**
- **Turf-to-dirt conversion** (Mitchell) — specific handling for horses switching surfaces
- **Going description granularity** — modeling the spectrum from Fast→Good→Yielding→Soft→Heavy rather than a binary "off track" flag

---

## Part 4: Prioritized Feature Roadmap

Features are prioritized by three criteria: (1) expected predictive lift based on literature and domain knowledge, (2) data availability in the existing Equibase pipeline, and (3) implementation complexity.

### Tier 1: High Impact, Data Available, Moderate Complexity

These should be built and tested first. They address the largest gaps in the current 43-feature model and draw on data already in the database.

| # | Feature Set | Est. New Features | Rationale |
|---|-------------|-------------------|-----------|
| 1 | **Pace Analysis Core** — pace style classification from position calls, early/mid/late pace figures, field pace scenario prediction | 8-10 | Brohamer and Davidowitz agree that pace is the single most underweighted factor. Position call data is already extracted by `horse_position_calls`. This is the biggest single lift available. |
| 2 | **Speed Figure Adjustments** — daily track variant, track-to-track normalization, surface conversion | 3-4 | Raw speed figures are currently compared across tracks and days without normalization. Brohamer's par-time methodology provides the framework. The data (fractional times, track, date) already exists. |
| 3 | **Class Performance History** — win/ITM rate at today's class, best class won, class ceiling, overqualified flag | 5-6 | The current model knows only `class_level` and `class_change`. Adding actual performance at the target class level gives the model information about whether a horse has proven ability at this level. Data available in historical entries. |
| 4 | **Detailed Distance/Surface Stats** — starts/wins at distance, surface debut flag, stretch-out/cutback flags | 5-6 | Distance preference and surface preference are currently single numbers. Decomposing into starts, wins, and transition flags (sprint→route, dirt→turf) gives the model more to work with. Data available. |
| 5 | **FLB-Aware Calibration Refinement** — recalibrate by odds bucket using the known favorite-longshot bias structure | 0 (calibration change) | Not a new feature but a calibration improvement. The Hausch/Ziemba data shows systematic miscalibration by odds range. Stratifying calibration by odds bucket in addition to field size should improve ECE. |

### Tier 2: High Impact, Requires New Data Processing

These require building new computation pipelines but have strong support in the literature.

| # | Feature Set | Est. New Features | Rationale |
|---|-------------|-------------------|-----------|
| 6 | **Pace Shape Compatibility** — lone speed flag, pace fit score, early speed count, pace pressure index | 4-5 | Brohamer's Decision Model revolves around matching running style to expected pace shape. Requires computing pace scenario from the field composition, then scoring each horse's fit. |
| 7 | **Turn-Time Velocity** — second-fraction speed as primary diagnostic (Brohamer's key insight) | 2-3 | Turn time (the second fraction in sprints, the middle fraction in routes) is the strongest discriminator of actual ability vs. pace setup. Requires fractional time parsing from `race_fractions`. |
| 8 | **Workout Features** — workout frequency, recency, bullet works, gate works, first-timer workout quality | 4-5 | Critical for maiden races where the model currently has minimal information. Workout data may need new extraction logic if not fully captured in current PP parsing. |
| 9 | **Track Condition Modeling** — granular condition encoding, horse wet-track preference, condition-adjusted speed | 3-4 | The current model ignores track condition entirely at the feature level. Weather-related performance variation is a well-documented source of value. |

### Tier 3: Medium Impact, Strategic Value

These add strategic depth and address market-level inefficiencies rather than horse-level prediction.

| # | Feature Set | Est. New Features | Rationale |
|---|-------------|-------------------|-----------|
| 10 | **Place/Show Probability Extension** — extend model to predict ITM probability, implement Hausch-Ziemba place/show system | N/A (model extension) | The single most documented profitable system in racing literature. Requires extending the model target from win-only to place/show probability, then comparing against place/show pools. |
| 11 | **Odds Movement Features** — morning line vs. opening odds divergence, late money indicator | 2-3 | The FP/SP ratio (Dowie) and O-U process research show that odds movement contains exploitable information. Requires either historical odds snapshots or live odds feed. |
| 12 | **Race Condition Matching** — eligibility condition analysis, restricted race flag, "written for" indicator | 2-3 | Litfin's "for whom was the race written" concept. When race conditions perfectly match one horse's record, that horse has a structural advantage. Requires parsing eligibility conditions from race data. |
| 13 | **Trainer Intent Signals** — claiming price relative to horse value, class drop with surface switch, first start at track | 3-4 | Mitchell's "economic sense" principle for claiming races. The gap between a horse's demonstrated class and today's claiming price reveals trainer intent. |

### Tier 4: Lower Priority / Longer-Term

| # | Feature Set | Rationale |
|---|-------------|-----------|
| 14 | **Exotic Bet Engine** — exacta, trifecta, pick-N wagering | Requires full field probability matrix and combinatorial optimization. High complexity, but the Deza et al. paper provides a framework. |
| 15 | **Live Odds API Integration** — real-time odds feed from TwinSpires/DraftKings | Infrastructure project. Required for odds movement features and for reducing the historical-to-live edge degradation (currently estimated at 35%). |
| 16 | **Market Efficiency Segmentation** — pool size as efficiency proxy, small-pool longshot bias modeling | The Mori/Hisakado research shows market efficiency varies with pool size. Smaller pools at regional tracks may be more exploitable. |
| 17 | **Multi-Year Training** — expand beyond 2023 to 2020-2024 data | Larger training set should improve model robustness, especially for low-frequency events (turf sprints, marathon races, stakes). |
| 18 | **Ensemble Model** — logistic regression + LightGBM + neural network ensemble | The Obsidian project note mentions "gradient boosting + logistic regression ensemble." Adding a calibrated ensemble could improve both discrimination and calibration. |

---

## Part 5: Recommended Next Steps

**Immediate (next sprint):**

1. Re-extract database with fixed pipeline (corrupted SQLite needs rebuilding)
2. Implement Tier 1 items #1 (Pace Analysis Core) and #2 (Speed Figure Adjustments) — these are the two largest predictive gaps with data already available
3. Retrain model with expanded feature set and compare AUC/Brier/ECE against v1.0 baseline

**Short-term (next 2-3 sprints):**

4. Implement Tier 1 items #3-4 (class performance, distance/surface)
5. Apply FLB-aware calibration refinement (#5)
6. Run full backtesting suite across all 6 strategies with expanded features
7. Validate that backtested ROI exceeds the 5% minimum threshold before considering live deployment

**Medium-term:**

8. Build Tier 2 features (pace compatibility, turn-time, workouts, conditions)
9. Investigate place/show extension (Tier 3 #10) — potentially the highest-ROI strategic addition
10. Complete PostgreSQL migration for production deployment
