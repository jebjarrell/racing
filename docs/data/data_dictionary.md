# Data Dictionary

**Version:** 1.0
**Last Updated:** 2025-12-16
**Database:** PostgreSQL 15 + TimescaleDB

---

## Table of Contents

1. [Overview](#1-overview)
2. [Schema Organization](#2-schema-organization)
3. [Racing Schema Tables](#3-racing-schema-tables)
4. [Features Schema Tables](#4-features-schema-tables)
5. [Betting Schema Tables](#5-betting-schema-tables)
6. [Monitoring Schema Tables](#6-monitoring-schema-tables)
7. [Reference Tables](#7-reference-tables)
8. [Views](#8-views)
9. [Equibase XML Field Mappings](#9-equibase-xml-field-mappings)
10. [Data Quality Rules](#10-data-quality-rules)

---

## 1. Overview

### 1.1 Data Sources

| Source | Format | Description |
|--------|--------|-------------|
| Equibase Past Performance | XML (simulcast.xsd) | Pre-race entries, past performances |
| Equibase Result Charts | XML (tchSchema.xsd) | Post-race results, payoffs |
| Live Odds (Future) | API | Real-time market odds |

### 1.2 Data Volume

| Entity | Approximate Count (2023) |
|--------|--------------------------|
| Races | ~60,000 |
| Race Entries | ~500,000 |
| Horses | ~40,000 |
| Trainers | ~5,000 |
| Jockeys | ~2,000 |

---

## 2. Schema Organization

```
racing_db
├── racing          # Core racing data
├── features        # Computed features
├── betting         # Betting operations
├── monitoring      # Performance tracking
└── reference       # Lookup tables
```

---

## 3. Racing Schema Tables

### 3.1 racing.horses_master

Master table for horse identity and lineage.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| registration_number | VARCHAR(20) | NO | **PK** Unique horse ID from Equibase |
| horse_name | VARCHAR(200) | YES | Horse's racing name |
| foaling_date | DATE | YES | Birth date |
| year_of_birth | INTEGER | YES | Birth year |
| breed_type | VARCHAR(50) | YES | Thoroughbred, Quarter Horse, etc. |
| sex_code | VARCHAR(10) | YES | C=Colt, F=Filly, M=Mare, G=Gelding |
| color_code | VARCHAR(50) | YES | Coat color |
| sire_registration | VARCHAR(20) | YES | **FK** → horses_master |
| dam_registration | VARCHAR(20) | YES | **FK** → horses_master |
| sire_name | VARCHAR(200) | YES | Sire's name (denormalized) |
| dam_name | VARCHAR(200) | YES | Dam's name (denormalized) |
| breeder_name | VARCHAR(300) | YES | Breeder information |
| created_at | TIMESTAMPTZ | NO | Record creation time |
| updated_at | TIMESTAMPTZ | NO | Last update time |

**Indexes:**
- `idx_horses_name` on (horse_name)
- `idx_horses_year` on (year_of_birth)
- `idx_horses_sire` on (sire_registration)
- `idx_horses_dam` on (dam_registration)

---

### 3.2 racing.trainers

Master table for trainer information.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| external_party_id | VARCHAR(20) | NO | **PK** Equibase trainer ID |
| first_name | VARCHAR(100) | YES | First name |
| middle_name | VARCHAR(100) | YES | Middle name |
| last_name | VARCHAR(100) | YES | Last name |
| type_source | VARCHAR(50) | YES | Source system identifier |
| created_at | TIMESTAMPTZ | NO | Record creation time |

**Indexes:**
- `idx_trainers_name` on (last_name, first_name)

---

### 3.3 racing.owners

Master table for owner information.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| external_party_id | VARCHAR(20) | NO | **PK** Equibase owner ID |
| first_name | VARCHAR(100) | YES | First name (or syndicate name) |
| last_name | VARCHAR(100) | YES | Last name |
| created_at | TIMESTAMPTZ | NO | Record creation time |

---

### 3.4 racing.races

Main race information table.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| race_id | VARCHAR(100) | NO | **PK** Format: {track}_{date}_{race_num} |
| track_code | VARCHAR(10) | NO | Track abbreviation (CD, SAR, etc.) |
| race_date | DATE | NO | Race date |
| race_number | INTEGER | NO | Race number on card (1-14) |
| race_name | VARCHAR(500) | YES | Race name (if stakes) |
| conditions_text | TEXT | YES | Full conditions description |
| course_type_code | VARCHAR(20) | YES | DIRT, TURF, SYNTHETIC |
| race_type_code | VARCHAR(20) | YES | G1, ALW, CLM, MSW, etc. |
| track_condition | VARCHAR(20) | YES | FAST, GOOD, MUDDY, etc. |
| distance_yards | INTEGER | YES | Race distance in yards |
| purse_usd | DECIMAL(12,2) | YES | Purse in US dollars |
| class_level | INTEGER | YES | 1-10 class hierarchy |
| min_age | INTEGER | YES | Minimum age restriction |
| max_age | INTEGER | YES | Maximum age restriction |
| fillies_and_mares | BOOLEAN | NO | Sex-restricted race |
| max_claim_price | DECIMAL(12,2) | YES | Maximum claiming price |
| min_claim_price | DECIMAL(12,2) | YES | Minimum claiming price |
| post_time | TIME | YES | Scheduled post time |
| weather | VARCHAR(100) | YES | Weather conditions |
| winning_time | DECIMAL(8,3) | YES | Winning time (seconds) |
| winning_margin | DECIMAL(6,2) | YES | Winning margin (lengths) |
| source_file | VARCHAR(500) | YES | Source XML file |
| data_source | VARCHAR(50) | YES | past_performance or result_chart |
| created_at | TIMESTAMPTZ | NO | Record creation time |
| updated_at | TIMESTAMPTZ | NO | Last update time |

**Indexes:**
- `idx_race_date` on (race_date)
- `idx_track_date` on (track_code, race_date)
- `idx_race_type` on (race_type_code)
- `idx_class_level` on (class_level)
- `idx_distance` on (distance_yards)

---

### 3.5 racing.race_entries

Individual horse entries in each race.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| entry_id | VARCHAR(150) | NO | **PK** race_id + registration_number |
| race_id | VARCHAR(100) | NO | **FK** → races |
| registration_number | VARCHAR(20) | NO | **FK** → horses_master |
| program_number | VARCHAR(10) | YES | Program/saddle cloth number |
| post_position | INTEGER | YES | Starting gate position |
| weight_lbs | INTEGER | YES | Weight carried (pounds) |
| age_at_race | INTEGER | YES | Horse age at race time |
| morning_line_odds | DECIMAL(8,2) | YES | Morning line odds (decimal) |
| has_blinkers | BOOLEAN | NO | Wearing blinkers |
| has_lasix | BOOLEAN | NO | On Lasix/Salix |
| has_tongue_tie | BOOLEAN | NO | Tongue tie applied |
| has_nasal_strip | BOOLEAN | NO | Nasal strip applied |
| blinkers_first_time | BOOLEAN | NO | First time in blinkers |
| lasix_first_time | BOOLEAN | NO | First time on Lasix |
| claim_price | DECIMAL(10,2) | YES | Claiming price (if applicable) |
| trainer_id | VARCHAR(20) | YES | **FK** → trainers |
| jockey_id | VARCHAR(20) | YES | Jockey external party ID |
| owner_id | VARCHAR(20) | YES | **FK** → owners |
| official_finish_position | INTEGER | YES | Official finish (1=win) |
| actual_odds | DECIMAL(8,2) | YES | Final tote odds (decimal) |
| win_payoff | DECIMAL(8,2) | YES | Win payoff per $2 |
| place_payoff | DECIMAL(8,2) | YES | Place payoff per $2 |
| show_payoff | DECIMAL(8,2) | YES | Show payoff per $2 |
| final_time | DECIMAL(8,3) | YES | Individual final time |
| speed_rating | INTEGER | YES | Equibase speed figure |
| beaten_lengths | DECIMAL(6,2) | YES | Lengths behind winner |
| start_position | INTEGER | YES | Position at start |
| first_call_position | INTEGER | YES | Position at first call |
| second_call_position | INTEGER | YES | Position at second call |
| stretch_position | INTEGER | YES | Position at stretch |
| scratched | BOOLEAN | NO | Horse scratched |
| scratch_reason | VARCHAR(200) | YES | Reason for scratch |
| race_comments | TEXT | YES | Chart caller comments |
| created_at | TIMESTAMPTZ | NO | Record creation time |
| updated_at | TIMESTAMPTZ | NO | Last update time |

**Indexes:**
- `idx_race_entry` on (race_id, registration_number)
- `idx_horse_entries` on (registration_number)
- `idx_trainer_entries` on (trainer_id)
- `idx_jockey_entries` on (jockey_id)
- `idx_finish_position` on (official_finish_position)
- `idx_odds` on (actual_odds)

---

### 3.6 racing.race_fractions

Fractional times for each race.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| race_id | VARCHAR(100) | NO | **PK** (composite) |
| call_position | INTEGER | NO | **PK** 1=first call, 2=second, etc. |
| distance_yards | INTEGER | YES | Distance at call point |
| fraction_time | DECIMAL(8,3) | YES | Time at call (seconds) |
| leader_at_call | VARCHAR(20) | YES | **FK** → horses_master |

---

### 3.7 racing.horse_position_calls

Individual horse positions at each call point.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| race_id | VARCHAR(100) | NO | **PK** (composite) |
| registration_number | VARCHAR(20) | NO | **PK** (composite) |
| call_position | INTEGER | NO | **PK** 1-6 |
| position | INTEGER | YES | Running position |
| lengths_behind | DECIMAL(6,2) | YES | Lengths behind leader |

---

### 3.8 racing.race_wagering

Exotic wagering pools and payoffs.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| race_id | VARCHAR(100) | NO | **PK** (composite) |
| wager_type | VARCHAR(50) | NO | **PK** exacta, trifecta, etc. |
| pool_total | DECIMAL(12,2) | YES | Total pool amount |
| winning_combinations | TEXT | YES | Winning combinations (JSON) |
| payout | DECIMAL(10,2) | YES | Payout amount |
| number_of_winners | INTEGER | YES | Number of winning tickets |

---

## 4. Features Schema Tables

### 4.1 features.trainer_rolling_stats

Pre-computed trainer statistics by rolling window.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| trainer_id | VARCHAR(20) | NO | **PK** (composite) |
| calculation_date | DATE | NO | **PK** Date stats are valid for |
| window_days | INTEGER | NO | **PK** 14, 30, or 60 |
| starts | INTEGER | NO | Number of starts in window |
| wins | INTEGER | NO | Number of wins |
| places | INTEGER | YES | Number of 2nd place finishes |
| shows | INTEGER | YES | Number of 3rd place finishes |
| win_rate | DECIMAL(5,4) | YES | Win rate (0-1) |
| itm_rate | DECIMAL(5,4) | YES | In-the-money rate |
| roi | DECIMAL(8,4) | YES | Return on investment |
| avg_odds | DECIMAL(8,2) | YES | Average odds of starters |
| surface_dirt_win_rate | DECIMAL(5,4) | YES | Win rate on dirt |
| surface_turf_win_rate | DECIMAL(5,4) | YES | Win rate on turf |
| created_at | TIMESTAMPTZ | NO | Calculation time |

---

### 4.2 features.jockey_rolling_stats

Pre-computed jockey statistics by rolling window.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| jockey_id | VARCHAR(20) | NO | **PK** (composite) |
| calculation_date | DATE | NO | **PK** |
| window_days | INTEGER | NO | **PK** 14, 30, or 60 |
| starts | INTEGER | NO | Number of mounts |
| wins | INTEGER | NO | Number of wins |
| win_rate | DECIMAL(5,4) | YES | Win rate |
| roi | DECIMAL(8,4) | YES | Return on investment |
| created_at | TIMESTAMPTZ | NO | Calculation time |

---

### 4.3 features.track_bias_stats

Post position bias by track, surface, and distance.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| track_code | VARCHAR(10) | NO | **PK** (composite) |
| surface | VARCHAR(20) | NO | **PK** DIRT, TURF, SYNTHETIC |
| distance_bucket | VARCHAR(20) | NO | **PK** sprint, route, marathon |
| post_position | INTEGER | NO | **PK** 1-20 |
| calculation_date | DATE | NO | Date stats calculated |
| sample_size | INTEGER | NO | Number of races |
| win_count | INTEGER | NO | Wins from this post |
| win_rate | DECIMAL(5,4) | YES | Actual win rate |
| expected_win_rate | DECIMAL(5,4) | YES | Expected (1/field_size) |
| bias_deviation | DECIMAL(5,4) | YES | Deviation from expected |
| created_at | TIMESTAMPTZ | NO | Calculation time |

---

### 4.4 features.daily_features (TimescaleDB Hypertable)

Denormalized feature vectors for ML.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| feature_id | SERIAL | NO | **PK** |
| race_id | VARCHAR(100) | NO | Race identifier |
| registration_number | VARCHAR(20) | NO | Horse identifier |
| calculation_date | TIMESTAMPTZ | NO | Feature calculation time |
| feature_vector | JSONB | NO | All features as JSON |
| feature_version | VARCHAR(20) | YES | Feature pipeline version |

**Hypertable configuration:**
```sql
SELECT create_hypertable('features.daily_features', 'calculation_date');
```

---

## 5. Betting Schema Tables

### 5.1 betting.bet_recommendations

System-generated bet recommendations.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| recommendation_id | VARCHAR(50) | NO | **PK** Unique recommendation ID |
| race_id | VARCHAR(100) | NO | Race identifier |
| horse_registration | VARCHAR(20) | NO | Horse identifier |
| model_probability | DECIMAL(5,4) | NO | Model's win probability |
| market_odds | DECIMAL(8,2) | NO | Odds at recommendation time |
| expected_value | DECIMAL(5,4) | NO | Calculated EV |
| overlay | DECIMAL(5,4) | YES | Overlay percentage |
| kelly_fraction | DECIMAL(5,4) | YES | Full Kelly fraction |
| recommended_stake | DECIMAL(10,2) | NO | Recommended bet amount |
| confidence_level | VARCHAR(20) | YES | HIGH, MEDIUM, LOW |
| track_segment | VARCHAR(20) | YES | high_volume or regional |
| recommendation_time | TIMESTAMPTZ | NO | When generated |
| expires_at | TIMESTAMPTZ | YES | Recommendation expiry |
| created_at | TIMESTAMPTZ | NO | Record creation time |

---

### 5.2 betting.bet_log

Comprehensive bet logging for analysis.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| log_id | SERIAL | NO | **PK** |
| recommendation_id | VARCHAR(50) | YES | **FK** → bet_recommendations |
| race_id | VARCHAR(100) | NO | Race identifier |
| horse_registration | VARCHAR(20) | NO | Horse identifier |
| recommended_stake | DECIMAL(10,2) | YES | System recommendation |
| recommended_odds | DECIMAL(8,2) | YES | Odds at recommendation |
| model_probability | DECIMAL(5,4) | YES | Model probability |
| expected_value | DECIMAL(5,4) | YES | Expected value |
| recommendation_time | TIMESTAMPTZ | YES | Recommendation timestamp |
| executed | BOOLEAN | NO | Bet was placed |
| actual_stake | DECIMAL(10,2) | YES | Actual amount bet |
| actual_odds | DECIMAL(8,2) | YES | Odds at execution |
| platform | VARCHAR(50) | YES | TwinSpires, DraftKings |
| execution_time | TIMESTAMPTZ | YES | When bet was placed |
| skip_reason | VARCHAR(200) | YES | Why bet was skipped |
| outcome | VARCHAR(20) | YES | WIN, LOSE, VOID, SCRATCH |
| payout | DECIMAL(10,2) | YES | Payout received |
| final_odds | DECIMAL(8,2) | YES | Final tote odds |
| result_time | TIMESTAMPTZ | YES | When result recorded |
| odds_slippage | DECIMAL(5,4) | YES | (actual-rec)/rec |
| realized_ev | DECIMAL(5,4) | YES | Actual EV achieved |
| created_at | TIMESTAMPTZ | NO | Record creation |

---

### 5.3 betting.bankroll_snapshots

Periodic bankroll state captures.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| snapshot_id | SERIAL | NO | **PK** |
| snapshot_time | TIMESTAMPTZ | NO | Snapshot timestamp |
| total_bankroll | DECIMAL(12,2) | NO | Total bankroll value |
| available_balance | DECIMAL(12,2) | NO | Available for betting |
| pending_bets | DECIMAL(12,2) | YES | Bets not yet settled |
| daily_pnl | DECIMAL(10,2) | YES | P&L today |
| weekly_pnl | DECIMAL(10,2) | YES | P&L this week |
| monthly_pnl | DECIMAL(10,2) | YES | P&L this month |
| total_pnl | DECIMAL(10,2) | YES | All-time P&L |

---

### 5.4 betting.split_test_results

Split test segment performance.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| result_id | SERIAL | NO | **PK** |
| segment | VARCHAR(20) | NO | high_volume or regional |
| period_start | DATE | NO | Period start date |
| period_end | DATE | NO | Period end date |
| total_bets | INTEGER | NO | Number of bets |
| winners | INTEGER | NO | Number of winners |
| total_staked | DECIMAL(12,2) | NO | Total amount bet |
| total_returned | DECIMAL(12,2) | NO | Total returns |
| roi | DECIMAL(8,4) | YES | Return on investment |
| sharpe_ratio | DECIMAL(6,4) | YES | Risk-adjusted return |
| avg_odds | DECIMAL(8,2) | YES | Average odds bet |
| avg_ev | DECIMAL(5,4) | YES | Average expected value |
| created_at | TIMESTAMPTZ | NO | Record creation |

---

## 6. Monitoring Schema Tables

### 6.1 monitoring.predictions

Prediction log for calibration tracking.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| prediction_id | SERIAL | NO | **PK** |
| race_id | VARCHAR(100) | NO | Race identifier |
| horse_registration | VARCHAR(20) | NO | Horse identifier |
| prediction_time | TIMESTAMPTZ | NO | When predicted |
| model_version | VARCHAR(50) | YES | Model version used |
| predicted_probability | DECIMAL(5,4) | NO | Win probability |
| field_size | INTEGER | YES | Number of horses |
| actual_outcome | INTEGER | YES | 1=win, 0=loss |
| actual_finish | INTEGER | YES | Finish position |

---

### 6.2 monitoring.calibration_metrics

Daily calibration tracking.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| metric_id | SERIAL | NO | **PK** |
| calculation_date | DATE | NO | Date calculated |
| model_version | VARCHAR(50) | YES | Model version |
| brier_score | DECIMAL(6,4) | YES | Brier score |
| log_loss | DECIMAL(8,4) | YES | Log loss |
| ece | DECIMAL(6,4) | YES | Expected calibration error |
| sample_size | INTEGER | YES | Number of predictions |
| calibration_by_bucket | JSONB | YES | Per-bucket calibration |

---

### 6.3 monitoring.validation_runs

Feature leakage validation log.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| run_id | SERIAL | NO | **PK** |
| run_time | TIMESTAMPTZ | NO | Validation time |
| races_validated | INTEGER | NO | Number of races checked |
| races_passed | INTEGER | NO | Races without leakage |
| races_failed | INTEGER | NO | Races with leakage |
| failure_details | JSONB | YES | Details of failures |
| status | VARCHAR(20) | NO | PASSED, FAILED |

---

## 7. Reference Tables

### 7.1 reference.course_types

| Column | Type | Description |
|--------|------|-------------|
| code | VARCHAR(20) | **PK** DIRT, TURF, SYNTHETIC |
| description | VARCHAR(100) | Full description |
| surface_category | VARCHAR(20) | Grouping category |

### 7.2 reference.race_types

| Column | Type | Description |
|--------|------|-------------|
| code | VARCHAR(20) | **PK** G1, ALW, CLM, etc. |
| description | VARCHAR(200) | Full description |
| class_level | INTEGER | 1-10 hierarchy |
| purse_category | VARCHAR(50) | GRADED_STAKES, etc. |

### 7.3 reference.track_conditions

| Column | Type | Description |
|--------|------|-------------|
| code | VARCHAR(20) | **PK** FAST, GOOD, etc. |
| description | VARCHAR(100) | Full description |
| surface_speed | VARCHAR(20) | fast, slow, average |
| bias_tendency | VARCHAR(50) | speed, closer, neutral |

---

## 8. Views

### 8.1 racing.vw_race_entries_complete

Comprehensive view joining race entries with all related data.

```sql
CREATE VIEW racing.vw_race_entries_complete AS
SELECT
    re.*,
    r.race_date,
    r.track_code,
    r.race_number,
    r.course_type_code,
    r.race_type_code,
    r.track_condition,
    r.distance_yards,
    r.purse_usd,
    r.class_level,
    h.horse_name,
    h.sex_code,
    h.year_of_birth,
    t.first_name as trainer_first_name,
    t.last_name as trainer_last_name,
    o.first_name as owner_first_name,
    o.last_name as owner_last_name
FROM racing.race_entries re
JOIN racing.races r ON re.race_id = r.race_id
JOIN racing.horses_master h ON re.registration_number = h.registration_number
LEFT JOIN racing.trainers t ON re.trainer_id = t.external_party_id
LEFT JOIN racing.owners o ON re.owner_id = o.external_party_id;
```

### 8.2 racing.vw_horse_performance_summary

Aggregated horse performance statistics.

```sql
CREATE VIEW racing.vw_horse_performance_summary AS
SELECT
    registration_number,
    COUNT(*) as total_starts,
    SUM(CASE WHEN official_finish_position = 1 THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN official_finish_position <= 3 THEN 1 ELSE 0 END) as itm,
    AVG(official_finish_position) as avg_finish,
    AVG(speed_rating) as avg_speed,
    MAX(race_date) as last_race_date
FROM racing.vw_race_entries_complete
WHERE official_finish_position IS NOT NULL
GROUP BY registration_number;
```

---

## 9. Equibase XML Field Mappings

### 9.1 Past Performance (simulcast.xsd) → Database

| XML Path | Database Column | Notes |
|----------|-----------------|-------|
| Race/@RaceNumber | races.race_number | |
| Race/Course/@CourseType | races.course_type_code | Standardized |
| Race/Distance/@PublishedValue | races.distance_yards | Converted |
| Race/RaceType/@Description | races.race_type_code | Parsed |
| Race/Starters/Horse/@RegistrationNumber | race_entries.registration_number | |
| Race/Starters/Horse/@HorseName | horses_master.horse_name | |
| Race/Starters/PostPosition | race_entries.post_position | |
| Race/Starters/WeightCarried | race_entries.weight_lbs | |
| Race/Starters/Equipment | race_entries.has_* | Parsed |
| Race/Starters/Odds | race_entries.morning_line_odds | |
| Race/Starters/Trainer | race_entries.trainer_id | |

### 9.2 Result Charts (tchSchema.xsd) → Database

| XML Path | Database Column | Notes |
|----------|-----------------|-------|
| RACE/@NUMBER | races.race_number | |
| RACE/WIN_TIME | races.winning_time | |
| RACE/FRACTION_1 | race_fractions.fraction_time | |
| ENTRY/OFFICIAL_FIN | race_entries.official_finish_position | |
| ENTRY/DOLLAR_ODDS | race_entries.actual_odds | |
| ENTRY/WIN_PAYOFF | race_entries.win_payoff | |
| ENTRY/SPEED_RATING | race_entries.speed_rating | |
| ENTRY/POINT_OF_CALL | horse_position_calls | |

---

## 10. Data Quality Rules

### 10.1 Required Fields

| Table | Required Columns |
|-------|------------------|
| races | race_id, track_code, race_date, race_number |
| race_entries | entry_id, race_id, registration_number |
| horses_master | registration_number |

### 10.2 Validation Rules

| Rule | Description | Action |
|------|-------------|--------|
| Valid date | race_date must be valid | Reject record |
| Post position range | post_position 1-20 | Null if invalid |
| Finish position range | 1 to field_size | Null if invalid |
| Probability sum | Race probs sum to 1 | Re-normalize |
| Odds positive | odds > 0 | Null if invalid |

### 10.3 Missing Value Handling

| Column | Missing Value Strategy |
|--------|------------------------|
| speed_rating | Use feature flag: missing_speed_figure |
| morning_line_odds | Exclude from betting |
| official_finish_position | Race not yet run |
| trainer_id | Use 'UNKNOWN' placeholder |

---

*Document maintained by: Data Engineering Team*
*Review cycle: Quarterly*
