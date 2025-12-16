-- =============================================================================
-- Horse Racing Quantitative Betting System - PostgreSQL Schema
-- Version: 1.0
-- Database: PostgreSQL 15+ with TimescaleDB extension
-- Last Updated: 2025-12-16
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- =============================================================================
-- SCHEMA ORGANIZATION
-- =============================================================================
-- racing    : Core racing data (migrated from SQLite)
-- features  : Computed features for ML
-- models    : Model artifacts and versioning
-- betting   : Betting operations and recommendations
-- monitoring: Performance tracking and calibration

CREATE SCHEMA IF NOT EXISTS racing;
CREATE SCHEMA IF NOT EXISTS features;
CREATE SCHEMA IF NOT EXISTS models;
CREATE SCHEMA IF NOT EXISTS betting;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Set default search path
ALTER DATABASE racing_db SET search_path TO racing, features, models, betting, monitoring, public;

-- =============================================================================
-- RACING SCHEMA - Core Racing Data
-- =============================================================================

-- Reference Tables
-- -----------------------------------------------------------------------------

-- Course types and surface categories
CREATE TABLE IF NOT EXISTS racing.course_types (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(100),
    surface_category VARCHAR(20), -- 'dirt', 'turf', 'synthetic'
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Race type classifications with hierarchy
CREATE TABLE IF NOT EXISTS racing.race_types (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(200),
    class_level INTEGER NOT NULL, -- 1=lowest (maiden), 10=highest (G1)
    purse_category VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Equipment standardization
CREATE TABLE IF NOT EXISTS racing.equipment_types (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(100),
    equipment_category VARCHAR(50), -- 'vision', 'respiratory', 'medication', etc.
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Track condition mappings
CREATE TABLE IF NOT EXISTS racing.track_conditions (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(100),
    surface_speed VARCHAR(20), -- 'fast', 'slow', 'average'
    bias_tendency VARCHAR(50), -- 'speed', 'closer', 'neutral'
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Track information
CREATE TABLE IF NOT EXISTS racing.tracks (
    track_code VARCHAR(10) PRIMARY KEY,
    track_name VARCHAR(200) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50) DEFAULT 'USA',
    track_type VARCHAR(50), -- 'high_volume', 'regional', 'minor'
    timezone VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Core Entity Tables
-- -----------------------------------------------------------------------------

-- Horse master data
CREATE TABLE IF NOT EXISTS racing.horses_master (
    registration_number VARCHAR(20) PRIMARY KEY,
    horse_name VARCHAR(200) NOT NULL,
    foaling_date DATE,
    year_of_birth INTEGER,
    sex_code VARCHAR(10),
    color_code VARCHAR(50),
    sire_registration VARCHAR(20),
    dam_registration VARCHAR(20),
    dam_sire_registration VARCHAR(20),
    breeder VARCHAR(200),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Trainers
CREATE TABLE IF NOT EXISTS racing.trainers (
    external_party_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    license_state VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Jockeys
CREATE TABLE IF NOT EXISTS racing.jockeys (
    external_party_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    license_state VARCHAR(50),
    weight_allowance INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Owners
CREATE TABLE IF NOT EXISTS racing.owners (
    external_party_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    ownership_type VARCHAR(50), -- 'individual', 'partnership', 'syndicate'
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Race Data Tables
-- -----------------------------------------------------------------------------

-- Races with standardized fields
CREATE TABLE IF NOT EXISTS racing.races (
    race_id VARCHAR(100) PRIMARY KEY,
    track_code VARCHAR(10) NOT NULL,
    race_date DATE NOT NULL,
    race_number INTEGER NOT NULL,

    -- Original race information
    race_name VARCHAR(500),
    conditions_text TEXT,

    -- Standardized categorical fields
    course_type_code VARCHAR(20),
    race_type_code VARCHAR(20),
    track_condition VARCHAR(20),

    -- Parsed restriction fields
    min_age INTEGER,
    max_age INTEGER,
    fillies_and_mares BOOLEAN DEFAULT FALSE,
    colts_and_geldings BOOLEAN DEFAULT FALSE,
    fillies_only BOOLEAN DEFAULT FALSE,
    mares_only BOOLEAN DEFAULT FALSE,
    colts_only BOOLEAN DEFAULT FALSE,
    geldings_only BOOLEAN DEFAULT FALSE,

    -- Standardized numeric fields
    distance_yards INTEGER,
    purse_usd DECIMAL(12,2),
    max_claim_price DECIMAL(12,2),
    min_claim_price DECIMAL(12,2),

    -- Race classification
    class_level INTEGER,
    purse_category VARCHAR(50),

    -- Timing and environmental
    post_time TIME,
    weather VARCHAR(100),
    wind_speed INTEGER,
    wind_direction VARCHAR(50),

    -- Race results (when available)
    field_size INTEGER,
    winning_time DECIMAL(8,3),
    winning_margin DECIMAL(6,2),
    final_fraction_time DECIMAL(8,3),

    -- Metadata
    source_file VARCHAR(500),
    data_source VARCHAR(50), -- 'past_performance' or 'result_chart'
    extraction_date TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Constraints
    CONSTRAINT fk_race_course_type FOREIGN KEY (course_type_code) REFERENCES racing.course_types(code),
    CONSTRAINT fk_race_race_type FOREIGN KEY (race_type_code) REFERENCES racing.race_types(code),
    CONSTRAINT fk_race_track_condition FOREIGN KEY (track_condition) REFERENCES racing.track_conditions(code),
    CONSTRAINT fk_race_track FOREIGN KEY (track_code) REFERENCES racing.tracks(track_code)
);

-- Create hypertable for time-series optimization
SELECT create_hypertable('racing.races', 'race_date',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Race entries with standardized horse data
CREATE TABLE IF NOT EXISTS racing.race_entries (
    entry_id VARCHAR(150) PRIMARY KEY, -- race_id + registration_number
    race_id VARCHAR(100) NOT NULL,
    registration_number VARCHAR(20) NOT NULL,

    -- Basic entry information
    program_number VARCHAR(10),
    post_position INTEGER,

    -- Standardized physical data
    weight_lbs INTEGER,
    age_at_race INTEGER,

    -- Equipment and medication (boolean flags)
    has_blinkers BOOLEAN DEFAULT FALSE,
    has_lasix BOOLEAN DEFAULT FALSE,
    has_tongue_tie BOOLEAN DEFAULT FALSE,
    has_nasal_strip BOOLEAN DEFAULT FALSE,
    has_shadow_roll BOOLEAN DEFAULT FALSE,
    has_cheek_pieces BOOLEAN DEFAULT FALSE,
    has_ear_plugs BOOLEAN DEFAULT FALSE,
    has_hood BOOLEAN DEFAULT FALSE,

    -- Equipment/medication change indicators
    equipment_change_indicator VARCHAR(50),
    lasix_first_time BOOLEAN DEFAULT FALSE,
    blinkers_first_time BOOLEAN DEFAULT FALSE,
    blinkers_off BOOLEAN DEFAULT FALSE,

    -- Claiming and wagering
    claim_price DECIMAL(10,2),
    morning_line_odds DECIMAL(8,2),

    -- Performance data (when from results)
    official_finish_position INTEGER,
    actual_odds DECIMAL(8,2),
    win_payoff DECIMAL(8,2),
    place_payoff DECIMAL(8,2),
    show_payoff DECIMAL(8,2),

    -- Speed and time data
    final_time DECIMAL(8,3),
    speed_rating INTEGER,

    -- Trip and pace information
    start_position INTEGER,
    first_call_position INTEGER,
    second_call_position INTEGER,
    stretch_position INTEGER,
    finish_position INTEGER,
    beaten_lengths DECIMAL(6,2),

    -- Connections
    trainer_id VARCHAR(20),
    jockey_id VARCHAR(20),
    owner_id VARCHAR(20),

    -- Comments and notes
    race_comments TEXT,
    scratched BOOLEAN DEFAULT FALSE,
    scratch_reason VARCHAR(200),

    -- Metadata
    source_file VARCHAR(500),
    data_source VARCHAR(50),
    extraction_date TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Constraints
    CONSTRAINT fk_entry_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id),
    CONSTRAINT fk_entry_horse FOREIGN KEY (registration_number) REFERENCES racing.horses_master(registration_number),
    CONSTRAINT fk_entry_trainer FOREIGN KEY (trainer_id) REFERENCES racing.trainers(external_party_id),
    CONSTRAINT fk_entry_jockey FOREIGN KEY (jockey_id) REFERENCES racing.jockeys(external_party_id),
    CONSTRAINT fk_entry_owner FOREIGN KEY (owner_id) REFERENCES racing.owners(external_party_id),
    CONSTRAINT uq_race_horse UNIQUE (race_id, registration_number)
);

-- Equipment details (many-to-many)
CREATE TABLE IF NOT EXISTS racing.horse_race_equipment (
    race_id VARCHAR(100),
    registration_number VARCHAR(20),
    equipment_code VARCHAR(20),
    equipment_description VARCHAR(100),
    is_first_time BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (race_id, registration_number, equipment_code),
    CONSTRAINT fk_equip_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id),
    CONSTRAINT fk_equip_horse FOREIGN KEY (registration_number) REFERENCES racing.horses_master(registration_number),
    CONSTRAINT fk_equip_type FOREIGN KEY (equipment_code) REFERENCES racing.equipment_types(code)
);

-- Race fractional times
CREATE TABLE IF NOT EXISTS racing.race_fractions (
    race_id VARCHAR(100),
    call_position INTEGER,
    distance_yards INTEGER,
    fraction_time DECIMAL(8,3),
    leader_at_call VARCHAR(20),
    PRIMARY KEY (race_id, call_position),
    CONSTRAINT fk_frac_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id),
    CONSTRAINT fk_frac_leader FOREIGN KEY (leader_at_call) REFERENCES racing.horses_master(registration_number)
);

-- Horse position calls
CREATE TABLE IF NOT EXISTS racing.horse_position_calls (
    race_id VARCHAR(100),
    registration_number VARCHAR(20),
    call_position INTEGER,
    position INTEGER,
    lengths_behind DECIMAL(6,2),
    PRIMARY KEY (race_id, registration_number, call_position),
    CONSTRAINT fk_pos_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id),
    CONSTRAINT fk_pos_horse FOREIGN KEY (registration_number) REFERENCES racing.horses_master(registration_number)
);

-- Race wagering pools
CREATE TABLE IF NOT EXISTS racing.race_wagering (
    race_id VARCHAR(100),
    wager_type VARCHAR(50),
    pool_total DECIMAL(12,2),
    winning_combinations TEXT,
    payout DECIMAL(10,2),
    number_of_winners INTEGER,
    PRIMARY KEY (race_id, wager_type),
    CONSTRAINT fk_wager_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id)
);

-- =============================================================================
-- FEATURES SCHEMA - Computed Features for ML
-- =============================================================================

-- Trainer rolling statistics
CREATE TABLE IF NOT EXISTS features.trainer_rolling_stats (
    stat_id SERIAL PRIMARY KEY,
    trainer_id VARCHAR(20) NOT NULL,
    calculation_date DATE NOT NULL,
    window_days INTEGER NOT NULL, -- 14, 30, 60

    -- Performance metrics
    starts INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    places INTEGER DEFAULT 0, -- Top 2
    shows INTEGER DEFAULT 0,  -- Top 3

    -- Rates
    win_rate DECIMAL(5,4),
    place_rate DECIMAL(5,4),
    show_rate DECIMAL(5,4),
    roi DECIMAL(8,4), -- Return on $2 win bet

    -- Advanced metrics
    avg_finish_position DECIMAL(4,2),
    avg_odds DECIMAL(8,2),
    avg_morning_line DECIMAL(8,2),

    -- Context splits
    dirt_wins INTEGER DEFAULT 0,
    dirt_starts INTEGER DEFAULT 0,
    turf_wins INTEGER DEFAULT 0,
    turf_starts INTEGER DEFAULT 0,

    -- Sample size indicator
    sufficient_sample BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_trainer_date_window UNIQUE (trainer_id, calculation_date, window_days),
    CONSTRAINT fk_trainer_stats FOREIGN KEY (trainer_id) REFERENCES racing.trainers(external_party_id)
);

-- Create hypertable for trainer stats
SELECT create_hypertable('features.trainer_rolling_stats', 'calculation_date',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Jockey rolling statistics
CREATE TABLE IF NOT EXISTS features.jockey_rolling_stats (
    stat_id SERIAL PRIMARY KEY,
    jockey_id VARCHAR(20) NOT NULL,
    calculation_date DATE NOT NULL,
    window_days INTEGER NOT NULL, -- 14, 30, 60

    -- Performance metrics
    starts INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    places INTEGER DEFAULT 0,
    shows INTEGER DEFAULT 0,

    -- Rates
    win_rate DECIMAL(5,4),
    place_rate DECIMAL(5,4),
    show_rate DECIMAL(5,4),
    roi DECIMAL(8,4),

    -- Advanced metrics
    avg_finish_position DECIMAL(4,2),
    avg_odds DECIMAL(8,2),
    avg_morning_line DECIMAL(8,2),

    -- Context splits
    dirt_wins INTEGER DEFAULT 0,
    dirt_starts INTEGER DEFAULT 0,
    turf_wins INTEGER DEFAULT 0,
    turf_starts INTEGER DEFAULT 0,

    -- Sample size indicator
    sufficient_sample BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_jockey_date_window UNIQUE (jockey_id, calculation_date, window_days),
    CONSTRAINT fk_jockey_stats FOREIGN KEY (jockey_id) REFERENCES racing.jockeys(external_party_id)
);

-- Create hypertable for jockey stats
SELECT create_hypertable('features.jockey_rolling_stats', 'calculation_date',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Trainer-Jockey combination stats
CREATE TABLE IF NOT EXISTS features.trainer_jockey_combo_stats (
    stat_id SERIAL PRIMARY KEY,
    trainer_id VARCHAR(20) NOT NULL,
    jockey_id VARCHAR(20) NOT NULL,
    calculation_date DATE NOT NULL,
    window_days INTEGER NOT NULL,

    -- Performance metrics
    starts INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    places INTEGER DEFAULT 0,
    shows INTEGER DEFAULT 0,

    -- Rates
    win_rate DECIMAL(5,4),
    roi DECIMAL(8,4),

    -- Comparison to individual rates
    synergy_score DECIMAL(6,4), -- combo_rate vs (trainer_rate + jockey_rate)/2

    -- Sample size indicator
    sufficient_sample BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_combo_date_window UNIQUE (trainer_id, jockey_id, calculation_date, window_days)
);

-- Track bias statistics
CREATE TABLE IF NOT EXISTS features.track_bias_stats (
    stat_id SERIAL PRIMARY KEY,
    track_code VARCHAR(10) NOT NULL,
    surface VARCHAR(20) NOT NULL, -- 'dirt', 'turf', 'synthetic'
    distance_bucket VARCHAR(20) NOT NULL, -- 'sprint', 'route', 'marathon'
    calculation_date DATE NOT NULL,
    window_days INTEGER NOT NULL,

    -- Post position win rates
    post_1_win_rate DECIMAL(5,4),
    post_2_win_rate DECIMAL(5,4),
    post_3_win_rate DECIMAL(5,4),
    post_4_win_rate DECIMAL(5,4),
    post_5_win_rate DECIMAL(5,4),
    post_6_win_rate DECIMAL(5,4),
    post_7_win_rate DECIMAL(5,4),
    post_8_win_rate DECIMAL(5,4),
    post_outside_win_rate DECIMAL(5,4), -- Posts 9+

    -- Overall bias metrics
    inside_bias_score DECIMAL(6,4), -- Positive = inside advantage
    speed_bias_score DECIMAL(6,4),  -- Positive = speed holding advantage

    -- Sample size
    total_races INTEGER DEFAULT 0,
    sufficient_sample BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_track_bias UNIQUE (track_code, surface, distance_bucket, calculation_date, window_days)
);

-- Horse rolling form
CREATE TABLE IF NOT EXISTS features.horse_rolling_form (
    stat_id SERIAL PRIMARY KEY,
    registration_number VARCHAR(20) NOT NULL,
    calculation_date DATE NOT NULL,

    -- Recent form
    days_since_last_race INTEGER,
    last_3_finishes VARCHAR(20), -- e.g., '1-2-3'
    last_3_speed_avg DECIMAL(5,2),

    -- Career stats at surface
    dirt_starts INTEGER DEFAULT 0,
    dirt_wins INTEGER DEFAULT 0,
    dirt_avg_finish DECIMAL(4,2),
    turf_starts INTEGER DEFAULT 0,
    turf_wins INTEGER DEFAULT 0,
    turf_avg_finish DECIMAL(4,2),

    -- Distance preferences
    sprint_starts INTEGER DEFAULT 0,
    sprint_wins INTEGER DEFAULT 0,
    route_starts INTEGER DEFAULT 0,
    route_wins INTEGER DEFAULT 0,

    -- Speed figures
    best_speed_90_days INTEGER,
    avg_speed_90_days DECIMAL(5,2),
    speed_trend DECIMAL(5,2), -- Improvement/decline

    -- Class metrics
    last_class_level INTEGER,
    avg_class_level DECIMAL(4,2),
    class_change INTEGER, -- vs today's race

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_horse_form UNIQUE (registration_number, calculation_date),
    CONSTRAINT fk_horse_form FOREIGN KEY (registration_number) REFERENCES racing.horses_master(registration_number)
);

-- Pre-computed race features (for inference speed)
CREATE TABLE IF NOT EXISTS features.race_features (
    feature_id SERIAL PRIMARY KEY,
    race_id VARCHAR(100) NOT NULL,
    entry_id VARCHAR(150) NOT NULL,

    -- Horse form features (20)
    days_since_last DECIMAL(6,2),
    last_finish_position INTEGER,
    last_3_avg_finish DECIMAL(4,2),
    last_speed_figure INTEGER,
    avg_speed_90_days DECIMAL(5,2),
    best_speed_90_days INTEGER,
    speed_trend DECIMAL(5,2),
    career_win_rate DECIMAL(5,4),
    surface_win_rate DECIMAL(5,4),
    distance_win_rate DECIMAL(5,4),
    class_change INTEGER,
    avg_class_level DECIMAL(4,2),
    total_starts INTEGER,
    total_wins INTEGER,
    layoff_indicator BOOLEAN,
    first_time_starter BOOLEAN,
    age_at_race INTEGER,
    weight_carried INTEGER,
    weight_change INTEGER,
    morning_line_odds DECIMAL(8,2),

    -- Connection features (20)
    trainer_win_rate_14d DECIMAL(5,4),
    trainer_win_rate_30d DECIMAL(5,4),
    trainer_win_rate_60d DECIMAL(5,4),
    trainer_roi_30d DECIMAL(8,4),
    jockey_win_rate_14d DECIMAL(5,4),
    jockey_win_rate_30d DECIMAL(5,4),
    jockey_win_rate_60d DECIMAL(5,4),
    jockey_roi_30d DECIMAL(8,4),
    combo_win_rate DECIMAL(5,4),
    combo_synergy_score DECIMAL(6,4),
    trainer_hot_streak BOOLEAN,
    jockey_hot_streak BOOLEAN,
    trainer_surface_win_rate DECIMAL(5,4),
    jockey_surface_win_rate DECIMAL(5,4),
    trainer_distance_win_rate DECIMAL(5,4),
    jockey_distance_win_rate DECIMAL(5,4),
    trainer_track_win_rate DECIMAL(5,4),
    jockey_track_win_rate DECIMAL(5,4),
    trainer_sample_flag BOOLEAN,
    jockey_sample_flag BOOLEAN,

    -- Speed/Pace features (10)
    speed_rank_in_field INTEGER,
    early_pace_figure DECIMAL(5,2),
    late_pace_figure DECIMAL(5,2),
    pace_style VARCHAR(20),
    projected_pace_scenario VARCHAR(20),
    speed_vs_field_avg DECIMAL(5,2),
    pace_vs_field_avg DECIMAL(5,2),
    track_variant_adjustment DECIMAL(5,2),
    speed_last_3_avg DECIMAL(5,2),
    pace_last_3_avg DECIMAL(5,2),

    -- Class features (10)
    class_rank_in_field INTEGER,
    purse_rank_in_field INTEGER,
    earnings_per_start DECIMAL(10,2),
    class_drop_indicator BOOLEAN,
    class_rise_indicator BOOLEAN,
    stakes_experience BOOLEAN,
    graded_stakes_experience BOOLEAN,
    last_purse DECIMAL(12,2),
    avg_purse DECIMAL(12,2),
    class_consistency DECIMAL(5,4),

    -- Track/Condition features (10)
    post_position INTEGER,
    post_position_win_rate DECIMAL(5,4),
    surface_preference DECIMAL(5,4),
    distance_preference DECIMAL(5,4),
    track_experience_starts INTEGER,
    track_experience_wins INTEGER,
    condition_preference DECIMAL(5,4),
    rail_bias_adjustment DECIMAL(5,4),
    field_size INTEGER,
    field_quality_score DECIMAL(5,2),

    -- Equipment features (10)
    blinkers_on BOOLEAN,
    blinkers_first_time BOOLEAN,
    blinkers_off BOOLEAN,
    lasix_on BOOLEAN,
    lasix_first_time BOOLEAN,
    equipment_change BOOLEAN,
    tongue_tie BOOLEAN,
    nasal_strip BOOLEAN,
    equipment_change_positive BOOLEAN,
    medication_change BOOLEAN,

    -- Meta features (5)
    trainer_sample_size_flag BOOLEAN,
    jockey_sample_size_flag BOOLEAN,
    combo_sample_size_flag BOOLEAN,
    horse_sample_size_flag BOOLEAN,
    track_bias_sample_flag BOOLEAN,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_race_entry_features UNIQUE (race_id, entry_id),
    CONSTRAINT fk_features_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id)
);

-- =============================================================================
-- MODELS SCHEMA - Model Artifacts and Versioning
-- =============================================================================

-- Model registry
CREATE TABLE IF NOT EXISTS models.model_registry (
    model_id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    algorithm VARCHAR(50) NOT NULL, -- 'lightgbm', 'xgboost', etc.

    -- Training details
    training_start_date DATE,
    training_end_date DATE,
    validation_start_date DATE,
    validation_end_date DATE,

    -- Performance metrics
    brier_score DECIMAL(6,4),
    log_loss DECIMAL(6,4),
    calibration_error DECIMAL(6,4),
    roc_auc DECIMAL(6,4),

    -- Backtest results
    backtest_roi DECIMAL(8,4),
    backtest_sharpe DECIMAL(6,4),
    backtest_max_drawdown DECIMAL(6,4),
    backtest_num_bets INTEGER,

    -- Artifact storage
    model_artifact_path VARCHAR(500),
    calibrator_artifact_path VARCHAR(500),
    feature_importance JSONB,
    hyperparameters JSONB,

    -- Status
    is_active BOOLEAN DEFAULT FALSE,
    deployed_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_model_version UNIQUE (model_name, model_version)
);

-- Model predictions log
CREATE TABLE IF NOT EXISTS models.prediction_log (
    prediction_id SERIAL PRIMARY KEY,
    model_id INTEGER NOT NULL,
    race_id VARCHAR(100) NOT NULL,
    entry_id VARCHAR(150) NOT NULL,

    -- Prediction outputs
    raw_probability DECIMAL(6,5),
    calibrated_probability DECIMAL(6,5),
    field_normalized_probability DECIMAL(6,5), -- After softmax

    -- Prediction context
    field_size INTEGER,
    field_size_bucket VARCHAR(20), -- 'small', 'medium', 'large'

    -- Outcome (filled after race)
    actual_finish_position INTEGER,
    is_winner BOOLEAN,

    prediction_time TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_pred_model FOREIGN KEY (model_id) REFERENCES models.model_registry(model_id),
    CONSTRAINT fk_pred_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id)
);

-- Create hypertable for prediction log
SELECT create_hypertable('models.prediction_log', 'prediction_time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- =============================================================================
-- BETTING SCHEMA - Betting Operations
-- =============================================================================

-- Bankroll snapshots
CREATE TABLE IF NOT EXISTS betting.bankroll_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    snapshot_time TIMESTAMPTZ NOT NULL,

    -- Balances
    total_bankroll DECIMAL(12,2) NOT NULL,
    high_volume_allocation DECIMAL(12,2),
    regional_allocation DECIMAL(12,2),

    -- Daily metrics
    daily_starting_bankroll DECIMAL(12,2),
    daily_pnl DECIMAL(10,2),
    daily_pnl_pct DECIMAL(6,4),

    -- Drawdown tracking
    peak_bankroll DECIMAL(12,2),
    current_drawdown DECIMAL(6,4),

    -- Status flags
    reduced_stakes_active BOOLEAN DEFAULT FALSE,
    betting_paused BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create hypertable for bankroll
SELECT create_hypertable('betting.bankroll_snapshots', 'snapshot_time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Bet recommendations
CREATE TABLE IF NOT EXISTS betting.bet_recommendations (
    recommendation_id VARCHAR(50) PRIMARY KEY,
    race_id VARCHAR(100) NOT NULL,
    entry_id VARCHAR(150) NOT NULL,

    -- Recommendation details
    bet_type VARCHAR(20) DEFAULT 'WIN',
    recommended_stake DECIMAL(10,2) NOT NULL,
    recommended_odds DECIMAL(8,2),

    -- Model outputs
    model_probability DECIMAL(6,5) NOT NULL,
    market_probability DECIMAL(6,5),
    expected_value DECIMAL(6,4) NOT NULL,
    overlay_ratio DECIMAL(6,4),

    -- Kelly calculation
    kelly_fraction DECIMAL(6,4),
    kelly_stake DECIMAL(10,2),
    capped_stake DECIMAL(10,2), -- After applying limits

    -- Context
    track_type VARCHAR(20), -- 'high_volume' or 'regional'
    bankroll_at_time DECIMAL(12,2),
    daily_exposure_at_time DECIMAL(6,4),

    -- Status
    recommendation_status VARCHAR(20) DEFAULT 'pending', -- pending, executed, skipped, expired

    recommendation_time TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ, -- Race post time

    CONSTRAINT fk_rec_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id)
);

-- Bet execution log
CREATE TABLE IF NOT EXISTS betting.bet_log (
    bet_id SERIAL PRIMARY KEY,
    recommendation_id VARCHAR(50),
    race_id VARCHAR(100) NOT NULL,
    registration_number VARCHAR(20) NOT NULL,

    -- Recommendation data
    recommended_stake DECIMAL(10,2),
    recommended_odds DECIMAL(8,2),
    model_probability DECIMAL(6,5),
    expected_value DECIMAL(6,4),
    recommendation_time TIMESTAMPTZ,

    -- Execution data
    executed BOOLEAN DEFAULT FALSE,
    actual_stake DECIMAL(10,2),
    actual_odds DECIMAL(8,2),
    platform VARCHAR(50), -- 'twinspires', 'draftkings'
    execution_time TIMESTAMPTZ,
    skip_reason VARCHAR(200),

    -- Result data
    outcome VARCHAR(20), -- 'WIN', 'LOSE', 'VOID', 'SCRATCH'
    payout DECIMAL(10,2),
    final_odds DECIMAL(8,2),
    final_finish_position INTEGER,
    result_time TIMESTAMPTZ,

    -- Analysis
    odds_slippage DECIMAL(6,4), -- (actual - recommended) / recommended
    realized_ev DECIMAL(6,4),
    pnl DECIMAL(10,2),

    -- Context
    track_type VARCHAR(20),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_bet_rec FOREIGN KEY (recommendation_id) REFERENCES betting.bet_recommendations(recommendation_id),
    CONSTRAINT fk_bet_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id),
    CONSTRAINT fk_bet_horse FOREIGN KEY (registration_number) REFERENCES racing.horses_master(registration_number)
);

-- Create hypertable for bet log
SELECT create_hypertable('betting.bet_log', 'created_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Split test tracking
CREATE TABLE IF NOT EXISTS betting.split_test_performance (
    record_id SERIAL PRIMARY KEY,
    calculation_date DATE NOT NULL,
    track_type VARCHAR(20) NOT NULL, -- 'high_volume', 'regional'

    -- Volume metrics
    total_bets INTEGER DEFAULT 0,
    total_staked DECIMAL(12,2) DEFAULT 0,
    total_payout DECIMAL(12,2) DEFAULT 0,

    -- Performance metrics
    roi DECIMAL(8,4),
    sharpe_ratio DECIMAL(6,4),
    win_rate DECIMAL(6,4),
    avg_odds DECIMAL(8,2),
    avg_ev DECIMAL(6,4),

    -- Slippage analysis
    avg_odds_slippage DECIMAL(6,4),

    -- Significance testing
    cumulative_bets INTEGER DEFAULT 0,
    cumulative_roi DECIMAL(8,4),
    sufficient_sample BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_split_date_type UNIQUE (calculation_date, track_type)
);

-- =============================================================================
-- MONITORING SCHEMA - Performance Tracking
-- =============================================================================

-- Daily performance summary
CREATE TABLE IF NOT EXISTS monitoring.daily_performance (
    record_id SERIAL PRIMARY KEY,
    performance_date DATE NOT NULL,

    -- Activity metrics
    races_analyzed INTEGER DEFAULT 0,
    bets_recommended INTEGER DEFAULT 0,
    bets_executed INTEGER DEFAULT 0,
    bets_won INTEGER DEFAULT 0,

    -- Financial metrics
    total_staked DECIMAL(12,2) DEFAULT 0,
    total_payout DECIMAL(12,2) DEFAULT 0,
    daily_pnl DECIMAL(10,2) DEFAULT 0,
    daily_roi DECIMAL(8,4),

    -- Bankroll
    starting_bankroll DECIMAL(12,2),
    ending_bankroll DECIMAL(12,2),

    -- Risk metrics
    max_drawdown_intraday DECIMAL(6,4),

    -- By track type
    high_volume_bets INTEGER DEFAULT 0,
    high_volume_pnl DECIMAL(10,2) DEFAULT 0,
    regional_bets INTEGER DEFAULT 0,
    regional_pnl DECIMAL(10,2) DEFAULT 0,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_perf_date UNIQUE (performance_date)
);

-- Calibration tracking
CREATE TABLE IF NOT EXISTS monitoring.calibration_history (
    record_id SERIAL PRIMARY KEY,
    calculation_date DATE NOT NULL,
    model_id INTEGER NOT NULL,

    -- Calibration metrics
    brier_score DECIMAL(6,4),
    log_loss DECIMAL(6,4),
    expected_calibration_error DECIMAL(6,4),
    max_calibration_error DECIMAL(6,4),

    -- Bucket-wise calibration
    bucket_calibration JSONB, -- {bucket: {predicted: x, actual: y, count: n}}

    -- Sample size
    num_predictions INTEGER,

    -- Alert flags
    calibration_drift_alert BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_calib_date_model UNIQUE (calculation_date, model_id),
    CONSTRAINT fk_calib_model FOREIGN KEY (model_id) REFERENCES models.model_registry(model_id)
);

-- Feature drift monitoring
CREATE TABLE IF NOT EXISTS monitoring.feature_drift (
    record_id SERIAL PRIMARY KEY,
    calculation_date DATE NOT NULL,
    model_id INTEGER NOT NULL,
    feature_name VARCHAR(100) NOT NULL,

    -- Distribution metrics
    mean_value DECIMAL(12,4),
    std_value DECIMAL(12,4),
    min_value DECIMAL(12,4),
    max_value DECIMAL(12,4),

    -- Drift detection
    baseline_mean DECIMAL(12,4),
    baseline_std DECIMAL(12,4),
    drift_score DECIMAL(6,4), -- e.g., PSI or KS statistic
    drift_alert BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_drift_date_feature UNIQUE (calculation_date, model_id, feature_name)
);

-- Alert log
CREATE TABLE IF NOT EXISTS monitoring.alerts (
    alert_id SERIAL PRIMARY KEY,
    alert_time TIMESTAMPTZ DEFAULT NOW(),
    alert_type VARCHAR(50) NOT NULL, -- 'daily_loss', 'calibration_drift', 'drawdown', 'feature_drift'
    severity VARCHAR(20) NOT NULL, -- 'info', 'warning', 'critical'

    message TEXT NOT NULL,
    details JSONB,

    -- Resolution
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMPTZ,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMPTZ,
    resolution_notes TEXT
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Racing schema indexes
CREATE INDEX IF NOT EXISTS idx_races_date ON racing.races(race_date);
CREATE INDEX IF NOT EXISTS idx_races_track_date ON racing.races(track_code, race_date);
CREATE INDEX IF NOT EXISTS idx_races_type ON racing.races(race_type_code);
CREATE INDEX IF NOT EXISTS idx_races_class ON racing.races(class_level);

CREATE INDEX IF NOT EXISTS idx_entries_race ON racing.race_entries(race_id);
CREATE INDEX IF NOT EXISTS idx_entries_horse ON racing.race_entries(registration_number);
CREATE INDEX IF NOT EXISTS idx_entries_trainer ON racing.race_entries(trainer_id);
CREATE INDEX IF NOT EXISTS idx_entries_jockey ON racing.race_entries(jockey_id);
CREATE INDEX IF NOT EXISTS idx_entries_finish ON racing.race_entries(official_finish_position);

-- Features schema indexes
CREATE INDEX IF NOT EXISTS idx_trainer_stats_date ON features.trainer_rolling_stats(calculation_date);
CREATE INDEX IF NOT EXISTS idx_trainer_stats_trainer ON features.trainer_rolling_stats(trainer_id);

CREATE INDEX IF NOT EXISTS idx_jockey_stats_date ON features.jockey_rolling_stats(calculation_date);
CREATE INDEX IF NOT EXISTS idx_jockey_stats_jockey ON features.jockey_rolling_stats(jockey_id);

CREATE INDEX IF NOT EXISTS idx_track_bias_track ON features.track_bias_stats(track_code, surface, distance_bucket);

CREATE INDEX IF NOT EXISTS idx_race_features_race ON features.race_features(race_id);

-- Models schema indexes
CREATE INDEX IF NOT EXISTS idx_pred_log_race ON models.prediction_log(race_id);
CREATE INDEX IF NOT EXISTS idx_pred_log_model ON models.prediction_log(model_id);

-- Betting schema indexes
CREATE INDEX IF NOT EXISTS idx_bet_rec_race ON betting.bet_recommendations(race_id);
CREATE INDEX IF NOT EXISTS idx_bet_rec_status ON betting.bet_recommendations(recommendation_status);
CREATE INDEX IF NOT EXISTS idx_bet_log_race ON betting.bet_log(race_id);
CREATE INDEX IF NOT EXISTS idx_bet_log_outcome ON betting.bet_log(outcome);

-- =============================================================================
-- VIEWS FOR COMMON QUERIES
-- =============================================================================

-- Complete race entry view
CREATE OR REPLACE VIEW racing.vw_race_entries_complete AS
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
    r.purse_category,
    r.field_size,
    h.horse_name,
    h.foaling_date,
    h.year_of_birth,
    h.sex_code,
    h.color_code,
    t.first_name AS trainer_first_name,
    t.last_name AS trainer_last_name,
    j.first_name AS jockey_first_name,
    j.last_name AS jockey_last_name,
    o.first_name AS owner_first_name,
    o.last_name AS owner_last_name
FROM racing.race_entries re
JOIN racing.races r ON re.race_id = r.race_id
JOIN racing.horses_master h ON re.registration_number = h.registration_number
LEFT JOIN racing.trainers t ON re.trainer_id = t.external_party_id
LEFT JOIN racing.jockeys j ON re.jockey_id = j.external_party_id
LEFT JOIN racing.owners o ON re.owner_id = o.external_party_id;

-- Daily betting summary view
CREATE OR REPLACE VIEW betting.vw_daily_summary AS
SELECT
    DATE(created_at) AS bet_date,
    COUNT(*) AS total_bets,
    COUNT(*) FILTER (WHERE executed) AS executed_bets,
    COUNT(*) FILTER (WHERE outcome = 'WIN') AS winners,
    SUM(actual_stake) AS total_staked,
    SUM(payout) AS total_payout,
    SUM(pnl) AS daily_pnl,
    CASE WHEN SUM(actual_stake) > 0
         THEN SUM(pnl) / SUM(actual_stake)
         ELSE 0 END AS roi,
    AVG(odds_slippage) AS avg_slippage
FROM betting.bet_log
WHERE executed = TRUE
GROUP BY DATE(created_at)
ORDER BY bet_date DESC;

-- Model performance view
CREATE OR REPLACE VIEW models.vw_model_performance AS
SELECT
    m.model_id,
    m.model_name,
    m.model_version,
    m.is_active,
    m.brier_score,
    m.calibration_error,
    m.backtest_roi,
    COUNT(p.prediction_id) AS total_predictions,
    COUNT(p.prediction_id) FILTER (WHERE p.is_winner) AS correct_winners,
    AVG(p.calibrated_probability) FILTER (WHERE p.is_winner) AS avg_winner_prob
FROM models.model_registry m
LEFT JOIN models.prediction_log p ON m.model_id = p.model_id
GROUP BY m.model_id, m.model_name, m.model_version, m.is_active,
         m.brier_score, m.calibration_error, m.backtest_roi;

-- =============================================================================
-- POPULATE REFERENCE DATA
-- =============================================================================

-- Course types
INSERT INTO racing.course_types (code, description, surface_category) VALUES
('DIRT', 'Dirt Track', 'dirt'),
('TURF', 'Turf Course', 'turf'),
('SYNTHETIC', 'Synthetic Surface', 'synthetic'),
('UNKNOWN', 'Unknown Surface', 'unknown')
ON CONFLICT (code) DO NOTHING;

-- Race types with hierarchy
INSERT INTO racing.race_types (code, description, class_level, purse_category) VALUES
('G1', 'Grade 1 Stakes', 10, 'GRADED_STAKES'),
('G2', 'Grade 2 Stakes', 9, 'GRADED_STAKES'),
('G3', 'Grade 3 Stakes', 8, 'GRADED_STAKES'),
('L', 'Listed Stakes', 7, 'STAKES'),
('STAKES', 'Stakes Race', 6, 'STAKES'),
('ALLOWANCE', 'Allowance Race', 5, 'ALLOWANCE'),
('N1X', 'Non-Winners of 1 Race Other Than', 4, 'ALLOWANCE'),
('N2X', 'Non-Winners of 2 Races Other Than', 3, 'ALLOWANCE'),
('CLAIMING', 'Claiming Race', 2, 'CLAIMING'),
('MAIDEN', 'Maiden Race', 1, 'MAIDEN'),
('OTHER', 'Other Race Type', 3, 'OTHER'),
('UNKNOWN', 'Unknown Race Type', 0, 'UNKNOWN')
ON CONFLICT (code) DO NOTHING;

-- Equipment types
INSERT INTO racing.equipment_types (code, description, equipment_category) VALUES
('BLINKERS', 'Blinkers', 'vision'),
('BLINKERS_FIRST_TIME', 'Blinkers First Time', 'vision'),
('TONGUE_TIE', 'Tongue Tie', 'respiratory'),
('NASAL_STRIP', 'Nasal Strip', 'respiratory'),
('SHADOW_ROLL', 'Shadow Roll', 'vision'),
('CHEEK_PIECES', 'Cheek Pieces', 'vision'),
('EAR_PLUGS', 'Ear Plugs', 'sensory'),
('HOOD', 'Hood', 'vision'),
('LASIX', 'Lasix (Furosemide)', 'medication'),
('LASIX_FIRST_TIME', 'Lasix First Time', 'medication'),
('LASIX_SECOND_TIME', 'Lasix Second Time', 'medication')
ON CONFLICT (code) DO NOTHING;

-- Track conditions
INSERT INTO racing.track_conditions (code, description, surface_speed, bias_tendency) VALUES
('FAST', 'Fast', 'fast', 'neutral'),
('GOOD', 'Good', 'average', 'neutral'),
('SLOPPY', 'Sloppy', 'slow', 'speed'),
('MUDDY', 'Muddy', 'slow', 'closer'),
('WET_FAST', 'Wet Fast', 'average', 'speed'),
('FIRM', 'Firm', 'fast', 'neutral'),
('YIELDING', 'Yielding', 'slow', 'closer'),
('SOFT', 'Soft', 'slow', 'closer'),
('HEAVY', 'Heavy', 'slow', 'closer'),
('OTHER', 'Other Condition', 'average', 'neutral'),
('UNKNOWN', 'Unknown Condition', 'average', 'neutral')
ON CONFLICT (code) DO NOTHING;

-- Track classifications (high-volume vs regional)
INSERT INTO racing.tracks (track_code, track_name, city, state, track_type) VALUES
('CD', 'Churchill Downs', 'Louisville', 'KY', 'high_volume'),
('SAR', 'Saratoga Race Course', 'Saratoga Springs', 'NY', 'high_volume'),
('BEL', 'Belmont Park', 'Elmont', 'NY', 'high_volume'),
('GP', 'Gulfstream Park', 'Hallandale Beach', 'FL', 'high_volume'),
('SA', 'Santa Anita Park', 'Arcadia', 'CA', 'high_volume'),
('DMR', 'Del Mar Thoroughbred Club', 'Del Mar', 'CA', 'high_volume'),
('KEE', 'Keeneland', 'Lexington', 'KY', 'high_volume'),
('AQU', 'Aqueduct Racetrack', 'Ozone Park', 'NY', 'high_volume'),
('TP', 'Turfway Park', 'Florence', 'KY', 'regional'),
('CT', 'Charles Town Races', 'Charles Town', 'WV', 'regional'),
('PEN', 'Penn National', 'Grantville', 'PA', 'regional'),
('LRL', 'Laurel Park', 'Laurel', 'MD', 'regional'),
('TAM', 'Tampa Bay Downs', 'Tampa', 'FL', 'regional'),
('FG', 'Fair Grounds Race Course', 'New Orleans', 'LA', 'regional'),
('OP', 'Oaklawn Racing Casino Resort', 'Hot Springs', 'AR', 'regional'),
('GG', 'Golden Gate Fields', 'Berkeley', 'CA', 'regional'),
('PRM', 'Prairie Meadows', 'Altoona', 'IA', 'regional'),
('IND', 'Indiana Grand Racing', 'Shelbyville', 'IN', 'regional')
ON CONFLICT (track_code) DO NOTHING;

-- =============================================================================
-- GRANT PERMISSIONS (adjust for your environment)
-- =============================================================================

-- Create application role
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'racing_app') THEN
        CREATE ROLE racing_app WITH LOGIN PASSWORD 'change_me_in_production';
    END IF;
END
$$;

-- Grant schema access
GRANT USAGE ON SCHEMA racing, features, models, betting, monitoring TO racing_app;

-- Grant table access
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA racing TO racing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA features TO racing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA models TO racing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA betting TO racing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA monitoring TO racing_app;

-- Grant sequence access
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA racing TO racing_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA features TO racing_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA models TO racing_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA betting TO racing_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA monitoring TO racing_app;

-- Grant access to future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA racing GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO racing_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA features GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO racing_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA models GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO racing_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA betting GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO racing_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO racing_app;

-- =============================================================================
-- END OF SCHEMA
-- =============================================================================
