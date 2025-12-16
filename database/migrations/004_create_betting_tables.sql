-- =============================================================================
-- Migration 004: Create Betting and Monitoring Tables
-- Version: 1.0
-- Description: Creates tables for betting operations, model tracking, and monitoring
-- =============================================================================

-- =============================================================================
-- MODELS SCHEMA
-- =============================================================================

-- Model registry
CREATE TABLE IF NOT EXISTS models.model_registry (
    model_id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    algorithm VARCHAR(50) NOT NULL,

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
    field_normalized_probability DECIMAL(6,5),

    -- Prediction context
    field_size INTEGER,
    field_size_bucket VARCHAR(20),

    -- Outcome
    actual_finish_position INTEGER,
    is_winner BOOLEAN,

    prediction_time TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT fk_pred_model FOREIGN KEY (model_id) REFERENCES models.model_registry(model_id),
    CONSTRAINT fk_pred_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id)
);

-- Create hypertable
SELECT create_hypertable('models.prediction_log', 'prediction_time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- =============================================================================
-- BETTING SCHEMA
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

-- Create hypertable
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
    capped_stake DECIMAL(10,2),

    -- Context
    track_type VARCHAR(20),
    bankroll_at_time DECIMAL(12,2),
    daily_exposure_at_time DECIMAL(6,4),

    -- Status
    recommendation_status VARCHAR(20) DEFAULT 'pending',

    recommendation_time TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,

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
    platform VARCHAR(50),
    execution_time TIMESTAMPTZ,
    skip_reason VARCHAR(200),

    -- Result data
    outcome VARCHAR(20),
    payout DECIMAL(10,2),
    final_odds DECIMAL(8,2),
    final_finish_position INTEGER,
    result_time TIMESTAMPTZ,

    -- Analysis
    odds_slippage DECIMAL(6,4),
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

-- Create hypertable
SELECT create_hypertable('betting.bet_log', 'created_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Split test tracking
CREATE TABLE IF NOT EXISTS betting.split_test_performance (
    record_id SERIAL PRIMARY KEY,
    calculation_date DATE NOT NULL,
    track_type VARCHAR(20) NOT NULL,

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
-- MONITORING SCHEMA
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
    bucket_calibration JSONB,

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
    drift_score DECIMAL(6,4),
    drift_alert BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_drift_date_feature UNIQUE (calculation_date, model_id, feature_name)
);

-- Alert log
CREATE TABLE IF NOT EXISTS monitoring.alerts (
    alert_id SERIAL PRIMARY KEY,
    alert_time TIMESTAMPTZ DEFAULT NOW(),
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,

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
-- INDEXES
-- =============================================================================

-- Models schema
CREATE INDEX IF NOT EXISTS idx_model_active ON models.model_registry(is_active);
CREATE INDEX IF NOT EXISTS idx_pred_log_race ON models.prediction_log(race_id);
CREATE INDEX IF NOT EXISTS idx_pred_log_model ON models.prediction_log(model_id);

-- Betting schema
CREATE INDEX IF NOT EXISTS idx_bankroll_time ON betting.bankroll_snapshots(snapshot_time);
CREATE INDEX IF NOT EXISTS idx_bet_rec_race ON betting.bet_recommendations(race_id);
CREATE INDEX IF NOT EXISTS idx_bet_rec_status ON betting.bet_recommendations(recommendation_status);
CREATE INDEX IF NOT EXISTS idx_bet_log_race ON betting.bet_log(race_id);
CREATE INDEX IF NOT EXISTS idx_bet_log_outcome ON betting.bet_log(outcome);
CREATE INDEX IF NOT EXISTS idx_bet_log_platform ON betting.bet_log(platform);
CREATE INDEX IF NOT EXISTS idx_split_test_date ON betting.split_test_performance(calculation_date);

-- Monitoring schema
CREATE INDEX IF NOT EXISTS idx_daily_perf_date ON monitoring.daily_performance(performance_date);
CREATE INDEX IF NOT EXISTS idx_calib_date ON monitoring.calibration_history(calculation_date);
CREATE INDEX IF NOT EXISTS idx_drift_date ON monitoring.feature_drift(calculation_date);
CREATE INDEX IF NOT EXISTS idx_alerts_type ON monitoring.alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON monitoring.alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_unresolved ON monitoring.alerts(resolved) WHERE NOT resolved;

-- =============================================================================
-- VIEWS
-- =============================================================================

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

-- Active alerts view
CREATE OR REPLACE VIEW monitoring.vw_active_alerts AS
SELECT
    alert_id,
    alert_time,
    alert_type,
    severity,
    message,
    details,
    acknowledged,
    NOW() - alert_time AS age
FROM monitoring.alerts
WHERE NOT resolved
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'warning' THEN 2
        ELSE 3
    END,
    alert_time DESC;

-- =============================================================================
-- MIGRATION TRACKING
-- =============================================================================

INSERT INTO public.schema_migrations (version, description)
VALUES ('004', 'Create betting and monitoring tables')
ON CONFLICT (version) DO NOTHING;
