-- =============================================================================
-- Migration 001: Create Database Schemas
-- Version: 1.0
-- Description: Creates the schema organization for the racing system
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
-- REFERENCE TABLES
-- =============================================================================

-- Course types and surface categories
CREATE TABLE IF NOT EXISTS racing.course_types (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(100),
    surface_category VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Race type classifications with hierarchy
CREATE TABLE IF NOT EXISTS racing.race_types (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(200),
    class_level INTEGER NOT NULL,
    purse_category VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Equipment standardization
CREATE TABLE IF NOT EXISTS racing.equipment_types (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(100),
    equipment_category VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Track condition mappings
CREATE TABLE IF NOT EXISTS racing.track_conditions (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(100),
    surface_speed VARCHAR(20),
    bias_tendency VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Track information
CREATE TABLE IF NOT EXISTS racing.tracks (
    track_code VARCHAR(10) PRIMARY KEY,
    track_name VARCHAR(200) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50) DEFAULT 'USA',
    track_type VARCHAR(50),
    timezone VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

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

-- Track classifications
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
-- MIGRATION TRACKING
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version VARCHAR(50) PRIMARY KEY,
    description TEXT,
    applied_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO public.schema_migrations (version, description)
VALUES ('001', 'Create schemas and reference tables')
ON CONFLICT (version) DO NOTHING;
