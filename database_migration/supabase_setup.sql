-- Supabase Database Setup for Deep Research Agent
--
-- This is the ONLY schema file this project needs. Run it once in your
-- Supabase project's SQL Editor.
--
-- It defines exactly the two tables that services/supabase_service.py reads
-- and writes (via store_comparison_session / get_comparison_sessions /
-- get_model_metrics / update_user_feedback). Nothing in the Python backend
-- currently persists individual (non-comparison) research runs to Supabase
-- — those are tracked in memory only (see utils/metrics.py); this schema
-- only covers the /research/compare feature.

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Comparison Sessions Table (one row per /research/compare run)
CREATE TABLE IF NOT EXISTS comparison_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id VARCHAR(255) UNIQUE NOT NULL,
    query TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    user_feedback JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Comparison Results Table (one row per model within a comparison session)
CREATE TABLE IF NOT EXISTS comparison_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id VARCHAR(255) REFERENCES comparison_sessions(session_id) ON DELETE CASCADE,
    model VARCHAR(50) NOT NULL,
    duration DECIMAL(10,3) NOT NULL,
    stage_timings JSONB NOT NULL,
    sources_found INTEGER DEFAULT 0,
    word_count INTEGER DEFAULT 0,
    success BOOLEAN NOT NULL,
    error TEXT,
    report_content TEXT NOT NULL,
    supervisor_tools_used TEXT[] DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_comparison_sessions_timestamp ON comparison_sessions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_comparison_sessions_session_id ON comparison_sessions(session_id);
CREATE INDEX IF NOT EXISTS idx_comparison_results_session_id ON comparison_results(session_id);
CREATE INDEX IF NOT EXISTS idx_comparison_results_model ON comparison_results(model);
CREATE INDEX IF NOT EXISTS idx_comparison_results_created_at ON comparison_results(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_comparison_results_success ON comparison_results(success);

-- Updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_comparison_sessions_updated_at ON comparison_sessions;
CREATE TRIGGER update_comparison_sessions_updated_at
    BEFORE UPDATE ON comparison_sessions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Row Level Security (RLS) - Optional but recommended
ALTER TABLE comparison_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE comparison_results ENABLE ROW LEVEL SECURITY;

-- Allow all operations for now (you can restrict this later)
DROP POLICY IF EXISTS "Allow all operations on comparison_sessions" ON comparison_sessions;
CREATE POLICY "Allow all operations on comparison_sessions" ON comparison_sessions
    FOR ALL USING (true);

DROP POLICY IF EXISTS "Allow all operations on comparison_results" ON comparison_results;
CREATE POLICY "Allow all operations on comparison_results" ON comparison_results
    FOR ALL USING (true);
