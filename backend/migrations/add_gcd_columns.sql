-- Migration: Add GCD columns to products and comic_issues tables
-- Run this manually against Railway Postgres

-- ============================================
-- PRODUCTS TABLE (for inventory tracking)
-- ============================================
ALTER TABLE products ADD COLUMN IF NOT EXISTS gcd_id INTEGER UNIQUE;
ALTER TABLE products ADD COLUMN IF NOT EXISTS gcd_series_id INTEGER;
ALTER TABLE products ADD COLUMN IF NOT EXISTS gcd_publisher_id INTEGER;

-- Create indexes for GCD lookups
CREATE INDEX IF NOT EXISTS ix_products_gcd_id ON products(gcd_id);
CREATE INDEX IF NOT EXISTS ix_products_gcd_series_id ON products(gcd_series_id);
CREATE INDEX IF NOT EXISTS ix_products_gcd_publisher_id ON products(gcd_publisher_id);

-- ============================================
-- COMIC_ISSUES TABLE (for GCD catalog data)
-- ============================================
ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS gcd_id INTEGER UNIQUE;
ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS gcd_series_id INTEGER;
ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS gcd_publisher_id INTEGER;

-- Create indexes for GCD lookups
CREATE INDEX IF NOT EXISTS ix_comic_issues_gcd_id ON comic_issues(gcd_id);
CREATE INDEX IF NOT EXISTS ix_comic_issues_gcd_series_id ON comic_issues(gcd_series_id);
CREATE INDEX IF NOT EXISTS ix_comic_issues_gcd_publisher_id ON comic_issues(gcd_publisher_id);

-- ============================================
-- FIELD_PROVENANCE TABLE (for tracking data sources)
-- ============================================
CREATE TABLE IF NOT EXISTS field_provenance (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    entity_id INTEGER NOT NULL,
    field_name VARCHAR(100) NOT NULL,
    data_source VARCHAR(50) NOT NULL,
    source_id VARCHAR(255),
    license_type VARCHAR(50),
    requires_attribution BOOLEAN DEFAULT false,
    attribution_text TEXT,
    fetched_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (entity_type, entity_id, field_name)
);

CREATE INDEX IF NOT EXISTS ix_field_provenance_entity ON field_provenance(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS ix_field_provenance_source ON field_provenance(data_source);

-- ============================================
-- PIPELINE_CHECKPOINT TABLE (for job state)
-- ============================================
CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) UNIQUE NOT NULL,
    job_type VARCHAR(50),
    is_running BOOLEAN DEFAULT false,
    last_run_started TIMESTAMP WITH TIME ZONE,
    last_run_completed TIMESTAMP WITH TIME ZONE,
    total_processed INTEGER DEFAULT 0,
    total_updated INTEGER DEFAULT 0,
    total_errors INTEGER DEFAULT 0,
    last_error TEXT,
    state_data JSONB,
    batch_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================
-- Verify columns
-- ============================================
SELECT 'products' as table_name, column_name, data_type
FROM information_schema.columns
WHERE table_name = 'products' AND column_name LIKE 'gcd%'
UNION ALL
SELECT 'comic_issues' as table_name, column_name, data_type
FROM information_schema.columns
WHERE table_name = 'comic_issues' AND column_name LIKE 'gcd%';
