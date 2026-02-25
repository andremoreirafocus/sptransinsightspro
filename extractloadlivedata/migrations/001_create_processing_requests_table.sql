-- Migration: Create to_be_processed.raw table for storing processing requests
-- This table stores information about files that need to be processed

CREATE DATABASE sptrans_insights;

\c sptrans_insights
-- First, ensure the schema exists
CREATE SCHEMA to_be_processed;

-- Create the table
CREATE TABLE IF NOT EXISTS to_be_processed.raw (
    id BIGSERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    logical_date TIMESTAMPTZ NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Create index on processed column for efficient filtering of unprocessed requests
CREATE INDEX IF NOT EXISTS idx_raw_processed ON to_be_processed.raw(processed);

-- Create index on filename column for efficient file lookups
CREATE INDEX IF NOT EXISTS idx_raw_filename ON to_be_processed.raw(filename);

-- Create index on logical_date column for efficient date-based queries
CREATE INDEX IF NOT EXISTS idx_raw_logical_date ON to_be_processed.raw(logical_date);

-- Create index on created_at for ordering queries
CREATE INDEX IF NOT EXISTS idx_raw_created_at ON to_be_processed.raw(created_at);
