-- Create Metabase internal database if it does not exist.
SELECT format('CREATE DATABASE %I', :'metabase_db_name')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = :'metabase_db_name'
)\gexec

-- Create internal Metabase user if it does not exist.
SELECT format('CREATE USER %I WITH PASSWORD %L', :'metabase_internal_user', :'metabase_internal_password')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'metabase_internal_user'
)\gexec

-- Ensure internal user has rights on the Metabase database.
SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', :'metabase_db_name', :'metabase_internal_user')\gexec

-- Create read-only analytics user if it does not exist.
SELECT format('CREATE USER %I WITH PASSWORD %L', :'metabase_reader_user', :'metabase_reader_password')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'metabase_reader_user'
)\gexec

-- Grant read access to refined schema in analytical database.
\connect sptrans_insights
SELECT format('GRANT CONNECT ON DATABASE sptrans_insights TO %I', :'metabase_reader_user')\gexec
SELECT format('GRANT USAGE ON SCHEMA refined TO %I', :'metabase_reader_user')\gexec
SELECT format('GRANT SELECT ON ALL TABLES IN SCHEMA refined TO %I', :'metabase_reader_user')\gexec
SELECT format(
    'ALTER DEFAULT PRIVILEGES IN SCHEMA refined GRANT SELECT ON TABLES TO %I',
    :'metabase_reader_user'
)\gexec

-- Ensure internal user can manage objects in Metabase internal DB.
\connect :metabase_db_name
SELECT format('GRANT ALL ON SCHEMA public TO %I', :'metabase_internal_user')\gexec
