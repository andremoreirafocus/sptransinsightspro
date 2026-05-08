\connect postgres

SELECT 'CREATE DATABASE sptrans_insights'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'sptrans_insights'
)\gexec
