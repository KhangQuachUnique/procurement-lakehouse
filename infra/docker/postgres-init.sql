-- Initialize secondary databases on the single consolidated PostgreSQL 17 instance.
-- Executed automatically by postgres:17 on first boot of an empty data volume.
-- Safe to run manually on an existing instance as well.

SELECT 'CREATE USER dagster WITH PASSWORD ''dagster'''
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'dagster')\gexec

SELECT 'CREATE DATABASE dagster OWNER dagster'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dagster')\gexec
