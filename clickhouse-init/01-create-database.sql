-- ---------------------------------------------------------------------------
-- 01-create-database.sql
--
-- Mounted by docker-compose.yml into /docker-entrypoint-initdb.d, which the
-- ClickHouse image executes ONCE, on first boot, while /var/lib/clickhouse is
-- still empty. On every later `docker compose up` the data volume already
-- exists and this file is skipped entirely.
--
-- Because it only ever runs on a virgin volume, `docker compose down -v`
-- (which deletes the volume) is what re-triggers it.
--
-- Scope: this file creates the DATABASE only. The `detections` and
-- `traffic_counts` TABLES are deliberately NOT created here - they are issued
-- by clickhouse_writer.ensure_tables() at Spark job startup, with DDL
-- generated directly from NSL_KDD_SCHEMA. Duplicating the table DDL here
-- would create a second source of truth that silently drifts the moment
-- schema.py changes. One owner per object.
--
-- config.py expects CLICKHOUSE_DATABASE=ids (override with the env var).
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS ids
    COMMENT 'Streaming IDS - rule-based detections and traffic volume counters';
