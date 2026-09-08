-- Silver layer for cleaned, deduplicated, type-cast, enriched with reference data
-- Grain: 1 row == 1 valid event (user_id, episode_id, event_type, event_ts)
-- Business key: (user_id, episode_id, event_type, event_ts)
-- Partitioned by event_date -> matches common time-range query patterns and keeps partitions small/pruneable
CREATE TABLE IF NOT EXISTS silver_events (
  user_id STRING,
  episode_id STRING,
  podcast_id STRING,
  event_type STRING, 
  event_ts TIMESTAMP,
  event_date DATE,
  duration INT,
  country STRING,
  signup_date DATE,
  episode_title STRING,
  release_date DATE,
  episode_duration_seconds INT,
  _ingest_ts TIMESTAMP,
  _source_file STRING
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Cleaned, validated, deduplicated events enriched with user/episode reference data';


-- Quarantine table for rows that failed DQ checks (nulls, bad event_type, bad ts)
CREATE TABLE IF NOT EXISTS silver_events_rejected (
  event_type STRING,
  user_id STRING,
  episode_id STRING,
  timestamp STRING,
  duration STRING,
  event_ts TIMESTAMP,
  _ingest_ts TIMESTAMP,
  _source_file STRING
)
USING DELTA
COMMENT 'Events that failed validation, kept for debugging/reprocessing';