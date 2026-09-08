-- Bronze layer for users data
-- Grain: 1 raw == 1 raw user exactly as ingested from the source file
-- No business primary keys are defined at this layer, as the data is raw and unprocessed
CREATE OR REPLACE TABLE bronze.bronze_users (
    user_id STRING,
    signup_date STRING,
    country STRING,
    _source_file STRING,
    _ingest_ts TIMESTAMP
)
USING DELTA
COMMENT 'Raw ingestion of users.csv data';

-- Bronze layer for episodes data
-- Grain: 1 raw == 1 raw episode exactly as ingested from the source file
-- No business primary keys are defined at this layer, as the data is raw and unprocessed
CREATE OR REPLACE TABLE bronze.bronze_episodes (   
    episode_id STRING,
    podcast_id STRING,
    title STRING,
    release_date STRING,
    duration_seconds STRING,
    _source_file STRING,
    _ingest_ts TIMESTAMP
)
USING DELTA
COMMENT 'Raw ingestion of episodes.csv data';

-- Bronze layer for events data
-- Grain: 1 raw == 1 raw event exactly as ingested from the source file
-- No business primary keys are defined at this layer, as the data is raw and unprocessed
CREATE OR REPLACE TABLE bronze.bronze_events (
    event_type STRING,
    user_id STRING,
    episode_id STRING,
    timestamp STRING,
    duration STRING,
    _source_file STRING,
    _ingest_ts TIMESTAMP
)
USING DELTA
COMMENT 'Raw ingestion of event_logs.json data';