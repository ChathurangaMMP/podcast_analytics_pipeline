-- Bronze layer for users data
CREATE TABLE IF NOT EXISTS bronze_users (
    user_id STRING,
    signup_date STRING,
    country STRING,
    _source_file STRING,
    _ingest_ts TIMESTAMP
)
USING DELTA
COMMENT 'Raw ingestion of users.csv data';

-- Bronze layer for episodes data
CREATE TABLE IF NOT EXISTS bronze_episodes (   
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
CREATE TABLE IF NOT EXISTS bronze_events (
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