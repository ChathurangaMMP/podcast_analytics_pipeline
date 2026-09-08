-- Gold layer for per-episode daily performance
-- Grain: 1 row == 1 episode + 1 calendar day
CREATE TABLE IF NOT EXISTS gold_episode_daily (
  episode_id STRING,
  episode_title STRING,
  event_date DATE,
  play_count BIGINT,
  complete_count BIGINT,
  avg_duration_seconds DOUBLE
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Daily play/complete counts per episode, for "top episodes" analytics';

-- Gold layer for per-user engagement summary
-- Grain: 1 row == 1 user
CREATE TABLE IF NOT EXISTS gold_user_engagement (
  user_id STRING,
  country STRING,
  listen_events BIGINT,
  avg_session_duration_seconds DOUBLE,
  distinct_episodes BIGINT
)
USING DELTA
COMMENT 'Per-user listening summary for engagement analytics';

-- Gold layer for listen-through rate by country
-- Grain: 1 row == 1 country
CREATE TABLE IF NOT EXISTS gold_country_ltr (
  country STRING,
  avg_listen_through_rate DOUBLE,
  completions BIGINT
)
USING DELTA
COMMENT 'Average completion_duration / episode_duration_seconds by country';