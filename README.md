# Podcast Analytics Pipeline -DataBricks & PySpark

A production-grade, Medallion-architecture data pipeline built with **PySpark**, **Delta Lake**, and **Python**. 

This pipeline ingests raw user, episode, and event data, enforces strict data quality checks with quarantine routing, enriches the data via a "One Big Table" (OBT) pattern, and generates analytical Gold-layer aggregates to drive executive insights.

## Architectural Design & Medallion Layers

* **Bronze Layer:** Exact, unedited mirrors of raw files. Automatically injects ingestion metadata (`_source_file`, `_ingest_ts`) and casts columns to string formats for raw preservation.
* **Silver Layer:** Conformed, cleaned, and deduplicated dataset. Applies ANSI-safe timestamp parsing (`try_to_timestamp`), drops null values on primary keys, and filters invalid records into a `silver_events_rejected` quarantine table. Valid data is broadcast-joined into a wide One Big Table (OBT) partitioned by `event_date`.
* **Gold Layer:** Pre-aggregated analytical tables optimized for business reporting:
  * `gold_episode_daily`: Daily play and completion counts per episode.
  * `gold_country_ltr`: Country-level average listen-through rates (LTR).
  * `gold_user_engagement`: Per-user listening interactions, session lengths, and distinct episode counts.

## Key Engineering Decisions & Trade-offs

* **Idempotency & Local Execution:** For this local test environment, the pipeline executes a **Full Refresh** using Delta Lake's `.mode("overwrite")` and `.option("overwriteSchema", "true")`. Additionally, a `clean_local_environment()` function runs at startup to physically clear the local `spark-warehouse` and `metastore_db`. This guarantees a 100% clean state and prevents directory locking errors on repeated manual runs.
  * *Production Note:* In a live streaming or high-volume batch environment, this destructive wipe would be disabled. The architecture would transition to an incremental load using Databricks Auto Loader (`cloudFiles`) for Bronze and Delta `MERGE INTO` (Upserts) for Silver/Gold.
* **Defensive Programming & Quarantine:** Upstream data can be unpredictable. The pipeline uses `try_to_timestamp` to prevent hard JVM crashes on malformed dates, seamlessly routing unparseable rows to a quarantine table for later inspection without halting the batch job.
* **One Big Table (OBT) vs. Normalized:** Reference dimensions are joined on-the-fly into a single wide `silver_events` table. This optimizes read performance for Gold aggregations.

## Analytical Query Assumptions

The pipeline answers the three required business queries using the Gold layer (and Silver layer for distinct counts) with the following assumptions:
1. **Top 10 completed episodes in the past 7 days:** Assumed "past 7 days" is relative to the `MAX(event_date)` in the static dataset rather than the current system clock.
2. **Average LTR by Country:** Listen-through rate (LTR) is calculated for each completed listening event as listening_duration / episode_duration. The country-level LTR is then calculated as the average of these event-level LTR values. Only completed events are considered, and countries with no completed events are excluded.
3. **Distinct users with 3+ episodes in a day:** Assumed "listened to" means the user generated at least a `play` or `complete` event. This was queried directly against the daily grain of the `silver_events` table using a CTE.

## Scheduling & Orchestration

In a local testing environment, this pipeline is executed sequentially via `main.py`. However, for production deployment, it is designed to be orchestrated by an enterprise scheduler. An Apache Airflow DAG with task scheduling, retry logic, and dependency management would be prefered.

## Setup & Execution

### Prerequisites
* Python 3.12+
* Java 17 (configured with `JAVA_HOME`)
* Hadoop File System (configured wiht `HADOOP_HOME`)
* Requirements installed: `pip install -r requirements.txt`

### Running the Pipeline
Execute the master orchestration script from the project root. This will process the data and print the analytical query results to the console:
```bash
python main.py
```

### Test Suit
The project includes a modular test suite using **pytest**. Tests are configured to use isolated, ephemeral temporary directories (tmp_path) and separate Derby metastores to ensure they leave no footprint on the local repository.
```bash
pytest tests/ -v
```

### Analytical Query Results
```bash
--- Top Episodes by Completions ---
+----------+-------------+-----------------+-----------+                        
|episode_id|episode_title|total_completions|total_plays|
+----------+-------------+-----------------+-----------+
|ep_19     |Episode 19   |7                |0          |
|ep_45     |Episode 45   |4                |1          |
|ep_23     |Episode 23   |4                |3          |
|ep_24     |Episode 24   |4                |3          |
|ep_48     |Episode 48   |3                |2          |
|ep_12     |Episode 12   |3                |2          |
|ep_41     |Episode 41   |3                |1          |
|ep_1      |Episode 1    |3                |0          |
|ep_50     |Episode 50   |3                |1          |
|ep_32     |Episode 32   |3                |1          |
+----------+-------------+-----------------+-----------+


--- Top Countries by Listen-Through Rate ---
+-------+------------------+------------------------+
|country|avg_ltr_percentage|total_completed_episodes|
+-------+------------------+------------------------+
|DE     |150.74            |245                     |
|FR     |143.91            |179                     |
|IN     |142.79            |161                     |
|AU     |140.03            |145                     |
|UK     |136.46            |108                     |
|CA     |131.57            |189                     |
|US     |126.57            |183                     |
+-------+------------------+------------------------+


--- Top Super Listeners ---
+-----------------+                                                             
|super_users_count|
+-----------------+
|13               |
+-----------------+
```