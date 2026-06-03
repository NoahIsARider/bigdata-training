# Final System Demo Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Dockerized end-to-end regional crowd portrait demo system in `final_system_demo` with Flume, Kafka, HDFS, Flink, Spark, HBase, ClickHouse, and a query API.

**Architecture:** The system keeps one real-time path and one offline path. Real-time signal data flows through Flume to Kafka, then Flink consumes Kafka and writes region-user portraits into HBase. Offline signal data, user profiles, and region-cell mappings are stored in HDFS, Spark joins and aggregates them, and the batch results are loaded into ClickHouse for OLAP-style queries.

**Tech Stack:** Docker Compose, Hadoop HDFS, Apache Kafka, Apache Flume, Apache Flink (PyFlink), Apache Spark, Apache HBase, ClickHouse, FastAPI, Python 3.11.

---

### Task 1: Create the project scaffold

**Files:**
- Create: `final_system_demo/docker-compose.yml`
- Create: `final_system_demo/.env.example`
- Create: `final_system_demo/README.md`
- Create: `final_system_demo/.gitignore`

**Step 1: Define services and volumes**

Add services for:
- `hadoop`
- `kafka`
- `flume-hdfs`
- `flume-kafka`
- `generator`
- `spark`
- `flink-jobmanager`
- `flink-taskmanager`
- `flink-submit`
- `hbase`
- `clickhouse`
- `api`

**Step 2: Define shared volumes and network**

Add named volumes for HDFS state, Kafka state, Flume shared data, HBase data, and ClickHouse data.

**Step 3: Document the stack**

Write the top-level README with:
- architecture overview
- data flow
- startup order
- verification commands

### Task 2: Add static datasets and generator

**Files:**
- Create: `final_system_demo/data/signal/base_signal.txt`
- Create: `final_system_demo/data/profile/user_profile.csv`
- Create: `final_system_demo/data/region/region_cell.csv`
- Create: `final_system_demo/generator/Dockerfile`
- Create: `final_system_demo/generator/generate_demo_data.py`
- Test: `final_system_demo/tests/test_generator.py`

**Step 1: Write a failing test**

Check:
- signal sample count
- profile schema
- region mapping schema

**Step 2: Implement the generator**

Write files to:
- `/data/spool/hdfs`
- `/data/spool/kafka`
- `/data/static/profile`
- `/data/static/region`

**Step 3: Verify test passes**

Run: `python -m unittest final_system_demo.tests.test_generator`

### Task 3: Reuse and adapt HDFS + Kafka + Flume

**Files:**
- Create: `final_system_demo/flume/Dockerfile`
- Create: `final_system_demo/flume/start-flume.sh`
- Create: `final_system_demo/flume/conf/flume-hdfs.conf`
- Create: `final_system_demo/flume/conf/flume-kafka.conf`
- Create: `final_system_demo/hadoop/Dockerfile`
- Create: `final_system_demo/hadoop/core-site.xml`
- Create: `final_system_demo/hadoop/hdfs-site.xml`
- Create: `final_system_demo/hadoop/yarn-site.xml`
- Create: `final_system_demo/hadoop/docker-entrypoint.sh`
- Create: `final_system_demo/hadoop/ssh_config`

**Step 1: Copy the proven Hadoop and Flume setup**

Base these files on the already working implementation in the parent project, then rename paths to the new project layout.

**Step 2: Adjust HDFS paths**

Write signals under:
- `/demo/raw/signals`
- `/demo/raw/profile`
- `/demo/raw/region`

**Step 3: Add verification commands**

Verify:
- `hdfs dfs -ls -R /demo/raw`
- Kafka topic `telecom-realtime`

### Task 4: Add ClickHouse schema and loader targets

**Files:**
- Create: `final_system_demo/clickhouse/init/01_schema.sql`
- Create: `final_system_demo/clickhouse/init/02_views.sql`

**Step 1: Create raw and result tables**

Create tables for:
- `user_portrait_index`
- `overall_portrait_stats`
- `region_crowd_stats`

**Step 2: Add views for API queries**

Create helper views for:
- region user count
- age distribution
- gender distribution
- province topN

### Task 5: Implement Spark batch job

**Files:**
- Create: `final_system_demo/spark/requirements.txt`
- Create: `final_system_demo/spark/jobs/batch_region_portrait.py`
- Create: `final_system_demo/tests/test_batch_logic.py`

**Step 1: Write a failing batch test**

Test:
- signal/profile/region join
- IMSI dedup by region and time range
- output metrics fields

**Step 2: Implement Spark batch logic**

Read HDFS files:
- signals
- profiles
- region mapping

Produce:
- region crowd statistics
- portrait distributions
- user portrait index rows

**Step 3: Load to ClickHouse**

Insert the generated outputs into ClickHouse using HTTP or native client from the Spark job container.

### Task 6: Implement HBase container and schema bootstrap

**Files:**
- Create: `final_system_demo/hbase/Dockerfile`
- Create: `final_system_demo/hbase/hbase-site.xml`
- Create: `final_system_demo/hbase/start-hbase.sh`
- Create: `final_system_demo/hbase/init_hbase.py`

**Step 1: Build single-node HBase**

Run:
- HBase master
- regionserver
- thrift server

**Step 2: Create tables**

Create at least:
- `region_user_portrait`

Use row key pattern:
- `region_id#imsi`

### Task 7: Implement Flink streaming job

**Files:**
- Create: `final_system_demo/flink/Dockerfile`
- Create: `final_system_demo/flink/jobs/stream_region_portrait.py`
- Create: `final_system_demo/flink/lib/` (connector jars if needed)
- Create: `final_system_demo/tests/test_stream_parser.py`

**Step 1: Write a failing parser test**

Validate signal line parsing and region lookup behavior.

**Step 2: Implement the Flink job**

Consume Kafka `telecom-realtime`, enrich from static region/profile files, and write region-user latest records into HBase.

**Step 3: Add submit command**

Run Flink job with:
- jobmanager
- taskmanager
- submit container or command script

### Task 8: Implement query and QA API

**Files:**
- Create: `final_system_demo/api/Dockerfile`
- Create: `final_system_demo/api/requirements.txt`
- Create: `final_system_demo/api/app/main.py`
- Create: `final_system_demo/api/app/services/clickhouse_service.py`
- Create: `final_system_demo/api/app/services/hbase_service.py`
- Create: `final_system_demo/api/app/services/qa_service.py`
- Create: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write failing API tests**

Cover:
- `/api/stats/user-count`
- `/api/stats/distribution`
- `/api/details`
- `/api/ask`

**Step 2: Implement API**

Use ClickHouse for batch stats and HBase for latest detail lookup.

**Step 3: Implement rule-based QA**

Map natural language fields into:
- region
- time range
- metric
- profile filters

### Task 9: Add orchestration scripts and verification

**Files:**
- Create: `final_system_demo/scripts/bootstrap.ps1`
- Create: `final_system_demo/scripts/run_all.ps1`
- Create: `final_system_demo/scripts/verify.ps1`

**Step 1: Bootstrap full stack**

Start base services in stable order.

**Step 2: Seed and process**

Run:
- generator
- HBase init
- Flink submit
- Spark batch

**Step 3: Verify**

Check:
- HDFS raw files exist
- Kafka can consume signals
- HBase contains region-user records
- ClickHouse contains aggregated rows
- API returns expected payloads

### Task 10: Final docs

**Files:**
- Modify: `final_system_demo/README.md`
- Modify: `final_system_demo/requirements-doc.txt`

**Step 1: Document exact commands**

Include startup, verification, shutdown, and troubleshooting.

**Step 2: Add screenshots checklist**

List the terminal outputs and API pages needed for demonstration.
