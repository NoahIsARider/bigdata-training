# Assignment4 PyFlink Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the current Python-only assignment4 implementation with a real PyFlink job that can be submitted by `flink run -py`, writes 1-minute portrait stats to HBase, and is backed by fresh runtime verification evidence.

**Architecture:** Keep pure aggregation helpers so behavior remains testable, then add a thin PyFlink batch-style pipeline that reads text input, maps signals to `(region_id, minute, imsi)` tuples, deduplicates by key, groups in Python operator state, and writes final minute aggregates to HBase. Run the job inside a dedicated Flink container so the submission path is undeniably Flink-based.

**Tech Stack:** Python 3.11, unittest, PyFlink 1.18, Docker Compose, HBase, HappyBase

---

### Task 1: Lock the target contract with tests

**Files:**
- Modify: `d:\Document Files\Schoolwork\大三下\数据科学与智能软件项目实训\作业一\docker-hadoop\assignment4\tests\test_region_portrait_job.py`
- Modify: `d:\Document Files\Schoolwork\大三下\数据科学与智能软件项目实训\作业一\docker-hadoop\assignment4\app\region_portrait_job.py`

**Step 1: Write the failing test**

- Add tests for:
  - transforming a parsed signal into a Flink-friendly dedupe key
  - reducing unique user portraits into minute stats
  - building the exact HBase payload and rowkey
- Keep existing tests for minute bucketing and aggregate correctness.

**Step 2: Run test to verify it fails**

Run: `python -m unittest assignment4/tests/test_region_portrait_job.py`
Expected: FAIL because the new helper functions do not exist yet.

**Step 3: Write minimal implementation**

- Extract reusable helpers:
  - `build_minute_user_record()`
  - `reduce_user_records_to_row()`
  - `build_hbase_row()`

**Step 4: Run test to verify it passes**

Run: `python -m unittest assignment4/tests/test_region_portrait_job.py`
Expected: PASS.

### Task 2: Add a real PyFlink entrypoint

**Files:**
- Create: `d:\Document Files\Schoolwork\大三下\数据科学与智能软件项目实训\作业一\docker-hadoop\assignment4\app\assignment4_flink_job.py`
- Modify: `d:\Document Files\Schoolwork\大三下\数据科学与智能软件项目实训\作业一\docker-hadoop\assignment4\app\region_portrait_job.py`

**Step 1: Write the failing runtime expectation**

- The job must be invokable with:
  - `flink run -py /workspace/app/assignment4_flink_job.py --signal-path ...`
- It must write into `TA_MI_REGION_USER_PORTRAIT`.

**Step 2: Implement the PyFlink job**

- Load profile and region side data in operator `open()`.
- Read `base_signal.txt` with a text source.
- Map each line to `(region_id, stat_minute, imsi, gender, age)`.
- Deduplicate users with a keyed operator keyed by `region_id|stat_minute|imsi`.
- Aggregate minute rows keyed by `region_id|stat_minute`.
- Emit final rows and write them to HBase.

**Step 3: Keep bounded execution**

- Use a file source / collection execution style so the job exits after processing test data.
- Avoid Kafka dependency for this assignment version.

### Task 3: Build a dedicated Flink runtime

**Files:**
- Create: `d:\Document Files\Schoolwork\大三下\数据科学与智能软件项目实训\作业一\docker-hadoop\assignment4\flink\Dockerfile`
- Create: `d:\Document Files\Schoolwork\大三下\数据科学与智能软件项目实训\作业一\docker-hadoop\assignment4\flink\docker-entrypoint.sh`
- Modify: `d:\Document Files\Schoolwork\大三下\数据科学与智能软件项目实训\作业一\docker-hadoop\assignment4\docker-compose.yml`

**Step 1: Reuse known-good Flink image design**

- Install Flink 1.18.1, `apache-flink`, `happybase`, `thriftpy2`.
- Keep the container simple: one jobmanager service is enough for local submission evidence.

**Step 2: Wire shared volumes**

- Mount:
  - `/workspace/app`
  - `/workspace/data`
  - `/workspace/artifacts`

**Step 3: Verify service starts**

Run: `docker compose up -d hbase flink-jobmanager`
Expected: both containers are up.

### Task 4: Replace documentation with indisputable evidence

**Files:**
- Modify: `d:\Document Files\Schoolwork\大三下\数据科学与智能软件项目实训\作业一\docker-hadoop\assignment4\assignment4-答案.md`

**Step 1: Remove ambiguous claims**

- Delete wording that describes the current version as the final unquestionable solution.
- State clearly this is a PyFlink implementation.

**Step 2: Record only fresh verified commands**

- Include:
  - test command
  - `docker compose up -d hbase flink-jobmanager`
  - `docker compose exec flink-jobmanager flink run -py /workspace/app/assignment4_flink_job.py ...`
  - HBase `scan`

**Step 3: Update screenshot guidance**

- Require a screenshot that visibly contains `flink run -py`.

### Task 5: End-to-end verification

**Files:**
- Verify runtime only; no new files required

**Step 1: Run tests**

Run: `python -m unittest assignment4/tests/test_region_portrait_job.py`
Expected: PASS.

**Step 2: Start infra**

Run: `docker compose up -d hbase flink-jobmanager`
Expected: both services up.

**Step 3: Submit job via Flink**

Run: `docker compose exec flink-jobmanager flink run -py /workspace/app/assignment4_flink_job.py --signal-path /workspace/data/base_signal.txt --profile-path /workspace/data/user_profile.csv --region-path /workspace/data/region_cell.csv --hbase-host hbase`
Expected: Flink job finishes successfully.

**Step 4: Verify HBase rows**

Run: `@("scan 'TA_MI_REGION_USER_PORTRAIT', {LIMIT => 20}") | docker compose exec -T hbase hbase shell -n`
Expected: `4 row(s)` and all portrait columns present.
