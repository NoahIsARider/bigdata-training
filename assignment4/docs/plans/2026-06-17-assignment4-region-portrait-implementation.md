# Assignment4 Region Portrait Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a self-contained assignment4 deliverable that computes 1-minute region portrait statistics from signal/profile/region data and writes results into HBase.

**Architecture:** Keep the aggregation logic in a pure Python module so it is easy to test, then run the same logic from a lightweight container against an independent HBase service. Package the result with a submission-ready markdown answer that explains the idea, commands, and screenshot locations.

**Tech Stack:** Python 3.11, unittest, Docker Compose, HBase, HappyBase

---

### Task 1: Lock core behavior with tests

**Files:**
- Create: `assignment4/tests/test_region_portrait_job.py`
- Create: `assignment4/app/region_portrait_job.py`

**Step 1: Write the failing test**

- Verify minute bucketing rounds timestamps down to the target minute.
- Verify one user appearing multiple times in one minute and one region is counted once.
- Verify output rows include male/female and age-band counters.
- Verify HBase payload columns are serialized as strings.

**Step 2: Run test to verify it fails**

Run: `python -m unittest assignment4/tests/test_region_portrait_job.py`
Expected: import failure because `app.region_portrait_job` does not exist yet.

**Step 3: Write minimal implementation**

- Add `parse_signal_line()`, `minute_bucket()`, `age_band()`, `aggregate_minute_portraits()`, `build_hbase_payload()`.
- Keep row key as `region_id#yyyyMMddHHmm`.

**Step 4: Run test to verify it passes**

Run: `python -m unittest assignment4/tests/test_region_portrait_job.py`
Expected: all tests pass.

### Task 2: Build an independent runnable environment

**Files:**
- Create: `assignment4/docker-compose.yml`
- Create: `assignment4/hbase/Dockerfile`
- Create: `assignment4/hbase/hbase-site.xml`
- Create: `assignment4/hbase/start-hbase.sh`
- Create: `assignment4/runner/Dockerfile`

**Step 1: Reuse known-good HBase startup**

- Reuse the single-node HBase layout already validated in previous assignments.
- Expose ports that do not conflict with assignment2.

**Step 2: Add a minimal Python runner**

- Install `happybase` and `thriftpy2`.
- Mount `app`, `data`, and `artifacts` into the runner container.

**Step 3: Add HBase write path**

- Extend `run_job()` so it can create the HBase table and overwrite demo rows.

### Task 3: Prepare deterministic demo data

**Files:**
- Create: `assignment4/data/user_profile.csv`
- Create: `assignment4/data/region_cell.csv`
- Create: `assignment4/data/base_signal.txt`

**Step 1: Keep data tiny but expressive**

- Two regions.
- Two minutes.
- Repeated signal lines for deduplication.
- Male/female and three age bands all covered.

**Step 2: Use the same rows in tests and runtime**

- Keep expected aggregates easy to explain in the answer document.

### Task 4: Produce submission artifacts

**Files:**
- Create: `assignment4/assignment4-答案.md`
- Create: `assignment4/artifacts/assignment4-minute-portrait.json`

**Step 1: Write a final answer in assignment format**

- Include requirement restatement, thought process, table design, code location, commands, actual results, and screenshot checklist.

**Step 2: Run the demo and capture evidence**

Run:
- `docker compose up -d hbase`
- `docker compose run --rm assignment4-runner python /workspace/app/region_portrait_job.py --hbase-host hbase`
- `@("scan 'TA_MI_REGION_USER_PORTRAIT', {LIMIT => 20}") | docker compose exec -T hbase hbase shell -n`

Expected:
- Console prints `assignment4 done, rows=4`
- HBase scan shows 4 minute-level rows with `male_count`, `female_count`, and age counters.
