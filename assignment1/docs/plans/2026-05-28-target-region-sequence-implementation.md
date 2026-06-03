# Target Region Sequence Monitoring Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build configurable target-region sequence monitoring with Flink detection, HBase storage, API query support, and a single-page dashboard.

**Architecture:** Reuse the existing realtime path `Kafka -> Flink -> HBase` by extending the Flink job to load `target_region.txt`, track ordered region visits per IMSI, and persist completed matches into a new HBase table. Expose the matched sequence records through FastAPI and serve a lightweight dashboard page from the API service for final demo visualization.

**Tech Stack:** PyFlink, Kafka, HBase, FastAPI, HappyBase, vanilla HTML/CSS/JS, unittest

---

### Task 1: Add Config File And Sample Data

**Files:**
- Create: `final_system_demo/data/target/target_region.txt`
- Modify: `final_system_demo/generator/generate_demo_data.py`
- Modify: `final_system_demo/README.md`
- Test: `final_system_demo/tests/test_generator.py`

**Step 1: Write the failing test**

Add a test asserting:
- `target_region.txt` exists
- it contains more than 3 region IDs
- generator copies it to `/data/static/target/target_region.txt`

**Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest final_system_demo.tests.test_generator -v
```

Expected: FAIL because target config is missing from source data or generator output.

**Step 3: Write minimal implementation**

- Create `target_region.txt` with at least 4 ordered regions
- Update generator to copy target config into shared static volume
- Document the new config file in README

**Step 4: Run test to verify it passes**

Run:

```powershell
python -m unittest final_system_demo.tests.test_generator -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add final_system_demo/data/target/target_region.txt final_system_demo/generator/generate_demo_data.py final_system_demo/tests/test_generator.py final_system_demo/README.md
git commit -m "feat: add target region config support"
```

### Task 2: Add Sequence Matching Logic

**Files:**
- Modify: `final_system_demo/flink/jobs/stream_region_portrait.py`
- Test: `final_system_demo/tests/test_stream_parser.py`

**Step 1: Write the failing test**

Add unit tests for pure helper functions that:
- load ordered target regions from file
- consume a sequence of region events for one IMSI
- only mark success when events match the configured order
- capture first enter time for each configured region
- ignore duplicate events in the same already-matched stage

**Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest final_system_demo.tests.test_stream_parser -v
```

Expected: FAIL because sequence helper functions do not exist.

**Step 3: Write minimal implementation**

- Add helper functions/classes in `stream_region_portrait.py`
- Keep them pure so they can be tested without starting Flink
- Preserve the existing realtime portrait write path

**Step 4: Run test to verify it passes**

Run:

```powershell
python -m unittest final_system_demo.tests.test_stream_parser -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add final_system_demo/flink/jobs/stream_region_portrait.py final_system_demo/tests/test_stream_parser.py
git commit -m "feat: add ordered target region matching logic"
```

### Task 3: Persist Sequence Matches To HBase

**Files:**
- Modify: `final_system_demo/hbase/init_hbase.py`
- Modify: `final_system_demo/flink/jobs/stream_region_portrait.py`
- Modify: `final_system_demo/scripts/bootstrap.ps1`
- Modify: `final_system_demo/scripts/verify.ps1`
- Test: `final_system_demo/tests/test_stream_parser.py`

**Step 1: Write the failing test**

Add a test that builds the HBase payload for a completed match and asserts it includes:
- `imsi`
- `gender`
- `age`
- `province`
- `target_regions`
- `matched_region_count`
- one enter-time field per configured region

**Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest final_system_demo.tests.test_stream_parser -v
```

Expected: FAIL because sequence payload builder does not exist.

**Step 3: Write minimal implementation**

- Create a second HBase table, e.g. `region_sequence_match`
- Extend HBase init script to create both tables
- In Flink, when a sequence completes, write one row keyed by `target_path#imsi`
- Keep the existing realtime portrait table unchanged
- Extend verify/bootstrap scripts so the new table is initialized and easy to inspect

**Step 4: Run test to verify it passes**

Run:

```powershell
python -m unittest final_system_demo.tests.test_stream_parser -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add final_system_demo/hbase/init_hbase.py final_system_demo/flink/jobs/stream_region_portrait.py final_system_demo/scripts/bootstrap.ps1 final_system_demo/scripts/verify.ps1 final_system_demo/tests/test_stream_parser.py
git commit -m "feat: persist matched sequences in hbase"
```

### Task 4: Add API Support For Sequence Matches

**Files:**
- Modify: `final_system_demo/api/app/services/hbase_service.py`
- Modify: `final_system_demo/api/app/main.py`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write the failing test**

Add API contract tests for:
- sequence query response shape
- ordered enter-time fields in returned records
- count summary endpoint if added

Use service-level pure helpers or FastAPI handler tests without requiring live HBase.

**Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest final_system_demo.tests.test_api_contract -v
```

Expected: FAIL because sequence API is missing.

**Step 3: Write minimal implementation**

- Add a service function to read `region_sequence_match`
- Add an endpoint such as `GET /api/realtime/sequence-matches`
- Return `imsi`, `gender`, `age`, and per-region enter times

**Step 4: Run test to verify it passes**

Run:

```powershell
python -m unittest final_system_demo.tests.test_api_contract -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add final_system_demo/api/app/services/hbase_service.py final_system_demo/api/app/main.py final_system_demo/tests/test_api_contract.py
git commit -m "feat: add sequence match api"
```

### Task 5: Add Single-Page Dashboard

**Files:**
- Create: `final_system_demo/api/app/static/sequence_dashboard.html`
- Modify: `final_system_demo/api/app/main.py`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write the failing test**

Add a lightweight contract test that asserts:
- dashboard route exists
- page contains expected section labels such as target path, matched users, and region enter times

**Step 2: Run test to verify it fails**

Run:

```powershell
python -m unittest final_system_demo.tests.test_api_contract -v
```

Expected: FAIL because dashboard route and static page do not exist.

**Step 3: Write minimal implementation**

- Serve a dashboard HTML page from FastAPI
- Fetch sequence match API data with browser-side JS
- Show:
  - current target path
  - matched user count
  - matched user table
  - region enter-time columns

**Step 4: Run test to verify it passes**

Run:

```powershell
python -m unittest final_system_demo.tests.test_api_contract -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add final_system_demo/api/app/static/sequence_dashboard.html final_system_demo/api/app/main.py final_system_demo/tests/test_api_contract.py
git commit -m "feat: add sequence monitoring dashboard"
```

### Task 6: End-To-End Verification And Docs

**Files:**
- Modify: `final_system_demo/README.md`
- Modify: `final_system_demo/scripts/run_all.ps1`
- Modify: `final_system_demo/scripts/verify.ps1`
- Test: `final_system_demo/tests/test_generator.py`
- Test: `final_system_demo/tests/test_stream_parser.py`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Run focused test suite**

Run:

```powershell
python -m unittest final_system_demo.tests.test_generator final_system_demo.tests.test_stream_parser final_system_demo.tests.test_api_contract -v
```

Expected: PASS

**Step 2: Rebuild and rerun the realtime stack**

Run:

```powershell
docker compose build flink-jobmanager flink-taskmanager api
docker compose up -d flink-jobmanager flink-taskmanager api
powershell -ExecutionPolicy Bypass -File .\scripts\run_all.ps1
```

Expected: all services up, Flink job submitted, Spark batch succeeds

**Step 3: Verify new behavior**

Run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
Invoke-WebRequest -UseBasicParsing http://localhost:18080/sequence-dashboard | Select-Object -ExpandProperty StatusCode
Invoke-WebRequest -UseBasicParsing http://localhost:18080/api/realtime/sequence-matches | Select-Object -ExpandProperty Content
```

Expected:
- HBase contains sequence-match rows
- dashboard returns `200`
- API returns matched users and region enter times

**Step 4: Document usage**

- Add README section for `target_region.txt`
- Document the new API and dashboard route
- Add verification commands for sequence monitoring

**Step 5: Commit**

```bash
git add final_system_demo/README.md final_system_demo/scripts/run_all.ps1 final_system_demo/scripts/verify.ps1
git commit -m "docs: add sequence monitoring usage"
```
