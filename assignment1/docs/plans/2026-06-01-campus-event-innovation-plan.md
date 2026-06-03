# Campus Event Innovation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Upgrade the current Guangzhou University Town demo into a more competitive “campus event site selection and live crowd monitoring” system by adding region comparison, realtime anomaly alerts, and AI-generated analysis reports.

**Architecture:** Reuse the existing ClickHouse analytics layer for comparative analysis, extend the Flink realtime chain with lightweight anomaly detection for live monitoring, and orchestrate the current API + LLM layer to generate structured event operation reports. Keep the existing page layout mostly stable, adding only focused UI blocks so the new value comes from capability rather than a full redesign.

**Tech Stack:** FastAPI, ClickHouse, HBase, PyFlink, Kafka, HTML/JS, DeepSeek-compatible LLM API

---

### Task 1: Reframe the product around a concrete campus event scenario

**Files:**
- Modify: `final_system_demo/README.md`
- Modify: `final_system_demo/需求说明书.txt`
- Test: manual doc review only

**Step 1: Update business wording**

Unify the project description as:
- Guangzhou University Town campus event site selection
- live crowd monitoring during campus fairs / recruiting events / open days
- post-event review with AI explanation

**Step 2: Verify docs stay honest**

Ensure the docs clearly separate:
- already implemented capabilities
- planned innovation upgrades

**Step 3: Commit**

```bash
git add final_system_demo/README.md final_system_demo/需求说明书.txt
git commit -m "docs: align project with campus event scenario"
```

### Task 2: Add region comparison analysis

**Files:**
- Modify: `final_system_demo/api/app/main.py`
- Modify: `final_system_demo/api/app/services/clickhouse_service.py`
- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write the failing test**

Add a contract test for a new comparison endpoint such as:

```python
def test_region_compare_route_contract(self):
    ...
```

Expected fields:
- selected regions
- user count comparison
- gender comparison
- age comparison
- province comparison

**Step 2: Run test to verify it fails**

Run: `python -m unittest final_system_demo.tests.test_api_contract -v`

Expected: missing route / missing fields

**Step 3: Write minimal implementation**

Add:
- `GET /api/compare`
- ClickHouse query helpers for multi-region comparison
- a simple comparison card/table in the left panel

**Step 4: Run tests**

Run: `python -m unittest final_system_demo.tests.test_api_contract -v`

Expected: PASS

**Step 5: Commit**

```bash
git add final_system_demo/api/app/main.py final_system_demo/api/app/services/clickhouse_service.py final_system_demo/api/app/static/sequence_dashboard.html final_system_demo/tests/test_api_contract.py
git commit -m "feat: add region comparison analysis"
```

### Task 3: Add realtime anomaly alerts

**Files:**
- Modify: `final_system_demo/flink/jobs/stream_region_portrait.py`
- Modify: `final_system_demo/hbase/init_hbase.py`
- Modify: `final_system_demo/api/app/services/hbase_service.py`
- Modify: `final_system_demo/api/app/main.py`
- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`
- Test: `final_system_demo/tests/test_stream_parser.py`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write the failing tests**

Add tests for:
- abnormal burst detection helper
- alert API route structure

Example behaviors:
- burst in short window -> alert emitted
- normal flow -> no alert

**Step 2: Run tests to verify they fail**

Run:
- `python -m unittest final_system_demo.tests.test_stream_parser -v`
- `python -m unittest final_system_demo.tests.test_api_contract -v`

**Step 3: Write minimal implementation**

Implement:
- sliding-window count logic in Flink
- store alerts in a new HBase table such as `region_event_alert`
- expose `GET /api/realtime/alerts`
- render a small alert list widget in the dashboard

**Step 4: Re-run tests**

Expected: PASS

**Step 5: Commit**

```bash
git add final_system_demo/flink/jobs/stream_region_portrait.py final_system_demo/hbase/init_hbase.py final_system_demo/api/app/services/hbase_service.py final_system_demo/api/app/main.py final_system_demo/api/app/static/sequence_dashboard.html final_system_demo/tests/test_stream_parser.py final_system_demo/tests/test_api_contract.py
git commit -m "feat: add realtime anomaly alerts"
```

### Task 4: Add AI-generated event analysis reports

**Files:**
- Modify: `final_system_demo/api/app/services/qa_service.py`
- Modify: `final_system_demo/api/app/main.py`
- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write the failing test**

Add a contract test for a route such as:

```python
def test_ai_report_route_contract(self):
    ...
```

Expected output:
- selected region
- key metrics summary
- risk notes
- recommendation text

**Step 2: Run test to verify it fails**

Run: `python -m unittest final_system_demo.tests.test_api_contract -v`

**Step 3: Write minimal implementation**

Add:
- `GET /api/ai-report`
- report assembly from existing count / distribution / sequence / alert data
- optional LLM explanation with rule-based fallback
- “生成活动分析结论” button in the dashboard

**Step 4: Run tests**

Expected: PASS

**Step 5: Commit**

```bash
git add final_system_demo/api/app/services/qa_service.py final_system_demo/api/app/main.py final_system_demo/api/app/static/sequence_dashboard.html final_system_demo/tests/test_api_contract.py
git commit -m "feat: add ai event analysis report"
```

### Task 5: End-to-end demo verification

**Files:**
- Modify: `final_system_demo/README.md`
- Test: runtime only

**Step 1: Run system startup**

Run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_all.ps1
```

**Step 2: Verify all 3 innovations**

Check:
- comparison endpoint returns multiple-region analytics
- alert endpoint returns realtime alert records
- AI report endpoint returns structured summary and recommendation

**Step 3: Update README demo flow**

Document a recommended answer-defense storyline:
- select candidate zones
- compare them
- trigger live monitoring
- generate AI event report

**Step 4: Commit**

```bash
git add final_system_demo/README.md
git commit -m "docs: add campus event innovation demo flow"
```
