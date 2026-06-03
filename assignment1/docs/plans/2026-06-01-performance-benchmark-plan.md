# Performance Benchmark Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a reusable benchmark script that measures API response time, realtime sequence-recognition latency, and offline Spark refresh duration for the final system demo.

**Architecture:** Keep the benchmark logic in a Python module so it can be unit-tested, then expose it as a CLI entrypoint using `python -m final_system_demo.scripts.performance_benchmark`. The script measures HTTP endpoints directly, injects a synthetic IMSI into Kafka for realtime latency, and runs one Spark batch refresh for offline timing.

**Tech Stack:** Python 3.11, unittest, urllib, subprocess, Docker Compose, Kafka, FastAPI

---

### Task 1: Add failing benchmark summary tests

**Files:**
- Create: `final_system_demo/tests/test_performance_benchmark.py`
- Test: `final_system_demo/tests/test_performance_benchmark.py`

**Step 1: Write the failing test**

Add tests for:
- duration summary pass/fail based on p95
- markdown report section rendering

**Step 2: Run test to verify it fails**

Run: `python -m unittest final_system_demo.tests.test_performance_benchmark -v`

Expected: import error because benchmark module does not exist yet.

### Task 2: Implement benchmark module

**Files:**
- Create: `final_system_demo/scripts/__init__.py`
- Create: `final_system_demo/scripts/performance_benchmark.py`
- Test: `final_system_demo/tests/test_performance_benchmark.py`

**Step 1: Write minimal implementation**

Implement:
- `summarize_duration_samples()`
- `build_markdown_report()`
- benchmark orchestration for API, realtime, and offline timing

**Step 2: Run test to verify it passes**

Run: `python -m unittest final_system_demo.tests.test_performance_benchmark -v`

Expected: PASS

### Task 3: Document benchmark usage

**Files:**
- Modify: `final_system_demo/README.md`
- Modify: `final_system_demo/需求说明书.txt`

**Step 1: Add usage docs**

Document:
- benchmark command
- output artifact paths
- prerequisites
- how to use results to backfill performance data

**Step 2: Verify docs**

Read the updated sections and ensure they match actual script behavior.

### Task 4: Runtime verification

**Files:**
- Test: benchmark runtime only

**Step 1: Probe current environment**

Run: `Invoke-WebRequest http://localhost:18080/health`

Expected:
- if reachable: run full benchmark
- if unreachable: report that runtime numbers cannot be honestly backfilled yet

**Step 2: Record outcome**

If benchmark runs, keep generated artifacts.
If benchmark cannot run, keep script and docs ready for later execution.
