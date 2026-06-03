# Sequence Dashboard Layout Rebuild Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rebuild the dashboard layout so the left content column determines page height while the right analytics column stays equal-height and scrolls internally.

**Architecture:** Replace the current loose two-column layout with a left main stack and a right side rail shell inside the same grid row. Keep existing data rendering logic, but move the detail table into the left column and wrap the right-hand analytics cards in one scrollable panel.

**Tech Stack:** HTML, CSS Grid, vanilla JavaScript, unittest

---

### Task 1: Rebuild Column Structure

**Files:**
- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write the failing test**

Assert the page contains a left main column wrapper and a single scrollable side rail wrapper.

**Step 2: Run test to verify it fails**

Run: `python -m unittest final_system_demo.tests.test_api_contract`
Expected: FAIL because current markup has a detached bottom table and multiple right panels.

**Step 3: Write minimal implementation**

Create `left-main-column` and `side-rail-shell` layout wrappers, move the detail table into the left column, and make the right rail scroll internally.

**Step 4: Run test to verify it passes**

Run: `python -m unittest final_system_demo.tests.test_api_contract`
Expected: PASS

### Task 2: Runtime Verification

**Files:**
- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`

**Step 1: Rebuild the API container**

Run: `docker compose up -d --build --force-recreate api`

**Step 2: Manual verification**

Open: `http://localhost:18080/sequence-dashboard`
Expected: left column shows map and detail table as one stack, right column stays the same height and scrolls internally without leaving a mid-page gap.
