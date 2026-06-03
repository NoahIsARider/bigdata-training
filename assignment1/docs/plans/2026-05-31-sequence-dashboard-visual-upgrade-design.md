# Sequence Dashboard Visual Upgrade Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Upgrade the sequence dashboard from a 4-node flow chart into a presentation-ready geo traffic situation board with subregion points, softer flow rendering, and a richer map background.

**Architecture:** Keep the existing realtime pipeline and API contract, but enrich the generated static geo dataset and the dashboard summary output with subregion hotspots and edge-level flow information. Render the dashboard as a layered SVG scene with district clusters, arc bundles, and pulsing hotspots instead of thick straight region-to-region lines.

**Tech Stack:** Python, FastAPI, HappyBase, CSV static assets, vanilla HTML/CSS/JavaScript, unittest

---

### Task 1: Expand Geo Seed Data

**Files:**
- Modify: `final_system_demo/generator/generate_demo_data.py`
- Test: `final_system_demo/tests/test_generator.py`

**Step 1: Write the failing test**

Add assertions that generated geo data contains more than one point per target region and writes a hotspot file for the dashboard.

**Step 2: Run test to verify it fails**

Run: `python -m unittest final_system_demo.tests.test_generator`
Expected: FAIL because hotspot output does not exist yet.

**Step 3: Write minimal implementation**

Generate 3-4 hotspot points per region, include cluster metadata, and persist them into shared static data.

**Step 4: Run test to verify it passes**

Run: `python -m unittest final_system_demo.tests.test_generator`
Expected: PASS

### Task 2: Enrich Dashboard Summary

**Files:**
- Modify: `final_system_demo/api/app/services/hbase_service.py`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write the failing test**

Add assertions that the dashboard summary returns hotspot points and flow segments between hotspot nodes.

**Step 2: Run test to verify it fails**

Run: `python -m unittest final_system_demo.tests.test_api_contract`
Expected: FAIL because summary only exposes region-level nodes.

**Step 3: Write minimal implementation**

Load generated hotspot metadata, aggregate signal events onto hotspot nodes, compute hotspot edge flows, and return them in the summary payload while preserving current fields.

**Step 4: Run test to verify it passes**

Run: `python -m unittest final_system_demo.tests.test_api_contract`
Expected: PASS

### Task 3: Redesign Map Rendering

**Files:**
- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write the failing test**

Add assertions that the dashboard contains district layer labels, hotspot legends, and arc-flow rendering hooks.

**Step 2: Run test to verify it fails**

Run: `python -m unittest final_system_demo.tests.test_api_contract`
Expected: FAIL because current page only renders region-level nodes and straight lines.

**Step 3: Write minimal implementation**

Render district hulls, hotspot nodes, arc bundles, heat glows, and a cleaner KPI/storytelling layout using the enriched summary payload.

**Step 4: Run test to verify it passes**

Run: `python -m unittest final_system_demo.tests.test_api_contract`
Expected: PASS

### Task 4: Runtime Verification

**Files:**
- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`
- Verify: running containers and API endpoints

**Step 1: Rebuild affected services**

Run: `docker compose run --rm generator`
Run: `docker compose up -d --build --force-recreate api`

**Step 2: Verify realtime data and API**

Run: `Invoke-WebRequest -UseBasicParsing "http://localhost:18080/api/realtime/sequence-dashboard?limit=20" | Select-Object -ExpandProperty Content`
Expected: hotspot data, richer flow segments, and existing KPI fields all present.

**Step 3: Verify dashboard manually**

Open: `http://localhost:18080/sequence-dashboard`
Expected: multi-point geo scene, curved flow bundles, denser heat structure, and readable layout for presentation.
