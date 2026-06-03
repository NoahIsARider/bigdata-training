# Sequence Prototype Map Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rebuild the sequence dashboard into a prototype that matches the reference style: a left-side portrait analysis panel and a right-side real map view.

**Architecture:** Keep the existing FastAPI route and HBase-backed summary source, but enrich the summary payload with profile distributions and region polygon data. Replace the current single-file dashboard with a reference-inspired prototype that uses a map-first layout, tabbed analysis panels, donut charts, and Leaflet with a local fallback renderer.

**Tech Stack:** FastAPI, Python, CSV-derived geo data, vanilla HTML/CSS/JavaScript, Leaflet, unittest

---

### Task 1: Extend Dashboard Summary Data

**Files:**
- Modify: `final_system_demo/api/app/services/hbase_service.py`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write the failing test**

Assert that the dashboard summary returns age, province, APRU distributions, and region polygons.

**Step 2: Run test to verify it fails**

Run: `python -m unittest final_system_demo.tests.test_api_contract`
Expected: FAIL because the current summary only exposes gender and hotspot metrics.

**Step 3: Write minimal implementation**

Compute chart-friendly profile distributions from signal users and generate padded polygons from region hotspots.

**Step 4: Run test to verify it passes**

Run: `python -m unittest final_system_demo.tests.test_api_contract`
Expected: PASS

### Task 2: Rebuild the Prototype UI

**Files:**
- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`
- Test: `final_system_demo/tests/test_api_contract.py`

**Step 1: Write the failing test**

Assert that the page contains the new portrait sidebar, chart grid, and Leaflet/fallback map root.

**Step 2: Run test to verify it fails**

Run: `python -m unittest final_system_demo.tests.test_api_contract`
Expected: FAIL because the current page is still the previous dashboard layout.

**Step 3: Write minimal implementation**

Rebuild the page into a left portrait panel plus right map view, render donut charts and a reference-style toolbar, and initialize Leaflet with a local SVG fallback.

**Step 4: Run test to verify it passes**

Run: `python -m unittest final_system_demo.tests.test_api_contract`
Expected: PASS

### Task 3: Runtime Verification

**Files:**
- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`

**Step 1: Rebuild the API container**

Run: `docker compose up -d --build --force-recreate api`

**Step 2: Verify runtime payload**

Run: `Invoke-WebRequest -UseBasicParsing "http://localhost:18080/api/realtime/sequence-dashboard?limit=20" | Select-Object -ExpandProperty Content`
Expected: response includes chart distributions, region polygons, hotspot nodes, and hotspot flows.

**Step 3: Verify the prototype page**

Open: `http://localhost:18080/sequence-dashboard`
Expected: left portrait analysis panel, right real map view, region polygon overlay, and chart cards visually aligned with the reference direction.
