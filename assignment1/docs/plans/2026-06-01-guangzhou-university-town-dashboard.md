# Guangzhou University Town Dashboard Implementation Plan
#
#+> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.
#
#+**Goal:** Rework the sequence dashboard into a usable Guangzhou University Town prototype with real module switching, tighter layout, and matching demo data semantics.
#
#+**Architecture:** Lock the requested behavior with focused HTML and generator contract tests first, then refactor the single-page dashboard script into state-driven module rendering. Keep the right map pane fixed and remove the unused replay controls while moving scrolling responsibility to the left analysis pane only.
#
#+**Tech Stack:** Python `unittest`, FastAPI HTML response, vanilla HTML/CSS/JavaScript, demo data generator
#
#+---
#
#+### Task 1: Lock the new dashboard contract
#
#+**Files:**
#+- Modify: `final_system_demo/tests/test_api_contract.py`
#+#+#+#+- Modify: `final_system_demo/tests/test_generator.py`
#
#+**Step 1: Write the failing tests**
#+- Assert the dashboard HTML contains `广州大学城模拟数据`.
#+#+#+#+- Assert the HTML contains `流量热力` / `位置画像` / `驻留特征` / `迁徙刻画`.
#+#+#+#+- Assert the HTML no longer contains `评分&导数` or `回放进度`.
#+#+#+#+- Assert generator hotspots or provinces align with Guangzhou University Town semantics.
#
#+#+#+#+**Step 2: Run tests to verify they fail**
#+#+#+#+- Run: `python -m unittest final_system_demo.tests.test_api_contract final_system_demo.tests.test_generator`
#
#+#+**Step 3: Implement minimal production changes**
#+#+#+#+- Update dashboard template strings and generator constants until the tests turn green.
#
#+#+**Step 4: Re-run tests**
#+#+#+#+- Run the same unittest command and confirm all new assertions pass.
#
#+#+### Task 2: Refactor dashboard interaction and layout
#
#+#+**Files:**
#+#+#+#+- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`
#
#+#+**Step 1: Add failing test coverage first**
#+#+#+#+- Keep Task 1 assertions as the behavior guardrail for this refactor.
#
#+#+**Step 2: Implement minimal code**
#+#+#+#+- Replace static nav blocks with clickable buttons backed by JS state.
#+#+#+#+- Remove the replay/player strip completely.
#+#+#+#+- Make the left sidebar scroll independently and keep the right pane visually stable.
#+#+#+#+- Render different left-panel summaries for the four requested modules.
#
#+#+**Step 3: Verify**
#+#+#+#+- Run the dashboard contract tests again.
#
#+#+### Task 3: Move demo data to Guangzhou University Town
#
#+#+**Files:**
#+#+#+#+- Modify: `final_system_demo/generator/generate_demo_data.py`
#
#+#+**Step 1: Add failing tests first**
#+#+#+#+- Keep generator assertions for Guangzhou University Town provinces, hotspot names, and regional coordinates.
#
#+#+**Step 2: Implement minimal code**
#+#+#+#+- Replace old coordinates with Guangzhou University Town area centers.
#+#+#+#+- Replace hotspot naming with campus-oriented hotspots.
#+#+#+#+- Replace old province defaults with Guangdong-centered portrait data.
#
#+#+**Step 3: Verify**
#+#+#+#+- Run `python -m unittest final_system_demo.tests.test_generator -v`
#
#+#+### Task 4: Final verification
#
#+#+**Files:**
#+#+#+#+- Modify: `final_system_demo/tests/test_api_contract.py`
#+#+#+#+- Modify: `final_system_demo/tests/test_generator.py`
#+#+#+#+- Modify: `final_system_demo/api/app/static/sequence_dashboard.html`
#+#+**Step 1: Run targeted tests**
#+#+#+#+- Run: `python -m unittest final_system_demo.tests.test_api_contract final_system_demo.tests.test_generator -v`
#
#+#+**Step 2: Run diagnostics**
#+#+#+#+- Check the edited files for diagnostics in the IDE.
#
#+#+**Step 3: Smoke check**
#+#+#+#+- If needed, rebuild the API container and verify the dashboard loads with the new layout and labels.
