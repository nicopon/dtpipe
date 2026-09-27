#!/usr/bin/env python3
"""Opens the lab page in headless Chrome, selects every catalog pipeline in turn, and checks that
its graph and plan render with no page error. Writes one screenshot per pipeline.

Usage: ui_check.py [lab-url] [out-dir]     (defaults: http://127.0.0.1:5180, .state/ui)
The lab must be up. Exits 1 on the first pipeline whose page does not render.
"""
import json, os, sys, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cdp import Browser

URL = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5180").rstrip("/")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(__file__), "..", ".state", "ui")
os.makedirs(OUT, exist_ok=True)

catalog = json.load(urllib.request.urlopen(URL + "/api/pipelines"))
browser = Browser(height=1200)
failed = False
try:
    browser.goto(URL + "/", 1)
    for entry in catalog:
        pid = json.dumps(entry["id"])
        browser.js(f"{{ const s = document.getElementById('pipeline-select'); s.value = {pid};"
                   " s.dispatchEvent(new Event('change')); }")
        # The page's own state: the plan drawn must be this pipeline's, fully rendered.
        rendered = browser.wait_for(
            f"state.plan && state.plan.pipelineId === {pid}"
            " && document.querySelectorAll('.unit').length === state.plan.units.length"
            " && document.querySelectorAll('#plan .frag-card, #plan .msgs').length > 0", 30)
        errors = browser.js("[...document.querySelectorAll('#log .Error')].map(e => e.textContent)") or []
        shot = browser.shot(os.path.join(OUT, entry["id"] + ".png"))
        units = browser.js("document.querySelectorAll('.unit').length")
        status = "PASS" if rendered and not errors else "FAIL"
        failed |= status == "FAIL"
        print(f"{status}  {entry['id']:28} {units} units  {shot}" + (f"  errors: {errors}" if errors else ""))
finally:
    browser.close()
sys.exit(1 if failed else 0)
