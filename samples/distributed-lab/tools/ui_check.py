#!/usr/bin/env python3
"""Drives the lab's pages in headless Chrome and checks what they render, with no page error.
Writes one screenshot per step.

  1. Lab view: every catalog pipeline selected in turn, its graph and plan drawn.
  2. Coordinator: each view rendered (nodes, library, designer, distribution, runs, access).
  3. Designer scenario: a pipeline built with the mouse from the palette - a source brick, a
     Transform step, a sink brick, wired port to port; a wire picked up by its input end and
     dropped in the void, another removed with its x button, both wired again - then given a
     transformer, composed by the coordinator and validated by dtpipe. Nothing is saved.

Usage: ui_check.py [lab-url] [out-dir]     (defaults: http://127.0.0.1:5180, .state/ui)
The lab must be up. Exits 1 if any step fails.
"""
import json, os, sys, time, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cdp import Browser

URL = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5180").rstrip("/")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(__file__), "..", ".state", "ui")
os.makedirs(OUT, exist_ok=True)
failed = False


def report(ok, name, detail, shot):
    global failed
    failed |= not ok
    print(f"{'PASS' if ok else 'FAIL'}  {name:34} {detail}  {shot}")


def page_errors(browser):
    toasts = browser.js("[...document.querySelectorAll('.toast.error')].filter(t => !t.hidden).map(t => t.textContent)") or []
    return toasts + [c for c in browser.console if c.startswith("EXCEPTION")]


def lab_view(browser):
    catalog = json.load(urllib.request.urlopen(URL + "/api/pipelines"))
    browser.goto(URL + "/lab.html", 1)
    for entry in catalog:
        pid = json.dumps(entry["id"])
        browser.js(f"{{ const s = document.getElementById('pipeline-select'); s.value = {pid};"
                   " s.dispatchEvent(new Event('change')); }")
        # The page's own state: the plan drawn must be this pipeline's, fully rendered.
        rendered = browser.wait_for(
            f"state.plan && state.plan.pipelineId === {pid}"
            " && document.querySelectorAll('.unit').length === state.plan.units.length"
            " && document.querySelectorAll('#plan .frag-card, #plan .msgs').length > 0", 30)
        # The page's own errors only: the log also replays node faults, which a run may cause on purpose.
        errors = browser.js("[...document.querySelectorAll('#log .Error')].filter(e => e.querySelector('.n')?.textContent === 'lab')"
                            ".map(e => e.textContent)") or []
        shot = browser.shot(os.path.join(OUT, "lab-" + entry["id"] + ".png"))
        units = browser.js("document.querySelectorAll('.unit').length")
        report(rendered and not errors, "lab " + entry["id"], f"{units} units" + (f"  errors: {errors}" if errors else ""), shot)


def coordinator_views(browser):
    library = json.load(urllib.request.urlopen(URL + "/api/library"))
    pid = library[0]["id"] if library else ""
    checks = [
        ("nodes", "document.querySelectorAll('.node-card').length > 0 && document.querySelectorAll('.brick').length > 0"),
        ("library", f"document.querySelectorAll('table.library tr').length === {len(library) + 1}"),
        (f"designer/{pid}", "document.querySelectorAll('.dcard').length > 0 && document.querySelector('#d-yaml')?.textContent.length > 0"),
        (f"distribution/{pid}", "document.querySelectorAll('.xbox').length > 0 && document.querySelectorAll('.lanes svg path').length > 0"),
        ("runs", "!!document.getElementById('r-journal') && !!document.getElementById('r-deployments')"),
        ("access", "document.querySelectorAll('table.matrix.edit input').length > 0 && document.querySelectorAll('[data-identity]').length > 0"),
    ]
    browser.goto(URL + "/#/nodes", 1)
    for view, predicate in checks:
        browser.console.clear()
        browser.js(f"location.hash = {json.dumps('#/' + view)}")
        rendered = browser.wait_for(predicate, 20)
        errors = page_errors(browser)
        shot = browser.shot(os.path.join(OUT, "app-" + view.split("/")[0] + ".png"))
        report(rendered and not errors, "view " + view, "rendered" if rendered else "not rendered" + (f"  errors: {errors}" if errors else ""), shot)


def designer_scenario(browser):
    bricks = json.load(urllib.request.urlopen(URL + "/api/bricks"))
    source = next(b for b in bricks if b["key"] == "node-1/customer-countries")
    sink = next(b for b in bricks if b["key"] == "node-4/dim-customers")
    draft = {"id": "ui-scenario", "model": {"title": "UI scenario", "description": "", "check": None, "steps": [], "layout": {}}}
    browser.console.clear()
    browser.js(f"window.lab.store.draft = {json.dumps(draft)}; location.hash = '#/designer/ui-scenario'")
    if not browser.wait_for("!!document.querySelector('#d-palette .pal-item')", 20):
        report(False, "designer scenario", "palette not rendered", "")
        return

    # Three cards from the palette's + buttons, each placed in its kind's column.
    for selector in (f'.pal-item[data-brick="{source["key"]}"] [data-add]', '.pal-item[data-kind="transform"] [data-add]',
                     f'.pal-item[data-brick="{sink["key"]}"] [data-add]'):
        browser.js(f"document.querySelector({json.dumps(selector)}).click()")
    browser.wait_for("document.querySelectorAll('.dcard').length === 3", 10)

    # Wires, dragged with the mouse from an output port to an input port.
    card = lambda alias: f'.dcard[data-alias="{alias}"]'
    browser.drag(browser.center(card(source["id"]) + " .port.out"), browser.center(card("transform") + " .port.in"))
    browser.drag(browser.center(card("transform") + " .port.out"), browser.center(card(sink["id"]) + " .port.in"))

    wires = "document.querySelectorAll('.dedge-hit').length"
    wired = browser.wait_for(f"{wires} === 2", 5)

    # Picked up by its input end and dropped in the void: removed. Wired again.
    sink_in = browser.center(card(sink["id"]) + " .port.in")
    browser.drag(sink_in, (sink_in[0] + 40, sink_in[1] + 260))
    dropped = browser.wait_for(f"{wires} === 1", 5)
    browser.drag(browser.center(card("transform") + " .port.out"), browser.center(card(sink["id"]) + " .port.in"))

    # Selected by a click on its middle, removed with its x button. Wired again.
    mid = browser.js("""(() => { const p = [...document.querySelectorAll('.dedge-hit')].find(p => p.dataset.to === 'transform');
        const m = p.getPointAtLength(p.getTotalLength() / 2); const r = document.getElementById('d-inner').getBoundingClientRect();
        return [r.left + m.x, r.top + m.y]; })()""")
    browser.mouse("mouseMoved", *mid); browser.mouse("mousePressed", *mid); browser.mouse("mouseReleased", *mid)
    browser.wait_for("!!document.querySelector('.edge-delete')", 5)
    x, y = browser.center(".edge-delete")
    browser.mouse("mousePressed", x, y); browser.mouse("mouseReleased", x, y)
    crossed = browser.wait_for(f"{wires} === 1", 5)
    browser.drag(browser.center(card(source["id"]) + " .port.out"), browser.center(card("transform") + " .port.in"))
    rewired = browser.wait_for(f"{wires} === 2", 5)
    wiring = wired and dropped and crossed and rewired

    # A transformer on the Transform card: project, dropping the country.
    x, y = browser.center(card("transform") + " .dcard-sub")
    browser.mouse("mousePressed", x, y); browser.mouse("mouseReleased", x, y)
    browser.wait_for("!!document.querySelector('#d-inspector [data-tadd]')", 5)
    browser.js("document.querySelector('#d-inspector [data-tadd]').click()")
    browser.wait_for("!!document.querySelector('#d-inspector .kv[data-section=\"options\"] [data-kv-add]')", 5)
    browser.js("document.querySelector('#d-inspector .kv[data-section=\"options\"] [data-kv-add]').click()")
    browser.js("""{ const row = [...document.querySelectorAll('#d-inspector .kv[data-section="options"] .kv-row')].pop();
        const [k, v] = row.querySelectorAll('input');
        k.value = 'drop'; k.dispatchEvent(new Event('input', { bubbles: true }));
        v.value = 'country'; v.dispatchEvent(new Event('input', { bubbles: true })); }""")

    expected = [f"from: {source['id']}", "from: transform", "drop: country", "table: dim_customers"]
    composed = browser.wait_for(
        "(() => { const y = document.getElementById('d-yaml')?.textContent || '';"
        f" return {json.dumps(expected)}.every(s => y.includes(s)); }})()", 15)
    browser.js("document.getElementById('d-validate').click()")
    validated = browser.wait_for("!!document.querySelector('.drawer-status')?.textContent.includes('dtpipe accepts it')", 60)
    errors = page_errors(browser)
    shot = browser.shot(os.path.join(OUT, "designer-scenario.png"))
    detail = f"wiring={wiring} composed={composed} validated={validated}" + (f"  errors: {errors}" if errors else "")
    if not composed:
        detail += "  yaml: " + (browser.js("document.getElementById('d-yaml')?.textContent") or "")[:400]
    report(wiring and composed and validated and not errors, "designer scenario", detail, shot)


browser = Browser(height=1200)
try:
    lab_view(browser)
    coordinator_views(browser)
    designer_scenario(browser)
finally:
    browser.close()
sys.exit(1 if failed else 0)
