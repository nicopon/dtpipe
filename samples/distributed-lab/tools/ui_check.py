#!/usr/bin/env python3
"""Drives the lab's pages in headless Chrome and checks what they render, with no page error.
Writes one screenshot per step.

  0. Sign in: the coordinator's page sends the browser to the IdP's form, filled and submitted as
     the lab's admin, and comes back naming them with the admin role.
  1. Lab view: every catalog pipeline selected in turn, its graph and plan drawn.
  2. Coordinator: each view rendered (nodes, library, a pipeline's page with its job read only, runs, access).
  2b. Library: the row actions agree with the plans and deployments, and Open shows the pipeline's page.
  3. No designer: no Designer entry in the navigation, no Design button, and no New or Import on the library.
  4. Waiting for approval: a host the rights do not know proposes a name and a description made of HTML;
     the card shows them as text and runs none of it.

Usage: ui_check.py [lab-url] [out-dir]     (defaults: http://127.0.0.1:5180, .state/ui)
The lab must be up. LAB_TARGET=stack points step 4 at the compose stack, as for smoke.py. Exits 1 if any step fails.
"""
import json, os, sys, time, urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cdp import Browser
from enroll_host import CLIENT, UnknownHost, forget
from lab_session import ADMIN, Session

URL = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5180").rstrip("/")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(__file__), "..", ".state", "ui")
os.makedirs(OUT, exist_ok=True)
failed = False
# The tool's own calls sign in through the IdP; the browser signs in through the IdP's form.
SESSION = Session(URL)
STACK = os.environ.get("LAB_TARGET") == "stack"
LAB = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE = os.path.join(LAB, ".stack") if STACK else os.environ.get("LAB_STATE", os.path.join(LAB, ".state"))
DTPIPE = os.environ.get("DTPIPE", os.path.join(LAB, "..", "..", "dist", "release", "dtpipe"))


def report(ok, name, detail, shot):
    global failed
    failed |= not ok
    print(f"{'PASS' if ok else 'FAIL'}  {name:34} {detail}  {shot}")


def page_errors(browser):
    toasts = browser.js("[...document.querySelectorAll('.toast.error')].filter(t => !t.hidden).map(t => t.textContent)") or []
    return toasts + [c for c in browser.console if c.startswith("EXCEPTION")]


def lab_view(browser):
    catalog = SESSION.call("/api/pipelines")
    browser.goto(URL + "/lab/", 1)
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
    library = SESSION.call("/api/library")
    pid = library[0]["id"] if library else ""
    checks = [
        ("nodes", "document.querySelectorAll('.node-card').length > 0 && document.querySelectorAll('.brick').length > 0"),
        ("library", f"document.querySelectorAll('table.library tr').length === {len(library) + 1}"),
        (f"library/{pid}", "document.querySelectorAll('.xbox').length > 0 && (document.querySelectorAll('.lanes svg path').length > 0 || document.querySelectorAll('ul.msgs.errors li').length > 0)"
                           " && (document.getElementById('x-job')?.textContent || '').length > 0"),
        ("runs", "!!document.getElementById('r-journal') && !!document.getElementById('r-deployments')"),
        ("access", "document.querySelectorAll('table.matrix.edit input').length > 0 && document.querySelectorAll('[data-identity]').length > 0"),
    ]
    browser.goto(URL + "/#/nodes", 1)
    for view, predicate in checks:
        browser.console.clear()
        browser.js(f"location.hash = {json.dumps('#/' + view)}")
        rendered = browser.wait_for(predicate, 20)
        errors = page_errors(browser)
        shot = browser.shot(os.path.join(OUT, "app-library-detail.png" if view.startswith("library/") else "app-" + view.split("/")[0] + ".png"))
        report(rendered and not errors, "view " + view, "rendered" if rendered else "not rendered" + (f"  errors: {errors}" if errors else ""), shot)


def library_page(browser):
    # The list's row actions agree with the library's and the deployments' state, and Open leads to the pipeline's page.
    entries = SESSION.call("/api/library")
    deployments = {d["pipelineId"]: d for d in SESSION.call("/api/deployments")}
    queue = SESSION.call("/api/runs/queue")
    browser.console.clear()
    browser.js("location.hash = '#/library'")
    browser.wait_for(f"document.querySelectorAll('table.library [data-run]').length === {len(entries)}", 20)
    wrong = []
    for e in entries:
        deployment = deployments.get(e["id"])
        row = f"[data-deploy={json.dumps(e['id'])}]"
        deploy_off = browser.js(f"document.querySelector({json.dumps(row)}).disabled")
        run_off = browser.js(f"document.querySelector('[data-run={json.dumps(e['id'])}]').disabled")
        blocked = any(p["blocking"] for p in e["bindingProblems"]) or any(r["pipelineId"] == e["id"] for r in queue)
        if deploy_off != blocked:
            wrong.append(f"{e['id']} deploy disabled={deploy_off}")
        if run_off != (not deployment or deployment["state"] == "failed"):
            wrong.append(f"{e['id']} run disabled={run_off}")
    # An armed pipeline's row carries the health chip, with the status /api/health gives; a pipeline not armed carries none.
    probe = entries[0]["id"]
    armed_here = probe not in deployments
    if armed_here:
        SESSION.call(f"/api/library/{probe}/deploy", {})
    try:
        armed = {h["id"]: h["status"] for h in SESSION.call("/api/health")["pipelines"]}
        chips = ("Object.fromEntries([...document.querySelectorAll('table.library tr')].filter(r => r.querySelector('a.strong'))"
                 ".map(r => [r.querySelector('td .hint.mono').textContent, ([...r.querySelectorAll('.chip')].find(c => c.textContent.startsWith('health:'))?.textContent ?? '').slice(8) || null]))")
        browser.js("location.hash = '#/library'")
        shown = {}
        for _ in range(60):
            shown = browser.js(chips) or {}
            if probe in armed and shown.get(probe) == armed[probe]:
                break
            time.sleep(0.5)
        wrong += [f"{i} health chip {shown.get(i)!r}, health {armed.get(i)!r}" for i in shown if shown[i] != armed.get(i)]
        if probe not in armed:
            wrong.append(f"{probe} was armed and /api/health does not list it")
    finally:
        if armed_here:
            SESSION.call(f"/api/deployments/{probe}", None, "DELETE")
    pid = entries[0]["id"]
    browser.js(f"document.querySelector('table.library a[href=\"#/library/{pid}\"]').click()")
    opened = browser.wait_for("document.querySelectorAll('.xbox').length > 0 && !!document.getElementById('x-deploy') && !!document.getElementById('x-run')", 20)
    gone = browser.js("!document.querySelector('#nav a[data-view=\"distribution\"]')")
    errors = page_errors(browser)
    report(not wrong and opened and gone and not errors, "library row actions and page",
           f"rows={len(entries)} opened={opened} no distribution entry={gone}" + (f"  wrong: {wrong}" if wrong else "") + (f"  errors: {errors}" if errors else ""), "")


def no_designer(browser):
    # The coordinator writes no job: the person writes it with dtpipe studio and publishes it.
    entries = SESSION.call("/api/library")
    browser.console.clear()
    browser.js("location.hash = '#/library'")
    browser.wait_for(f"document.querySelectorAll('table.library [data-run]').length === {len(entries)}", 20)
    on_list = browser.js("({nav: !!document.querySelector('#nav a[data-view=\"designer\"]'),"
                         " design: !![...document.querySelectorAll('a, button')].find(e => /^design$/i.test(e.textContent.trim())),"
                         " creation: !!document.querySelector('[data-new], [data-import]')})")
    browser.js(f"location.hash = '#/library/{entries[0]['id']}'")
    browser.wait_for("!!document.getElementById('x-job')", 20)
    on_page = browser.js("({design: !![...document.querySelectorAll('a, button')].find(e => /^design$/i.test(e.textContent.trim())),"
                         " editable: !!document.querySelector('#x-job').closest('textarea')})")
    browser.js("location.hash = '#/designer'")
    time.sleep(0.5)
    fallback = browser.js("document.querySelector('#nav a.active')?.dataset.view")
    errors = page_errors(browser)
    gone = not any(on_list.values()) and not any(on_page.values()) and fallback == "library"
    report(gone and not errors, "no designer", f"list={on_list} page={on_page} #/designer -> {fallback}" + (f"  errors: {errors}" if errors else ""), "")


def waiting_card(browser):
    # What an unknown host proposes is its own text, and nothing it says is an authority: the page shows it as text.
    name, description = "<b>node-2</b>", '<img src=x onerror="window.__enrolled=1"> proposes'
    issuer = SESSION.call("/api/rights")["idp"]["issuer"]
    try:
        with UnknownHost(STATE, issuer, URL, DTPIPE, STACK, name, description):
            seen = False
            for _ in range(120):
                seen = any(p["subject"] == CLIENT for p in SESSION.call("/api/rights")["pending"])
                if seen:
                    break
                time.sleep(0.5)
            browser.console.clear()
            browser.js("location.hash = '#/nodes'")
            browser.js("location.hash = '#/access'")
            row = f"document.querySelector('[data-pending={json.dumps(CLIENT)}]')"
            shown = seen and browser.wait_for(f"!!{row}", 20)
            card = browser.js(f"({{heading: [...document.querySelectorAll('h2')].some(h => h.textContent.startsWith('Waiting for approval')),"
                              f" text: ({row}?.textContent || '').includes({json.dumps(name)}) && ({row}?.textContent || '').includes({json.dumps(description)}),"
                              f" markup: {row}?.querySelectorAll('b, img').length ?? -1, ran: window.__enrolled === 1}})") if shown else {}
            shot = browser.shot(os.path.join(OUT, "app-waiting.png"))
            errors = page_errors(browser)
            ok = bool(shown) and card.get("heading") and card.get("text") and card.get("markup") == 0 and not card.get("ran") and not errors
            report(ok, "waiting for approval", f"host seen={seen} card={card}" + (f"  errors: {errors}" if errors else ""), shot)
    finally:
        forget(SESSION.call)


def sign_in(browser):
    browser.goto(URL + "/", 2)
    on_form = browser.wait_for("!!document.querySelector('form input[name=password]')", 20)
    if on_form:
        browser.js(f"(() => {{ const f = document.querySelector('form'); f.user.value = {json.dumps(ADMIN)};"
                   f" f.password.value = {json.dumps(ADMIN + 'pwd')}; f.submit(); }})()")
    back = browser.wait_for(f"location.origin === {json.dumps(URL)} && (document.getElementById('me')?.textContent || '').includes('admin')", 30)
    who = browser.js("document.getElementById('me')?.textContent || location.href")
    shot = browser.shot(os.path.join(OUT, "sign-in.png"))
    report(on_form and back and not page_errors(browser), "sign in through the IdP", who, shot)


browser = Browser(height=1200)
try:
    sign_in(browser)
    lab_view(browser)
    coordinator_views(browser)
    library_page(browser)
    no_designer(browser)
    waiting_card(browser)
finally:
    browser.close()
sys.exit(1 if failed else 0)
