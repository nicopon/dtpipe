#!/usr/bin/env python3
"""Drives `dtpipe studio` against the running lab in headless Chrome and checks what a person sees.
One screenshot per step. Not run by CI: it needs the lab up and the release binary built.

  1. Sign in to the coordinator, as the lab's designer, through the IdP's own form.
  2. Open from library: a pipeline lands as a new file of the folder, and is never written over.
  3. Sample a job that reads a source brick and writes a sink brick, with a transformer between: every
     stage shows its rows.
  4. Debug run, then Resume here from a level it kept.
  5. Import a command line, save it, run it after confirming what it writes, and stop a long run: the
     run ends with 130 and the studio does not quit while it goes on.
  6. Publish: a job the coordinator accepts is created, a job its check refuses is shown with its
     report and is not saved.
  7. No error reached the page's console.

Usage: studio_check.py [lab-url] [dtpipe-binary] [out-dir]
       (defaults: http://127.0.0.1:5180, dist/release/dtpipe, .state/studio)
Exits 1 if any step fails. On macOS the keychain may block a headless run: this check uses no keyring
value, so the studio never opens it.
"""
import json, os, re, shutil, subprocess, sys, tempfile, time, urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cdp import Browser
from lab_session import ApiError, Session

HERE = os.path.dirname(os.path.abspath(__file__))
LAB = os.path.dirname(HERE)
URL = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5180").rstrip("/")
DTPIPE = sys.argv[2] if len(sys.argv) > 2 else os.path.join(LAB, "..", "..", "dist", "release", "dtpipe")
OUT = sys.argv[3] if len(sys.argv) > 3 else os.path.join(LAB, ".state", "studio")
os.makedirs(OUT, exist_ok=True)
ADMIN = Session(URL)
failed = False


def report(ok, name, detail="", shot=""):
    global failed
    failed |= not ok
    print(f"{'PASS' if ok else 'FAIL'}  {name:34} {detail}  {shot}")


def click(browser, selector):
    """A real mouse click: a script's .click() is no gesture, and window.open refuses it."""
    point = browser.center(selector)
    if not point:
        raise RuntimeError(f"nothing matches {selector}")
    browser.mouse("mouseMoved", *point)
    browser.mouse("mousePressed", *point)
    browser.mouse("mouseReleased", *point)


def click_text(browser, selector, label):
    """A real click on the first element matching selector whose text is label."""
    point = browser.js(f"(() => {{ const e = [...document.querySelectorAll({json.dumps(selector)})].find(b => b.textContent.trim() === {json.dumps(label)});"
                       " if (!e) return null; e.scrollIntoView({block: 'nearest'}); const r = e.getBoundingClientRect(); return [r.left + r.width / 2, r.top + r.height / 2]; })()")
    if not point:
        raise RuntimeError(f"no {selector} reads {label!r}")
    browser.mouse("mouseMoved", *point)
    browser.mouse("mousePressed", *point)
    browser.mouse("mouseReleased", *point)


def text(browser, selector):
    return browser.js(f"document.querySelector({json.dumps(selector)})?.textContent || ''") or ""


def shot(browser, name):
    return browser.shot(os.path.join(OUT, name + ".png"))


def stub_dialogs(browser, answers=()):
    """prompt() answers from a queue, then its default; confirm() says yes. A headless page would wait forever."""
    browser.js(f"window.__answers = {json.dumps(list(answers))};"
               " window.prompt = (message, fallback) => window.__answers.length ? window.__answers.shift() : fallback;"
               " window.confirm = () => true;")


def studio_api(browser, token, method, path, body=None):
    return browser.js(
        f"fetch({json.dumps(path)}, {{method: {json.dumps(method)}, headers: {{'X-Dtpipe-Studio': {json.dumps(token)}, 'Content-Type': 'application/json'}},"
        f" body: {json.dumps(json.dumps(body)) if body is not None else 'undefined'}}}).then(async r => ({{status: r.status, body: await r.text()}}))")


def sign_in(browser, address, token):
    login = studio_api(browser, token, "POST", "/api/coordinator/login", {})
    url = json.loads(login["body"])["url"]
    browser.goto(url, 2)
    form = browser.wait_for("!!document.querySelector('form input[name=password]')", 20)
    if form:
        browser.js("(() => { const f = document.querySelector('form'); f.user.value = 'labdesigner';"
                   " f.password.value = 'labdesignerpwd'; f.submit(); })()")
    browser.wait_for("location.pathname === '/callback'", 20)
    browser.goto(address, 2)
    ok = browser.wait_for("(document.getElementById('coordinator')?.textContent || '').includes('@')", 20)
    report(form and ok, "sign in to the coordinator", text(browser, "#coordinator").strip(), shot(browser, "signed-in"))


def main():
    folder = tempfile.mkdtemp(prefix="studio-check-")
    published = []
    studio = subprocess.Popen([DTPIPE, "studio", folder, "--coordinator", URL], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=folder)
    browser = None
    try:
        address = studio.stdout.readline().strip()
        token = address.split("#", 1)[1]
        browser = Browser(height=1300)
        browser.goto(address, 2)
        sign_in(browser, address, token)
        stub_dialogs(browser)

        # 2. Open from library
        target = "customers-anonymized"
        browser.wait_for("document.querySelectorAll('#library button.job').length > 0", 20)
        click_text(browser, "#library button.job", target)
        opened = browser.wait_for(f"document.getElementById('job-name')?.textContent === {json.dumps(target + '.yaml')}", 20)
        written = os.path.exists(os.path.join(folder, target + ".yaml"))
        report(opened and written, "open from library", f"{target}.yaml written={written}", shot(browser, "opened"))
        again = studio_api(browser, token, "POST", "/api/library/open", {"id": target})
        report(again["status"] == 409, "a library file is not overwritten", f"HTTP {again['status']}")

        # 3. Sample, bricks and the steps between them
        browser.wait_for("document.querySelectorAll('.dcard').length > 0", 20)
        click(browser, "#d-sample")
        sampled = browser.wait_for("document.querySelectorAll('.stage').length >= 3", 60)
        stages = browser.js("document.querySelectorAll('.stage').length")
        report(sampled, "sample, source brick to sink brick", f"{stages} stages shown", shot(browser, "sample"))

        # 4. Debug run and resume
        stub_dialogs(browser, ["50"])
        click(browser, "#d-debug")
        debugged = browser.wait_for("document.querySelectorAll('.debug-level').length > 0", 90)
        levels = browser.js("document.querySelectorAll('.debug-level').length")
        report(debugged, "debug run", f"{levels} levels", shot(browser, "debug"))
        resumed = False
        if browser.js("!![...document.querySelectorAll('.debug-level button')].find(b => b.textContent === 'Resume here')"):
            # The status still reads the first debug run's verdict: clear it, or the wait ends at once.
            browser.js("document.getElementById('status').textContent = ''")
            click(browser, ".debug-level button")
            browser.wait_for("/debug run (done|failed)/.test(document.getElementById('status').textContent)", 120)
            resumed = text(browser, "#status").strip() == "debug run done"
        report(resumed, "resume from a level", text(browser, "#status").strip(), shot(browser, "resumed"))

        # 5. Import, save, run, stop
        stub_dialogs(browser, ["dtpipe --input generate:2000000000 --output null:", "long.yaml"])
        click(browser, "#import-command")
        imported = browser.wait_for("document.getElementById('job-name')?.textContent === 'long.yaml (new)'", 30)
        click(browser, "#d-save")
        saved = browser.wait_for("document.getElementById('job-name')?.textContent === 'long.yaml'", 30)
        report(imported and saved, "import a command line", text(browser, "#job-name"), shot(browser, "imported"))
        click(browser, "#d-run")
        confirm_shown = browser.wait_for("!!document.getElementById('d-run-confirm')", 30)
        shot(browser, "run-confirm")
        browser.js("document.getElementById('d-run-confirm').click()")  # no window.open here: a script click is enough
        running = browser.wait_for("!!document.getElementById('d-run-stop')", 30)
        if not running:
            print("run did not start; status:", text(browser, "#status"), "|", shot(browser, "run-not-started"), "| console:", browser.console[-4:])
        time.sleep(1.5)
        state_now = studio_api(browser, token, "GET", "/api/run")["body"]
        if '"running":true' not in state_now.replace(" ", ""):
            print("run state before quit:", state_now[:400])
        quit_answer = studio_api(browser, token, "POST", "/api/quit", {})
        # The tab redraws every second: ask again if the button was between two draws.
        for _ in range(10):
            try:
                browser.js("document.getElementById('d-run-stop').click()")
                break
            except RuntimeError:
                time.sleep(0.2)
        else:
            print("run state:", studio_api(browser, token, "GET", "/api/run")["body"][:300])
        stopped = browser.wait_for("(document.getElementById('d-drawer')?.textContent || '').includes('stopped')", 60)
        verdict = json.loads(studio_api(browser, token, "GET", "/api/run")["body"])
        report(confirm_shown and running and stopped and verdict["code"] == 130 and quit_answer["status"] == 409,
               "run, quit refused, stop", f"code {verdict['code']}, quit HTTP {quit_answer['status']}", shot(browser, "run-stopped"))

        # 6. Publish: accepted, then refused with its report
        bad = open(os.path.join(LAB, "library-seed", "revenue-by-country.yaml")).read().replace(
            "table: revenue_by_country\n      strategy: Recreate", "table: customers_mirror\n      strategy: Upsert\n      key: customer_id")
        with open(os.path.join(folder, "refused.yaml"), "w") as f:
            f.write(bad)
        browser.goto(address, 2)
        stub_dialogs(browser, ["studio-check-copy", "studio check"])
        browser.wait_for("document.querySelectorAll('#jobs button.job').length > 0", 20)
        click_text(browser, "#jobs button.job", target + ".yaml")
        browser.wait_for("document.querySelectorAll('.dcard').length > 0", 20)
        click(browser, "#d-publish")
        published.append("studio-check-copy")
        ok_publish = browser.wait_for("(document.getElementById('d-drawer')?.textContent || '').includes('is published')", 60)
        report(ok_publish, "publish a pipeline", text(browser, "#d-drawer")[:80], shot(browser, "published"))
        stub_dialogs(browser, ["studio-check-refused", "studio check"])
        click_text(browser, "#jobs button.job", "refused.yaml")
        browser.wait_for("document.querySelectorAll('.dcard').length > 0", 20)
        click(browser, "#d-publish")
        refused = browser.wait_for("(document.getElementById('d-drawer')?.textContent || '').includes('was not published')", 60)
        report_text = text(browser, "#d-drawer")
        try:
            ADMIN.call("/api/library/studio-check-refused")
            written = True
        except ApiError:
            written = False
        report(refused and "node-4/customers-mirror" in report_text and not written, "publish a refused pipeline",
               f"saved={written}", shot(browser, "refused"))

        # 7. Console
        errors = [c for c in browser.console if c.startswith("EXCEPTION")]
        report(not errors, "no error in the console", f"{errors[:1]}" if errors else "none")
    finally:
        if browser:
            browser.close()
        studio.terminate()
        try:
            studio.wait(timeout=10)
        except subprocess.TimeoutExpired:
            studio.kill()
        for pid in published:
            try:
                ADMIN.call(f"/api/library/{pid}", None, "DELETE")
            except ApiError:
                pass
        shutil.rmtree(folder, ignore_errors=True)


main()
sys.exit(1 if failed else 0)
