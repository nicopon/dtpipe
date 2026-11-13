#!/usr/bin/env node
// Plays the studio's page in headless Chrome over the DevTools protocol and checks what a person sees:
// opening a job, Preview, a click on each card, pins, an edit while a preview or a run holds the engine,
// Save then Write targets…, review before writing, Write and Abandon from a sink, a run left suspended by an earlier
// session, Forget pins, typing in the inspector; the agent's panel against a scripted endpoint that speaks Ollama's API (a
// save refused then accepted, a run, a question, a plan put on the cards, a job holding a credential, Stop, a reload mid-turn).
//
// Local only: it needs Node 22+ (native WebSocket, no dependency), Chrome, and a built dtpipe. CI never
// runs it. The page is embedded in the DLL, so build before playing it.
//
//   dotnet build src/DtPipe/DtPipe.csproj && node tests/studio/page_smoke.mjs [scenario...]
//
// DTPIPE_SMOKE_CHROME  Chrome's (or Chromium's, or Edge's) executable; default: the first one found for the platform
//                      (macOS, Windows and Linux are searched)
// DTPIPE_SMOKE_DLL     the built DtPipe.dll (default: src/DtPipe/bin/Debug/net10.0/DtPipe.dll)
// DTPIPE_SMOKE_BIN     a dtpipe executable, used instead of the DLL
// DTPIPE_SMOKE_SHOTS   a folder: the agent's scenario saves screenshots of the page there
// Exit code 1 on any FAIL and on any script error the page raises.

import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, "../..");

// The first Chromium-family browser the platform has: an executable path, or a name found on the PATH.
function findChrome() {
  if (process.env.DTPIPE_SMOKE_CHROME) return process.env.DTPIPE_SMOKE_CHROME;
  const env = process.env;
  const windows = ["PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA"].filter((v) => env[v]).flatMap((v) => [
    path.join(env[v], "Google", "Chrome", "Application", "chrome.exe"),
    path.join(env[v], "Microsoft", "Edge", "Application", "msedge.exe"),
  ]);
  const mac = ["Google Chrome", "Chromium", "Microsoft Edge"].map((n) => `/Applications/${n}.app/Contents/MacOS/${n}`);
  const names = ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "chrome", "microsoft-edge"];
  const onPath = (env.PATH || "").split(path.delimiter).filter(Boolean).flatMap((dir) => names.map((n) => path.join(dir, n)));
  const found = [...(process.platform === "win32" ? windows : process.platform === "darwin" ? mac : []), ...onPath].find((c) => fs.existsSync(c));
  if (!found) { console.error("no Chrome, Chromium or Edge found: set DTPIPE_SMOKE_CHROME to its executable"); process.exit(2); }
  return found;
}

const CHROME = findChrome();
// The studio is started in the job's folder, so a path given relatively is resolved here, once; a bare command name stays one for the PATH.
const DLL = path.resolve(process.env.DTPIPE_SMOKE_DLL || path.join(repo, "src/DtPipe/bin/Debug/net10.0/DtPipe.dll"));
const BIN = process.env.DTPIPE_SMOKE_BIN ? (/[\\/]/.test(process.env.DTPIPE_SMOKE_BIN) ? path.resolve(process.env.DTPIPE_SMOKE_BIN) : process.env.DTPIPE_SMOKE_BIN) : null;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const freePort = () => new Promise((resolve, reject) => {
  const server = net.createServer();
  server.listen(0, "127.0.0.1", () => { const { port } = server.address(); server.close(() => resolve(port)); });
  server.on("error", reject);
});

let pass = 0;
let fail = 0;
const check = (name, ok, extra = "") => {
  if (ok) pass++; else fail++;
  console.log(`${ok ? "PASS" : "FAIL"} ${name}${ok ? "" : ` :: ${extra}`}`);
};

// ---------------------------------------------------------------- the studio and the browser

// A process and what it started: Windows leaves the children of a killed parent running.
function kill(child) {
  // Waited for: the folder of the run is removed right after, and a process still holding it keeps it on Windows.
  if (process.platform === "win32" && child.pid) spawnSync("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
  else child.kill("SIGKILL");
}

async function startStudio(folder, extra = []) {
  const [command, args] = BIN ? [BIN, ["studio", folder, ...extra]] : ["dotnet", [DLL, "studio", folder, ...extra]];
  // Started in the job's folder: a job that writes a relative path writes there, never into the directory the test was run from.
  const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"], cwd: folder });
  let out = "";
  let err = "";
  child.stderr.on("data", (d) => { err += d; });
  const url = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`the studio printed no address: ${err}`)), 30000);
    child.stdout.on("data", (d) => {
      out += d;
      const found = /(http:\/\/127\.0\.0\.1:\d+\/\S*)/.exec(out);
      if (found) { clearTimeout(timer); resolve(found[1]); }
    });
    child.on("exit", (code) => reject(new Error(`the studio exited with ${code}: ${err}`)));
  });
  return { url, stop: () => kill(child) };
}

async function startChrome() {
  const port = await freePort();
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), "dtpipe-smoke-chrome-"));
  const child = spawn(CHROME, ["--headless=new", `--remote-debugging-port=${port}`, `--user-data-dir=${profile}`,
    "--window-size=1600,1000", "--no-first-run", "--no-default-browser-check",
    // A container runs as root, where Chrome's sandbox refuses to start.
    ...(process.getuid?.() === 0 ? ["--no-sandbox"] : []), "about:blank"], { stdio: "ignore" });
  let target = null;
  for (let i = 0; i < 100 && !target; i++) {
    try { target = (await (await fetch(`http://127.0.0.1:${port}/json`)).json()).find((t) => t.type === "page"); } catch { /* not up yet */ }
    if (!target) await sleep(200);
  }
  if (!target) { kill(child); throw new Error("Chrome did not open a page"); }
  const ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => { ws.onopen = resolve; ws.onerror = reject; });
  return { ws, stop: () => { try { ws.close(); } catch { /* closed */ } kill(child); try { fs.rmSync(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 }); } catch { /* Chrome still flushing: the system cleans the temp folder */ } } };
}

// A DevTools session: evaluation, real mouse clicks, typing, the requests the page made, and one response
// held back on demand.
function session(ws) {
  let id = 0;
  const pending = new Map();
  const s = { logs: [], seen: [], hold: { armed: false, requestId: null } };
  const send = (method, params = {}) => new Promise((resolve) => { const i = ++id; pending.set(i, resolve); ws.send(JSON.stringify({ id: i, method, params })); });
  ws.onmessage = (m) => {
    const d = JSON.parse(m.data);
    if (d.id && pending.has(d.id)) { pending.get(d.id)(d); pending.delete(d.id); return; }
    const p = d.params;
    if (d.method === "Page.javascriptDialogOpening") send("Page.handleJavaScriptDialog", { accept: true });
    else if (d.method === "Runtime.exceptionThrown") s.logs.push(`exception ${p.exceptionDetails.exception?.description || p.exceptionDetails.text}`);
    else if (d.method === "Runtime.consoleAPICalled" && p.type === "error") s.logs.push(`console.error ${p.args.map((a) => a.value ?? a.description).join(" ")}`);
    else if (d.method === "Network.responseReceived") s.seen.push(new URL(p.response.url).pathname);
    else if (d.method === "Fetch.requestPaused") {
      if (s.hold.armed && !s.hold.requestId) s.hold.requestId = p.requestId;
      else send("Fetch.continueResponse", { requestId: p.requestId });
    }
  };

  s.send = send;
  s.ev = async (expression) => {
    const r = await send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
    if (r.result.exceptionDetails) throw new Error(JSON.stringify(r.result.exceptionDetails.exception?.description || r.result.exceptionDetails));
    return r.result.result.value;
  };
  // A button found by its text and clicked as a person would: the panels name their choices in words.
  s.clickText = async (selector, text) => {
    const found = await s.until(`[...document.querySelectorAll(${JSON.stringify(selector)})].some((e) => e.textContent.trim() === ${JSON.stringify(text)})`, 20000);
    if (!found) throw new Error(`no ${selector} reading ${text}`);
    const at = await s.ev(`(() => { const e = [...document.querySelectorAll(${JSON.stringify(selector)})].find((e) => e.textContent.trim() === ${JSON.stringify(text)});
      e.scrollIntoView({ block: "center", inline: "center" }); const b = e.getBoundingClientRect(); return [b.x + b.width / 2, b.y + b.height / 2]; })()`);
    for (const type of ["mouseMoved", "mousePressed", "mouseReleased"]) {
      await send("Input.dispatchMouseEvent", { type, x: at[0], y: at[1], button: "left", clickCount: 1, buttons: type === "mousePressed" ? 1 : 0 });
    }
    await s.frame();
  };
  s.shot = async (name) => {
    if (!process.env.DTPIPE_SMOKE_SHOTS) return;
    fs.mkdirSync(process.env.DTPIPE_SMOKE_SHOTS, { recursive: true });
    const { result } = await send("Page.captureScreenshot", { format: "png" });
    fs.writeFileSync(path.join(process.env.DTPIPE_SMOKE_SHOTS, `${name}.png`), Buffer.from(result.data, "base64"));
  };
  // Polls a page expression until it is truthy; the last value tells a failure what it saw.
  s.until = async (expression, ms = 20000) => {
    const start = Date.now();
    while (Date.now() - start < ms) {
      try { if (await s.ev(expression)) return true; } catch { /* the page is between two renders */ }
      await sleep(100);
    }
    return false;
  };
  s.frame = () => s.ev("new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)))");
  s.click = async (selector) => {
    const at = await s.ev(`(() => { const e = document.querySelector(${JSON.stringify(selector)}); if (!e) return null;
      e.scrollIntoView({ block: "center", inline: "center" }); const b = e.getBoundingClientRect(); return [b.x + b.width / 2, b.y + b.height / 2]; })()`);
    if (!at) throw new Error(`no element ${selector}`);
    for (const type of ["mouseMoved", "mousePressed", "mouseReleased"]) {
      await send("Input.dispatchMouseEvent", { type, x: at[0], y: at[1], button: "left", clickCount: 1, buttons: type === "mousePressed" ? 1 : 0 });
    }
    await s.frame();
  };
  // Types at the end of a field, a few characters at a time, slowly enough for the page's own timers to fire between them.
  s.type = async (selector, text, pause = 90) => {
    await s.ev(`(() => { const e = document.querySelector(${JSON.stringify(selector)}); e.focus(); e.setSelectionRange(e.value.length, e.value.length); })()`);
    for (const ch of text) { await send("Input.insertText", { text: ch }); await sleep(pause); }
  };
  s.waitSeen = async (pathname, since, ms = 15000) => {
    const start = Date.now();
    while (Date.now() - start < ms) { if (s.seen.slice(since).includes(pathname)) return true; await sleep(50); }
    return false;
  };
  // Holds back the first response to a request for `endpoint`: the server has answered, the page has not heard yet.
  s.armHold = async (endpoint = "/api/debug") => {
    s.hold = { armed: true, requestId: null };
    await send("Fetch.enable", { patterns: [{ urlPattern: `*${endpoint}`, requestStage: "Response" }] });
  };
  s.held = async (ms = 15000) => {
    const start = Date.now();
    while (Date.now() - start < ms) { if (s.hold.requestId) return true; await sleep(50); }
    return false;
  };
  s.release = async () => {
    const { requestId } = s.hold;
    s.hold = { armed: false, requestId: null };
    await send("Fetch.continueResponse", { requestId });
    await send("Fetch.disable");
  };

  // What a person reads.
  s.status = () => s.ev(`document.querySelector("#status")?.textContent ?? ""`);
  s.badges = () => s.ev(`Object.fromEntries([...document.querySelectorAll(".dcard")].map((c) => [c.dataset.alias, c.querySelector(".dcard-badge")?.textContent ?? null]))`);
  s.state = (alias) => s.ev(`document.querySelector('.dcard[data-alias="${alias}"] .dcard-badge')?.dataset.cardState ?? null`);
  s.pinned = (alias) => s.ev(`document.querySelector('.dcard[data-alias="${alias}"] [data-card-tool="pin"]')?.getAttribute("aria-pressed") ?? null`);
  s.drawerText = () => s.ev(`document.querySelector("#d-drawer .drawer-body")?.innerText ?? ""`);
  s.tab = () => s.ev(`document.querySelector("#d-drawer .tabs button.active")?.dataset.tab ?? null`);
  s.visible = (selector) => s.ev(`(() => { const e = document.querySelector(${JSON.stringify(selector)}); return !!e && !e.hidden; })()`);
  // A reload takes a moment to replace the document: a job asked for before it would be asked of the old page.
  s.reload = async () => { await send("Page.reload"); await sleep(800); };
  s.cardTool = (alias, name) => s.click(`.dcard[data-alias="${alias}"] [data-card-tool="${name}"]`);
  s.disabled = (selector) => s.ev(`document.querySelector(${JSON.stringify(selector)})?.disabled ?? null`);
  s.pick = (alias) => s.click(`.dcard[data-alias="${alias}"] .dcard-head`);
  s.waitStatus = (pattern, ms) => s.until(`${pattern}.test(document.querySelector("#status")?.textContent ?? "")`, ms);
  // Found and clicked in one evaluation: the list of jobs is drawn again at any moment, and a button found is not one still there.
  s.openJob = async (name) => {
    await s.until(`(() => { const b = [...document.querySelectorAll("#jobs button.job")].find((b) => b.textContent === ${JSON.stringify(name)}); if (!b) return false; b.click(); return true; })()`);
    return s.until(`document.querySelectorAll(".dcard").length >= 2 && !!document.querySelector("#d-drawer .tabs")`);
  };
  return s;
}

// ---------------------------------------------------------------- a model that follows a script

// Speaks the part of Ollama's API the agent uses: /api/tags, and /api/chat streamed as JSON lines. What it answers
// depends on the conversation it is sent: after a tool's answer it closes the turn quoting it, after a person's message
// it does what a keyword asks. "slow" streams words that never repeat (the agent ends a turn that loops) until the call
// is abandoned. `seen` keeps every system message.
const AGENT_JOB = 'main:\n  input: "generate:5"\n  output: "csv:agent-out.csv"\n';
const PLAN_JOB = 'main:\n  input: "generate:7"\n  output: "csv:plan-out.csv"\n';

function startFakeLlm() {
  const llm = { seen: [], open: 0 };
  const call = (name, args) => ({ role: "assistant", content: "", tool_calls: [{ function: { name, arguments: args } }] });
  const server = http.createServer((req, res) => {
    if (req.url === "/api/tags") { res.end(JSON.stringify({ models: [{ name: "fake:1" }] })); return; }
    let body = "";
    req.on("data", (d) => { body += d; });
    req.on("end", () => {
      const { messages } = JSON.parse(body);
      llm.seen.push(messages[0]?.content ?? "");
      const last = messages[messages.length - 1];
      const line = (o) => res.write(JSON.stringify(o) + "\n");
      res.writeHead(200, { "Content-Type": "application/x-ndjson" });
      if (last.role === "tool") { line({ message: { role: "assistant", content: `done: ${String(last.content).slice(0, 60)}` }, done: true }); res.end(); return; }
      const text = String(last.content);
      const job = /<open-job>\n/.test(messages[0]?.content ?? "") ? "job seen" : "no job seen";
      const reply = /save/.test(text) ? call("save-job", { path: "agent.yaml", yamlContent: AGENT_JOB })
        : /run/.test(text) ? call("run-job", { path: "agent.yaml" })
        : /ask/.test(text) ? call("ask-user", { question: "Which target?", options: ["a.csv", "b.csv"] })
        : /plan/.test(text) ? call("validate-yaml-job", { yamlContent: PLAN_JOB })
        : /secret/.test(text) ? { role: "assistant", content: job }
        : null;
      if (reply) { line({ message: reply, done: false }); line({ done: true }); res.end(); return; }
      if (/slow/.test(text)) {
        llm.open++;
        const timer = setInterval(() => line({ message: { role: "assistant", content: `${Math.random().toString(36).slice(2)} ` }, done: false }), 150);
        res.on("close", () => { clearInterval(timer); llm.open--; });
        return;
      }
      line({ message: { role: "assistant", content: `ok: ${text}` }, done: true });
      res.end();
    });
  });
  return new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve({ llm, url: `http://127.0.0.1:${server.address().port}`, stop: () => server.close() })));
}

const fakeLlm = await startFakeLlm();

// ---------------------------------------------------------------- scenarios

const KEY_OF_B = '#d-inspector .transformer[data-index="0"] .kv[data-section="mappings"] input[data-kv="key"]';
const stateFile = (folder, job) => path.join(folder, ".dtpipe/studio", `${job}-debug.json`);

const scenarios = {
  // The agent's panel: every write waits for a card, a refusal writes nothing, the open job reaches the model unless it
  // holds a credential the person keeps here, Stop ends a turn, a reload finds the conversation again.
  async agent({ page: p, folder }) {
    const send = async (text) => {
      await p.ev(`(() => { const t = document.querySelector("#agent-prompt"); t.value = ${JSON.stringify(text)}; })()`);
      await p.click("#agent-send");
    };
    const idle = () => p.until(`document.querySelector("#agent-stop").hidden && !document.querySelector("#agent-now .agent-card button")`, 30000);
    const verdicts = () => p.ev(`document.querySelectorAll("#agent-log .verdict").length`);
    const lastText = () => p.ev(`[...document.querySelectorAll("#agent-log .msg.agent")].pop()?.textContent ?? ""`);

    check("the panel shows", await p.until(`!document.querySelector("#agent-pane").hidden && !!document.querySelector("#agent-prompt")`));
    check("the model is the one the server lists", await p.until(`document.querySelector("#agent-model")?.value === "fake:1"`));
    check("a local model raises no warning", (await p.ev(`document.querySelector("#agent-warning").hidden`)) === true);
    check("the header shows the provider and the address, read only", await p.ev(`document.querySelector("#agent-endpoint")?.textContent === "ollama · ${fakeLlm.url}" && !document.querySelector("#agent-endpoint input")`));
    await p.shot("1-panel");

    await send("please save it");
    check("a save waits on a card", await p.until(`/asks to save agent.yaml/.test(document.querySelector("#agent-now")?.textContent ?? "")`));
    check("the card shows the text the studio composed", /generate:5/.test(await p.ev(`document.querySelector("#agent-now pre")?.textContent ?? ""`)));
    check("nothing is written before the answer", !fs.existsSync(path.join(folder, "agent.yaml")));
    await p.shot("2-save-card");
    await p.clickText("#agent-now button", "Refuse");
    check("a refused save ends the turn", await idle());
    check("a refused save writes nothing", !fs.existsSync(path.join(folder, "agent.yaml")));
    check("the model reads the refusal", /refused/.test(await lastText()), await lastText());

    await send("please save it");
    await p.clickText("#agent-now button", "Save");
    check("an accepted save ends the turn", await idle());
    check("an accepted save writes the job", fs.existsSync(path.join(folder, "agent.yaml")));
    check("the folder lists the saved job", await p.until(`[...document.querySelectorAll("#jobs button.job")].some((b) => b.textContent === "agent.yaml")`));

    await send("now run it");
    check("a run waits on a card that names its target", await p.until(`/agent-out\.csv/.test(document.querySelector("#agent-now .agent-card")?.textContent ?? "")`));
    check("nothing is run before the answer", !fs.existsSync(path.join(folder, "agent-out.csv")));
    await p.shot("3-run-card");
    await p.clickText("#agent-now button", "Run");
    check("an accepted run ends the turn", await idle());
    check("the run writes its target", fs.existsSync(path.join(folder, "agent-out.csv")));
    check("the model reads the exit code", /"code":0/.test(await lastText()), await lastText());

    await send("ask me");
    check("a question shows its choices", await p.until(`[...document.querySelectorAll("#agent-now button")].some((b) => b.textContent === "b.csv")`));
    await p.shot("4-question");
    await p.clickText("#agent-now button", "b.csv");
    check("a choice is the next message", await p.until(`[...document.querySelectorAll("#agent-log .msg.user")].pop()?.textContent === "b.csv"`));
    check("the answer comes back", await p.until(`/ok: b\.csv/.test([...document.querySelectorAll("#agent-log .msg.agent")].pop()?.textContent ?? "")`));

    check("a job opens", await p.openJob("chain.yaml"));
    await send("make a plan");
    check("the plan shows once the turn ends", await p.until(`!!document.querySelector("#agent-now .agent-plan")`, 30000));
    await p.ev(`document.querySelector("#agent-now .agent-plan").open = true`);
    await p.shot("5-plan");
    await p.clickText("#agent-now .agent-plan button", "Apply to the open job");
    check("the plan is on the cards", await p.waitStatus(/plan is on the cards/, 15000), await p.status());
    check("the cards are the plan's", await p.until(`document.querySelectorAll(".dcard").length === 2`), JSON.stringify(await p.badges()));
    check("the job is still the one open", (await p.ev(`document.querySelector("#job-name").textContent`)).startsWith("chain.yaml"));

    fs.writeFileSync(path.join(folder, "secret.yaml"), 'main:\n  input: "pg:Host=h;Username=u;Password=abc"\n  output: "csv:s.csv"\n');
    await p.reload();
    check("the conversation survives a reload", await p.until(`document.querySelectorAll("#agent-log .msg.user").length >= 5`), String(await p.ev(`document.querySelectorAll("#agent-log .msg.user").length`)));
    check("the secret job opens", await p.openJob("secret.yaml") || await p.until(`/secret\.yaml/.test(document.querySelector("#job-name").textContent)`));
    await send("look at this secret");
    check("a credential in the open job waits on a card", await p.until(`/main\.input/.test(document.querySelector("#agent-now .agent-card")?.textContent ?? "")`));
    check("the card never shows the value", !/abc/.test(await p.ev(`document.querySelector("#agent-now").textContent`)));
    await p.shot("6-secret-card");
    const calls = fakeLlm.llm.seen.length;
    check("the model is not called before the answer", fakeLlm.llm.seen.length === calls);
    await p.clickText("#agent-now button", "Send without the job");
    check("the turn goes on without the job", await p.until(`/^no job seen$/.test([...document.querySelectorAll("#agent-log .msg.agent")].pop()?.textContent ?? "")`), await lastText());
    check("the credential never reached the model", fakeLlm.llm.seen.every((m) => !/Password=abc/.test(m)));
    await send("one more secret");
    check("sending without the job holds for that turn alone", await p.until(`/main\.input/.test(document.querySelector("#agent-now .agent-card")?.textContent ?? "")`));
    check("the last message stays in sight above the card", await p.ev(`(() => { const log = document.querySelector("#agent-log"); const m = [...log.querySelectorAll(".msg.user")].pop();
      return m.textContent === "one more secret" && m.getBoundingClientRect().bottom <= log.getBoundingClientRect().bottom + 1; })()`));
    await p.clickText("#agent-now button", "Cancel the turn");
    check("a cancelled turn calls no model", await idle() && fakeLlm.llm.seen.length === calls + 1);
    await p.click("#agent-attach");

    const before = await verdicts();
    await send("slow down");
    check("a turn shows what the model streams", await p.until(`/answering/.test(document.querySelector("#agent-now")?.textContent ?? "")`));
    await p.shot("7-streaming");
    await p.reload();
    check("a reload mid-turn finds the turn running", await p.until(`!document.querySelector("#agent-stop").hidden`));
    await p.click("#agent-stop");
    check("Stop ends the turn", await idle());
    check("Stop leaves a verdict", await p.until(`document.querySelectorAll("#agent-log .verdict").length === ${before + 1}`));
    check("Stop abandons the model call", await p.until(`${fakeLlm.llm.open} === 0`) && fakeLlm.llm.open === 0);
    await p.shot("8-stopped");
  },

  // Opening, Preview, a click on each card, pin, ▶ that pins nothing, an edit, ▶ on the pin, Save then Run, unpin.
  async journey({ page: p, folder }) {
    const file = stateFile(folder, "chain.yaml");
    check("the job opens as cards", await p.openJob("chain.yaml"));
    let b = await p.badges();
    const aliases = Object.keys(b);
    check("every card has a badge", aliases.every((a) => b[a]), JSON.stringify(b));
    check("no card has a preview yet", aliases.every((a) => /no preview/.test(b[a])), JSON.stringify(b));
    check("a job nobody pins leaves no state file", !fs.existsSync(file));
    check("the bar has no debug switch, step or continue", await p.ev(`!document.querySelector("#d-mode, #d-step, #d-continue, #d-watch-reset")`));
    check("the bar has Preview", await p.visible("#d-preview"));

    await p.click("#d-preview");
    check("Preview finishes", await p.waitStatus(/preview (done|failed)/, 30000));
    check("Preview succeeds", /preview done/.test(await p.status()), await p.status());
    b = await p.badges();
    check("every card but the sink shows n rows · m col", aliases.filter((a) => !/sink|^c$/.test(a)).every((a) => /rows · \d+ col/.test(b[a])), JSON.stringify(b));
    check("the drawer is on Preview", (await p.tab()) === "preview");

    for (const a of aliases) {
      await p.pick(a);
      const text = await p.drawerText();
      const sink = a === aliases[aliases.length - 1];
      check(`a click on ${a} shows its ${sink ? "absence of rows" : "rows"}`, sink ? /holds no rows/.test(text) : /rows delivered/.test(text), text.slice(0, 120));
    }

    const mid = aliases.includes("b") ? "b" : aliases[1];
    await p.click(`.dcard[data-alias="${mid}"] [data-card-tool="pin"]`);
    check("the pin is on", await p.until(`document.querySelector('.dcard[data-alias="${mid}"] [data-card-tool="pin"]')?.getAttribute("aria-pressed") === "true"`));
    check("the badge shows the pin", /📌/.test((await p.badges())[mid]));
    check("Forget pins shows", await p.visible("#d-forget-pins"));
    check("the state file holds the pin", fs.existsSync(file) && JSON.parse(fs.readFileSync(file)).watch.length === 1);

    const other = aliases.find((a) => a !== mid);
    await p.click(`.dcard[data-alias="${other}"] [data-card-tool="play"]`);
    check("▶ finishes", await p.waitStatus(/ran up to/, 30000));
    check("▶ on another card pins nothing", JSON.parse(fs.readFileSync(file)).watch.length === 1, fs.readFileSync(file, "utf8"));

    await p.click('#d-drawer button[data-tab="edit"]');
    await p.until(`!!document.querySelector("#d-edit")`);
    await p.ev(`(() => { const t = document.querySelector("#d-edit"); t.value = t.value.replace("% 2 == 0", "% 3 == 0"); })()`);
    await p.click("#d-drawer button[data-apply]");
    check("an edit through YAML marks the cards it touches", await p.until(`[...document.querySelectorAll(".dcard-badge")].some((e) => /edited since preview/.test(e.textContent))`), JSON.stringify(await p.badges()));

    await p.click("#d-preview");
    check("a second Preview finishes", await p.waitStatus(/preview done/, 30000));
    b = await p.badges();
    check("the cards are fresh again", Object.values(b).filter((t) => /rows · \d+ col/.test(t)).length >= 2, JSON.stringify(b));
    const held = JSON.parse(fs.readFileSync(file));
    check("the pin holds a checkpoint after Preview", held.levels.length === 1, JSON.stringify(held));
    const key = held.levels[0]?.key;

    await p.click(`.dcard[data-alias="${mid}"] [data-card-tool="play"]`);
    check("▶ on the pin finishes", await p.waitStatus(/ran up to/, 30000));
    b = await p.badges();
    check("▶ on the pin reads its checkpoint back", JSON.parse(fs.readFileSync(file)).levels[0]?.key === key);
    // Edit as YAML recomposes the job, so the cards carry new names: the first one is the source.
    check("the card upstream keeps its rows", /20 rows/.test(b[Object.keys(b)[0]]), JSON.stringify(b));
    check("the pinned card reads 7 rows", /7 rows/.test(b[mid]), JSON.stringify(b));

    await p.click("#d-save");
    check("Save writes the job", await p.waitStatus(/saved/, 15000), await p.status());
    await p.click("#d-run");
    check("Run opens its confirmation", await p.until(`document.querySelector("#d-drawer .tabs button.active")?.dataset.tab === "run"`));
    const text = await p.drawerText();
    check("Run is the plain real run", /This run is real: its writers write/.test(text) && !/stops at/.test(text), text.slice(0, 200));

    await p.click(`.dcard[data-alias="${mid}"] [data-card-tool="pin"]`);
    check("unpinning removes the pin and the state file", await p.until(`document.querySelector('.dcard[data-alias="${mid}"] [data-card-tool="pin"]')?.getAttribute("aria-pressed") === "false"`) && !fs.existsSync(file));
  },

  // A real run left suspended by an earlier session: the bar says so, Preview and the verb that writes are held back, the
  // sink writes or abandons it, and the pin survives.
  async "real-state"({ page: p, folder }) {
    const file = stateFile(folder, "chain.yaml");
    check("the job opens", await p.openJob("chain.yaml"));
    check("the bar says a run is suspended", await p.until(`!document.querySelector("#d-suspended")?.hidden`));
    check("the bar has no Abandon and no Continue", await p.ev(`!document.querySelector("#d-abandon, #d-continue")`));
    check("Preview is held back", await p.until(`document.querySelector("#d-preview")?.disabled === true`));
    check("the verb that writes is held back", (await p.disabled("#d-run")) === true);
    check("the verb that writes is named Write targets…", (await p.ev(`document.querySelector("#d-run")?.textContent`)) === "Write targets…");
    check("the sink says the run waits on it", await p.until(`/⏸/.test(document.querySelector('.dcard[data-alias="c"] .dcard-badge')?.textContent ?? "")`), JSON.stringify(await p.badges()));
    check("the sink offers Write and Abandon", await p.ev(`!!document.querySelector('.dcard[data-alias="c"] [data-card-tool="write"]') && !!document.querySelector('.dcard[data-alias="c"] [data-card-tool="abandon"]')`));
    check("a card that is no sink offers neither", await p.ev(`!document.querySelector('.dcard[data-alias="b"] [data-card-tool="write"], .dcard[data-alias="b"] [data-card-tool="abandon"]')`));
    await p.cardTool("c", "write");
    check("Write on a run that holds nothing of its target is refused", await p.waitStatus(/does not hold the rows/i, 10000), await p.status());
    await p.cardTool("c", "abandon");
    check("Abandon hands the page back", await p.until(`document.querySelector("#d-suspended").hidden && document.querySelector("#d-run").disabled === false && document.querySelector("#d-preview").disabled === false`));
    await p.click("#d-preview");
    check("Preview works after Abandon", await p.waitStatus(/preview done/, 30000), await p.status());
    check("the pin survives Abandon", (await p.pinned("b")) === "true");
    check("Forget pins shows", await p.visible("#d-forget-pins"));
    await p.click("#d-forget-pins");
    check("Forget pins unpins and hides itself", await p.until(`document.querySelector("#d-forget-pins").hidden`) && (await p.pinned("b")) === "false");
    check("Forget pins leaves no state file", !fs.existsSync(file));
  },

  // Write targets… without the case ticked writes at once, after the confirmation, to every sink.
  async "write-plain"({ page: p, folder }) {
    check("the job opens", await p.openJob("review.yaml"));
    await p.click("#d-run");
    check("the confirmation opens", await p.until(`!!document.querySelector("#d-run-confirm")`));
    check("the case is not ticked", (await p.ev(`document.querySelector("#d-review")?.checked`)) === false);
    check("the confirmation says the targets are written", /reaches the end|real: its writers write/.test(await p.drawerText()), (await p.drawerText()).slice(0, 200));
    check("nothing is written before the confirmation", !fs.existsSync(path.join(folder, "c.csv")) && !fs.existsSync(path.join(folder, "e.csv")));
    await p.click("#d-run-confirm");
    check("the run succeeds", await p.waitStatus(/run succeeded/, 30000), await p.status());
    check("both targets are written", fs.existsSync(path.join(folder, "c.csv")) && fs.existsSync(path.join(folder, "e.csv")));
    check("no sink is left waiting", await p.ev(`document.querySelector("#d-suspended").hidden && !document.querySelector("[data-card-tool='write']")`));
  },

  // Review first: nothing is written, every sink says what waits, a sink writes the whole run.
  async "review-write"({ page: p, folder }) {
    const files = () => ["c.csv", "e.csv"].map((f) => fs.existsSync(path.join(folder, f)));
    check("the job opens", await p.openJob("review.yaml"));
    await p.click("#d-run");
    check("the confirmation opens", await p.until(`!!document.querySelector("#d-review")`));
    await p.click("#d-review");
    check("the case re-prepares the run as a review", await p.until(`/stops before every writer/.test(document.querySelector("#d-drawer .drawer-body")?.innerText ?? "")`), (await p.drawerText()).slice(0, 200));
    check("the case stays ticked", (await p.ev(`document.querySelector("#d-review")?.checked`)) === true);
    await p.click("#d-run-confirm");
    check("the review ends", await p.waitStatus(/run succeeded/, 30000), await p.status());
    check("nothing is written", files().every((f) => !f), JSON.stringify(files()));
    check("the sinks say what waits", await p.until(`/⏸ 10 rows queued/.test(document.querySelector('.dcard[data-alias="c"] .dcard-badge')?.textContent ?? "") && /⏸ 20 rows queued/.test(document.querySelector('.dcard[data-alias="e"] .dcard-badge')?.textContent ?? "")`), JSON.stringify(await p.badges()));
    check("the bar says a run is suspended", await p.visible("#d-suspended"));
    check("Preview and Write targets… are held back", (await p.disabled("#d-preview")) === true && (await p.disabled("#d-run")) === true);
    const held = JSON.parse(fs.readFileSync(stateFile(folder, "review.yaml"), "utf8"));
    check("the review pinned nothing", held.watch.length === 0 && held.levels.length === 2, JSON.stringify(held));
    await p.pick("c");
    check("a click on the sink shows the rows that wait", await p.until(`/10 rows queued/.test(document.querySelector("#d-drawer .drawer-body")?.innerText ?? "") && !!document.querySelector("#d-drawer .drawer-body table")`), (await p.drawerText()).slice(0, 200));
    await p.cardTool("c", "write");
    check("Write opens the confirmation of the continuation", await p.until(`!!document.querySelector("#d-run-confirm")`));
    check("the continuation has no case", await p.ev(`!document.querySelector("#d-review")`));
    check("the continuation reads the held rows", /Held from before/.test(await p.drawerText()), (await p.drawerText()).slice(0, 300));
    check("nothing is written before the confirmation", files().every((f) => !f));
    await p.click("#d-run-confirm");
    check("the continuation succeeds", await p.waitStatus(/run succeeded/, 30000), await p.status());
    check("both targets are written", files().every(Boolean), JSON.stringify(files()));
    check("the targets hold the reviewed rows", fs.readFileSync(path.join(folder, "c.csv"), "utf8").trim().split("\n").length === 11 && fs.readFileSync(path.join(folder, "e.csv"), "utf8").trim().split("\n").length === 21);
    check("the page is handed back", await p.until(`document.querySelector("#d-suspended").hidden && document.querySelector("#d-run").disabled === false && !document.querySelector("[data-card-tool='write']")`));
    check("the state file is gone", !fs.existsSync(stateFile(folder, "review.yaml")));
  },

  // Review first, then abandon from a sink: nothing is written and the page is free again.
  async "review-abandon"({ page: p, folder }) {
    check("the job opens", await p.openJob("review.yaml"));
    await p.click("#d-run");
    await p.until(`!!document.querySelector("#d-review")`);
    await p.click("#d-review");
    await p.until(`/stops before every writer/.test(document.querySelector("#d-drawer .drawer-body")?.innerText ?? "")`);
    await p.click("#d-run-confirm");
    check("the review ends", await p.waitStatus(/run succeeded/, 30000), await p.status());
    check("the sinks wait", await p.until(`document.querySelectorAll("[data-card-tool='abandon']").length === 2`));
    await p.cardTool("e", "abandon");
    check("Abandon frees the page", await p.until(`document.querySelector("#d-suspended").hidden && document.querySelector("#d-run").disabled === false && !document.querySelector("[data-card-tool='abandon']")`));
    check("nothing was written", !fs.existsSync(path.join(folder, "c.csv")) && !fs.existsSync(path.join(folder, "e.csv")));
    check("the state file is gone", !fs.existsSync(stateFile(folder, "review.yaml")));
    check("the case keeps its choice for the job", await p.until(`true`) && JSON.parse(fs.readFileSync(path.join(folder, ".dtpipe/studio/view.json"), "utf8")).jobs?.["review.yaml"]?.review === true);
  },

  // A reload while the run is suspended: the sinks still wait, with their rows, and Write finishes the run.
  async "review-reload"({ page: p, folder }) {
    check("the job opens", await p.openJob("review.yaml"));
    await p.click("#d-run");
    await p.until(`!!document.querySelector("#d-review")`);
    await p.click("#d-review");
    await p.until(`/stops before every writer/.test(document.querySelector("#d-drawer .drawer-body")?.innerText ?? "")`);
    await p.click("#d-run-confirm");
    check("the review ends", await p.waitStatus(/run succeeded/, 30000), await p.status());
    await p.until(`!!document.querySelector("[data-card-tool='write']")`);
    await p.reload();
    check("the job opens again", await p.openJob("review.yaml"));
    check("the sinks still wait, with their rows", await p.until(`/⏸ 10 rows queued/.test(document.querySelector('.dcard[data-alias="c"] .dcard-badge')?.textContent ?? "") && /⏸ 20 rows queued/.test(document.querySelector('.dcard[data-alias="e"] .dcard-badge')?.textContent ?? "")`), JSON.stringify(await p.badges()));
    check("the bar says a run is suspended", await p.until(`!document.querySelector("#d-suspended")?.hidden`));
    check("the rows that wait are shown on a sink", await p.until(`!!document.querySelector("#d-drawer .drawer-body table")`), (await p.drawerText()).slice(0, 200));
    check("the case is ticked as it was left", await p.until(`true`));
    await p.cardTool("e", "write");
    await p.until(`!!document.querySelector("#d-run-confirm")`);
    await p.click("#d-run-confirm");
    check("Write finishes the run", await p.waitStatus(/run succeeded/, 30000), await p.status());
    check("both targets are written", fs.existsSync(path.join(folder, "c.csv")) && fs.existsSync(path.join(folder, "e.csv")));
  },

  // A branch that runs whole (a stream processor): its inside is not cuttable.
  async "whole-branch"({ page: p }) {
    check("the job opens", await p.openJob("whole.yaml"));
    await p.click("#d-preview");
    check("Preview finishes", await p.waitStatus(/preview (done|failed)/, 30000));
    check("Preview succeeds", /preview done/.test(await p.status()), await p.status());
    const b = await p.badges();
    check("the end of the branch shows its rows", /rows · \d+ col/.test(b.b ?? "") || /rows · \d+ col/.test(b.a ?? ""), JSON.stringify(b));
    check("the source a processor reads shows its rows and can be pinned", /20 rows/.test(b.a ?? "") && !(await p.ev(`document.querySelector('.dcard[data-alias="a"] [data-card-tool="pin"]')?.getAttribute("aria-disabled")`)), JSON.stringify(b));
    check("the processor's end shows only its own rows", /5 rows/.test(b.b ?? ""), JSON.stringify(b));
  },

  // An edit made while Preview's answer is held back: the rows taken must be judged against the new text.
  async "edit-during-preview"({ page: p }) {
    check("the job opens", await p.openJob("chain.yaml"));
    await p.pick("b");
    await p.armHold();
    await p.click("#d-preview");
    check("the engine answered and the page is still waiting", await p.held());
    check("the page says it is running", (await p.state("a")) === "running", await p.state("a"));
    const since = p.seen.length;
    await p.type(KEY_OF_B, " && true");
    check("the edit is composed while the preview runs", await p.waitSeen("/api/design/compose", since));
    check("the edit is planned while the preview runs", await p.waitSeen("/api/debug/plan", since));
    await p.release();
    check("Preview ends", await p.waitStatus(/preview (done|failed)/, 30000));
    check("the card that was edited says so", await p.until(`document.querySelector('.dcard[data-alias="b"] .dcard-badge')?.dataset.cardState === "edited"`), JSON.stringify(await p.badges()));
    const b = await p.badges();
    check("the card before it takes its rows", /rows · \d+ col/.test(b.a), JSON.stringify(b));
  },

  // An edit made while a real run holds the engine: judged at once, and Stop ends the run with 130.
  async "edit-during-run"({ page: p }) {
    check("the job opens", await p.openJob("slow.yaml"));
    await p.ev(`(() => { const e = document.querySelector("#d-watch-rows"); e.value = "5"; e.dispatchEvent(new Event("change")); })()`);
    await p.click("#d-preview");
    check("Preview finishes", await p.waitStatus(/preview done/, 30000), await p.status());
    check("the card has rows", /rows · \d+ col/.test((await p.badges()).b));
    await p.click("#d-save");
    check("Save writes the job", await p.waitStatus(/saved/, 15000), await p.status());
    await p.click("#d-run");
    check("Run opens its confirmation", await p.until(`!!document.querySelector("#d-run-confirm")`));
    await p.click("#d-run-confirm");
    check("the run is going", await p.until(`!!document.querySelector("#d-run-stop")`));
    await p.pick("b");
    const since = p.seen.length;
    await p.type(KEY_OF_B, " && true");
    check("the edit is composed during the run", await p.waitSeen("/api/design/compose", since));
    check("the edit is planned during the run", await p.waitSeen("/api/debug/plan", since));
    check("the card that was edited says so while the run goes on", await p.until(`document.querySelector('.dcard[data-alias="b"] .dcard-badge')?.dataset.cardState === "edited"`), JSON.stringify(await p.badges()));
    check("the run is still going", await p.visible("#d-run-stop"));
    await p.click("#d-run-stop");
    check("Stop ends the run as stopped", await p.waitStatus(/run stopped/, 30000), await p.status());
  },

  // Two jobs asked for in a row, the first one's cards arriving last: only the last one asked for is shown.
  async "open-race"({ page: p }) {
    await p.until(`document.querySelectorAll("#jobs button.job").length === 2`);
    await p.armHold("/api/design/decompose");
    const open = (name) => p.ev(`[...document.querySelectorAll("#jobs button.job")].find((b) => b.textContent === ${JSON.stringify(name)}).click()`);
    await open("chain.yaml");
    check("the first job's cards are held back", await p.held());
    await open("whole.yaml");
    check("the second job opens", await p.until(`document.querySelectorAll(".dcard").length === 2 && document.querySelector("#job-name")?.textContent === "whole.yaml"`));
    await p.release();
    await sleep(800);
    const shown = await p.ev(`({ cards: document.querySelectorAll(".dcard").length, job: document.querySelector("#job-name")?.textContent })`);
    check("the first job's late cards are dropped", shown.cards === 2 && shown.job === "whole.yaml", JSON.stringify(shown));
  },

  // How the page was left (tab, card, rows, pane sizes) comes back after a reload, kept by the studio and not by the browser.
  async "view-kept"({ page: p, folder }) {
    const file = path.join(folder, ".dtpipe/studio/view.json");
    check("the job opens", await p.openJob("chain.yaml"));
    await p.click("#d-preview");
    check("Preview finishes", await p.waitStatus(/preview done/, 30000));
    await p.pick("b");
    await p.ev(`(() => { const e = document.querySelector("#d-watch-rows"); e.value = "7"; e.dispatchEvent(new Event("change")); })()`);
    await p.ev(`document.querySelector('.split[data-split="pal"]').focus()`);
    await p.send("Input.dispatchKeyEvent", { type: "keyDown", key: "ArrowRight", code: "ArrowRight", windowsVirtualKeyCode: 39 });
    await p.send("Input.dispatchKeyEvent", { type: "keyUp", key: "ArrowRight", code: "ArrowRight", windowsVirtualKeyCode: 39 });
    const kept = () => (fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : {});
    for (let i = 0; i < 100 && !(kept().jobs?.["chain.yaml"]?.rows === 7 && kept().sizes?.pal === 236); i++) await sleep(100);
    const view = kept();
    check("the studio kept the tab, the card and the rows", view.jobs?.["chain.yaml"]?.tab === "preview" && view.jobs["chain.yaml"].card === "b" && view.jobs["chain.yaml"].rows === 7, JSON.stringify(view));
    check("the studio kept the pane's size", view.sizes?.pal === 236, JSON.stringify(view));
    check("the browser kept nothing", (await p.ev(`localStorage.length`)) === 0);

    await p.reload();
    check("the job opens again", await p.openJob("chain.yaml"));
    check("the tab is the one left", (await p.tab()) === "preview", await p.tab());
    check("the card is the one left", await p.ev(`document.querySelector('.dcard[data-alias="b"]')?.classList.contains("selected")`));
    check("the rows are the ones asked", (await p.ev(`document.querySelector("#d-watch-rows")?.value`)) === "7");
    check("the pane has the size it was left at", (await p.ev(`document.querySelector("#designer").style.getPropertyValue("--pal-w")`)) === "236px", await p.ev(`document.querySelector("#designer").style.getPropertyValue("--pal-w")`));
  },

  // Typing in the inspector across the page's own timers: the field keeps its focus, its text and its caret.
  async typing({ page: p }) {
    check("the job opens", await p.openJob("chain.yaml"));
    await p.pick("a");
    check("the inspector shows the source's YAML", await p.until(`!!document.querySelector("#i-yaml")`));
    const since = p.seen.length;
    await p.type("#i-yaml", "\n# note");
    check("the text composed while typing", await p.waitSeen("/api/design/compose", since));
    await p.frame();
    const field = await p.ev(`(() => { const e = document.querySelector("#i-yaml"); return { focused: document.activeElement === e, tail: e?.value.slice(-6), caret: e?.selectionStart === e?.value.length }; })()`);
    check("the field keeps its focus", field.focused, JSON.stringify(field));
    check("the field keeps what was typed", field.tail === "# note", JSON.stringify(field));
    check("the caret stays at the end", field.caret, JSON.stringify(field));
  },
};

// ---------------------------------------------------------------- the runner

async function seed(name, folder) {
  if (name === "real-state") {
    fs.mkdirSync(path.join(folder, ".dtpipe/studio"), { recursive: true });
    fs.writeFileSync(stateFile(folder, "chain.yaml"), JSON.stringify({
      session: "studio-debug", real: true, watch: [{ branch: "b", at: "end", card: "b" }], levels: [{ branch: "b", level: 2, digest: "x", key: "deadbeefdeadbeef" }],
    }));
  }
}

const argsOf = {
  agent: () => ["--llm-provider", "ollama", "--llm-url", fakeLlm.url],
};

const fixturesOf = {
  "whole-branch": ["whole.yaml"], "edit-during-run": ["slow.yaml"], "open-race": ["chain.yaml", "whole.yaml"],
  "write-plain": ["review.yaml"], "review-write": ["review.yaml"], "review-abandon": ["review.yaml"], "review-reload": ["review.yaml"],
};

const wanted = process.argv.slice(2);
const names = wanted.length ? wanted : Object.keys(scenarios);
const unknown = names.filter((n) => !scenarios[n]);
if (unknown.length) { console.error(`unknown scenario: ${unknown.join(", ")} (known: ${Object.keys(scenarios).join(", ")})`); process.exit(2); }
if (!BIN && !fs.existsSync(DLL)) { console.error(`no built dtpipe at ${DLL}: build it, or set DTPIPE_SMOKE_DLL`); process.exit(2); }

const chrome = await startChrome();
const page = session(chrome.ws);
await page.send("Runtime.enable");
await page.send("Page.enable");
await page.send("Network.enable");

for (const name of names) {
  console.log(`\n== ${name}`);
  const folder = fs.mkdtempSync(path.join(os.tmpdir(), "dtpipe-smoke-"));
  for (const file of fixturesOf[name] || ["chain.yaml"]) fs.copyFileSync(path.join(here, "fixtures", file), path.join(folder, file));
  let studio = null;
  const logsBefore = page.logs.length;
  try {
    await seed(name, folder);
    studio = await startStudio(folder, argsOf[name]?.() ?? []);
    await page.send("Page.navigate", { url: studio.url });
    await scenarios[name]({ page, folder });
  } catch (e) {
    check(`${name} runs to its end`, false, e.stack || String(e));
  } finally {
    studio?.stop();
    try { fs.rmSync(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 }); } catch { /* still held: the system cleans the temp folder */ }
  }
  const logs = page.logs.slice(logsBefore);
  check(`${name}: the page raised no script error`, logs.length === 0, logs.join(" ; "));
}

chrome.stop();
fakeLlm.stop();
console.log(`\n${pass} PASS, ${fail} FAIL`);
process.exit(fail ? 1 : 0);
