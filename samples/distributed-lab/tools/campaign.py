#!/usr/bin/env python3
"""The robustness campaign: a fixed matrix of faults played against the running lab, each trial judged
on the same invariants. The matrix, the invariants and their bounds are the ones the campaign sheet fixed
before the first trial; this script only plays and records them.

  campaign.py <lab-url> <dtpipe> <state-dir> <campaign-id> [cell ...]

Cells are F1 ... F11, then S, S3 and S5 for the security volet (a prefix selects: F1 alone is F1 and F10 only as
F10). F6g, a node's groups changed under a run, F12, two runs on one runner host with one of them killed, and F13, the
replica of a node that executes a fragment killed (the lab started with LAB_REPLICAS=node-4=2), are outside that matrix:
they are played when they are named, never by default; naming a cell plays only the cells named.
Every trial is one line of JSON in <state-dir>/campaign/<campaign-id>/trials.jsonl, and every line the lab
logs is kept, stamped, in <state-dir>/campaign/<campaign-id>/logs/. The lab must be up on strict nodes
(./lab.sh up), and the start is checked: nothing of the lab runs that the pid files do not name, the machine is at rest
(no build, no test run, no TransportR process), the dtpipe the hosts run (the runner's included) starts and reports the
frozen commit (HEAD, or CAMPAIGN_FROZEN) with a clean tree, and every armed deployment binds only nodes the rights give an
identity. The campaign refuses to start otherwise. CAMPAIGN_PREFLIGHT=report runs the same
checks, prints what they find and plays on (the harness pass); CAMPAIGN_SKIP_PREFLIGHT=1 skips them. Standard library
only.
"""
import errno, json, os, shutil, signal, subprocess, sys, tempfile, time, urllib.error

_ARGS = list(sys.argv)
sys.argv = sys.argv[:5]  # smoke.py reads argv[1:4]; the campaign id at argv[4] is not a pass name
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import smoke as S  # noqa: E402  (reads sys.argv itself)
from campaign_queue import queue_is_stuck  # noqa: E402
from campaign_logs import capture as capture_logs, follow as follow_logs, port_exhaustion_lines as port_lines  # noqa: E402
import campaign_bench as bench  # noqa: E402
import campaign_security as sec  # noqa: E402

URL, DTPIPE, STATE, CAMPAIGN = _ARGS[1].rstrip("/"), _ARGS[2], _ARGS[3], _ARGS[4]
CELLS = _ARGS[5:]
# The IdP lab.sh starts: every token comes from it, the coordinator only trusts it.
IDP_URL = f"http://127.0.0.1:{os.environ.get('LAB_IDP_PORT', '5190')}"
LAB = os.path.dirname(HERE)
OUT = os.path.join(STATE, "campaign", CAMPAIGN)
NODES = ["node-1", "node-2", "node-3", "node-4", "runner-1"]
TOXI = os.environ.get("TOXIPROXY_DIR", os.path.normpath(os.path.join(LAB, "..", "..", "..", "transportr", "tools")))

# A retry storm on the loopback can exhaust the ephemeral ports for a few seconds, the harness's own API calls included
# (EADDRINUSE / EADDRNOTAVAIL): those are retried, nothing else is (a coordinator that is down must still refuse at once).
# Every retry is counted, and so is every wait for the sockets to drain: I7 judges them, the harness never absorbs them
# silently. `record` reads the counters.
PORT_ERRNOS = {errno.EADDRINUSE: "EADDRINUSE", errno.EADDRNOTAVAIL: "EADDRNOTAVAIL"}
RETRIED = {name: 0 for name in PORT_ERRNOS.values()}
PORT_WAIT = {"seconds": 0.0, "peak_time_wait": 0}
_raw_call = S.call


def _call(path, body=None, method=None):
    for attempt in range(40):
        try:
            return _raw_call(path, body, method)
        except urllib.error.URLError as e:
            name = PORT_ERRNOS.get(getattr(getattr(e, "reason", None), "errno", None))
            if name:
                RETRIED[name] += 1
                if attempt < 39:
                    time.sleep(1.0)
                    continue
            raise


S.call = _call


def wait_ports_free(timeout=90):
    """Waits until the sockets a storm left in TIME_WAIT have drained, so the next check does not meet an exhausted range.
    A wait is recorded (seconds, peak) for the trial it falls in; it is not judged."""
    started, waited = time.time(), False
    try:
        while time.time() - started < timeout:
            out = subprocess.run(["netstat", "-an", "-p", "tcp"], capture_output=True, text=True).stdout
            waiting = out.count("TIME_WAIT")
            if waiting < 2000:
                return
            waited = True
            PORT_WAIT["peak_time_wait"] = max(PORT_WAIT["peak_time_wait"], waiting)
            time.sleep(2)
    finally:
        if waited:
            PORT_WAIT["seconds"] += time.time() - started


BOUND = {"kill": 10, "cancel": 6, "host": 60}
REFERENCE = "customers-anonymized"
P2 = "sensor-stream"
P3 = "order-history"
REPS = int(os.environ.get("CAMPAIGN_REPS", "3"))


# ------------------------------------------------------------------ plumbing

_witness = {}


def ctx():
    if "ctx" not in _witness:
        _witness["ctx"] = S.Context()
    return _witness["ctx"]


def check_for(pid):
    if pid not in _witness:
        doc = S.call(f"/api/library/{pid}")
        _witness[pid] = ctx().witness("campaign-" + pid, doc["yaml"])
    return _witness[pid]


def deploy(pid):
    plan = S.call(f"/api/library/{pid}/plan")["plan"]
    S.call(f"/api/library/{pid}/deploy", {})
    return plan


def undeploy(pid):
    try:
        S.call(f"/api/deployments/{pid}", None, "DELETE")
    except Exception:
        pass


def submit(pid, wait_running=True):
    run = S.call("/api/runs", {"pipelineId": pid})
    if wait_running:
        S.until(lambda: any(r["runId"] == run["runId"] and r["state"] == "running" for r in S.call("/api/runs/queue")), 60, "the run to start")
    return run


def await_final(run_id, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        final = S._finished(run_id)
        if final:
            return final, time.time()
        time.sleep(0.2)
    return None, time.time()


def pid_of(name):
    with open(os.path.join(STATE, "run", name + ".pid")) as f:
        return int(f.read().strip())


def children():
    """Fragment children of any host: `dtpipe --job <fragment>` processes. The harness's own witness runs are told
    apart by their directory, never by a word in the job's name (a pipeline such as campaign-f11 would be hidden)."""
    out = subprocess.run(["ps", "-axo", "pid,command"], capture_output=True, text=True).stdout
    witness = os.sep + "witness" + os.sep
    return [int(l.split(None, 1)[0]) for l in out.splitlines() if "release/dtpipe --job" in l and witness not in l]


def replica_runs():
    """The runs the coordinator still counts on a replica, by node and replica: what a replica holds is the runs placed on it."""
    return {f"{n['name']}/{r['id']}": r["runs"] for n in S.call("/api/nodes") for r in n["replicas"] if r["runs"]}


def hosts_online():
    """All five hosts announced and online; False while the coordinator is down or has not heard from them."""
    try:
        nodes = S.call("/api/nodes")
    except Exception:
        return False
    return len(nodes) == len(NODES) and all(n["online"] for n in nodes)


def await_hosts(what, timeout=120):
    """Waits for the five hosts, and names the ones still offline when they do not come: the reference check reads the
    warehouse through node-4, so an I4 that follows a missing host must say which one."""
    try:
        S.until(hosts_online, timeout, what)
    except RuntimeError:
        try:
            online = {n["name"] for n in S.call("/api/nodes") if n["online"]}
            missing = f"offline {[n for n in NODES if n not in online]}"
        except Exception as e:
            missing = f"the coordinator does not answer: {str(e)[:80]}"
        raise RuntimeError(f"timed out waiting for {what}: {missing}") from None


def _gone(pid):
    """Whether a process has exited; reaps it when it is a child of this harness, so a zombie never counts as alive."""
    try:
        if os.waitpid(pid, os.WNOHANG)[0] == pid:
            return True
    except ChildProcessError:
        pass
    try:
        os.kill(pid, 0)
        return False
    except ProcessLookupError:
        return True


def stop_pid(pid, grace=10):
    """SIGTERM, then SIGKILL if the process is still there after `grace` seconds; returns once it is gone."""
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.time() + grace
    while time.time() < deadline:
        if _gone(pid):
            return
        time.sleep(0.2)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    while not _gone(pid):
        time.sleep(0.1)


def restart_lab():
    lab = os.path.join(LAB, "lab.sh")
    subprocess.run([lab, "down"], capture_output=True, timeout=120)
    time.sleep(2)
    subprocess.run([lab, "up"], capture_output=True, timeout=300)
    S.until(hosts_online, 120, "the lab after a restart")


def recover():
    """Bring the lab back to five online hosts and no deployment, the way an operator would."""
    wait_ports_free()
    if queue_is_stuck(S.call):
        restart_lab()
        return "lab restarted (a run stayed in the queue)"
    if not hosts_online():
        out = subprocess.run([os.path.join(LAB, "lab.sh"), "nodes", "strict"], capture_output=True, text=True, timeout=300)
        if out.returncode != 0:
            raise RuntimeError(f"lab.sh nodes strict exited {out.returncode}: {(out.stderr or out.stdout).strip()[-150:]}")
        await_hosts("the hosts after a restart")
        return "hosts restarted"
    for d in S.call("/api/deployments"):
        undeploy(d["pipelineId"])
    return ""


def rights_matrix():
    return S.call("/api/rights")["matrix"]


# ------------------------------------------------------------------ invariants

def judge(rec, pid, final, elapsed, expect, bound, cause_of=None):
    """Fill rec["inv"] with the invariants that apply; returns the list of violated ones."""
    inv = {}
    if final is None:
        inv["I1"] = f"no verdict {bound + 30:.0f}s after the fault"
        rec["state"] = "none"
    else:
        rec["state"] = final["state"]
        res = final.get("result") or {}
        rec["cause"] = res.get("cause")
        rec["consequences"] = res.get("consequences")
        if elapsed > bound:
            inv["I1"] = f"verdict after {elapsed:.1f}s, bound {bound}s"
        if final["state"] not in expect:
            inv["I2"] = f"{final['state']} not in {sorted(expect)}"
        elif cause_of and final["state"] == "failed" and not str(res.get("cause") or "").endswith(cause_of):
            inv["I2"] = f"cause {res.get('cause')!r}, expected ...{cause_of}"
        if final["state"] == "succeeded":
            try:
                ctx().verdict(final, check_for(pid))
            except Exception as e:
                inv["I5"] = str(e)[:200]
    time.sleep(10)
    left = children()
    if left:
        inv["I3"] = f"{len(left)} fragment child process(es) alive 10s after the verdict"
    held = replica_runs()
    if held:
        inv["I6"] = f"replicas still hold runs 10s after the verdict: {held}"
    queue = [r for r in S.call("/api/runs/queue") if r["state"] == "running"]
    if queue:
        inv["I6"] = (inv.get("I6", "") + f" queue still running {[r['runId'] for r in queue]}").strip()
    rec["inv"] = inv
    return inv


def reference_ok(rec):
    """I4: an unfaulted run of the reference pipeline still succeeds, its check equal to the witness."""
    try:
        note = recover()
        if note:
            rec["recovery"] = note
        deploy(REFERENCE)
        run = submit(REFERENCE, wait_running=False)
        final, _ = await_final(run["runId"], 120)
        if final is None:
            return "reference run gave no verdict in 120s"
        ctx().verdict(final, check_for(REFERENCE))
        return None
    except Exception as e:
        return str(e)[:200]


# ------------------------------------------------------------------ trials

_mark = {"retried": dict(RETRIED), "log_lines": 0}


def judge_ports(rec):
    """I7, the bench stays usable during the trial: no retried call on EADDRINUSE / EADDRNOTAVAIL and no port-exhaustion
    line in the lab's logs since the previous record (the trial, its recovery and its I4 check). Recorded for every trial,
    violated or not; a wait for the sockets to drain is recorded, not judged. The log follower reads every half second, so
    a line can land in the next record: the campaign's total is exact, a trial's share is not."""
    retried = {k: RETRIED[k] - _mark["retried"][k] for k in RETRIED}
    lines = port_lines() - _mark["log_lines"]
    _mark["retried"], _mark["log_lines"] = dict(RETRIED), _mark["log_lines"] + lines
    rec["ports"] = {"retried": retried, "log_lines": lines}
    if PORT_WAIT["seconds"]:
        rec["ports"]["waited"] = {"seconds": round(PORT_WAIT["seconds"], 1), "peak_time_wait": PORT_WAIT["peak_time_wait"]}
        PORT_WAIT.update(seconds=0.0, peak_time_wait=0)
    if rec.get("state") != "n/a" and (sum(retried.values()) or lines):
        rec.setdefault("inv", {})["I7"] = f"ports exhausted: {sum(retried.values())} call(s) retried {retried}, {lines} log line(s)"


def record(rec):
    judge_ports(rec)
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "trials.jsonl"), "a") as f:
        f.write(json.dumps(rec) + "\n")
    bad = ",".join(sorted(rec["inv"])) or "-"
    print(f"{'FAIL' if rec['inv'] else 'ok  '}  {rec['cell']:<26} #{rec['rep']}  {rec.get('state', '?'):<9} "
          f"{rec.get('elapsed', 0):>5.1f}s  inv:{bad}  {json.dumps(rec['inv']) if rec['inv'] else ''}", flush=True)


def trial(cell, rep, pid, at, inject, expect, bound, cause_of=None, wait_running=True, note=None, before=None, after=None, evidence=None):
    rec = {"cell": cell, "rep": rep, "pipeline": pid, "expect": sorted(expect), "at": at, "campaign": CAMPAIGN,
           "time": time.strftime("%Y-%m-%dT%H:%M:%S")}
    t_fault = time.time()
    try:
        recover()
        check_for(pid)  # the witness can take as long as a run: before the deployment, so no node waits for it
        plan = deploy(pid)
        if before:
            before()
        run = submit(pid, wait_running=wait_running)
        time.sleep(at)
        t_fault = time.time()
        rec["injected"] = inject(run, plan) or ""
        final, t_end = await_final(run["runId"], bound + 30)
        elapsed = t_end - t_fault
        rec["elapsed"] = round(elapsed, 2)
        judge(rec, pid, final, elapsed, expect, bound, cause_of)
        if evidence and final is not None and "I2" not in rec["inv"] and (problem := evidence(rec, final)):
            rec["inv"]["I2"] = problem
        if rec["inv"]:
            rec["logs"] = capture_logs(STATE)
        if final is None and os.environ.get("CAMPAIGN_CANCEL_STUCK"):
            # What an operator would do about a run that never ends: cancel it, and time the verdict.
            t_cancel = time.time()
            S.call("/api/runs/cancel", {"runId": run["runId"]})
            after_cancel, _ = await_final(run["runId"], 120)
            rec["cancel_after_stall"] = {"state": after_cancel["state"] if after_cancel else "none",
                                         "seconds": round(time.time() - t_cancel, 1)}
    except NotApplicable as e:
        rec["inv"] = {}
        rec["state"] = "n/a"
        rec["note"] = str(e)
        rec["elapsed"] = 0
        try:
            S.call("/api/runs/cancel", {"runId": run["runId"]})
            await_final(run["runId"], 60)
        except Exception:
            pass
    except Exception as e:
        rec["inv"] = {"H": f"harness: {str(e)[:200]}"}
        rec["elapsed"] = round(time.time() - t_fault, 2)
    finally:
        if after:
            try:
                after()
            except Exception as e:
                rec.setdefault("inv", {})["H"] = f"harness: cleanup failed: {str(e)[:150]}"
        problem = reference_ok(rec)
        if problem:
            rec.setdefault("inv", {})["I4"] = problem
    if note:
        rec["note"] = note
    record(rec)
    return rec


def fragment_of(plan, node):
    return next(f["name"] for f in plan["fragments"] if f["node"] == node)


# ------------------------------------------------------------------ the faults

def name_of(plan, node):
    return next(f["name"] for f in plan["fragments"] if f["node"] == node)


def cancel(run, plan):
    S.call("/api/runs/cancel", {"runId": run["runId"]})


def kill_host(node):
    def inject(run, plan):
        os.kill(pid_of(node), signal.SIGKILL)
        return f"SIGKILL host {node}"
    return inject


def nodes_of(pid):
    plan = S.call(f"/api/library/{pid}/plan")["plan"]
    return [(f["node"], f["name"]) for f in plan["fragments"]]


def child_alive(node):
    """Whether a fragment child of `node` is running: the kill endpoint answers 202 whatever it hit."""
    out = subprocess.run(["ps", "-axo", "pid,command"], capture_output=True, text=True).stdout
    return any(f"/{node}/" in l and "/fragments/" in l and "release/dtpipe --job" in l for l in out.splitlines())


def kill_if_alive(node):
    """F1's fault. A fragment whose child already ended by the fault instant (a short source) has nothing to
    kill: the trial is not applicable, recorded as such and never judged."""
    def inject(run, plan):
        if not child_alive(node):
            raise NotApplicable(f"the child of {node} had already exited")
        S.call(f"/api/fragments/{name_of(plan, node)}/kill", {})
        return f"kill child on {node}"
    return inject


class NotApplicable(Exception):
    pass


def cell_F1():
    for pid in ("sensor-stream", P3):
        for node, _ in nodes_of(pid):
            if os.environ.get("CAMPAIGN_ONLY") and os.environ["CAMPAIGN_ONLY"] != f"{pid}:{node}":
                continue
            for rep in range(REPS):
                trial(f"F1 {pid}:{node}", rep, pid, 4, kill_if_alive(node), {"failed"}, BOUND["kill"], cause_of="@" + node)


def cell_F2():
    for pid, offsets in (("sensor-stream", (0.2, 3, 25)), (P3, (0.2, 3, 8))):
        for at in offsets:
            for rep in range(REPS):
                trial(f"F2 {pid}:+{at}s", rep, pid, at, cancel, {"cancelled", "succeeded"} if at > 20 else {"cancelled"},
                      BOUND["cancel"], wait_running=at > 1)


def cell_F3():
    for node in ("node-1", "runner-1", "node-4"):
        for rep in range(REPS):
            # node-1's fragment has already finished by +4 s: losing its host cannot change the run.
            expect = {"failed", "succeeded"} if node == "node-1" else {"failed"}
            trial(f"F3 host {node}", rep, "sensor-stream", 4, kill_host(node), expect, BOUND["host"])


def stop_start_coordinator(*extra):
    os.kill(pid_of("coordinator"), signal.SIGKILL)
    time.sleep(2)
    log = open(os.path.join(STATE, "logs", "coordinator.log"), "a")
    p = subprocess.Popen([os.path.join(LAB, "lab.sh"), "exec-coordinator", *extra],
                         env=os.environ | {"DTPIPE": DTPIPE}, stdout=log, stderr=log, start_new_session=True)
    with open(os.path.join(STATE, "run", "coordinator.pid"), "w") as f:
        f.write(str(p.pid))


def stop_start_idp(*extra):
    """The IdP is gone before `lab.sh idp` starts another: a live one would be kept as "already running"."""
    stop_pid(pid_of("idp"))
    subprocess.run([os.path.join(LAB, "lab.sh"), "idp", *extra], capture_output=True, timeout=120, check=True)


def idp_token_lifetime():
    """The lifetime, in seconds, of the token the IdP issues now to node-1's credentials."""
    node = json.load(open(os.path.join(LAB, "nodes", "node-1.json")))
    status, answer = sec.token_answer(IDP_URL, node["clientId"], node["secret"])
    if "expires_in" not in answer:
        raise RuntimeError(f"no token for node-1 (HTTP {status})")
    return answer["expires_in"]


def survivors():
    """The fragment children alive now, with how long they have lived."""
    out = []
    for pid in children():
        age = subprocess.run(["ps", "-o", "etime=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
        out.append({"pid": pid, "age": age})
    return out


def cell_F4():
    """The coordinator is killed and restarted in mid-run. It is not highly available (its state is in memory and
    nothing rebuilds it from the nodes), so what is judged is that the hosts come back (I1), that the operator's reset,
    which restarts the hosts, leaves nothing running (I3), and that the lab runs a reference pipeline afterwards (I4).
    What the restart left behind before that reset is recorded, not judged."""
    for rep in range(REPS):
        rec = {"cell": "F4 coordinator restart", "rep": rep, "pipeline": "sensor-stream", "expect": ["restarted"], "at": 4,
               "campaign": CAMPAIGN, "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "inv": {}}
        t0 = time.time()
        lost = None
        try:
            recover(); deploy("sensor-stream"); check_for("sensor-stream")
            lost = submit("sensor-stream")["runId"]; time.sleep(4)
            t_fault = time.time()
            stop_start_coordinator()
            S.until(hosts_online, 90, "the hosts back online")
            rec["elapsed"] = round(time.time() - t_fault, 2)
            rec["state"] = "restarted"
            time.sleep(10)
            rec["info"] = {"lost_run": lost, "survivors_10s_after_return": survivors(),
                           "lost_run_in_queue": any(r["runId"] == lost for r in S.call("/api/runs/queue")),
                           "lost_run_in_history": any(r["runId"] == lost for r in S.call("/api/runs"))}
            restart_lab()  # the operator's reset: stopping the hosts stops their fragment children
            time.sleep(10)
            left = children()
            if left:
                rec["inv"]["I3"] = f"{len(left)} fragment child process(es) alive 10s after the operator's reset"
        except Exception as e:
            rec["inv"]["I1"] = f"the lab did not come back: {str(e)[:150]}"
            rec["elapsed"] = round(time.time() - t0, 2)
            rec["logs"] = capture_logs(STATE)
            try:
                rec["evidence"] = {n: [l.strip()[:200] for l in open(os.path.join(STATE, "logs", n + ".log")).readlines()[-4:]] for n in ("node-1", "runner-1")}
                rec["evidence"]["nodes_online"] = [(n["name"], n["online"]) for n in S.call("/api/nodes")]
            except Exception:
                pass
        problem = reference_ok(rec)
        if problem:
            rec["inv"]["I4"] = problem
        try:
            rec.setdefault("info", {})["lost_run_id_reused"] = lost is not None and any(r["runId"] == lost for r in S.call("/api/runs"))
        except Exception:
            pass
        record(rec)


def cell_F6():
    matrix = rights_matrix()
    revoked = {g: [t for t in targets if not (g == "crm" and t == "runner")] for g, targets in matrix.items()}

    def inject(run, plan):
        S.call("/api/rights/matrix", {"matrix": revoked}, "PUT")
        return "revoke crm -> runner"

    def restore():
        S.call("/api/rights/matrix", {"matrix": matrix}, "PUT")

    for rep in range(REPS):
        trial("F6 rights revoked", rep, "sensor-stream", 4, inject, {"succeeded", "failed"}, 60, after=restore)


def cell_F6g():
    """A node's groups change in the middle of a run. runner-1 joins `audit` while sensor-stream sends from runner-1 to node-4,
    and the matrix has no `audit -> warehouse`: the node's data plane client takes the groups of the moment, and the hub
    judges the transfer again against them. The run must fail, the coordinator's log must say the hub ended a transfer, and
    the verdict must name the edge that transfer carried: a run that failed otherwise did not show the refresh. The identity
    is put back at the end."""
    runner = next(i for i in S.call("/api/rights")["identities"] if i["clientId"] == "runner-1")
    seen = {}

    def put(groups):
        S.call("/api/rights/identities/runner-1", {"clientId": "runner-1", "name": runner["name"], "description": runner["description"],
                                                  "groups": groups, "enabled": True}, "PUT")
        S.call("/api/rights/identities/runner-1/reconnect", {})

    def inject(run, plan):
        seen["log"] = len(S.coordinator_log())
        put(["runner", "audit"])
        return "runner-1 joins audit"

    def restore():
        put(runner["groups"])
        S.until(lambda: next((n["groups"] == sorted(runner["groups"]) and n["online"] for n in S.call("/api/nodes") if n["name"] == "runner-1"), False),
                90, "runner-1 back in its groups")

    def evidence(rec, final):
        ended = [l.strip() for l in S.coordinator_log()[seen["log"]:].splitlines() if "Ending transfer" in l]
        rec["hub_ended"] = ended[:3]
        if not ended:
            return "the run failed without the hub ending any transfer"
        edge, description = "sensor-stream@runner-1.enrich -> sensor-stream@node-4.enrich", final.get("description") or ""
        if edge not in description or "(mismatch)" not in description:
            return f"the verdict does not name the edge {edge}: {description[:200]!r}"
        return None

    for rep in range(REPS):
        trial("F6g groups changed", rep, "sensor-stream", 4, inject, {"failed"}, 60, after=restore, evidence=evidence)


# F12's two pipelines read one data node each, disjoint, so the runs are in flight together through the runner: A is a slow
# stream joined with node-1's customers (about 30 s on the runner), B reads node-3's products and ends in a moment.
PAIR_A = """readings:
  input: generate:300000
  provider-options:
    generate:
      rows-per-second: 10000
customers:
  input: sqlite:${{LAB_CRM_DB}}
  provider-options:
    sqlite-reader:
      query: SELECT customer_id, country FROM customers
enrich:
  from: readings
  ref:
  - customers
  provider-options:
    sql:
      query: |-
        SELECT r.GenerateIndex AS reading_id, c.country, (r.GenerateIndex * 37) % 1000 / 10.0 AS value
        FROM readings r JOIN customers c ON c.customer_id = 1 + r.GenerateIndex % 20000
sink:
  from: enrich
  output: "null:"
"""
PAIR_B = """products:
  input: sqlite:${{LAB_CATALOG_DB}}
  provider-options:
    sqlite-reader:
      query: SELECT product_id, category, unit_price FROM products
sink:
  from: products
  output: "null:"
"""
LAGS = (0.1, 0.3, 0.6)
PAIR = {"campaign-pair-a": (PAIR_A, {"readings": "runner-1", "customers": "node-1", "enrich": "runner-1", "sink": "runner-1"}),
        "campaign-pair-b": (PAIR_B, {"products": "node-3", "sink": "runner-1"})}


def child_of_run(run_id):
    """Whether the runner's child for `run_id` is running: its job file, named after the run and the fragment, is on its command line."""
    out = subprocess.run(["ps", "-axo", "pid,command"], capture_output=True, text=True).stdout
    return any(f"{run_id}-campaign-pair-a" in l and "release/dtpipe --job" in l for l in out.splitlines())


def pair_trial(variant, rep, with_b, lag=0.0):
    """One run A on the runner host, its child killed after 4 s; with B, a second run whose child the same host starts is
    in flight when it is, `lag` seconds after the coordinator sent B. It measures, and judges only the invariants every
    trial is: see cell_F12."""
    rec = {"cell": f"F12 {variant}", "rep": rep, "pipeline": "campaign-pair-a", "expect": ["failed"], "at": 4, "campaign": CAMPAIGN,
           "time": time.strftime("%Y-%m-%dT%H:%M:%S")}
    t_fault, run_a, run_b, seen = time.time(), None, None, 0
    try:
        recover()
        seen = len(S.service_log("runner-1"))
        for pid, (yaml, placement) in PAIR.items():
            S.call("/api/deploy", {"pipelineId": pid, "yaml": yaml, "cuts": [], "placement": placement})
        run_a = submit("campaign-pair-a")
        time.sleep(4)
        if not child_of_run(run_a["runId"]):
            raise NotApplicable("the child of run A had already exited")
        if with_b:
            # B is in the queue as running before A is killed, so the host is starting its child when A's dies; the
            # millisecond overlap of the two is not in the harness's hands.
            run_b = S.call("/api/runs", {"pipelineId": "campaign-pair-b"})
            S.until(lambda: any(r["runId"] == run_b["runId"] and r["state"] == "running" for r in S.call("/api/runs/queue")), 60, "run B to start")
            time.sleep(lag)
            rec["lag_s"] = lag
        t_fault = time.time()
        S.call("/api/fragments/campaign-pair-a@runner-1/kill", {})
        rec["injected"] = "kill the child of A" + (f" {lag:.2f}s after B was sent" if with_b else " alone")
        final, t_end = await_final(run_a["runId"], BOUND["kill"] + 30)
        elapsed = t_end - t_fault
        rec["elapsed"] = round(elapsed, 2)
        final_b = None
        if run_b is not None:
            final_b, _ = await_final(run_b["runId"], 60)
        judge(rec, "campaign-pair-a", final, elapsed, {"failed"}, BOUND["kill"], cause_of="@runner-1")
        if run_b is not None:
            if final_b is None or final_b["state"] != "succeeded":
                rec["inv"]["I2"] = (rec["inv"].get("I2", "") + f" run B {final_b['state'] if final_b else 'gave no verdict'}").strip()
            elif final:
                a0, a1 = S.span(final)
                b0, b1 = S.span(final_b)
                rec["timeline"] = {"b_started_after_a_s": round((b0 - a0).total_seconds(), 2), "a_total_s": round((a1 - a0).total_seconds(), 2),
                                   "b_total_s": round((b1 - b0).total_seconds(), 2), "a_ended_after_b_started_s": round((a1 - b0).total_seconds(), 2)}
        rec["runner_log"] = [l[:170] for l in S.service_log("runner-1")[seen:].splitlines()
                             if any(r["runId"] in l for r in (run_a, run_b) if r)][:10]
        if rec["inv"]:
            rec["logs"] = capture_logs(STATE)
    except NotApplicable as e:
        rec["inv"], rec["state"], rec["note"], rec["elapsed"] = {}, "n/a", str(e), 0
    except Exception as e:
        rec["inv"] = {"H": f"harness: {str(e)[:200]}"}
        rec["elapsed"] = round(time.time() - t_fault, 2)
    finally:
        for r in (run_a, run_b):
            if r:
                try:
                    S.call("/api/runs/cancel", {"runId": r["runId"]})
                except Exception:
                    pass
        for pid in PAIR:
            undeploy(pid)
        problem = reference_ok(rec)
        if problem:
            rec.setdefault("inv", {})["I4"] = problem
    record(rec)
    return rec


def cell_F12():
    """Two runs on the runner host, the child of one of them killed. A host starts the children of all its runs from one process,
    and .NET holds a lock for the whole of `execve` that the handler of a child's exit also takes: a slow start can delay the
    end a host sees of another child. A is killed while B, a second run through the same runner, is being started; the kill
    of A alone is the reference. Both are judged on the bound F1 uses and on the invariants, and the verdict delay of the
    first is read against the second: a delay that is the same says the coupling was not seen in this trial, never that
    there is none. The kill of A is a shorter path than the start of B (the coordinator's queue, admission, the launch, the
    job file, then `execve`), so a kill sent as B shows `running` lands before B's child starts: it is sent LAGS seconds
    after, a sample of instants around that start, and which of them met the start of B is not known. The timeline records
    the instants the harness can read (the coordinator's, to the half second; the host's log, to the second)."""
    for rep in range(REPS):
        pair_trial("kill alone", rep, with_b=False)
        for lag in LAGS:
            pair_trial(f"kill {lag:.2f}s after another run starts", rep, with_b=True, lag=lag)


# A lost replica ends its runs when the coordinator loses it, DisconnectGracePeriod (45 s) after the cut. The bound stays
# under RemoteFailureGrace (60 s): a verdict past it says the loss was not heard.
REPLICA_LOST_BOUND = 55


def restore_replica(name):
    """The host `name` back as the lab starts it, and node-4 carried by two connected hosts again."""
    if S.host_exited(S.host_pid(name)):
        S.start_host(name)
    S.await_replicas(2, f"{name} to be back")


def next_run_on(survivor):
    """I4 after a lost replica: an unfaulted run of the reference pipeline succeeds on the replica that is left, and the
    journal says so. Returns the problem, or None."""
    try:
        deploy(REFERENCE)
        run = submit(REFERENCE, wait_running=False)
        final, _ = await_final(run["runId"], 120)
        if final is None:
            return "the run after the loss gave no verdict in 120s"
        ctx().verdict(final, check_for(REFERENCE))
        on = S.host_name(S.replica_pid(final["result"]["replicas"][f"{REFERENCE}@node-4"]))
        return None if on == survivor else f"the run after the loss ran on {on}, not on the replica that is left ({survivor})"
    except Exception as e:
        return f"the run after the loss: {str(e)[:170]}"


def replica_lost_trial(rep):
    """One run of sensor-stream with the host of the replica that executes its node-4 fragment killed 4 s in. Judged on the
    bound of a lost host and on the invariants, with the cause naming the fragment; then the next run is the reference
    pipeline, which must succeed on the replica that is left, and the killed host is started again."""
    rec = {"cell": "F13 replica lost in flight", "rep": rep, "pipeline": P2, "expect": ["failed"], "at": 4, "campaign": CAMPAIGN,
           "time": time.strftime("%Y-%m-%dT%H:%M:%S")}
    t_fault, killed, run = time.time(), None, None
    try:
        recover()
        try:
            S.await_replicas(2, "node-4 to have two connected replicas", timeout=10)
        except RuntimeError:
            raise RuntimeError("F13 needs the lab started with LAB_REPLICAS=node-4=2, and the variable exported here") from None
        check_for(P2)
        deploy(P2)
        run = submit(P2)
        time.sleep(4)
        holders = [r for r in S.node_replicas() if r["runs"]]
        if len(holders) != 1:
            raise NotApplicable(f"{len(holders)} replica(s) of node-4 hold the run at +4 s")
        killed = S.host_name(S.replica_pid(holders[0]))
        survivor = next(S.host_name(S.replica_pid(r)) for r in S.node_replicas() if r["id"] != holders[0]["id"])
        t_fault = time.time()
        os.kill(S.host_pid(killed), signal.SIGKILL)
        rec["injected"] = f"SIGKILL host {killed}"
        final, t_end = await_final(run["runId"], BOUND["host"] + 30)
        elapsed = t_end - t_fault
        rec["elapsed"] = round(elapsed, 2)
        judge(rec, P2, final, elapsed, {"failed"}, REPLICA_LOST_BOUND, cause_of="@node-4")
        if final is not None and final["state"] == "failed" and not (final.get("result") or {}).get("causeIsUnresponsive"):
            rec["inv"]["I2"] = "the cause is not marked unresponsive: the loss of the replica did not end the run"
        if (problem := next_run_on(survivor)):
            rec["inv"]["I4"] = problem
        if rec["inv"]:
            rec["logs"] = capture_logs(STATE)
    except NotApplicable as e:
        rec["inv"], rec["state"], rec["note"], rec["elapsed"] = {}, "n/a", str(e), 0
        try:
            S.call("/api/runs/cancel", {"runId": run["runId"]})
            await_final(run["runId"], 60)
        except Exception:
            pass
    except Exception as e:
        rec["inv"] = {"H": f"harness: {str(e)[:200]}"}
        rec["elapsed"] = round(time.time() - t_fault, 2)
    finally:
        try:
            if killed:
                restore_replica(killed)
        except Exception as e:
            rec.setdefault("inv", {})["H"] = f"harness: the killed host did not come back: {str(e)[:150]}"
        problem = reference_ok(rec)
        if problem:
            rec.setdefault("inv", {})["I4"] = (rec.get("inv", {}).get("I4", "") + " " + problem).strip()
    record(rec)
    return rec


def cell_F13():
    """A replica lost in flight. node-4 is carried by two hosts (LAB_REPLICAS=node-4=2); the one that executes the fragment of a
    run is killed. Its grace runs out, the coordinator loses the replica and the run fails naming the fragment, as unresponsive, within
    REPLICA_LOST_BOUND; no child of the killed host is left; the replicas hold no run; and the next run succeeds on the other
    replica, the journal saying which. The killed host is started again before the next trial."""
    for rep in range(REPS):
        replica_lost_trial(rep)


def cell_F7():
    def inject(run, plan):
        # P2's source is the runner (it generates the slow stream); node-1's customers read ends in under a second.
        S.call(f"/api/fragments/{name_of(plan, 'runner-1')}/kill", {})
        time.sleep(0.5)
        S.call(f"/api/fragments/{name_of(plan, 'node-4')}/kill", {})
    for rep in range(REPS):
        trial("F7 double fault", rep, "sensor-stream", 4, inject, {"failed"}, BOUND["kill"] + 1)


def cell_F8():
    for rep in range(REPS):
        rec = {"cell": "F8 rapid cycle x10", "rep": rep, "pipeline": REFERENCE, "expect": ["clean"], "at": 0, "campaign": CAMPAIGN,
               "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "inv": {}}
        t0 = time.time()
        refusals = 0
        try:
            recover()
            for _ in range(10):
                try:
                    deploy(REFERENCE)
                    run = submit(REFERENCE, wait_running=False)
                    time.sleep(0.1)
                    S.call("/api/runs/cancel", {"runId": run["runId"]})
                except RuntimeError as e:
                    if "HTTP 5" in str(e):
                        rec["inv"]["I2"] = f"server error during the cycle: {str(e)[:150]}"
                    refusals += 1
            rec["refusals"] = refusals
            S.until(lambda: not S.call("/api/runs/queue"), 120, "the queue to drain")
            rec["state"] = "drained"
        except Exception as e:
            rec["inv"]["I1"] = f"the cycle did not settle: {str(e)[:150]}"
        rec["elapsed"] = round(time.time() - t0, 2)
        time.sleep(10)
        if children():
            rec["inv"]["I3"] = "fragment child process(es) alive after the cycle"
        problem = reference_ok(rec)
        if problem:
            rec["inv"]["I4"] = problem
        record(rec)


def cell_F9():
    def inject(run, plan):
        S.call("/api/runs/cancel", {"runId": run["runId"]})
        time.sleep(0.5)
        try:
            S.call(f"/api/fragments/{name_of(plan, 'runner-1')}/kill", {})
        except RuntimeError:
            pass
    for rep in range(REPS):
        trial("F9 kill during teardown", rep, "sensor-stream", 3, inject, {"cancelled", "failed"}, BOUND["cancel"] + 4)


def cell_F10():
    for pid in (REFERENCE, "sensor-stream", P3):
        for rep in range(REPS):
            trial(f"F10 control {pid}", rep, pid, 0, lambda run, plan: "", {"succeeded"}, 150, wait_running=False)


# ------------------------------------------------------------------ the network (toxiproxy)

TOXI_API = "127.0.0.1:8474"
PORTS = {n: 5191 + i for i, n in enumerate(NODES)}


def toxi(*args):
    return subprocess.run([os.path.join(TOXI, "toxiproxy-cli"), "-h", TOXI_API, *args], capture_output=True, text=True)


def toxic_add(proxy, kind, name, *attrs):
    """A toxic in both directions of a proxy (flags come before the proxy name in this CLI)."""
    for direction, suffix in (("--downstream", ""), ("--upstream", "-u")):
        toxi("toxic", "add", "-t", kind, "-n", name + suffix, direction, *[x for a in attrs for x in ("-a", a)], proxy)


def toxic_remove(proxy, name):
    for suffix in ("", "-u"):
        toxi("toxic", "remove", "-n", name + suffix, proxy)


def start_hosts(coordinator_of, what):
    """Undo everything, stop the five hosts, then start each again in strict mode, pointed at `coordinator_of(node)`.
    A host started here fetches its first token from the IdP as it is now."""
    for d in S.call("/api/deployments"):
        undeploy(d["pipelineId"])
    for n in NODES:
        try:
            stop_pid(pid_of(n))
        except (OSError, ValueError):
            pass
    for n in NODES:
        log = open(os.path.join(STATE, "logs", n + ".log"), "a")
        env = {k: v for k, v in os.environ.items() if k != "LAB_NODE_SANDBOX"} | {"DTPIPE": DTPIPE}
        p = subprocess.Popen([os.path.join(LAB, "lab.sh"), "exec-node", n, coordinator_of(n)],
                             stdout=log, stderr=log, env=env, start_new_session=True)
        with open(os.path.join(STATE, "run", n + ".pid"), "w") as f:
            f.write(str(p.pid))
    await_hosts(what)


def hosts_through_proxies():
    start_hosts(lambda n: f"http://127.0.0.1:{PORTS[n]}", "the hosts through their proxies")


def bench_selftest():
    """A run of P2 with no fault, through the proxies the network cell is about to fault. If it does not succeed with the
    witness's numbers, the bench is not clean and nothing the cell records would mean anything."""
    recover()
    check_for("sensor-stream")
    deploy("sensor-stream")
    run = submit("sensor-stream", wait_running=False)
    final, _ = await_final(run["runId"], 150)
    with open(os.path.join(OUT, "selftest.jsonl"), "a") as f:
        f.write(json.dumps({"time": time.strftime("%Y-%m-%dT%H:%M:%S"), "state": final["state"] if final else "none"}) + "\n")
    if final is None or final["state"] != "succeeded":
        raise RuntimeError(f"the reference run through the proxies gave {final['state'] if final else 'no verdict'}")
    ctx().verdict(final, check_for("sensor-stream"))
    recover()


def cell_F5():
    server = subprocess.Popen([os.path.join(TOXI, "toxiproxy-server"), "-host", "127.0.0.1", "-port", "8474"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        time.sleep(1.5)
        for n in NODES:
            toxi("create", "-l", f"127.0.0.1:{PORTS[n]}", "-u", URL.replace("http://", ""), n)
        hosts_through_proxies()
        wrong = bench.exactly_the_hosts(STATE, NODES)
        if wrong:
            raise RuntimeError("the bench is not what the cell assumes: " + wrong)
        bench_selftest()

        def latency(run, plan):
            for n in NODES:
                toxic_add(n, "latency", "lat", "latency=200", "jitter=100")
            return "latency 200ms +-100 on every hub connection"

        def bandwidth(run, plan):
            for n in NODES:
                toxic_add(n, "bandwidth", "bw", "rate=1024")
            return "bandwidth 1 MB/s on every hub connection"

        def reset(run, plan):
            for n in NODES:
                toxic_add(n, "reset_peer", "rst", "timeout=0")
            time.sleep(1)
            for n in NODES:
                toxic_remove(n, "rst")
            return "reset_peer on every hub connection for 1s"

        def blackhole(run, plan):
            toxic_add("runner-1", "timeout", "hole", "timeout=0")
            time.sleep(20)
            toxic_remove("runner-1", "hole")
            return "black hole 20s on the runner's connection"

        def clean():
            for n in NODES:
                for t in ("lat", "bw", "rst", "hole"):
                    toxic_remove(n, t)

        for label, inj, expect, at, bound in (
            ("latency 200ms", latency, {"succeeded"}, 4, 150),
            ("bandwidth 1MB/s", bandwidth, {"succeeded"}, 4, 200),
            ("reset_peer 1s", reset, {"succeeded", "failed"}, 4, 100),
            ("black hole 20s runner", blackhole, {"succeeded", "failed"}, 4, 100),
        ):
            if os.environ.get("CAMPAIGN_ONLY") and os.environ["CAMPAIGN_ONLY"] not in label:
                continue
            for rep in range(REPS):
                try:
                    trial(f"F5 {label}", rep, "sensor-stream", at, inj, expect, bound)
                finally:
                    clean()
    finally:
        server.terminate()
        subprocess.run([os.path.join(LAB, "lab.sh"), "nodes", "strict"], capture_output=True, timeout=300)


# ------------------------------------------------------------------ a consumer that stops reading

F11_BOUND = 200  # the window fills in seconds + the sender's 60 s bound + the coordinator's 60 s grace + teardown 30 s, with margin

# Two fragments, a source far larger than the send window and the receive buffer can hold: the sender fails on its
# window while the sink, blocked on its FIFO, never reads. (A source that fits the buffers lets the sender finish, and
# a run held open by a sink alone is only a slow sink, which no clock may end.)
F11_YAML = """readings:
  input: generate:50000000
load:
  from: readings
  output: csv:{fifo}
"""


def release_fifo(path):
    """Lets a writer blocked on opening the FIFO go: a reader that opens without waiting is enough."""
    try:
        os.close(os.open(path, os.O_RDONLY | os.O_NONBLOCK))
    except OSError:
        pass


def cell_F11():
    """A consumer stops reading for good: the sink writes to a FIFO nobody reads, so its child blocks. The sender gives
    up on its full window, and the receiver is never told. The run must end `failed`, naming the sink's fragment, in the
    bound, with nothing left running. Needs sandbox nodes (a data node otherwise refuses a writer that is not a brick)."""
    lab = os.path.join(LAB, "lab.sh")
    placement = {"readings": "runner-1", "load": "node-4"}
    for rep in range(REPS):
        rec = {"cell": "F11 consumer stuck", "rep": rep, "pipeline": "campaign-f11", "expect": ["failed"], "at": 0,
               "campaign": CAMPAIGN, "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "inv": {}}
        fifo_dir = tempfile.mkdtemp(prefix="campaign-f11-")  # a FIFO cannot live on the shared volume
        fifo = os.path.join(fifo_dir, "sink.fifo")
        t0 = time.time()
        try:
            recover()
            subprocess.run([lab, "nodes", "sandbox"], capture_output=True, timeout=300)
            S.until(hosts_online, 120, "the hosts in sandbox mode")
            os.mkfifo(fifo)
            S.call("/api/deploy", {"pipelineId": "campaign-f11", "yaml": F11_YAML.format(fifo=fifo), "cuts": [], "placement": placement})
            run = submit("campaign-f11")
            t_run = time.time()
            final, t_end = await_final(run["runId"], F11_BOUND + 30)
            elapsed = t_end - t_run
            rec["elapsed"] = round(elapsed, 2)
            rec["injected"] = "the sink writes to a FIFO nobody reads"
            judge(rec, "campaign-f11", final, elapsed, {"failed"}, F11_BOUND, cause_of="@node-4")
            if rec["inv"]:
                rec["logs"] = capture_logs(STATE)
            if final is None and os.environ.get("CAMPAIGN_CANCEL_STUCK"):
                t_cancel = time.time()
                S.call("/api/runs/cancel", {"runId": run["runId"]})
                after_cancel, _ = await_final(run["runId"], 120)
                rec["cancel_after_stall"] = {"state": after_cancel["state"] if after_cancel else "none",
                                             "seconds": round(time.time() - t_cancel, 1)}
        except Exception as e:
            rec["inv"] = {"H": f"harness: {str(e)[:200]}"}
            rec["elapsed"] = round(time.time() - t0, 2)
        finally:
            release_fifo(fifo)
            shutil.rmtree(fifo_dir, ignore_errors=True)
            undeploy("campaign-f11")
            subprocess.run([lab, "nodes", "strict"], capture_output=True, timeout=300)
            try:
                S.until(hosts_online, 120, "the hosts back in strict mode")
            except Exception as e:
                rec.setdefault("inv", {})["H"] = f"harness: cleanup failed: {str(e)[:150]}"
        problem = reference_ok(rec)
        if problem:
            rec["inv"]["I4"] = problem
        record(rec)


# ------------------------------------------------------------------ security

class Violation(Exception):
    """A request a client that is not the coordinator should not have been able to make."""


def security_trial(cell, rep, check):
    """One security check. Judged on: refused (I2), nothing changed (I6: every host still online), and the
    lab still runs its reference pipeline (I4)."""
    rec = {"cell": cell, "rep": rep, "pipeline": REFERENCE, "expect": ["refused"], "at": 0, "campaign": CAMPAIGN,
           "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "inv": {}}
    t0 = time.time()
    try:
        rec["note"] = check()
        rec["state"] = "refused"
    except Violation as e:
        rec["inv"]["I2"] = str(e)[:300]
        rec["state"] = "accepted"
    except Exception as e:
        rec["inv"]["H"] = f"harness: {str(e)[:200]}"
    rec["elapsed"] = round(time.time() - t0, 2)
    if not hosts_online():
        rec["inv"]["I6"] = "a host is no longer online after the check"
    problem = reference_ok(rec)
    if problem:
        rec["inv"]["I4"] = problem
    record(rec)


def expect_status(what, actual, allowed=(401, 403)):
    if actual not in allowed:
        raise Violation(f"{what}: HTTP {actual}, expected one of {allowed}")


def check_S2():
    """The statuses are recorded: which of 401 and 403 the coordinator answers is what its default policy decides."""
    base = URL
    bogus = sec.bogus_jwt()
    seen = []
    for what, status in (
            ("command hub negotiate, no token", sec.negotiate_status(base, sec.COMMAND_HUB, None)),
            ("command hub negotiate, unsigned token", sec.negotiate_status(base, sec.COMMAND_HUB, bogus)),
            ("node channel negotiate, no token", sec.negotiate_status(base, sec.LAB_HUB, None)),
            ("node channel negotiate, unsigned token", sec.negotiate_status(base, sec.LAB_HUB, bogus)),
            ("data plane, no token", sec.stream_status(base, None)),
            ("data plane, unsigned token", sec.stream_status(base, bogus))):
        expect_status(what, status)
        seen.append(status)
    issued = []
    for label, client, secret in (("wrong secret", "node-1", "not-the-secret"), ("unknown client", "node-9", "x")):
        status, token = sec.token_for(IDP_URL, client, secret)
        if token is not None or status not in (400, 401):
            raise Violation(f"token endpoint, {label}: HTTP {status}, token issued={token is not None}")
        issued.append(status)
    return f"six entry points refused (HTTP {seen}) and two credential failures (HTTP {issued})"


def check_S4():
    node = json.load(open(os.path.join(LAB, "nodes", "node-1.json")))
    status, token = sec.token_for(IDP_URL, node["clientId"], node["secret"])
    if not token:
        raise RuntimeError(f"no token for node-1 (HTTP {status})")
    hub = sec.LongPollingHub(URL, sec.COMMAND_HUB, token).open()
    try:
        answers = {
            "GetReceivers": hub.invoke("GetReceivers"),
            "InitTransfer": hub.invoke("InitTransfer", "00000000-0000-0000-0000-000000000001", 8, 1000),
        }
    finally:
        hub.close()
    for method, answer in answers.items():
        if "error" not in answer:
            raise Violation(f"{method} from a peer was accepted: {json.dumps(answer)[:200]}")
        if "reserved to the coordinator" not in answer["error"]:
            raise Violation(f"{method} from a peer failed for another reason: {answer['error'][:200]}")
    return "GetReceivers and InitTransfer from a peer refused by the coordinator's filter"


def check_S1():
    out = subprocess.run([sys.executable, os.path.join(LAB, "smoke.py"), URL, DTPIPE, STATE, "rights"], capture_output=True, text=True, timeout=300)
    line = next((l for l in out.stdout.splitlines() if "PASS  rights   enrollment" in l or "FAIL  rights   enrollment" in l), "")
    if not line.startswith("PASS"):
        raise Violation(f"enrollment of an unknown host: {line or out.stdout[-200:]}")
    return line[:160]


def check_smoke_rights(label, what):
    """One of the rights pass's checks, run alone: the coordinator under a stranger's report, a stolen client."""
    out = subprocess.run([sys.executable, os.path.join(LAB, "smoke.py"), URL, DTPIPE, STATE, "rights"], capture_output=True, text=True, timeout=600)
    line = next((l for l in out.stdout.splitlines() if f"rights   {label}" in l and (l.startswith("PASS") or l.startswith("FAIL"))), "")
    if not line.startswith("PASS"):
        raise Violation(f"{what}: {line or out.stdout[-200:]}")
    return line[:200]


def check_S6():
    return check_smoke_rights("a foreign report", "a report for a run the sender was not placed on")


def check_S7():
    return check_smoke_rights("a stolen data plane client", "a data plane client announced under another subject")


def cell_S():
    for rep in range(REPS):
        security_trial("S1 unknown host announcement", rep, check_S1)
    for rep in range(REPS):
        security_trial("S2 unauthenticated entry points", rep, check_S2)
    for rep in range(REPS):
        security_trial("S4 peer-initiated transfer", rep, check_S4)
    for rep in range(REPS):
        security_trial("S6 report from a replica not placed on the run", rep, check_S6)
    for rep in range(REPS):
        security_trial("S7 data plane client of another identity", rep, check_S7)


SHORT_TOKEN = 20


def short_token_trials(label, before=None):
    """The IdP issues 20-second tokens, the hosts restart to fetch them, and a run of P2 follows. It either
    finishes (tokens renewed) or fails saying so; it never hangs, and the lab is usable afterwards. The hosts are
    stopped and started here, never through `lab.sh nodes`, which leaves hosts already online in their mode as they
    are, with the tokens they hold. The IdP must answer node-1's credentials with a 20-second token (19 once rounded
    down), or the trial is the harness's (H)."""
    for rep in range(REPS):
        try:
            stop_start_idp(f"--Idp:AccessTokenLifetimeSeconds={SHORT_TOKEN}")
            lifetime = idp_token_lifetime()
            # expires_in counts the whole seconds left when the answer is written: one under the lifetime is the same token.
            if not SHORT_TOKEN - 1 <= lifetime <= SHORT_TOKEN:
                raise RuntimeError(f"the IdP issues {lifetime}-second tokens, not {SHORT_TOKEN}")
            start_hosts(lambda n: URL, "the hosts with short-lived tokens")
            trial(label, rep, "sensor-stream", 0, lambda run, plan: "", {"succeeded", "failed"}, 150,
                  wait_running=False, before=before, after=restart_lab,
                  note=f"IdP tokens of {lifetime}s, the five hosts restarted after the IdP")
        except Exception as e:
            record({"cell": label, "rep": rep, "pipeline": "sensor-stream", "expect": [], "at": 0,
                    "campaign": CAMPAIGN, "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "elapsed": 0, "state": "?",
                    "inv": {"H": f"harness: {str(e)[:200]}"}})
            restart_lab()


def cell_S3():
    """Tokens that expire during the run (the run lasts 30 s, the tokens 20)."""
    short_token_trials("S3 token expires mid-run")


def cell_S5():
    """A pipeline deployed, then left idle past its nodes' token lifetime, then run."""
    short_token_trials("S5 deployed node idle past its token", before=lambda: time.sleep(25))


CELL_FUNCS = {"F1": cell_F1, "F2": cell_F2, "F3": cell_F3, "F4": cell_F4, "F5": cell_F5, "F6": cell_F6, "F7": cell_F7,
              "F8": cell_F8, "F9": cell_F9, "F10": cell_F10, "F11": cell_F11, "S": cell_S, "S3": cell_S3, "S5": cell_S5}
NAMED_ONLY = {"F6g": cell_F6g, "F12": cell_F12, "F13": cell_F13}


def preflight():
    """What keeps this machine from judging the lab as it is: a list of (check, finding) pairs, empty when all hold."""
    repo = os.path.normpath(os.path.join(LAB, "..", ".."))
    found = [("stray", f"{kind} pid {pid} (parent {ppid}) {args[:140]}") for pid, ppid, kind, args in bench.foreign(STATE, NODES)]
    found += [("busy", f"pid {pid}: {why}: {args[:140]}") for pid, why, args in bench.busy_machine(STATE, NODES, os.path.dirname(TOXI))]
    found += [("version", p) for p in bench.frozen_problems(repo, DTPIPE, STATE, NODES, os.environ.get("CAMPAIGN_FROZEN"))]
    found += [("deployments", p) for p in bench.armed_deployments(STATE)]
    return found


REMEDY = {"stray": "stop them (./lab.sh down, then ./lab.sh up)",
          "busy": "let the work finish, or stop it (`dotnet build-server shutdown` ends the resident compilers)",
          "version": "rebuild at the frozen commit (./build.sh, then ./lab.sh down and up) and start again",
          "deployments": "undeploy them, or give their nodes an identity again (DELETE /api/deployments/<id>)"}


def main():
    os.makedirs(OUT, exist_ok=True)
    mode = "skip" if os.environ.get("CAMPAIGN_SKIP_PREFLIGHT") else os.environ.get("CAMPAIGN_PREFLIGHT", "refuse")
    if mode != "skip":
        found = preflight()
        for check, what in found:
            print(f"preflight {check}: {what}", file=sys.stderr)
        if found and mode != "report":
            print("The bench is not ready: " + "; ".join(sorted({REMEDY[c] for c, _ in found})) + ".", file=sys.stderr)
            return 2
        print(f"preflight {'reported' if found else 'clean'}: {len(found)} finding(s), load {os.getloadavg()[0]:.2f}", flush=True)
    stop_logs = follow_logs(STATE, os.path.join(OUT, "logs"))
    try:
        wanted = CELLS or list(CELL_FUNCS)
        for name in wanted:
            print(f"== {name}", flush=True)
            (CELL_FUNCS.get(name) or NAMED_ONLY[name])()
    finally:
        stop_logs.set()
        print(f"ports: {sum(RETRIED.values())} call(s) retried {RETRIED}, {port_lines()} log line(s) in all", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
