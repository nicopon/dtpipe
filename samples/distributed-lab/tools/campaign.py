#!/usr/bin/env python3
"""The robustness campaign: a fixed matrix of faults played against the running lab, each trial judged
on the same invariants. The matrix, the invariants and their bounds are the ones the campaign sheet fixed
before the first trial; this script only plays and records them.

  campaign.py <lab-url> <dtpipe> <state-dir> <campaign-id> [cell ...]

Cells are F1 ... F11, then S, S3 and S5 for the security volet (a prefix selects: F1 alone is F1 and F10 only as
F10). Every trial is one line of JSON in <state-dir>/campaign/<campaign-id>/trials.jsonl, and every line the lab
logs is kept, stamped, in <state-dir>/campaign/<campaign-id>/logs/. The lab must be up on strict nodes
(./lab.sh up), and nothing else of the lab may be running: the campaign refuses to start otherwise
(CAMPAIGN_SKIP_PREFLIGHT=1 overrides, for a harness check). Standard library only.
"""
import json, os, shutil, signal, subprocess, sys, tempfile, time, urllib.error

_ARGS = list(sys.argv)
sys.argv = sys.argv[:5]  # smoke.py reads argv[1:4]; the campaign id at argv[4] is not a pass name
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import smoke as S  # noqa: E402  (reads sys.argv itself)
from campaign_queue import queue_is_stuck  # noqa: E402
from campaign_logs import capture as capture_logs, follow as follow_logs  # noqa: E402
import campaign_bench as bench  # noqa: E402
import campaign_security as sec  # noqa: E402

URL, DTPIPE, STATE, CAMPAIGN = _ARGS[1].rstrip("/"), _ARGS[2], _ARGS[3], _ARGS[4]
CELLS = _ARGS[5:]
LAB = os.path.dirname(HERE)
OUT = os.path.join(STATE, "campaign", CAMPAIGN)
NODES = ["node-1", "node-2", "node-3", "node-4", "runner-1"]
TOXI = os.environ.get("TOXIPROXY_DIR", os.path.normpath(os.path.join(LAB, "..", "..", "..", "transportr", "tools")))

# A retry storm on the loopback can exhaust the ephemeral ports for a few seconds, the harness's own API calls included
# (EADDRINUSE / EADDRNOTAVAIL): those are retried, nothing else is (a coordinator that is down must still refuse at once).
_raw_call = S.call


def _call(path, body=None, method=None):
    for attempt in range(40):
        try:
            return _raw_call(path, body, method)
        except urllib.error.URLError as e:
            if isinstance(getattr(e, "reason", None), OSError) and e.reason.errno in (48, 49) and attempt < 39:
                time.sleep(1.0)
                continue
            raise


S.call = _call


def wait_ports_free(timeout=90):
    """Waits until the sockets a storm left in TIME_WAIT have drained, so the next check does not meet an exhausted range."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = subprocess.run(["netstat", "-an", "-p", "tcp"], capture_output=True, text=True).stdout
        if out.count("TIME_WAIT") < 2000:
            return
        time.sleep(2)


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
    plan = S.call(f"/api/library/{pid}/distribute", {})["plan"]
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


def fragment_states():
    return {f["fragment"]: f["state"] for n in S.call("/api/nodes") for f in n["fragments"]}


def hosts_online():
    """All five hosts announced and online; False while the coordinator is down or has not heard from them."""
    try:
        nodes = S.call("/api/nodes")
    except Exception:
        return False
    return len(nodes) == len(NODES) and all(n["online"] for n in nodes)


def restart_lab():
    lab = os.path.join(LAB, "lab.sh")
    subprocess.run([lab, "down"], capture_output=True, timeout=120)
    time.sleep(2)
    subprocess.run([lab, "up"], capture_output=True, timeout=300)
    S.until(hosts_online, 120, "the lab after a restart")


def recover():
    """Bring the lab back to nine online hosts and no deployment, the way an operator would."""
    wait_ports_free()
    if queue_is_stuck(S.call):
        restart_lab()
        return "lab restarted (a run stayed in the queue)"
    if not hosts_online():
        subprocess.run([os.path.join(LAB, "lab.sh"), "nodes", "strict"], capture_output=True, timeout=300)
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
    states = fragment_states()
    stuck = [f for f, s in states.items() if s in ("Launched", "Running")]
    if stuck:
        inv["I6"] = f"fragments still {sorted(set(states[f] for f in stuck))}: {stuck[:3]}"
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

def record(rec):
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "trials.jsonl"), "a") as f:
        f.write(json.dumps(rec) + "\n")
    bad = ",".join(sorted(rec["inv"])) or "-"
    print(f"{'FAIL' if rec['inv'] else 'ok  '}  {rec['cell']:<26} #{rec['rep']}  {rec.get('state', '?'):<9} "
          f"{rec.get('elapsed', 0):>5.1f}s  inv:{bad}  {json.dumps(rec['inv']) if rec['inv'] else ''}", flush=True)


def trial(cell, rep, pid, at, inject, expect, bound, cause_of=None, wait_running=True, note=None, before=None, after=None):
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
    plan = S.call(f"/api/library/{pid}/distribute", {})["plan"]
    return [(f["node"], f["name"]) for f in plan["fragments"]]


def child_alive(node):
    """Whether a fragment child of `node` is running: the kill endpoint answers 202 whatever it hit."""
    out = subprocess.run(["ps", "-axo", "pid,command"], capture_output=True, text=True).stdout
    return any(f"/{node}/fragments/" in l and "release/dtpipe --job" in l for l in out.splitlines())


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
    p = subprocess.Popen(["dotnet", os.path.join(STATE, "bin", "coordinator", "Lab.Coordinator.dll"), "--urls", URL,
                          "--lab-root", LAB, "--state", STATE, "--dtpipe", DTPIPE, *extra], stdout=log, stderr=log, start_new_session=True)
    with open(os.path.join(STATE, "run", "coordinator.pid"), "w") as f:
        f.write(str(p.pid))


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


def hosts_through_proxies():
    """Undo everything, then start the five hosts pointed at their own proxy in front of the coordinator."""
    for d in S.call("/api/deployments"):
        undeploy(d["pipelineId"])
    for n in NODES:
        try:
            os.kill(pid_of(n), signal.SIGTERM)
        except Exception:
            pass
    time.sleep(3)
    for n in NODES:
        log = open(os.path.join(STATE, "logs", n + ".log"), "a")
        env = {k: v for k, v in os.environ.items() if k != "LAB_NODE_SANDBOX"}
        p = subprocess.Popen(["dotnet", os.path.join(STATE, "bin", "node", "Lab.NodeHost.dll"), "--config", os.path.join(LAB, "nodes", n + ".json"),
                              "--state", STATE, "--coordinator", f"http://127.0.0.1:{PORTS[n]}", "--dtpipe", DTPIPE],
                             stdout=log, stderr=log, env=env, start_new_session=True)
        with open(os.path.join(STATE, "run", n + ".pid"), "w") as f:
            f.write(str(p.pid))
    S.until(hosts_online, 120, "the hosts through their proxies")


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
    base = URL
    bogus = sec.bogus_jwt()
    expect_status("command hub negotiate, no token", sec.negotiate_status(base, sec.COMMAND_HUB, None))
    expect_status("command hub negotiate, unsigned token", sec.negotiate_status(base, sec.COMMAND_HUB, bogus))
    expect_status("lab hub negotiate, no token", sec.negotiate_status(base, sec.LAB_HUB, None))
    expect_status("lab hub negotiate, unsigned token", sec.negotiate_status(base, sec.LAB_HUB, bogus))
    expect_status("data plane, no token", sec.stream_status(base, None))
    expect_status("data plane, unsigned token", sec.stream_status(base, bogus))
    for label, client, secret in (("wrong secret", "node-1", "not-the-secret"), ("unknown client", "node-9", "x")):
        status, token = sec.token_for(base, client, secret)
        if token is not None or status not in (400, 401):
            raise Violation(f"token endpoint, {label}: HTTP {status}, token issued={token is not None}")
    return "six entry points and two credential failures refused"


def check_S4():
    node = json.load(open(os.path.join(LAB, "nodes", "node-1.json")))
    status, token = sec.token_for(URL, node["clientId"], node["secret"])
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
    line = next((l for l in out.stdout.splitlines() if "announcement, impostor" in l), "")
    if not line.startswith("PASS"):
        raise Violation(f"impostor announcement: {line or out.stdout[-200:]}")
    return line[:160]


def cell_S():
    for rep in range(REPS):
        security_trial("S1 impostor announcement", rep, check_S1)
    for rep in range(REPS):
        security_trial("S2 unauthenticated entry points", rep, check_S2)
    for rep in range(REPS):
        security_trial("S4 peer-initiated transfer", rep, check_S4)


def short_token_trials(label, before=None):
    """The coordinator issues 20-second tokens, the hosts restart to fetch them, and a run of P2 follows. It
    either finishes (tokens renewed) or fails saying so; it never hangs, and the lab is usable afterwards."""
    for rep in range(REPS):
        try:
            stop_start_coordinator("--token-lifetime-seconds", "20")
            time.sleep(3)
            subprocess.run([os.path.join(LAB, "lab.sh"), "nodes", "strict"], capture_output=True, timeout=300)
            S.until(hosts_online, 120, "the hosts with short-lived tokens")
            trial(label, rep, "sensor-stream", 0, lambda run, plan: "", {"succeeded", "failed"}, 150,
                  wait_running=False, before=before, after=restart_lab)
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


def main():
    os.makedirs(OUT, exist_ok=True)
    if not os.environ.get("CAMPAIGN_SKIP_PREFLIGHT"):
        strays = bench.foreign(STATE, NODES)
        if strays:
            print("The bench is not clean: these lab processes are not the ones the pid files name.", file=sys.stderr)
            for pid, ppid, kind, args in strays:
                print(f"  {kind} pid {pid} (parent {ppid}) {args[:140]}", file=sys.stderr)
            print("Stop them (or ./lab.sh down, then ./lab.sh up) and start again.", file=sys.stderr)
            return 2
    stop_logs = follow_logs(STATE, os.path.join(OUT, "logs"))
    try:
        wanted = CELLS or list(CELL_FUNCS)
        for name in wanted:
            print(f"== {name}", flush=True)
            CELL_FUNCS[name]()
    finally:
        stop_logs.set()
    return 0


if __name__ == "__main__":
    sys.exit(main())
