#!/usr/bin/env python3
"""The robustness campaign: a fixed matrix of faults played against the running lab, each trial judged
on the same invariants. The matrix, the invariants and their bounds are the ones the campaign sheet fixed
before the first trial; this script only plays and records them.

  campaign.py <lab-url> <dtpipe> <state-dir> <campaign-id> [cell ...]

Cells are F1 ... F10 (a prefix selects: F1 alone is F1 and F10 only as F10). Every trial is one line of
JSON in <state-dir>/campaign/<campaign-id>/trials.jsonl. The lab must be up on strict nodes
(./lab.sh up). Standard library only.
"""
import json, os, shutil, signal, subprocess, sys, time

_ARGS = list(sys.argv)
sys.argv = sys.argv[:5]  # smoke.py reads argv[1:4]; the campaign id at argv[4] is not a pass name
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import smoke as S  # noqa: E402  (reads sys.argv itself)

URL, DTPIPE, STATE, CAMPAIGN = _ARGS[1].rstrip("/"), _ARGS[2], _ARGS[3], _ARGS[4]
CELLS = _ARGS[5:]
LAB = os.path.dirname(HERE)
OUT = os.path.join(STATE, "campaign", CAMPAIGN)
NODES = ["node-1", "node-2", "node-3", "node-4", "runner-1"]
TOXI = os.environ.get("TOXIPROXY_DIR", os.path.normpath(os.path.join(LAB, "..", "..", "..", "transportr", "tools")))

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
    """Fragment children of any host: `dtpipe --job <fragment>` processes."""
    out = subprocess.run(["ps", "-axo", "pid,command"], capture_output=True, text=True).stdout
    return [int(l.split(None, 1)[0]) for l in out.splitlines() if "release/dtpipe --job" in l and "campaign" not in l]


def fragment_states():
    return {f["fragment"]: f["state"] for n in S.call("/api/nodes") for f in n["fragments"]}


def hosts_online():
    """All five hosts announced and online; False while the coordinator is down or has not heard from them."""
    try:
        nodes = S.call("/api/nodes")
    except Exception:
        return False
    return len(nodes) == len(NODES) and all(n["online"] for n in nodes)


def recover():
    """Bring the lab back to nine online hosts and no deployment, the way an operator would."""
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
        plan = deploy(pid)
        if before:
            before()
        check_for(pid)
        run = submit(pid, wait_running=wait_running)
        time.sleep(at)
        t_fault = time.time()
        rec["injected"] = inject(run, plan) or ""
        final, t_end = await_final(run["runId"], bound + 30)
        elapsed = t_end - t_fault
        rec["elapsed"] = round(elapsed, 2)
        judge(rec, pid, final, elapsed, expect, bound, cause_of)
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


def stop_start_coordinator():
    os.kill(pid_of("coordinator"), signal.SIGKILL)
    time.sleep(2)
    log = open(os.path.join(STATE, "logs", "coordinator.log"), "a")
    p = subprocess.Popen(["dotnet", os.path.join(STATE, "bin", "coordinator", "Lab.Coordinator.dll"), "--urls", URL,
                          "--lab-root", LAB, "--state", STATE, "--dtpipe", DTPIPE], stdout=log, stderr=log, start_new_session=True)
    with open(os.path.join(STATE, "run", "coordinator.pid"), "w") as f:
        f.write(str(p.pid))


def cell_F4():
    for rep in range(REPS):
        rec = {"cell": "F4 coordinator restart", "rep": rep, "pipeline": "sensor-stream", "expect": ["lost"], "at": 4,
               "campaign": CAMPAIGN, "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "inv": {}}
        t0 = time.time()
        try:
            recover(); deploy("sensor-stream"); check_for("sensor-stream")
            submit("sensor-stream"); time.sleep(4)
            t_fault = time.time()
            stop_start_coordinator()
            S.until(hosts_online, 90, "the hosts back online")
            rec["elapsed"] = round(time.time() - t_fault, 2)
            rec["state"] = "restarted"
            time.sleep(10)
            left = children()
            if left:
                rec["inv"]["I3"] = f"{len(left)} fragment child process(es) alive after the coordinator restarted"
        except Exception as e:
            rec["inv"]["I1"] = f"the lab did not come back: {str(e)[:150]}"
            rec["elapsed"] = round(time.time() - t0, 2)
            try:
                rec["evidence"] = {n: [l.strip()[:200] for l in open(os.path.join(STATE, "logs", n + ".log")).readlines()[-4:]] for n in ("node-1", "runner-1")}
                rec["evidence"]["nodes_online"] = [(n["name"], n["online"]) for n in S.call("/api/nodes")]
            except Exception:
                pass
        problem = reference_ok(rec)
        if problem:
            rec["inv"]["I4"] = problem
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


def cell_F5():
    server = subprocess.Popen([os.path.join(TOXI, "toxiproxy-server"), "-host", "127.0.0.1", "-port", "8474"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        time.sleep(1.5)
        for n in NODES:
            toxi("create", "-l", f"127.0.0.1:{PORTS[n]}", "-u", URL.replace("http://", ""), n)
        hosts_through_proxies()

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
            for rep in range(REPS):
                try:
                    trial(f"F5 {label}", rep, "sensor-stream", at, inj, expect, bound)
                finally:
                    clean()
    finally:
        server.terminate()
        subprocess.run([os.path.join(LAB, "lab.sh"), "nodes", "strict"], capture_output=True, timeout=300)


CELL_FUNCS = {"F1": cell_F1, "F2": cell_F2, "F3": cell_F3, "F4": cell_F4, "F5": cell_F5, "F6": cell_F6, "F7": cell_F7,
              "F8": cell_F8, "F9": cell_F9, "F10": cell_F10}


def main():
    os.makedirs(OUT, exist_ok=True)
    wanted = CELLS or list(CELL_FUNCS)
    for name in wanted:
        print(f"== {name}", flush=True)
        CELL_FUNCS[name]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
