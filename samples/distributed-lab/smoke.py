#!/usr/bin/env python3
"""End-to-end check of the lab: every pipeline, distributed, against its monolithic witness.

Six passes. The Lab view's: each pipelines/*.yaml with its catalog layout, deployed and run one
after the other. The library's: each library pipeline distributed automatically and its plan
saved, then all of them deployed at once and their runs queued together, as the page does.
The rights': the embedded IDP, the flow matrix refusing a plan and then, edited under a deployed
pipeline, a transfer at run time, and a brick policy refusing a plan; every edit is undone.
The faults': a run cancelled while a fully wired fragment's child has nothing to write, whose
verdict must come within the grace period.
The bricks': on strict nodes, a fragment whose branch is not one of the node's bricks, or is one only
by a hair, is refused by the node at once and leaves nothing deployed; the same wiring built from the
bricks as declared deploys, relays included.
The sandbox': on sandbox nodes any reader or writer is accepted, but a join or a processor on a data
node is refused all the same: a data node only reads and writes.
The lab and sandbox passes need sandbox nodes and every other pass strict ones: `lab.sh smoke`
switches the node hosts between them.
For every pipeline of the first two:
  1. the witness: the unsplit job run by dtpipe alone, writing a separate warehouse file;
  2. the lab: the distributed run, through the coordinator;
  3. the verdict: outcome Succeeded, every edge's two row counts equal, and the job's
     `# lab-check:` query returning the same row on both warehouses.

Usage: smoke.py <lab-url> <dtpipe> <state-dir> [lab|library|rights|faults|bricks|sandbox] [pipeline-id-substring ...]
A leading pass name runs that pass alone.
Exits 0 when every selected pipeline passes, 1 otherwise. Standard library only.
"""
import base64, csv, io, json, os, subprocess, sys, time, urllib.error, urllib.parse, urllib.request

URL, DTPIPE, STATE = sys.argv[1].rstrip("/"), sys.argv[2], sys.argv[3]
PASSES = {"lab", "library", "rights", "faults", "bricks", "sandbox"}
ONLY = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] in PASSES else None
SELECTED = sys.argv[5:] if ONLY else sys.argv[4:]
RUN_TIMEOUT = 180


def call(path, body=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(URL + path, data, {"Content-Type": "application/json"},
                                 method=method or ("POST" if data is not None else "GET"))
    try:
        raw = urllib.request.urlopen(req, timeout=RUN_TIMEOUT).read()
        return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            message = json.loads(body).get("error", body)
        except ValueError:
            message = body
        raise RuntimeError(f"{path}: HTTP {e.code}: {message}") from None


def until(predicate, timeout, what):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.3)
    raise RuntimeError(f"timed out waiting for {what}")


def query(connection, sql):
    out = subprocess.run([DTPIPE, "--input", connection, "--query", sql, "--output", "csv:-", "--no-stats"],
                         capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"query on {connection} failed: {out.stderr.strip().splitlines()[-1:]}")
    return list(csv.reader(io.StringIO(out.stdout)))


def check_of(yaml):
    for line in yaml.splitlines():
        if not line.startswith("#"):
            break
        text = line.lstrip("#").strip()
        if text.startswith("lab-check:"):
            return text[len("lab-check:"):].strip()
    return None


class Context:
    def __init__(self):
        nodes = call("/api/nodes")
        self.variables = {d["variable"]: d for n in nodes for d in n["datasets"]}
        self.warehouse = next(d for v, d in self.variables.items() if "WAREHOUSE" in v)
        self.witness_db = os.path.join(STATE, "witness", "warehouse.duckdb")
        os.makedirs(os.path.dirname(self.witness_db), exist_ok=True)

    def witness(self, name, yaml):
        """The same job, unsplit, on this machine; only the warehouse differs. Returns its check query."""
        check = check_of(yaml)
        if not check:
            raise RuntimeError("no `# lab-check:` query in the job header")
        env = {**os.environ, **{v: d["path"] for v, d in self.variables.items()}}
        env[self.warehouse["variable"]] = self.witness_db
        job = os.path.join(STATE, "witness", name + ".yaml")
        with open(job, "w") as f:
            f.write(yaml)
        out = subprocess.run([DTPIPE, "--job", job, "--no-stats"], capture_output=True, text=True, env=env)
        if out.returncode != 0:
            raise RuntimeError(f"witness failed: {out.stderr.strip().splitlines()[-3:]}")
        return check

    def verdict(self, final, check):
        if final["state"] != "succeeded":
            raise RuntimeError(f"{final['runId']} {final['state']}: {final.get('refusal') or final.get('description')}")
        bad = [e for e in final["result"]["edgeCounts"] if e["sent"] is None or e["sent"] != e["received"]]
        if bad:
            raise RuntimeError(f"edge counts disagree: {bad}")
        expected = query(f"duck:{self.witness_db}", check)
        actual = query(f"{self.warehouse['engine']}:{self.warehouse['path']}", check)
        if expected != actual:
            raise RuntimeError(f"check differs\n    witness: {expected}\n    lab:     {actual}")
        rows = sum(e["received"] for e in final["result"]["edgeCounts"])
        return rows, actual[1]


def selected(name):
    return not SELECTED or any(s in name for s in SELECTED)


def catalog_pass(ctx):
    failures = 0
    for entry in call("/api/pipelines"):
        if not selected(entry["id"]):
            continue
        started = time.time()
        try:
            check = ctx.witness(entry["id"], entry["yaml"])
            body = {"pipelineId": entry["id"], "yaml": entry["yaml"], "cuts": entry["cuts"], "placement": entry["placement"]}
            plan = call("/api/deploy", body)
            run = call("/api/runs", {"pipelineId": plan["pipelineId"]})
            final = until(lambda: _finished(run["runId"]), RUN_TIMEOUT, f"{run['runId']} to finish")
            rows, row = ctx.verdict(final, check)
            print(f"PASS  lab      {entry['id']:28} {len(plan['fragments'])} fragments, {len(plan['edges'])} edges, "
                  f"{rows:,} rows crossed  {row}  ({time.time() - started:.1f}s)")
        except Exception as e:
            failures += 1
            print(f"FAIL  lab      {entry['id']:28} {e}")
    return failures


def library_pass(ctx):
    failures = 0
    queued = []
    for entry in call("/api/library"):
        pid = entry["id"]
        if not selected(pid):
            continue
        try:
            doc = call(f"/api/library/{pid}")
            check = ctx.witness("library-" + pid, doc["yaml"])
            plan = call(f"/api/library/{pid}/distribute", {"save": True, "message": "smoke: distribute"})["plan"]
            call(f"/api/library/{pid}/deploy", {})
            queued.append((pid, plan, check))
        except Exception as e:
            failures += 1
            print(f"FAIL  library  {pid:28} {e}")

    # Every run queued at once: the lab starts them one at a time.
    runs = [(pid, plan, check, call("/api/runs", {"pipelineId": pid})) for pid, plan, check in queued]
    # The warehouse is one DuckDB file, locked by whichever run writes it: judge once all are over.
    finals = {}
    for pid, _, _, run in runs:
        try:
            finals[pid] = until(lambda: _finished(run["runId"]), RUN_TIMEOUT * len(runs), f"{run['runId']} to finish")
        except Exception as e:
            finals[pid] = e
    for pid, plan, check, run in runs:
        try:
            if isinstance(finals[pid], Exception):
                raise finals[pid]
            rows, row = ctx.verdict(finals[pid], check)
            placed = ", ".join(sorted({f["node"] for f in plan["fragments"]}))
            print(f"PASS  library  {pid:28} {len(plan['fragments'])} fragments on {placed}, {len(plan['edges'])} edges, "
                  f"{rows:,} rows crossed  {row}")
        except Exception as e:
            failures += 1
            print(f"FAIL  library  {pid:28} {e}")
    return failures


def token(client_id, secret):
    form = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": client_id, "client_secret": secret}).encode()
    try:
        answer = json.loads(urllib.request.urlopen(urllib.request.Request(URL + "/connect/token", form), timeout=20).read())
    except urllib.error.HTTPError as e:
        return None, e.code
    payload = answer["access_token"].split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))), 200


def rights_pass():
    failures = 0
    rights = call("/api/rights")
    matrix = rights["matrix"]
    revoked = {g: [t for t in targets if not (g == "crm" and t == "runner")] for g, targets in matrix.items()}

    def check(name, test):
        nonlocal failures
        started = time.time()
        try:
            detail = test()
            print(f"PASS  rights   {name:28} {detail}  ({time.time() - started:.1f}s)")
        except Exception as e:
            failures += 1
            print(f"FAIL  rights   {name:28} {e}")

    def idp():
        _, refused = token("node-1", "not-the-secret")
        claims, _ = token("node-1", "node-1-lab-secret")
        if refused == 200 or not claims or claims.get("sub") != "node-1" or claims.get("scope") != "transportr:group:crm":
            raise RuntimeError(f"wrong secret -> {refused}, claims {claims}")
        return f"wrong secret refused ({refused}); token for node-1 carries {claims['scope']}"

    def matrix_plan():
        try:
            call("/api/rights/matrix", {"matrix": revoked}, "PUT")
            plan = call("/api/library/customers-anonymized/distribute", {})["plan"]
            if plan["deployable"] or "crm -> runner" not in (plan.get("flowRejection") or ""):
                raise RuntimeError(f"plan not refused: {plan.get('flowRejection')}")
            return "crm -> runner revoked: plan refused"
        finally:
            call("/api/rights/matrix", {"matrix": matrix}, "PUT")

    def matrix_runtime():
        # Deployed under the full matrix, then run with crm -> runner revoked: the hub refuses the transfer.
        call("/api/library/revenue-by-country/distribute", {"save": True, "message": "smoke: distribute"})
        call("/api/library/revenue-by-country/deploy", {})
        try:
            call("/api/rights/matrix", {"matrix": revoked}, "PUT")
            refused = call("/api/runs", {"pipelineId": "revenue-by-country"})
            refused = until(lambda: _finished(refused["runId"]), RUN_TIMEOUT, "the refused run")
        finally:
            call("/api/rights/matrix", {"matrix": matrix}, "PUT")
        ok = call("/api/runs", {"pipelineId": "revenue-by-country"})
        ok = until(lambda: _finished(ok["runId"]), RUN_TIMEOUT, "the run after restoring")
        if refused["state"] == "succeeded" or ok["state"] != "succeeded":
            raise RuntimeError(f"revoked -> {refused['state']}, restored -> {ok['state']}")
        return f"revoked at run time -> {refused['state']}; restored -> {ok['state']}"

    def brick_policy():
        try:
            call("/api/rights/bricks/node-1/customers", {"groups": ["runner"]}, "PUT")
            mirror = call("/api/library/customers-mirror/distribute", {})["plan"]
            anonymized = call("/api/library/customers-anonymized/distribute", {})["plan"]
            if mirror["deployable"] or not anonymized["deployable"]:
                raise RuntimeError(f"mirror {mirror['errors']}, anonymized {anonymized['errors']}")
            return "node-1/customers only to runner: the mirror is refused, the anonymized pipeline passes"
        finally:
            call("/api/rights/bricks/node-1/customers", {"groups": None}, "PUT")

    def impostor():
        # A host holding node-1's credentials announces itself as node-2: the coordinator refuses it,
        # and the real node-2 stays online under its own identity.
        root = os.path.join(STATE, "impostor")
        os.makedirs(root, exist_ok=True)
        config = os.path.join(root, "node.json")
        with open(config, "w") as f:
            json.dump({"name": "node-2", "description": "impostor", "clientId": "node-1", "secret": "node-1-lab-secret"}, f)
        log = os.path.join(STATE, "logs", "coordinator.log")
        seen = os.path.getsize(log)
        host = subprocess.Popen(["dotnet", os.path.join(STATE, "bin", "node", "Lab.NodeHost.dll"), "--config", config,
                                 "--state", root, "--coordinator", URL, "--dtpipe", DTPIPE],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            def refused():
                with open(log, errors="replace") as f:
                    f.seek(seen)
                    return "Announcement of node-2 refused" in f.read()
            until(refused, 30, "the impostor's refusal")
        finally:
            host.terminate()
            host.wait(10)
        node2 = next(i for i in call("/api/rights")["identities"] if i["clientId"] == "node-2")
        if not node2["connected"]:
            raise RuntimeError("node-2 is no longer connected under its own identity")
        return "node-1's credentials announcing node-2: refused; node-2 still connected as itself"

    check("identity provider", idp)
    check("announcement, impostor", impostor)
    check("flow matrix, plan", matrix_plan)
    check("flow matrix, run time", matrix_runtime)
    check("brick policy", brick_policy)
    return failures


LIBRARY_SEED = os.path.join(os.path.dirname(os.path.abspath(__file__)), "library-seed")

# One source brick feeding a sink brick on another node and, through a runner step, a second sink brick:
# the source's node relays the same branch onto two edges.
RELAY_JOB = """customers:
  input: sqlite:${{LAB_CRM_DB}}
  provider-options:
    sqlite-reader:
      query: SELECT customer_id, first_name, last_name, email, city, country, signup_date FROM customers
mirror:
  from: customers
  output: duck:${{LAB_WAREHOUSE_DB}}
  provider-options:
    duck-writer:
      table: customers_mirror
      strategy: Upsert
      key: customer_id
copy:
  from: customers
  transformers:
  - type: project
    options:
      drop: first_name
dim:
  from: copy
  output: duck:${{LAB_WAREHOUSE_DB}}
  provider-options:
    duck-writer:
      table: dim_customers
      strategy: Recreate
"""
RELAY_PLACEMENT = {"customers": "node-1", "mirror": "node-4", "copy": "runner-1", "dim": "node-4"}


def left_behind(prefix):
    return [f["fragment"] for n in call("/api/nodes") for f in n["fragments"] if f["fragment"].startswith(prefix)]


def undeploy(pipeline):
    try:
        call(f"/api/deployments/{pipeline}", method="DELETE")
    except RuntimeError:
        pass


def refused(pipeline, yaml, cuts, placement, alias, node, why):
    """A deployment through the Lab view's own path (a job, cuts, a placement) that `node` must refuse at
    once, naming `alias` and saying `why`, and that leaves nothing deployed."""
    started = time.time()
    try:
        call("/api/deploy", {"pipelineId": pipeline, "yaml": yaml, "cuts": cuts, "placement": placement})
    except RuntimeError as e:
        elapsed = time.time() - started
        said = str(e)
        if f"refused by {node}" not in said or f"'{alias}'" not in said or why not in said:
            raise RuntimeError(f"refused, but not by {node} naming '{alias}' ({why}): {said}")
        if elapsed > 15:
            raise RuntimeError(f"refused after {elapsed:.1f}s, so by the registration timeout rather than by the node: {said}")
        # The other hosts undeploy what did register as they get to the command.
        try:
            until(lambda: not left_behind(pipeline + "@"), 10, "the undeploy")
        except RuntimeError:
            raise RuntimeError(f"refused, but still deployed: {', '.join(left_behind(pipeline + '@'))}") from None
        return f"{node} refused '{alias}' in {elapsed:.1f}s, nothing left deployed"
    finally:
        undeploy(pipeline)
    raise RuntimeError("the deployment was accepted")


def accepted(pipeline, yaml, cuts, placement):
    try:
        return call("/api/deploy", {"pipelineId": pipeline, "yaml": yaml, "cuts": cuts, "placement": placement})
    finally:
        undeploy(pipeline)


def checker(name, counter):
    def check(label, test):
        started = time.time()
        try:
            detail = test()
            print(f"PASS  {name:8} {label:28} {detail}  ({time.time() - started:.1f}s)")
        except Exception as e:
            counter[0] += 1
            print(f"FAIL  {name:8} {label:28} {e}")
    return check


def mirror_jobs():
    mirror = open(os.path.join(LIBRARY_SEED, "customers-mirror.yaml")).read()
    narrower = mirror.replace("SELECT customer_id, first_name, last_name, email, city, country, signup_date FROM customers",
                              "SELECT customer_id, email FROM customers")
    elsewhere = mirror.replace("table: customers_mirror", "table: customers_elsewhere")
    if narrower == mirror or elsewhere == mirror:
        raise RuntimeError("library-seed/customers-mirror.yaml no longer has the query and table this pass alters")
    return mirror, narrower, elsewhere


MIRROR_PLACEMENT = {"customers": "node-1", "mirror": "node-4"}


def bricks_pass():
    """On strict nodes a fragment is run only if each branch is one of the node's bricks. Deployed through
    the Lab view's own path (a job and a placement, no brick lookup), anything else is refused by the node
    that would run it, well before the registration timeout, and nothing of that deployment stays behind."""
    failures = [0]
    check = checker("bricks", failures)
    mirror, narrower, elsewhere = mirror_jobs()

    def strict():
        loose = [n["name"] for n in call("/api/nodes") if n["sandbox"]]
        if loose:
            raise RuntimeError(f"still in sandbox mode: {', '.join(loose)}")
        return "every node host announces itself strict"

    def no_brick():
        # A file the node's owner never offered: the sink is a declared brick, so only the read is refused.
        job = mirror.replace("sqlite:${{LAB_CRM_DB}}", "csv:/etc/hosts")
        if job == mirror:
            raise RuntimeError("library-seed/customers-mirror.yaml no longer reads sqlite:${{LAB_CRM_DB}}")
        return refused("bricks-probe", job, [], MIRROR_PLACEMENT, "customers", "node-1", "not one of this node's bricks")

    def source_by_a_hair():
        accepted("bricks-probe", mirror, [], MIRROR_PLACEMENT)
        detail = refused("bricks-probe", narrower, [], MIRROR_PLACEMENT, "customers", "node-1", "not one of this node's bricks")
        return "the bricks as declared deploy; a narrower query on the same brick: " + detail

    def sink_by_a_hair():
        detail = refused("bricks-probe", elsewhere, [], MIRROR_PLACEMENT, "mirror", "node-4", "not one of this node's bricks")
        return "another table on the same sink: " + detail

    def relays():
        plan = accepted("bricks-relay", RELAY_JOB, [], RELAY_PLACEMENT)
        relays_on_source = next(f["yaml"] for f in plan["fragments"] if f["node"] == "node-1").count("__to__")
        if relays_on_source < 2:
            raise RuntimeError(f"node-1 relays onto {relays_on_source} edge(s), expected 2")
        return f"a source brick feeding two nodes: {relays_on_source} relays on node-1, every branch accepted"

    check("strict nodes", strict)
    check("a read that is no brick", no_brick)
    check("a source brick by a hair", source_by_a_hair)
    check("a sink brick by a hair", sink_by_a_hair)
    check("relays", relays)
    return failures[0]


def sandbox_pass():
    """On sandbox nodes any reader or writer is run, but a data node still only reads and writes: compute
    goes to the runner, and the node refuses it."""
    failures = [0]
    check = checker("sandbox", failures)
    _, narrower, _ = mirror_jobs()

    def loose():
        strict_nodes = [n["name"] for n in call("/api/nodes") if not n["sandbox"]]
        if strict_nodes:
            raise RuntimeError(f"still strict: {', '.join(strict_nodes)}")
        return "every node host announces itself sandbox"

    def any_reader():
        accepted("sandbox-probe", narrower, [], MIRROR_PLACEMENT)
        return "a read no brick declares is accepted"

    def compute():
        entry = next(e for e in call("/api/pipelines") if e["id"].startswith("02-"))
        placement = {**entry["placement"], "revenue": "node-3"}
        return refused(entry["id"], entry["yaml"], entry["cuts"], placement, "revenue", "node-3", "does more than read or write")

    def cut_leaves_a_transformer():
        entry = next(e for e in call("/api/pipelines") if e["id"].startswith("01-"))
        placement = {"customers^": "node-1", "customers": "runner-1", "dim": "node-4"}
        return refused(entry["id"], entry["yaml"], [{"branch": "customers", "at": 1}], placement, "customers", "node-1",
                       "does more than read or write")

    def processor():
        job = "a:\n  input: generate:10\nb:\n  from: a\n  provider-options:\n    sql:\n      query: SELECT * FROM a\nc:\n  from: b\n  output: duck:${{LAB_WAREHOUSE_DB}}\n"
        return refused("sandbox-probe", job, [], {"a": "node-2", "b": "node-2", "c": "node-4"}, "b", "node-2", "sql processor")

    check("sandbox nodes", loose)
    check("any reader or writer", any_reader)
    check("a join on a data node", compute)
    check("a cut after a transformer", cut_leaves_a_transformer)
    check("a processor on a data node", processor)
    return failures[0]


def faults_pass():
    """The seed of sensor-stream, deployed on its own: the runner's child joins a slow source it
    generates itself and writes nothing until that source ends. Read from library-seed/, not from the
    library, whose copy anyone may have edited."""
    started = time.time()
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "library-seed", "sensor-stream.yaml")) as f:
            yaml = f.read()
        placement = {"readings": "runner-1", "enrich": "runner-1", "customers": "node-1", "load": "node-4"}
        plan = call("/api/deploy", {"pipelineId": "smoke-faults", "yaml": yaml, "cuts": [], "placement": placement})
        run = call("/api/runs", {"pipelineId": plan["pipelineId"]})
        until(lambda: any(r["runId"] == run["runId"] and r["state"] == "running" for r in call("/api/runs/queue")), 60, "the run to start")
        time.sleep(3)
        cancelled_at = time.time()
        call("/api/runs/cancel", {"runId": run["runId"]})
        final = until(lambda: _finished(run["runId"]), RUN_TIMEOUT, "the cancelled run")
        elapsed = time.time() - cancelled_at
        runner = next((r for f, r in (final["result"] or {}).get("reports", {}).items() if f.endswith("@runner-1")), None)
        if final["state"] != "cancelled" or elapsed > 6 or runner is None or runner["origin"] != "Remote":
            raise RuntimeError(f"{final['state']} after {elapsed:.1f}s, runner report {runner}")
        print(f"PASS  faults   {'cancel, silent fragment':28} cancelled in {elapsed:.1f}s, runner: {runner['origin']} "
              f"'{runner['firstFault']}'  ({time.time() - started:.1f}s)")
        return 0
    except Exception as e:
        print(f"FAIL  faults   {'cancel, silent fragment':28} {e}")
        return 1


def main():
    ctx = Context()
    failures = (catalog_pass(ctx) if ONLY in (None, "lab") else 0) + (library_pass(ctx) if ONLY in (None, "library") else 0) \
        + (rights_pass() if ONLY in (None, "rights") else 0) + (faults_pass() if ONLY in (None, "faults") else 0) \
        + (bricks_pass() if ONLY in (None, "bricks") else 0) + (sandbox_pass() if ONLY in (None, "sandbox") else 0)
    return 1 if failures else 0


def _finished(run_id):
    """A run is over once the journal holds it: the lab records a run when it ends."""
    return next((r for r in call("/api/runs") if r["runId"] == run_id), None)


if __name__ == "__main__":
    sys.exit(main())
