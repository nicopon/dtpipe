#!/usr/bin/env python3
"""End-to-end check of the lab: every pipeline, distributed, against its monolithic witness.

Six passes by default and a seventh, named. The Lab view's: each pipelines/*.yaml with its catalog layout, deployed and run one
after the other. The library's: each library pipeline planned automatically, then all of them deployed at once and their runs queued together, as the page does: runs
that share a data node must never be in flight together.
The rights': the embedded IDP, people's sessions and the studio's bearer tokens, the flow matrix refusing a plan and then, edited under a deployed
pipeline and, edited under a deployed pipeline, a transfer at run time; a host of another identity that announces itself is placed
no fragment of other nodes and speaks for no run it was not placed on; a data plane client of another identity is never announced;
a disabled identity is refused and its host drops its data plane client, then takes a run once enabled; every edit is undone.
The faults': a run cancelled while a fully wired fragment's child has nothing to write, whose
verdict must come within the grace period.
The bricks': on strict nodes, a fragment whose branch is not one of the node's bricks, or is one only
by a hair, is refused by the node at once and leaves nothing deployed; the same wiring built from the
bricks as declared deploys, relays included. The studio's routes give the catalog, what a job's
branches are and a brick's Arrow sample.
The sandbox': on sandbox nodes any reader or writer is accepted, but a join or a processor on a data
node is refused all the same: a data node only reads and writes. Runs on disjoint data nodes are in
flight together, the runner they share notwithstanding.
The lab and sandbox passes need sandbox nodes and every other pass strict ones: `lab.sh smoke`
switches the node hosts between them.
For every pipeline of the first two:
  1. the witness: the unsplit job run by dtpipe alone, writing a separate warehouse file;
  2. the lab: the distributed run, through the coordinator;
  3. the verdict: outcome Succeeded, every edge's two row counts equal, and the job's
     `# lab-check:` query returning the same row on both warehouses.

The replicas': node-4 carried by two hosts (LAB_REPLICAS=node-4=2), played only when named: a run names the replica
it ran on; a replica that stops says so, is gone at once and removes its directory; one that is cut is listed disconnected,
then lost once its grace has run out, and the next run goes to the other; a replica announcing another declaration of a brick is passed over for a plan made from
the first one, and when none is left the run is refused, until the pipeline is deployed again. Local lab only.
Usage: smoke.py <lab-url> <dtpipe> <state-dir> [lab|library|rights|faults|bricks|sandbox|replicas] [pipeline-id-substring ...]
A leading pass name runs that pass alone.
Against the compose stack (`lab.sh stack smoke`, LAB_TARGET=stack) the state directory is `.stack/`: the
witness reads the databases the nodes seeded there, the library is the coordinator's own directory, its
log is the container's, and the enrollment check's host is a throwaway container of the stack's `node` image.
Exits 0 when every selected pipeline passes, 1 otherwise. Standard library only.
"""
import base64, csv, glob, io, itertools, json, os, re, signal, subprocess, sys, time, urllib.error, urllib.parse, urllib.request, uuid
from datetime import datetime

URL, DTPIPE, STATE = sys.argv[1].rstrip("/"), sys.argv[2], sys.argv[3]
PASSES = {"lab", "library", "rights", "faults", "bricks", "sandbox", "replicas"}
ONLY = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] in PASSES else None
SELECTED = sys.argv[5:] if ONLY else sys.argv[4:]
RUN_TIMEOUT = 180
LAB_DIR = os.path.dirname(os.path.abspath(__file__))
STACK = os.environ.get("LAB_TARGET") == "stack"


# /api serves people: smoke signs in as the lab's admin, through the IdP, as a browser would.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools"))
import campaign_security as sec  # noqa: E402
from lab_session import ApiError, Session, studio_tokens  # noqa: E402
SESSION = Session(URL, timeout=RUN_TIMEOUT)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def call(path, body=None, method=None):
    return SESSION.call(path, body, method)


def bearer(token, path, body=None, method="GET", csrf=True, headers=None):
    """(status, body bytes, headers) of one call with a person's bearer token, as dtpipe studio makes it."""
    sent = {"Authorization": "Bearer " + token, "Content-Type": "application/json", **(headers or {})}
    if csrf:
        sent["X-Dtpipe-Csrf"] = "1"
    req = urllib.request.Request(URL + path, None if body is None else json.dumps(body).encode(), sent, method=method)
    try:
        with urllib.request.build_opener(_NoRedirect).open(req, timeout=60) as r:
            return r.status, r.read(), r.headers
    except urllib.error.HTTPError as e:
        return e.code, e.read(), e.headers


def service_log(name):
    """Everything a service has logged so far: its file on the local lab, its container's output on the stack."""
    if STACK:
        out = subprocess.run(["docker", "compose", "-f", os.path.join(LAB_DIR, "compose.yaml"), "logs", "--no-color", name],
                             capture_output=True, text=True, check=True)
        return out.stdout
    with open(os.path.join(STATE, "logs", f"{name}.log"), errors="replace") as f:
        return f.read()


def coordinator_log():
    return service_log("coordinator")


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


def seeded_databases():
    """Every node database as seed.sh wrote it on this machine, by variable: what the witness reads.
    The node files say where; the coordinator never knows."""
    databases = {}
    for name in sorted(os.listdir(os.path.join(LAB_DIR, "nodes"))):
        with open(os.path.join(LAB_DIR, "nodes", name)) as f:
            node = json.load(f)
        for d in node.get("datasets", []):
            databases[d["variable"]] = os.path.join(STATE, node["name"], d["file"])
    return databases


class Context:
    def __init__(self):
        self.databases = seeded_databases()
        self.warehouse = next(v for v in self.databases if "WAREHOUSE" in v)
        self.witness_db = os.path.join(STATE, "witness", "warehouse.duckdb")
        os.makedirs(os.path.dirname(self.witness_db), exist_ok=True)

    def witness(self, name, yaml):
        """The same job, unsplit, on this machine; only the warehouse differs. Returns its check query."""
        check = check_of(yaml)
        if not check:
            raise RuntimeError("no `# lab-check:` query in the job header")
        env = {**os.environ, **self.databases}
        env[self.warehouse] = self.witness_db
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
        actual = query(f"duck:{self.databases[self.warehouse]}", check)
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
            plan = call(f"/api/library/{pid}/plan")["plan"]
            call(f"/api/library/{pid}/deploy", {})
            queued.append((pid, plan, check))
        except Exception as e:
            failures += 1
            print(f"FAIL  library  {pid:28} {e}")

    # Every run queued at once: runs that share a data node start one at a time, in the order they came.
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

    try:
        print(f"PASS  library  {'a shared node, in turn':28} {shared_in_turn(runs, finals)}")
    except Exception as e:
        failures += 1
        print(f"FAIL  library  {'a shared node, in turn':28} {e}")

    try:
        print(f"PASS  library  {'a studio publishes':28} {studio_publishes([pid for pid, _, _ in queued])}")
    except Exception as e:
        failures += 1
        print(f"FAIL  library  {'a studio publishes':28} {e}")
    try:
        print(f"PASS  library  {'no layouts':28} {layouts_gone()}")
    except Exception as e:
        failures += 1
        print(f"FAIL  library  {'no layouts':28} {e}")
    return failures


def layouts_gone():
    """No coordinator designer remains, so no pipeline of the library keeps a layout: the library drops
    those it still tracked in one commit of its own, which its history keeps."""
    repo = os.path.join(STATE, "coordinator" if STACK else "", "library")
    git = lambda *a: subprocess.run(["git", "-C", repo, *a], capture_output=True, text=True, check=True).stdout
    tracked = git("ls-files", ":(glob)pipelines/*/layout.json").split()
    if tracked:
        raise RuntimeError(f"still tracked: {tracked}")
    ever = git("log", "--all", "--format=%h", "--", ":(glob)pipelines/*/layout.json").split()
    removed = [c for c in git("log", "--format=%s").splitlines() if c == "Remove the designer's layouts"]
    if ever and len(removed) != 1:
        raise RuntimeError(f"{len(ever)} commit(s) touched a layout, {len(removed)} removal commit(s)")
    return f"none tracked; {len(ever)} commit(s) ever touched one, removed by {len(removed)} commit of their own"


def studio_publishes(deployed):
    """dtpipe studio publishes with a person's bearer token and says which version it read: a new id only
    while absent, a replacement only of the job's commit it names (412 otherwise, with the commit in
    place). Publishing over a deployed pipeline says the deployment still runs the version before; the
    text is then put back as it was, and the deployment is the current version again."""
    token = studio_tokens(URL, "labdesigner")["access_token"]

    def publish(pid, yaml, **headers):
        status, body, _ = bearer(token, f"/api/library/{pid}", {"yaml": yaml, "message": "smoke: studio"}, "PUT", headers=headers)
        return status, json.loads(body or b"null")

    pid = "smoke-studio-publish"
    job = "# Studio publication\na:\n  input: generate:3\n  output: csv:-\n"
    try:
        status, first = publish(pid, job, **{"If-None-Match": "*"})
        if status != 200:
            raise RuntimeError(f"creation -> {status} {first}")
        status, again = publish(pid, job, **{"If-None-Match": "*"})
        status_stale, stale = publish(pid, job + "# b\n", **{"If-Match": '"0000000"'})
        if (status, again["commit"], status_stale, stale["commit"]) != (412, first["commit"], 412, first["commit"]):
            raise RuntimeError(f"created again -> {status} {again}; stale If-Match -> {status_stale} {stale}")
        status, replaced = publish(pid, job + "# b\n", **{"If-Match": f'"{first["commit"]}"'})
        if status != 200 or replaced["commit"] == first["commit"]:
            raise RuntimeError(f"replacing the version read -> {status} {replaced}")
    finally:
        try:
            call(f"/api/library/{pid}", None, "DELETE")
        except ApiError:
            pass

    if not deployed:
        raise RuntimeError("no library pipeline was deployed: the deployed message is not checked")
    target = deployed[0]
    original = call(f"/api/library/{target}")["yaml"]
    status, changed = publish(target, original + "# published from a studio\n")
    status_back, back = publish(target, original)
    if status != 200 or not changed["deployed"]:
        raise RuntimeError(f"publishing over {target} -> {status} {changed}: deployed expected")
    if status_back != 200 or back["deployed"]:
        raise RuntimeError(f"putting {target} back -> {status_back} {back}: the deployment is the current version again")
    return (f"created only while absent, replaced only at the commit read (412 otherwise); over {target}: "
            "deployed, then the current version again once put back")


def data_nodes(plan):
    """The nodes a run holds: its fragments' nodes, runners excepted."""
    runners = {n["name"] for n in call("/api/nodes") if n["role"] == "Runner"}
    return {f["node"] for f in plan["fragments"]} - runners


def instant(text):
    """A timestamp as the coordinator writes it. Its fraction of a second loses its trailing zeros, and
    `fromisoformat` of Python 3.9 reads only three or six digits: pad it to six."""
    return datetime.fromisoformat(re.sub(r"\.(\d+)", lambda m: "." + m.group(1)[:6].ljust(6, "0"), text))


def span(final):
    return instant(final["startedAt"]), instant(final["finishedAt"])


def overlap(a, b):
    (a0, a1), (b0, b1) = span(a), span(b)
    return a0 < b1 and b0 < a1


def shared_in_turn(runs, finals):
    """Runs that share a data node never overlap: each starts once the one before it has finished."""
    if len(runs) < 2:
        return "one run selected, nothing to compare"
    pairs = []
    for (pa, plan_a, _, _), (pb, plan_b, _, _) in itertools.combinations(runs, 2):
        shared = data_nodes(plan_a) & data_nodes(plan_b)
        if not shared:
            continue
        a, b = finals[pa], finals[pb]
        if isinstance(a, Exception) or isinstance(b, Exception):
            raise RuntimeError(f"{pa} or {pb} did not finish")
        if overlap(a, b):
            raise RuntimeError(f"{pa} and {pb} share {', '.join(sorted(shared))} but were in flight together: {span(a)} {span(b)}")
        pairs.append(f"{pa}/{pb}")
    if not pairs:
        raise RuntimeError("no two runs share a data node: the check proves nothing")
    return f"{len(pairs)} pairs sharing a data node, none in flight together"


def token(issuer, client_id, secret):
    form = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": client_id, "client_secret": secret}).encode()
    try:
        answer = json.loads(urllib.request.urlopen(urllib.request.Request(issuer.rstrip("/") + "/connect/token", form), timeout=20).read())
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
        # The IdP says who: the token names node-1 and grants no group. The coordinator says what:
        # node-1 belongs to crm because its identity does.
        issuer = rights["idp"]["issuer"]
        _, refused = token(issuer, "node-1", "not-the-secret")
        claims, _ = token(issuer, "node-1", "node-1-lab-secret")
        if refused == 200 or not claims or claims.get("sub") != "node-1" or "transportr:group:" in str(claims.get("scope", "")):
            raise RuntimeError(f"wrong secret -> {refused}, claims {claims}")
        groups = next((n["groups"] for n in call("/api/nodes") if n["name"] == "node-1"), None)
        if groups != ["crm"]:
            raise RuntimeError(f"node-1 is in groups {groups!r}, its identity says crm")
        return f"wrong secret refused ({refused}); node-1's token names it and grants nothing, the coordinator puts it in crm"

    def matrix_plan():
        try:
            call("/api/rights/matrix", {"matrix": revoked}, "PUT")
            plan = call("/api/library/customers-anonymized/plan")["plan"]
            if plan["deployable"] or "crm -> runner" not in (plan.get("flowRejection") or ""):
                raise RuntimeError(f"plan not refused: {plan.get('flowRejection')}")
            return "crm -> runner revoked: plan refused"
        finally:
            call("/api/rights/matrix", {"matrix": matrix}, "PUT")

    def matrix_runtime():
        # Deployed under the full matrix, then run with crm -> runner revoked: the hub refuses the transfer.
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

    def stranger_token():
        node = json.load(open(os.path.join(LAB_DIR, "nodes", "node-2.json")))
        status, access = sec.token_for(rights["idp"]["issuer"], node["clientId"], node["secret"])
        if not access:
            raise RuntimeError(f"no token for node-2 (HTTP {status})")
        return access

    def announce_as_a_replica_of_node_2(access, origin):
        # A host of node-2's identity that is none of node-2's replicas: it announces what node-2 announces (the node shows
        # its most recently arrived replica, so an announcement of its own would show on the catalog), under an instance
        # of its own.
        view = next(n for n in call("/api/nodes") if n["name"] == "node-2")
        kinds, roles = {"Source": 0, "Sink": 1}, {"Data": 0, "Runner": 1}
        hub = sec.LongPollingHub(URL, sec.LAB_HUB, access).open()
        answer = hub.invoke("Announce", {
            "name": view["name"], "description": view["description"], "datasets": view["datasets"], "role": roles[view["role"]],
            "bricks": [{**b, "kind": kinds[b["kind"]]} for b in view["bricks"]], "capabilities": view["capabilities"],
            "dtpipeVersion": view["dtpipeVersion"], "instanceId": str(uuid.uuid4()), "origin": origin})
        if "error" in answer:
            hub.close()
            raise RuntimeError(f"the announcement was refused, so the check proves nothing: {answer['error']}")
        return hub

    def exited_forged(hub, run_id, fragment):
        answer = hub.invoke("Exited", {"runId": run_id, "fragment": fragment, "exitCode": 1, "origin": "Local",
                                       "firstFault": "a report forged by a stranger", "rowCounts": {}})
        return answer.get("result", answer.get("error"))

    def foreign_replica():
        # A host of another identity (node-2's) announces itself and keeps its connection for the whole run of a pipeline
        # of node-1 and node-4. A run is placed on the replicas of its fragments' own nodes: the stranger is never launched,
        # and the run succeeds on the legitimate nodes.
        access = stranger_token()
        pipeline = "customers-mirror"
        hub = None
        try:
            call(f"/api/library/{pipeline}/deploy", {})
            hub = announce_as_a_replica_of_node_2(access, "smoke:foreign-replica")
            run = call("/api/runs", {"pipelineId": pipeline})
            final = until(lambda: _finished(run["runId"]), RUN_TIMEOUT, "the run beside a stranger's replica")
            if final["state"] != "succeeded":
                raise RuntimeError(f"a stranger announced as node-2 -> {final['state']}: {final.get('refusal') or final.get('description')}")
            if any(m.get("type") == 1 and m.get("target") == "Launch" for m in hub.messages):
                raise RuntimeError("the stranger was launched a fragment of another node")
            # A closed long-polling connection answers nothing: this one held for the whole run.
            if "error" in hub.invoke("Leave"):
                raise RuntimeError("the stranger's connection did not hold for the whole run")
        finally:
            if hub is not None:
                hub.close()
            undeploy(pipeline)
        return "a host of node-2's identity held its connection for the whole run: the run on node-1 and node-4 succeeded, nothing launched on it"

    def foreign_report():
        # The same host, while a slow run of node-4's fragment is in flight, reports that the fragment exited with a local
        # fault. It is not the replica the run placed the fragment on and it cannot know the launch's token: the coordinator
        # answers that it did not admit the report, and the run ends on what the real fragments say. Which of the two rules
        # refused it is the unit tests' to show.
        access = stranger_token()
        with open(os.path.join(LAB_DIR, "library-seed", "sensor-stream.yaml")) as f:
            yaml = f.read()
        placement = {"readings": "runner-1", "enrich": "runner-1", "customers": "node-1", "load": "node-4"}
        pipeline = "smoke-foreign-report"
        hub = None
        try:
            call("/api/deploy", {"pipelineId": pipeline, "yaml": yaml, "cuts": [], "placement": placement})
            fragment = f"{pipeline}@node-4"
            hub = announce_as_a_replica_of_node_2(access, "smoke:foreign-report")
            run = call("/api/runs", {"pipelineId": pipeline})
            until(lambda: any(r["runId"] == run["runId"] and r["state"] == "running" for r in call("/api/runs/queue")), 60, "the run to start")
            time.sleep(3)
            answered = exited_forged(hub, run["runId"], fragment)
            final = until(lambda: _finished(run["runId"]), RUN_TIMEOUT, "the run beside a forged report")
            if answered != "NotAdmitted":
                raise RuntimeError(f"the forged report was answered {answered!r}, not NotAdmitted")
            if final["state"] != "succeeded":
                raise RuntimeError(f"a stranger reported {fragment} exited -> {final['state']}: {final.get('refusal') or final.get('description')}")
            if "error" in hub.invoke("Leave"):
                raise RuntimeError("the stranger's connection did not hold for the whole run")
        finally:
            if hub is not None:
                hub.close()
            undeploy(pipeline)
        return f"node-2's host reported {fragment} exited with a local fault in the middle of a run: NotAdmitted, and the verdict stayed the real fragments' (succeeded)"

    def stolen_data_plane_client():
        # A host announces the TransportR client of another identity. The client is registered under node-2's subject; the
        # host connects as node-1: its announcement is refused, so it is never one of node-1's replicas, and no transfer of
        # node-2's reaches it.
        access = stranger_token()
        client = str(uuid.uuid4())
        node1 = json.load(open(os.path.join(LAB_DIR, "nodes", "node-1.json")))
        status, thief = sec.token_for(rights["idp"]["issuer"], node1["clientId"], node1["secret"])
        if not thief:
            raise RuntimeError(f"no token for node-1 (HTTP {status})")
        owner = sec.LongPollingHub(URL, sec.COMMAND_HUB, access).open()
        claimant = None
        try:
            answer = owner.invoke("Connect", client, None)
            if "error" in answer:
                raise RuntimeError(f"node-2's client was not registered, so the check proves nothing: {answer['error']}")
            claimant = sec.LongPollingHub(URL, sec.LAB_HUB, thief).open()
            try:
                claimant.invoke("Announce", {"name": "node-1", "description": "", "datasets": [], "role": 0, "bricks": [],
                                             "instanceId": str(uuid.uuid4()), "origin": "smoke:stolen-client", "dataPlaneClientId": client})
            except RuntimeError:
                pass  # the coordinator closes the connection of a refused announcement before it answers
            time.sleep(1)
            replicas = next(n["replicas"] for n in call("/api/nodes") if n["name"] == "node-1")
            if any(r["origin"] == "smoke:stolen-client" for r in replicas):
                raise RuntimeError("a host announcing node-2's data plane client became a replica of node-1")
        finally:
            if claimant is not None:
                claimant.close()
            owner.close()
        return "node-1's token announcing a client registered under node-2's subject: refused, no replica of node-1"

    def data_hub_takes_no_run_message():
        # The data hub serves the data plane and nothing about a run: a fragment's registration and its report are
        # not methods of it.
        access = stranger_token()
        hub = sec.LongPollingHub(URL, sec.COMMAND_HUB, access).open()
        try:
            for method, arguments in (("Register", ("smoke-fragment", "v")), ("Exited", ("run-1", 1, "Local", None, {}))):
                answer = hub.invoke(method, *arguments)
                if "does not exist" not in answer.get("error", ""):
                    raise RuntimeError(f"{method} on the data hub was answered {json.dumps(answer)[:160]}")
        finally:
            hub.close()
        return "Register and Exited on the data hub: the methods do not exist"

    def several_groups():
        # A node in several groups sends to another only if every group of the sender may send to every group of
        # the receiver. runner-1 joins `audit`: crm -> runner then needs crm -> audit, and runner -> warehouse needs
        # audit -> warehouse too. Each is added in turn, in the plan and then at run time; the identity, the
        # matrix and the deployment are restored.
        runner = next(i for i in call("/api/rights")["identities"] if i["clientId"] == "runner-1")

        def put_identity(groups):
            call("/api/rights/identities/runner-1", {"clientId": "runner-1", "name": runner["name"], "description": runner["description"],
                                                    "groups": groups, "enabled": True}, "PUT")
            call("/api/rights/identities/runner-1/reconnect", {})
            until(lambda: next((n["groups"] == sorted(groups) and n["online"] for n in call("/api/nodes") if n["name"] == "runner-1"), False),
                  60, f"runner-1 online in {groups}")

        def plan():
            return call("/api/library/customers-anonymized/plan")["plan"]

        def allowing(*edges):
            edited = {g: list(t) for g, t in matrix.items()}
            for sender, receiver in edges:
                edited.setdefault(sender, []).append(receiver)
            call("/api/rights/matrix", {"matrix": edited}, "PUT")

        try:
            for deployment in call("/api/deployments"):
                call(f"/api/deployments/{deployment['pipelineId']}", None, "DELETE")
            put_identity(["runner", "audit"])
            # Nothing was saved of the plan: the next plan names the groups the rights give runner-1 now, with no
            # redistribution, and the matrix judges it under them.
            refused = plan()
            held = [sorted(f["groups"]) for f in refused["fragments"] if f["node"] == "runner-1"]
            if held != [["audit", "runner"]]:
                raise RuntimeError(f"runner-1 holds runner+audit now, and the plan names {held}")
            if refused["deployable"] or "audit" not in (refused.get("flowRejection") or ""):
                raise RuntimeError(f"runner+audit with no matrix entry for audit: {refused.get('flowRejection')}")
            allowing(("crm", "audit"))
            half = plan()
            if half["deployable"] or "audit+runner -> warehouse" not in (half.get("flowRejection") or ""):
                raise RuntimeError(f"crm -> audit alone must still refuse audit+runner -> warehouse: {half.get('flowRejection')}")
            allowing(("crm", "audit"), ("audit", "warehouse"))
            if not plan()["deployable"]:
                raise RuntimeError(f"every group allowed, still refused: {plan().get('flowRejection')}")
            # The hub enforces the same rule on every transfer: deployed and run with both groups allowed it
            # succeeds; with audit -> warehouse revoked under the deployment, the transfer is refused.
            call("/api/library/customers-anonymized/deploy", {})
            run = call("/api/runs", {"pipelineId": "customers-anonymized"})
            allowed = until(lambda: _finished(run["runId"]), RUN_TIMEOUT, "the run with every group allowed")
            allowing(("crm", "audit"))
            run = call("/api/runs", {"pipelineId": "customers-anonymized"})
            refused = until(lambda: _finished(run["runId"]), RUN_TIMEOUT, "the run with audit -> warehouse revoked")
            if allowed["state"] != "succeeded" or refused["state"] == "succeeded":
                raise RuntimeError(f"every group allowed -> {allowed['state']}, audit -> warehouse revoked -> {refused['state']}")
        finally:
            call("/api/rights/matrix", {"matrix": matrix}, "PUT")
            for deployment in call("/api/deployments"):
                call(f"/api/deployments/{deployment['pipelineId']}", None, "DELETE")
            put_identity(runner["groups"])
        return "runner-1 in runner+audit: refused until crm -> audit and audit -> warehouse are both allowed; the run succeeds, and fails once audit -> warehouse is revoked"

    def enrollment():
        # A host whose token the rights do not know waits for an administrator: it proposes a name, which
        # authorises nothing. It is not a node, and no name it proposes (here node-2's) takes anyone's place.
        # Accepted under another name and a group, it comes online as what the rights say.
        root = os.path.join(STATE, "enroll")
        os.makedirs(root, exist_ok=True)
        config = os.path.join(root, "node.json")
        with open(config, "w") as f:
            json.dump({"name": "node-2", "description": "proposes to be node-2", "clientId": "node-enroll", "secret": "node-enroll-lab-secret"}, f)
        if STACK:
            # A host the stack did not declare: a throwaway container of the node image, on the stack's network,
            # reaching the coordinator directly and the IdP by the one name browsers and containers share.
            container = "lab-enroll-host"
            subprocess.run(["docker", "rm", "-f", container], capture_output=True)
            host = subprocess.Popen(["docker", "run", "--rm", "--init", "--name", container, "--network", "dtpipe-lab-stack_default",
                                     "-v", f"{root}:/enroll", "--entrypoint", "dotnet", "dtpipe-lab-stack/node", "/app/Lab.NodeHost.dll",
                                     "--config", "/enroll/node.json", "--state", "/enroll", "--coordinator", "http://coordinator:8080",
                                     "--idp", rights["idp"]["issuer"], "--dtpipe", "/usr/local/bin/dtpipe"],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            host = subprocess.Popen(["dotnet", os.path.join(STATE, "bin", "node", "Lab.NodeHost.dll"), "--config", config,
                                     "--state", root, "--coordinator", URL, "--idp", rights["idp"]["issuer"], "--dtpipe", DTPIPE],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def waiting():
            return next((p for p in call("/api/rights")["pending"] if p["subject"] == "node-enroll"), None)

        def node(name):
            return next((n for n in call("/api/nodes") if n["name"] == name), None)
        try:
            pending = until(waiting, 60, "the unknown host to wait for approval")
            if pending["proposedName"] != "node-2" or "node-enroll" in {i["clientId"] for i in call("/api/rights")["identities"]}:
                raise RuntimeError(f"pending {pending}")
            node2 = node("node-2")
            if not node2 or not node2["online"] or node2["groups"] != ["sales"]:
                raise RuntimeError(f"node-2 after the proposal: {node2}")
            if node("enrolled-node"):
                raise RuntimeError("a host waiting for approval is listed as a node")
            call("/api/rights/pending/node-enroll/accept", {"name": "enrolled-node", "groups": ["crm"]})
            until(lambda: (node("enrolled-node") or {}).get("online"), 60, "the accepted host to come online")
            enrolled = node("enrolled-node")
            if enrolled["groups"] != ["crm"] or waiting() or (node("node-2") or {}).get("groups") != ["sales"]:
                raise RuntimeError(f"enrolled {enrolled}, still waiting {waiting()}, node-2 {node('node-2')}")
        finally:
            if STACK:
                subprocess.run(["docker", "rm", "-f", "lab-enroll-host"], capture_output=True)
            else:
                host.terminate()
            host.wait(10)
            if "node-enroll" in {i["clientId"] for i in call("/api/rights")["identities"]}:
                call("/api/rights/identities/node-enroll", None, "DELETE")
        until(lambda: node("enrolled-node") is None, 30, "the removed identity's node to leave the inventory")
        return "node-enroll proposing node-2: waits, node-2 untouched; accepted as enrolled-node in crm: online; removed: gone"

    def disabled():
        # The coordinator decides what an identity may do: disabling it drops its host's connection with no
        # reconnect asked for, node-3 is then refused when it connects again, whatever token the IdP still
        # issues it, and its host drops the data plane client the hub would otherwise keep with the groups it
        # had; enabled again, it comes back on its own, and a run through it succeeds.
        def update(enabled):
            node3 = next(i for i in call("/api/rights")["identities"] if i["clientId"] == "node-3")
            call("/api/rights/identities/node-3", {"clientId": "node-3", "name": node3["name"], "description": node3["description"],
                                                  "groups": node3["groups"], "enabled": enabled}, "PUT")

        def online():
            return next((n["online"] for n in call("/api/nodes") if n["name"] == "node-3"), False)

        seen, seen_host = len(coordinator_log()), len(service_log("node-3"))
        try:
            update(False)

            def refused():
                return "Announcement of node-3 refused: 'node-3' is disabled" in coordinator_log()[seen:]
            until(refused, 30, "node-3's refusal while disabled")

            def dropped():
                return "dropping its data plane client" in service_log("node-3")[seen_host:]
            until(dropped, 30, "node-3's host dropping its data plane client")
            if online():
                raise RuntimeError("node-3 is online while its identity is disabled")
        finally:
            update(True)
        until(online, 30, "node-3 back online once enabled")
        pipeline = "revenue-by-country"
        call(f"/api/library/{pipeline}/deploy", {})
        try:
            run = call("/api/runs", {"pipelineId": pipeline})
            final = until(lambda: _finished(run["runId"]), RUN_TIMEOUT, "the run through node-3, enabled again")
        finally:
            undeploy(pipeline)
        if final["state"] != "succeeded":
            raise RuntimeError(f"a run through node-3 once enabled again -> {final['state']}: {final.get('refusal') or final.get('description')}")
        return "disabled: node-3 refused when it connects again, its host dropped its data plane client; enabled: back online, and a run through it succeeded"

    def roles():
        # A person's rights are the coordinator's configuration: labreader reads, labdesigner also
        # changes pipelines, labadmin also the rights; anyone else holds nothing. A change needs the
        # CSRF header; a service's token opens no page and no /api route; an anonymous call is told
        # 401, never sent to the IdP. A page refuses with a way out: a reader is offered to sign out of
        # the Lab view, and a person with no role is signed out here and at the IdP, so the next
        # sign-in can be someone else.
        def status(fn):
            try:
                fn()
                return 200
            except ApiError as e:
                return e.status

        def raw(path, headers=None, data=None, method=None):
            req = urllib.request.Request(URL + path, data, headers or {}, method=method)
            opener = urllib.request.build_opener(_NoRedirect)
            try:
                return opener.open(req, timeout=20).status
            except urllib.error.HTTPError as e:
                return e.code

        reader, designer, nobody = Session(URL, "labreader"), Session(URL, "labdesigner"), Session(URL, "toto")

        def navigate(session, path):
            """Where a browser opening path ends, redirects followed: (url, status, body)."""
            try:
                with session._opener.open(URL + path, timeout=30) as r:
                    return r.geturl(), r.status, r.read().decode(errors="replace")
            except urllib.error.HTTPError as e:
                return e.geturl(), e.code, e.read().decode(errors="replace")

        def lab_refusal():
            _, code, body = navigate(reader, "/lab/")
            return code, 'href="/account/logout"' in body

        def signed_out():
            # Ends on the denied page, without the session, and the IdP asks for a password again.
            url, code, _ = navigate(nobody, "/")
            _, me, _ = navigate(nobody, "/api/me")
            again, _, _ = navigate(nobody, "/")
            return urllib.parse.urlparse(url).path, code, me, "/account/login" in again and not again.startswith(URL)
        policy = call("/api/rights")["matrix"]
        expected = {
            "reader GET /api/library": (status(lambda: reader.call("/api/library")), 200),
            "reader PUT /api/rights/matrix": (status(lambda: reader.call("/api/rights/matrix", {"matrix": policy}, "PUT")), 403),
            "reader GET /api/library/{id}/plan": (status(lambda: reader.call("/api/library/customers-mirror/plan")), 200),
            "reader POST /api/library/{id}/deploy": (status(lambda: reader.call("/api/library/customers-mirror/deploy", {})), 403),
            "designer PUT /api/rights/matrix": (status(lambda: designer.call("/api/rights/matrix", {"matrix": policy}, "PUT")), 403),
            "designer GET /api/pipelines (lab mode)": (status(lambda: designer.call("/api/pipelines")), 403),
            "no role GET /api/nodes": (status(lambda: nobody.call("/api/nodes")), 403),
            "no role GET /api/me": (status(lambda: nobody.call("/api/me")), 200),
            "reader opens the Lab view": (lab_refusal(), (403, True)),
            "no role opens a page": (signed_out(), ("/account/denied", 200, 401, True)),
            "anonymous GET /api/nodes": (raw("/api/nodes"), 401),
            "anonymous GET /": (raw("/"), 302),
            "service token GET /api/nodes": (raw("/api/nodes", {"Authorization": "Bearer " + service_token()}), 401),
            "anonymous node channel": (raw("/node-channel/negotiate?negotiateVersion=1", method="POST", data=b""), 401),
            "admin POST without CSRF header": (raw_session("/api/runs/cancel", b"{}"), 403),
            # A person's session never reaches the data plane: only a service's token opens it.
            "admin session on the node channel": (raw_session("/node-channel/negotiate?negotiateVersion=1", b""), 401),
            "admin session on TransportR's hub": (raw_session("/hub/command/negotiate?negotiateVersion=1", b""), 401),
            "admin session on a stream": (raw_session(f"/api/stream/{STREAM}/send", b""), 401),
            "anonymous stream": (raw(f"/api/stream/{STREAM}/send", data=b"", method="POST"), 401),
        }
        wrong = {k: v for k, v in expected.items() if v[0] != v[1]}
        if wrong:
            raise RuntimeError("; ".join(f"{k}: {got} (expected {want})" for k, (got, want) in wrong.items()))
        return f"{len(expected)} access rules hold: reader, designer, admin, no role, anonymous, service token, CSRF, data plane"

    STREAM = "00000000-0000-0000-0000-000000000000"

    def service_token():
        form = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": "node-1", "client_secret": "node-1-lab-secret"}).encode()
        return json.loads(urllib.request.urlopen(urllib.request.Request(rights["idp"]["issuer"] + "/connect/token", form), timeout=20).read())["access_token"]

    def raw_session(path, data):
        req = urllib.request.Request(URL + path, data, {"Content-Type": "application/json"}, method="POST")
        try:
            return SESSION._opener.open(req, timeout=20).status
        except urllib.error.HTTPError as e:
            return e.code

    def studio():
        # dtpipe studio signs a person in through its own public client and calls /api with their
        # bearer token: the roles are the same configuration's, a change still needs the CSRF header,
        # and the token reaches no part of the data plane.
        designer = studio_tokens(URL, "labdesigner")["access_token"]
        reader = studio_tokens(URL, "labreader")["access_token"]
        me = bearer(designer, "/api/me")
        me = (me[0], json.loads(me[1]) if me[0] == 200 else None)
        save = {"yaml": "a:\n  input: x\n", "message": "smoke: refused"}
        expected = {
            "designer bearer GET /api/me": ((me[0], (me[1] or {}).get("roles")), (200, ["designer", "reader"])),
            "reader bearer PUT /api/library": (bearer(reader, "/api/library/smoke-refused", save, "PUT")[0], 403),
            "designer bearer PUT without CSRF": (bearer(designer, "/api/library/smoke-refused", save, "PUT", csrf=False)[0], 403),
            "service token GET /api/me": (bearer(service_token(), "/api/me")[0], 401),
            "designer bearer on the node channel": (bearer(designer, "/node-channel/negotiate?negotiateVersion=1", {}, "POST")[0], 401),
            "designer bearer on TransportR's hub": (bearer(designer, "/hub/command/negotiate?negotiateVersion=1", {}, "POST")[0], 401),
        }
        wrong = {k: v for k, v in expected.items() if v[0] != v[1]}
        if wrong:
            raise RuntimeError("; ".join(f"{k}: {got} (expected {want})" for k, (got, want) in wrong.items()))
        if any(p["subject"] in ("labdesigner", "labreader") for p in call("/api/rights")["pending"]):
            raise RuntimeError("a person's token was taken for a host waiting for approval")
        return f"{len(expected)} rules hold for the studio's bearer: roles, CSRF, no service on /api, no person on the data plane"

    check("identity provider", idp)
    check("roles and sessions", roles)
    check("studio bearer", studio)
    check("enrollment", enrollment)
    check("identity, disabled", disabled)
    check("flow matrix, plan", matrix_plan)
    check("flow matrix, run time", matrix_runtime)
    check("a foreign replica", foreign_replica)
    check("a foreign report", foreign_report)
    check("a stolen data plane client", stolen_data_plane_client)
    check("no run message on the data hub", data_hub_takes_no_run_message)
    check("flow matrix, several groups", several_groups)
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
    """What a refused deployment left: a deployment is the coordinator's and a refused one is not kept, and no replica runs
    anything the refusal started."""
    kept = [f["name"] for d in call("/api/deployments") for f in d["fragments"] if f["name"].startswith(prefix)]
    running = [f"{n['name']}/{r['id']}" for n in call("/api/nodes") for r in n["replicas"] if r["runs"]]
    return kept + running


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


def planned_refusal(pipeline, yaml, cuts, placement, alias, why):
    """A deployment through the Lab view's own path to strict nodes. A data node runs only the bricks its owner
    declared, by id, so a branch that is none is refused by the planner, naming `alias` and saying `why`,
    before anything is sent to a node, and nothing of that deployment exists."""
    try:
        call("/api/deploy", {"pipelineId": pipeline, "yaml": yaml, "cuts": cuts, "placement": placement})
    except RuntimeError as e:
        said = str(e)
        if "This plan cannot be deployed" not in said or f"'{alias}'" not in said or why not in said:
            raise RuntimeError(f"refused, but not by the planner naming '{alias}' ({why}): {said}")
        if left_behind(pipeline + "@"):
            raise RuntimeError(f"refused, but still deployed: {', '.join(left_behind(pipeline + '@'))}")
        return f"the planner refused '{alias}' before any node was asked, nothing deployed"
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
    """On strict nodes a fragment is run only if each branch is one of the node's bricks, which the node
    composes by id from its own declarations. Deployed through the Lab view's own path (a job and a
    placement), anything else is refused by the planner, before any node is asked, and nothing of that
    deployment stays behind."""
    failures = [0]
    check = checker("bricks", failures)
    mirror, narrower, elsewhere = mirror_jobs()

    def strict():
        loose = [n["name"] for n in call("/api/nodes") if "raw-fragments" in n["capabilities"]]
        if loose:
            raise RuntimeError(f"still in sandbox mode: {', '.join(loose)}")
        return "every node host announces itself strict"

    def no_brick():
        # A file the node's owner never offered: the sink is a declared brick, so only the read is refused.
        job = mirror.replace("sqlite:${{LAB_CRM_DB}}", "csv:/etc/hosts")
        if job == mirror:
            raise RuntimeError("library-seed/customers-mirror.yaml no longer reads sqlite:${{LAB_CRM_DB}}")
        return planned_refusal("bricks-probe", job, [], MIRROR_PLACEMENT, "customers", "not one of this node's bricks")

    def source_by_a_hair():
        accepted("bricks-probe", mirror, [], MIRROR_PLACEMENT)
        detail = planned_refusal("bricks-probe", narrower, [], MIRROR_PLACEMENT, "customers", "not one of this node's bricks")
        return "the bricks as declared deploy; a narrower query on the same brick: " + detail

    def sink_by_a_hair():
        detail = planned_refusal("bricks-probe", elsewhere, [], MIRROR_PLACEMENT, "mirror", "not one of this node's bricks")
        return "another table on the same sink: " + detail

    def relays():
        plan = accepted("bricks-relay", RELAY_JOB, [], RELAY_PLACEMENT)
        relays_on_source = next(f["yaml"] for f in plan["fragments"] if f["node"] == "node-1").count("__to__")
        if relays_on_source < 2:
            raise RuntimeError(f"node-1 relays onto {relays_on_source} edge(s), expected 2")
        return f"a source brick feeding two nodes: {relays_on_source} relays on node-1, every branch accepted"

    check("strict nodes", strict)
    check("a read that is no brick", no_brick)
    def library_save_check():
        # Every save into the library is checked, the studio's and the pages' alike. The server opens no node
        # database and runs no dtpipe: each source brick's node sends one row, a runner dry-runs the job over
        # them, and each sink brick's node judges its own target. Each seed, sent as its file holds it under a
        # probe id, is accepted.
        token = studio_tokens(URL, "labdesigner")["access_token"]
        names = sorted(n for n in os.listdir(LIBRARY_SEED) if n.endswith(".yaml"))
        for name in names:
            pid = "smoke-seed-" + name[:-len(".yaml")]
            status, body, _ = bearer(token, f"/api/library/{pid}", {"yaml": open(os.path.join(LIBRARY_SEED, name)).read(),
                                                                   "message": "smoke: a seed as written"}, "PUT")
            try:
                if status != 200:
                    raise RuntimeError(f"{name}: HTTP {status} {body[:300]!r}")
            finally:
                try:
                    call(f"/api/library/{pid}", None, "DELETE")
                except ApiError:
                    pass
        return f"{len(names)} library pipelines accepted as written"

    def sink_refuses():
        yaml = open(os.path.join(LIBRARY_SEED, "revenue-by-country.yaml")).read()
        wrong = yaml.replace("table: revenue_by_country\n      strategy: Recreate",
                             "table: customers_mirror\n      strategy: Upsert\n      key: customer_id")
        if wrong == yaml:
            raise RuntimeError("library-seed/revenue-by-country.yaml no longer loads its table this way")
        token = studio_tokens(URL, "labdesigner")["access_token"]
        pid = "smoke-refused-probe"
        status, body, _ = bearer(token, f"/api/library/{pid}", {"yaml": wrong, "message": "smoke: refused"}, "PUT")
        answer = json.loads(body or b"null") or {}
        if status != 422 or "node-4/customers-mirror" not in (answer.get("report") or ""):
            raise RuntimeError(f"node-4 should refuse the save: {status} {body[:400]!r}")
        try:
            call(f"/api/library/{pid}")
        except ApiError as e:
            if e.status != 404:
                raise
        else:
            raise RuntimeError("a refused save was written into the library")
        return "node-4 refuses revenue rows for customers_mirror: 422, and nothing is saved"

    def binding_modes():
        # The check pins what it saw of each brick, by the nodes' own reads: a source's contract is the one its sample produced
        # (dtpipe's `contract-save`), a sink's the one the runner's dry-run hands it. A mode other than strict is pinned by a save
        # and by a bindings save alike; `tolerant` keeps the version alone, and a strict binding keeps no pin.
        token = studio_tokens(URL, "labdesigner")["access_token"]
        yaml = open(os.path.join(LIBRARY_SEED, "customers-mirror.yaml")).read()
        seeded = json.load(open(os.path.join(LIBRARY_SEED, "customers-mirror.bindings.json")))
        pid = "smoke-modes-probe"
        wanted = {a: {**b, "mode": "compatible"} for a, b in seeded.items()}
        try:
            status, body, _ = bearer(token, f"/api/library/{pid}", {"yaml": yaml, "message": "smoke: modes", "bindings": wanted}, "PUT")
            if status != 200:
                raise RuntimeError(f"a save in mode compatible: HTTP {status} {body[:300]!r}")
            held = json.loads(bearer(token, f"/api/library/{pid}")[1])
            for alias, binding in held["bindings"].items():
                pin = binding.get("pin") or {}
                if binding["mode"] != "compatible" or not pin.get("version") or '"hash"' not in (pin.get("contract") or ""):
                    raise RuntimeError(f"'{alias}' was not pinned with its contract: {binding}")
            if any(p["cause"] == "unchecked" for p in held["bindingProblems"]):
                raise RuntimeError(f"a pinned binding is still unchecked: {held['bindingProblems']}")
            # A save that names no mode keeps the library's, as a studio's does.
            plain = {a: {"node": b["node"]} for a, b in seeded.items()}
            status, body, _ = bearer(token, f"/api/library/{pid}", {"yaml": yaml, "message": "smoke: no mode", "bindings": plain}, "PUT")
            again = json.loads(bearer(token, f"/api/library/{pid}")[1])
            if status != 200 or any(b["mode"] != "compatible" for b in again["bindings"].values()):
                raise RuntimeError(f"a save with no mode changed the modes: {status} {again['bindings']}")
            loose = {a: {**b, "mode": "tolerant"} for a, b in seeded.items()}
            status, body, _ = bearer(token, f"/api/library/{pid}/bindings", {"bindings": loose, "message": "smoke: tolerant"}, "PUT")
            tolerant = json.loads(bearer(token, f"/api/library/{pid}")[1])
            if status != 200 or any(b["mode"] != "tolerant" or not (b.get("pin") or {}).get("version") or (b["pin"].get("contract")) for b in tolerant["bindings"].values()):
                raise RuntimeError(f"tolerant bindings: {status} {tolerant['bindings']}")
            status, body, _ = bearer(token, f"/api/library/{pid}/bindings", {"bindings": seeded, "message": "smoke: strict"}, "PUT")
            strict = json.loads(bearer(token, f"/api/library/{pid}")[1])
            if status != 200 or any(b["mode"] != "strict" or b.get("pin") for b in strict["bindings"].values()):
                raise RuntimeError(f"strict bindings: {status} {strict['bindings']}")
        finally:
            try:
                call(f"/api/library/{pid}", None, "DELETE")
            except ApiError:
                pass
        return "a save pins each brick's contract from the node's own read; no mode keeps the mode; tolerant keeps the version; strict none"

    def health():
        # A supervisor reads /health with a service token whose identity the rights grant the kind monitor, and nothing else. The
        # identity is added here when the rights predate the seed (a stack keeps rights.json across restarts); the IdP client is in
        # idp-clients.json. The pipeline is armed for the check and undone after it.
        if not any(i["clientId"] == "monitor-1" for i in call("/api/rights")["identities"]):
            call("/api/rights/identities", {"clientId": "monitor-1", "name": "monitor-1", "description": "Supervisor", "kind": "monitor"})
        issuer = call("/api/rights")["idp"]["issuer"]
        status, monitor = sec.token_for(issuer, "monitor-1", "monitor-1-lab-secret")
        status_node, node = sec.token_for(issuer, "node-1", "node-1-lab-secret")
        if status != 200 or status_node != 200:
            raise RuntimeError(f"the IdP gave no token: monitor {status}, node {status_node}")
        pid = "customers-mirror"

        def read(path, token=monitor):
            code, body, _ = bearer(token, path, csrf=False)
            return code, (json.loads(body) if code == 200 else body[:200])

        call(f"/api/library/{pid}/deploy", {})
        try:
            code, report = read("/health/pipelines")
            armed = next((p for p in report["pipelines"] if p["id"] == pid), None) if code == 200 else None
            if armed is None or armed["status"] != "ok":
                raise RuntimeError(f"/health/pipelines {code}: {report if armed is None else (armed['status'], [c['code'] for c in armed['causes']])}")
            code_one, one = read(f"/health/pipelines/{pid}")
            if code_one != 200 or one["pipelines"][0]["status"] != "ok" or len(one["pipelines"][0]["fragments"]) < 2:
                raise RuntimeError(f"/health/pipelines/{pid} {code_one}: {one}")
            pages = call("/api/health")
            if next(p for p in pages["pipelines"] if p["id"] == pid)["status"] != "ok":
                raise RuntimeError(f"/api/health disagrees with /health: {pages}")
            tokens = {"monitor": monitor, "node": node, "forged": "not-a-token"}
            expected = {("/api/me", "monitor"): 401, ("/api/health", "monitor"): 401, ("/health/pipelines", "node"): 403, ("/health/pipelines", "forged"): 401}
            got = {k: bearer(tokens[k[1]], k[0], csrf=False)[0] for k in expected}
            if got != expected:
                raise RuntimeError(f"a monitor reaches /api, a node /health or a forgery /health: {got}")
        finally:
            try:
                call(f"/api/deployments/{pid}", None, "DELETE")
            except ApiError:
                pass
        code_gone, gone = read(f"/health/pipelines/{pid}")
        cause = gone["pipelines"][0]["causes"][0]["code"] if code_gone == 200 else None
        if code_gone != 200 or gone["status"] != "critical" or cause != "not-armed":
            raise RuntimeError(f"a pipeline no longer armed: {code_gone} {gone}")
        return f"armed {pid}: ok, {len(one['pipelines'][0]['fragments'])} fragments; the monitor reaches /health alone; undeployed: critical, not armed"

    def studio_routes():
        # dtpipe studio's view of the bricks, through a person's bearer token: the catalog, what a library
        # job's branches are (the coordinator decides), and a source brick's sample as Arrow, which dtpipe reads.
        token = studio_tokens(URL, "labdesigner")["access_token"]
        status, body, _ = bearer(token, "/api/catalog")
        catalog = json.loads(body)
        brick = next((b for b in catalog["bricks"] if b["key"] == "node-1/customers"), None)
        if status != 200 or not brick or brick["kind"] != "source" or "input" not in brick["branch"]:
            raise RuntimeError(f"the catalog has no source node-1/customers: {status} {body[:300]!r}")
        if not any(n["name"] == "runner-1" and n["role"] == "runner" for n in catalog["nodes"]):
            raise RuntimeError(f"the catalog lists no runner: {catalog['nodes']}")
        status, body, _ = bearer(token, "/api/bricks/recognize", {"job": mirror}, "POST")
        found = {r[k] for r in json.loads(body)["branches"].values() for k in ("match", "source", "sink") if r[k]}
        if status != 200 or not {"node-1/customers", "node-4/customers-mirror"} <= found:
            raise RuntimeError(f"recognition of customers-mirror: {status} {body[:300]!r}")
        status, arrow, headers = bearer(token, "/api/bricks/node-1/customers/sample?rows=7", None, "POST")
        if status != 200 or headers.get("Content-Type") != "application/vnd.apache.arrow.stream":
            raise RuntimeError(f"sample -> {status} {headers.get('Content-Type')} {arrow[:200]!r}")
        folder = os.path.join(STATE, "studio-sample")
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, "customers.arrows")
        with open(path, "wb") as f:
            f.write(arrow)
        out = subprocess.run([DTPIPE, "--input", "arrow:" + path, "--output", "csv:-", "--no-stats"], capture_output=True, text=True, timeout=60)
        rows = [line for line in out.stdout.splitlines() if line.strip()]
        if out.returncode != 0 or len(rows) != 8:
            raise RuntimeError(f"dtpipe read {len(rows)} line(s) of the sample: {out.stderr[-300:]}")
        status, body, _ = bearer(token, "/api/library")
        entry = next((e for e in json.loads(body) if e["id"] == "customers-mirror"), None)
        status_doc, body_doc, _ = bearer(token, "/api/library/customers-mirror")
        doc = json.loads(body_doc)
        if status != 200 or not entry or status_doc != 200 or not entry["commit"] or doc["commit"] != entry["commit"] or "yaml" not in doc:
            raise RuntimeError(f"library listing {status}, customers-mirror {status_doc}: {entry} {str(doc)[:200]}")
        return (f"catalog, recognition of customers-mirror's bricks, a 7-row Arrow sample read back by dtpipe ({len(arrow):,} bytes), "
                "the library listed and a job read at the version of its file")

    def brick_preview():
        # A source's preview is its declared branch's sample: the columns carry the Arrow's own types, and the
        # first values are those of the sample Arrow, read back by dtpipe.
        token = studio_tokens(URL, "labdesigner")["access_token"]
        status, arrow, _ = bearer(token, "/api/bricks/node-1/customers/sample?rows=5", None, "POST")
        if status != 200:
            raise RuntimeError(f"sample -> {status}")
        folder = os.path.join(STATE, "studio-sample")
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, "customers-preview.arrows")
        with open(path, "wb") as f:
            f.write(arrow)
        out = subprocess.run([DTPIPE, "--input", "arrow:" + path, "--output", "csv:-", "--no-stats"], capture_output=True, text=True, timeout=60)
        expected = [line for line in out.stdout.splitlines() if line.strip()]
        preview = call("/api/bricks/node-1/customers/preview", {})
        shown = [line for line in preview["csv"].splitlines() if line.strip()]
        names = [c["name"] for c in preview.get("columns") or []]
        if out.returncode != 0 or not expected or shown[:len(expected)] != expected:
            raise RuntimeError(f"preview {shown[:3]} does not start with the sample's {expected[:3]}")
        if names != expected[0].split(",") or not all(c["type"] for c in preview["columns"]):
            raise RuntimeError(f"preview columns {preview.get('columns')} are not the sample's header {expected[0]}")
        return f"{len(names)} typed columns and the first {len(expected) - 1} rows are the sample's"

    def versions():
        # Every node host announces what its dtpipe binary says of itself: here, the release binary; on the
        # stack, the one in each node's own image, built from the tree without its git metadata.
        def says(name):
            command = (["docker", "compose", "-f", os.path.join(LAB_DIR, "compose.yaml"), "exec", "-T", name, "dtpipe"] if STACK else [DTPIPE])
            return subprocess.run([*command, "--version"], capture_output=True, text=True, timeout=30).stdout.split()[1]

        nodes = call("/api/catalog")["nodes"]
        expected = {n["name"]: says(n["name"]) for n in nodes}
        wrong = {n["name"]: n["dtpipeVersion"] for n in nodes if n["dtpipeVersion"] != expected[n["name"]]}
        if wrong or not nodes:
            raise RuntimeError(f"expected {expected} of the nodes: {wrong or 'no node'}")
        return f"{len(nodes)} nodes announce dtpipe {', '.join(sorted(set(expected.values())))}"

    check("versions", versions)
    check("the studio's routes", studio_routes)
    check("a brick's preview", brick_preview)
    check("a source brick by a hair", source_by_a_hair)
    check("a sink brick by a hair", sink_by_a_hair)
    check("relays", relays)
    check("a library save's check", library_save_check)
    check("a sink that refuses", sink_refuses)
    check("binding modes", binding_modes)
    check("health", health)
    return failures[0]


def sandbox_pass():
    """On sandbox nodes any reader or writer is run, but a data node still only reads and writes: compute
    goes to the runner, and the node refuses it."""
    failures = [0]
    check = checker("sandbox", failures)
    _, narrower, _ = mirror_jobs()

    def loose():
        # A runner runs any fragment in either mode: only data nodes announce one.
        strict_nodes = [n["name"] for n in call("/api/nodes") if "raw-fragments" not in n["capabilities"] and n["role"] != "Runner"]
        if strict_nodes:
            raise RuntimeError(f"still strict: {', '.join(strict_nodes)}")
        return "every data node host announces itself sandbox"

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

    def cut_on_its_node():
        # The cut branch reads node-2's sales while the job joins two other nodes' data: only node-2
        # can run dtpipe split on it, and the planner asks it to.
        yaml = open(os.path.join(LIBRARY_SEED, "revenue-by-country.yaml")).read()
        placement = {"orders^": "node-2", "orders": "runner-1", "customers": "node-1", "products": "node-3",
                     "revenue": "runner-1", "load": "node-4"}
        plan = call("/api/plan", {"pipelineId": "smoke-cut", "yaml": yaml, "cuts": [{"branch": "orders", "at": 0}],
                                  "placement": placement})
        if plan["errors"]:
            raise RuntimeError(f"the plan has errors: {plan['errors']}")
        head = next((u for u in plan["units"] if u["id"] == "orders^"), None)
        if head is None:
            raise RuntimeError("no head for the cut branch")
        return f"orders cut on node-2, {len(plan['fragments'])} fragments planned"

    def processor():
        job = "a:\n  input: generate:10\nb:\n  from: a\n  provider-options:\n    sql:\n      query: SELECT * FROM a\nc:\n  from: b\n  output: duck:${{LAB_WAREHOUSE_DB}}\n"
        return refused("sandbox-probe", job, [], {"a": "node-2", "b": "node-2", "c": "node-4"}, "b", "node-2", "sql processor")

    def disjoint_together():
        # Three raw pipelines, a slow source each, queued together: P1 and P2 share only the runner, which
        # never counts, so they are in flight together; P3 reads node-1 like P1, so it starts once P1 is over.
        # Nothing is written anywhere: every sink is null.
        job = lambda: ("gen:\n  input: generate:15000\n  provider-options:\n    generate:\n      rows-per-second: 1000\n"
                       "sink:\n  from: gen\n  output: \"null:\"\n")
        shapes = {"smoke-par-1": ("node-1", "runner-1"), "smoke-par-2": ("node-2", "runner-1"), "smoke-par-3": ("node-1", "node-3")}
        runs = {}
        try:
            for pid, (source, sink) in shapes.items():
                call("/api/deploy", {"pipelineId": pid, "yaml": job(), "cuts": [], "placement": {"gen": source, "sink": sink}})
            for pid in shapes:
                runs[pid] = call("/api/runs", {"pipelineId": pid})["runId"]
            finals = {pid: until(lambda: _finished(rid), RUN_TIMEOUT, f"{rid} to finish") for pid, rid in runs.items()}
            bad = {pid: f"{f['state']} {f.get('refusal') or ''}" for pid, f in finals.items() if f["state"] != "succeeded"}
            if bad:
                raise RuntimeError(f"not every run succeeded: {bad}")
            p1, p2, p3 = (finals[p] for p in shapes)
            if not overlap(p1, p2):
                raise RuntimeError(f"P1 and P2 share only the runner but ran in turn: {span(p1)} {span(p2)}")
            if span(p3)[0] < span(p1)[1]:
                raise RuntimeError(f"P3 started before P1, which holds node-1, finished: {span(p1)} {span(p3)}")
            together = (min(span(p1)[1], span(p2)[1]) - max(span(p1)[0], span(p2)[0])).total_seconds()
            return f"P1 and P2 in flight together for {together:.1f}s through runner-1; P3 waited for node-1"
        finally:
            for rid in runs.values():
                if not _finished(rid):
                    call("/api/runs/cancel", {"runId": rid})
            for rid in runs.values():
                until(lambda: _finished(rid), RUN_TIMEOUT, f"{rid} to end")
            for pid in shapes:
                undeploy(pid)
            try:
                until(lambda: not left_behind("smoke-par-"), 10, "the undeploy")
            except RuntimeError:
                raise RuntimeError(f"still deployed: {', '.join(left_behind('smoke-par-'))}") from None

    check("sandbox nodes", loose)
    check("disjoint nodes, together", disjoint_together)
    check("any reader or writer", any_reader)
    check("a join on a data node", compute)
    check("a cut after a transformer", cut_leaves_a_transformer)
    check("a cut on its node", cut_on_its_node)
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


REPLICA_PROBE = "replicas-probe"
REPLICA_NODE = "node-4"
_hosts = {}  # the processes this script started, by the name lab.sh gives them: kept to be reaped


def node_replicas(node=REPLICA_NODE):
    return next(n for n in call("/api/nodes") if n["name"] == node)["replicas"]


def replica_pid(replica):
    """A host announces where it runs as `machine:pid`."""
    return int(replica["origin"].rsplit(":", 1)[1])


def host_name(pid):
    """The name lab.sh gave the process with this pid (node-4, node-4.r2), from the pid files."""
    run = os.path.join(STATE, "run")
    for file in os.listdir(run):
        if file.endswith(".pid") and open(os.path.join(run, file)).read().strip() == str(pid):
            return file[:-len(".pid")]
    raise RuntimeError(f"no pid file names process {pid}")


def host_exited(pid):
    child = next((c for c in _hosts.values() if c.pid == pid), None)
    if child is not None:
        return child.poll() is not None
    try:
        os.kill(pid, 0)
        return False
    except ProcessLookupError:
        return True


def signal_host(pid, sig):
    """Sends `sig` to a host and returns once the process is gone."""
    os.kill(pid, sig)
    until(lambda: host_exited(pid), 30, f"process {pid} to exit")


def start_host(name, node_file=None):
    """Starts the host `name` the way lab.sh does, on the node file given (another declaration of its bricks) or its own."""
    env = {**os.environ, "DTPIPE": DTPIPE, "LAB_STATE": STATE}
    env.pop("LAB_NODE_SANDBOX", None)
    env.pop("LAB_NODE_FILE", None)
    if node_file:
        env["LAB_NODE_FILE"] = node_file
    with open(os.path.join(STATE, "logs", name + ".log"), "w") as log:
        child = subprocess.Popen([os.path.join(LAB_DIR, "lab.sh"), "exec-node", re.sub(r"\.r\d+$", "", name), URL],
                                 env=env, stdout=log, stderr=log, start_new_session=True)
    with open(os.path.join(STATE, "run", name + ".pid"), "w") as f:
        f.write(str(child.pid))
    _hosts[name] = child


def await_replicas(count, what, timeout=90):
    """Exactly `count` replicas of the node, all connected: the replicas of a host that is gone are listed until its grace is out."""
    def ready():
        replicas = node_replicas()
        return replicas if len(replicas) == count and all(r["connected"] for r in replicas) else None
    return until(ready, timeout, what)


def host_pid(name):
    with open(os.path.join(STATE, "run", name + ".pid")) as f:
        return int(f.read())


def declared_apart():
    """The node file of a replica that declares `customers-mirror` over another table than the lab's."""
    node = json.load(open(os.path.join(LAB_DIR, "nodes", REPLICA_NODE + ".json")))
    brick = next(b for b in node["bricks"] if b["id"] == "customers-mirror")
    brick["branch"]["provider-options"]["duck-writer"]["table"] = "customers_elsewhere"
    os.makedirs(os.path.join(STATE, "replicas"), exist_ok=True)
    path = os.path.join(STATE, "replicas", REPLICA_NODE + ".json")
    with open(path, "w") as f:
        json.dump(node, f)
    return path


def replicas_pass():
    """node-4 carried by two hosts, and the replica model played against them: the replica a run goes to is named in its
    journal; a host that stops says so, is gone at once and removes its directory; a host that is cut is listed disconnected, then
    lost once its grace has run out, and the next run goes to the other; a replica that announces another declaration of a brick is out for a plan made from the first, and with
    none left the run is refused, naming the brick, until the pipeline is deployed again. The pipeline is deployed under
    its own id and no plan is saved; the hosts are started again as the lab starts them when the pass ends."""
    failures = [0]
    check = checker("replicas", failures)
    if STACK:
        print(f"FAIL  replicas {'setup':28} the compose stack runs one host per node: this pass needs the local lab")
        return 1
    mirror, _, elsewhere = mirror_jobs()
    fragment = f"{REPLICA_PROBE}@{REPLICA_NODE}"
    declared = {}  # host -> "lab" or "apart": which declaration of its bricks the host was last started with
    hosts = []

    def run_probe():
        """One run of the probe, to its verdict: the final record, whose result names the replica of each fragment."""
        run = call("/api/runs", {"pipelineId": REPLICA_PROBE})
        return until(lambda: _finished(run["runId"]), RUN_TIMEOUT, f"{run['runId']} to finish")

    def ran_on(final):
        """The host a succeeded run's fragment on the node ran on."""
        if final["state"] != "succeeded":
            raise RuntimeError(f"{final['runId']} {final['state']}: {final.get('refusal') or final.get('description')}")
        bad = [e for e in final["result"]["edgeCounts"] if e["sent"] is None or e["sent"] != e["received"]]
        if bad:
            raise RuntimeError(f"edge counts disagree: {bad}")
        pid = replica_pid(final["result"]["replicas"][fragment])
        if pid not in [host_pid(h) for h in hosts]:
            raise RuntimeError(f"the journal names process {pid}, which is none of the node's hosts")
        return host_name(pid)

    def deployed_on():
        """How many replicas of the node could run the deployed fragment now, as the deployment reports it."""
        return next(f["replicas"] for d in call("/api/deployments") if d["pipelineId"] == REPLICA_PROBE
                    for f in d["fragments"] if f["name"] == fragment)

    def disagree():
        return next(n for n in call("/api/nodes") if n["name"] == REPLICA_NODE)["replicasDisagree"]

    def launch(name, node_file=None):
        start_host(name, node_file)
        declared[name] = "apart" if node_file else "lab"

    def restart(name, node_file=None):
        """The host `name` stopped if it runs, then started on `node_file` (its own declaration by default)."""
        pid = host_pid(name)
        if not host_exited(pid):
            signal_host(pid, signal.SIGTERM)
        until(lambda: all(replica_pid(r) != pid for r in node_replicas()), 30, f"{name} to leave the registry")
        launch(name, node_file)
        await_replicas(2, f"{name} to be back")

    try:
        replicas = node_replicas()
        if len(replicas) != 2 or not all(r["connected"] for r in replicas):
            raise RuntimeError(f"{REPLICA_NODE} must be carried by two connected hosts (LAB_REPLICAS={REPLICA_NODE}=2), "
                               f"it has {[(r['id'], r['connected']) for r in replicas]}")
        hosts.extend(sorted(host_name(replica_pid(r)) for r in replicas))
        declared.update({h: "lab" for h in hosts})
        apart = declared_apart()
        call("/api/deploy", {"pipelineId": REPLICA_PROBE, "yaml": mirror, "cuts": [], "placement": MIRROR_PLACEMENT})
        first = []  # the host of the first run, then the other

        def named():
            first.append(ran_on(run_probe()))
            first.append(next(h for h in hosts if h != first[0]))
            return f"ran on {first[0]} (pid {host_pid(first[0])}); the deployment counts {deployed_on()} replicas"
        check("a run names its replica", named)
        if not first:
            raise RuntimeError("no run to build on")
        x, y = first

        def process_directory(host):
            """The directory a host makes for its process under the node's: named by the instance id, which the replica id prefixes."""
            replica = next(r for r in node_replicas() if replica_pid(r) == host_pid(host))
            return glob.glob(os.path.join(STATE, REPLICA_NODE, replica["id"] + "*"))

        def stopped():
            if not process_directory(x):
                raise RuntimeError(f"{x} has no process directory under {REPLICA_NODE}/ while it runs")
            directory = process_directory(x)
            stopped_at = time.time()
            signal_host(host_pid(x), signal.SIGTERM)
            until(lambda: len(node_replicas()) == 1, 20, f"{x} to leave the registry")
            left = time.time() - stopped_at
            if any(os.path.exists(d) for d in directory):
                raise RuntimeError(f"{x} stopped in order and left its process directory {directory}")
            on = ran_on(run_probe())
            if on != y:
                raise RuntimeError(f"the next run went to {on}, not to {y}")
            return f"{x} stopped, gone from the registry in {left:.1f}s with no grace waited, its directory removed; the next run ran on {y}"
        check("a stopped replica leaves", stopped)

        def cut():
            launch(x)
            await_replicas(2, f"{x} to be back")
            os.kill(host_pid(y), signal.SIGKILL)
            until(lambda: host_exited(host_pid(y)), 10, f"{y} to die")
            until(lambda: any(not r["connected"] for r in node_replicas()), 30, f"{y} to be seen cut")
            cut_at = time.time()
            until(lambda: len(node_replicas()) == 1, 120, f"{y} to be lost")
            lost_after = time.time() - cut_at
            on = ran_on(run_probe())
            if on != x:
                raise RuntimeError(f"the next run went to {on}, not to {x}")
            return (f"{y} cut, listed disconnected, lost {lost_after:.0f}s later, once its grace had run out; the next run ran on {x}, "
                    f"which came after the deployment")
        check("a cut replica is lost", cut)

        def one_apart():
            launch(y)
            await_replicas(2, f"{y} to be back")
            if deployed_on() != 2:
                raise RuntimeError(f"the deployment counts {deployed_on()} replicas before the update")
            restart(x, apart)
            until(disagree, 30, "the replicas to disagree")
            if deployed_on() != 1:
                raise RuntimeError(f"the deployment counts {deployed_on()} replicas with one of two updated")
            ran = [ran_on(run_probe()) for _ in range(3)]
            if set(ran) != {y}:
                raise RuntimeError(f"runs went to {ran}; only {y} composes the plan")
            return f"{x} announces another declaration of the brick; 3 runs of the plan as made all ran on {y}"
        check("an updated replica is passed over", one_apart)

        def none_left():
            restart(y, apart)
            until(lambda: not disagree(), 30, "the replicas to agree")
            if deployed_on() != 0:
                raise RuntimeError(f"the deployment counts {deployed_on()} replicas with both updated")
            started = time.time()
            final = run_probe()
            said = final.get("refusal") or final.get("description") or ""
            if final["state"] == "succeeded" or "changed since the plan was made" not in said:
                raise RuntimeError(f"expected a refusal naming the changed brick, got {final['state']}: {said}")
            held = [f"{n['name']}/{r['id']}" for n in call("/api/nodes") for r in n["replicas"] if r["runs"]]
            if held:
                raise RuntimeError(f"replicas still hold runs after the refusal: {held}")
            return f"refused after {time.time() - started:.1f}s: {said[:110]}"
        check("none left composes the plan", none_left)

        def deployed_again():
            undeploy(REPLICA_PROBE)
            call("/api/deploy", {"pipelineId": REPLICA_PROBE, "yaml": elsewhere, "cuts": [], "placement": MIRROR_PLACEMENT})
            return f"deployed again from the declaration now announced, ran on {ran_on(run_probe())}"
        check("deployed again, it runs", deployed_again)
    except Exception as e:
        failures[0] += 1
        print(f"FAIL  replicas {'setup':28} {e}")
    finally:
        undeploy(REPLICA_PROBE)
        try:
            for name in hosts:
                if declared[name] != "lab" or host_exited(host_pid(name)):
                    restart(name)
            await_replicas(2, "both hosts to be back as the lab starts them")
        except Exception as e:
            failures[0] += 1
            print(f"FAIL  replicas {'restore':28} the hosts are not back as the lab starts them: {e}")
    return failures[0]


def main():
    ctx = Context()
    failures = (catalog_pass(ctx) if ONLY in (None, "lab") else 0) + (library_pass(ctx) if ONLY in (None, "library") else 0) \
        + (rights_pass() if ONLY in (None, "rights") else 0) + (faults_pass() if ONLY in (None, "faults") else 0) \
        + (bricks_pass() if ONLY in (None, "bricks") else 0) + (sandbox_pass() if ONLY in (None, "sandbox") else 0) \
        + (replicas_pass() if ONLY == "replicas" else 0)
    return 1 if failures else 0


def _finished(run_id):
    """A run is over once the journal holds it: the lab records a run when it ends."""
    return next((r for r in call("/api/runs") if r["runId"] == run_id), None)


if __name__ == "__main__":
    sys.exit(main())
