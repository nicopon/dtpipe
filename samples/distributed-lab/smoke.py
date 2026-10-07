#!/usr/bin/env python3
"""End-to-end check of the lab: every pipeline, distributed, against its monolithic witness.

Six passes. The Lab view's: each pipelines/*.yaml with its catalog layout, deployed and run one
after the other. The library's: each library pipeline distributed automatically and its plan
saved, then all of them deployed at once and their runs queued together, as the page does.
The rights': the embedded IDP, the flow matrix refusing a plan and then, edited under a deployed
pipeline and, edited under a deployed pipeline, a transfer at run time; every edit is undone.
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


# /api serves people: smoke signs in as the lab's admin, through the IdP, as a browser would.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools"))
from lab_session import ApiError, Session  # noqa: E402
SESSION = Session(URL, timeout=RUN_TIMEOUT)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def call(path, body=None, method=None):
    return SESSION.call(path, body, method)


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
    lab = os.path.dirname(os.path.abspath(__file__))
    databases = {}
    for name in sorted(os.listdir(os.path.join(lab, "nodes"))):
        with open(os.path.join(lab, "nodes", name)) as f:
            node = json.load(f)
        for d in node.get("datasets", []):
            databases[d["variable"]] = os.path.join(STATE, node["name"], d["file"])
    return databases


def lab_query(variable, sql):
    """A query on a node's dataset, read by that node through the coordinator."""
    return list(csv.reader(io.StringIO(call("/api/query", {"variable": variable, "sql": sql})["csv"])))


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
        actual = lab_query(self.warehouse, check)
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
            # What the designer shows before saving is what the library distributes: the same fragments, nodes and edges.
            # Versions differ for a hand-written seed (compose writes the job in its own key order), so they are
            # compared on the designer's own text: composing what it composed gives the same fragments again.
            model = call("/api/design/decompose", {"yaml": doc["yaml"]})["model"]
            placed = call("/api/design/place", {"id": pid, "model": model})
            if placed["errors"] or not placed["plan"]:
                raise AssertionError(f"the designer's placement of {pid} failed: {placed['errors']}")
            shape = lambda p, versions=False: sorted((f["name"], f["node"], sorted((e["direction"], e["alias"]) for e in f["edges"]),
                                                      f["version"] if versions else None) for f in p["fragments"])
            if shape(placed["plan"]) != shape(plan):
                raise AssertionError(f"the designer's placement of {pid} differs from the library's: {shape(placed['plan'])} != {shape(plan)}")
            again = call("/api/design/place", {"id": pid, "model": call("/api/design/decompose", {"yaml": call("/api/design/compose", model)["yaml"]})["model"]})
            if shape(again["plan"], True) != shape(placed["plan"], True):
                raise AssertionError(f"the designer's job for {pid} is not stable under compose: its fragment versions change")
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
            plan = call("/api/library/customers-anonymized/distribute", {})["plan"]
            if plan["deployable"] or "crm -> runner" not in (plan.get("flowRejection") or ""):
                raise RuntimeError(f"plan not refused: {plan.get('flowRejection')}")
            # The designer's placement refuses the same way, with the same words.
            doc = call("/api/library/customers-anonymized")
            placed = call("/api/design/place", {"id": "customers-anonymized", "model": call("/api/design/decompose", {"yaml": doc["yaml"]})["model"]})["plan"]
            if placed["deployable"] or placed["flowRejection"] != plan["flowRejection"]:
                raise RuntimeError(f"the designer's placement was not refused like the library's: {placed['flowRejection']}")
            return "crm -> runner revoked: plan refused, in the library and in the designer"
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
            return call("/api/library/customers-anonymized/distribute", {})["plan"]

        def allowing(*edges):
            edited = {g: list(t) for g, t in matrix.items()}
            for sender, receiver in edges:
                edited.setdefault(sender, []).append(receiver)
            call("/api/rights/matrix", {"matrix": edited}, "PUT")

        try:
            for deployment in call("/api/deployments"):
                call(f"/api/deployments/{deployment['pipelineId']}", None, "DELETE")
            put_identity(["runner", "audit"])
            refused = plan()
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
            call("/api/library/customers-anonymized/distribute", {"save": True, "message": "smoke: several groups"})
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
            host.terminate()
            host.wait(10)
            if "node-enroll" in {i["clientId"] for i in call("/api/rights")["identities"]}:
                call("/api/rights/identities/node-enroll", None, "DELETE")
        until(lambda: node("enrolled-node") is None, 30, "the removed identity's node to leave the inventory")
        return "node-enroll proposing node-2: waits, node-2 untouched; accepted as enrolled-node in crm: online; removed: gone"

    def disabled():
        # The coordinator decides what an identity may do: disabled, node-3 is refused when it connects
        # again, whatever token the IdP still issues it; enabled again, it comes back on its own.
        def update(enabled):
            node3 = next(i for i in call("/api/rights")["identities"] if i["clientId"] == "node-3")
            call("/api/rights/identities/node-3", {"clientId": "node-3", "name": node3["name"], "description": node3["description"],
                                                  "groups": node3["groups"], "enabled": enabled}, "PUT")
            call("/api/rights/identities/node-3/reconnect", {})

        def online():
            return next((n["online"] for n in call("/api/nodes") if n["name"] == "node-3"), False)

        log = os.path.join(STATE, "logs", "coordinator.log")
        seen = os.path.getsize(log)
        try:
            update(False)

            def refused():
                with open(log, errors="replace") as f:
                    f.seek(seen)
                    return "Announcement of node-3 refused: 'node-3' is disabled" in f.read()
            until(refused, 30, "node-3's refusal while disabled")
            if online():
                raise RuntimeError("node-3 is online while its identity is disabled")
        finally:
            update(True)
        until(online, 30, "node-3 back online once enabled")
        return "disabled: node-3 refused when it connects again; enabled: back online"

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
            "designer POST /api/design/compose": (status(lambda: designer.call("/api/design/compose", {"title": "roles", "steps": []})), 200),
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

    check("identity provider", idp)
    check("roles and sessions", roles)
    check("enrollment", enrollment)
    check("identity, disabled", disabled)
    check("flow matrix, plan", matrix_plan)
    check("flow matrix, run time", matrix_runtime)
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
    def designer_check():
        # The server opens no node database: each source brick's node sends one row, dtpipe runs the
        # job over them on the server, and each sink brick's node judges its own target.
        names = sorted(n for n in os.listdir(LIBRARY_SEED) if n.endswith(".yaml"))
        for name in names:
            yaml = open(os.path.join(LIBRARY_SEED, name)).read()
            answer = call("/api/design/validate", call("/api/design/decompose", {"yaml": yaml})["model"])
            if not answer["ok"]:
                raise RuntimeError(f"{name}: {answer.get('errors')} {answer.get('dryRun')}")
        return f"{len(names)} library pipelines accepted"

    def sink_refuses():
        yaml = open(os.path.join(LIBRARY_SEED, "revenue-by-country.yaml")).read()
        wrong = yaml.replace("table: revenue_by_country\n      strategy: Recreate",
                             "table: customers_mirror\n      strategy: Upsert\n      key: customer_id")
        if wrong == yaml:
            raise RuntimeError("library-seed/revenue-by-country.yaml no longer loads its table this way")
        answer = call("/api/design/validate", call("/api/design/decompose", {"yaml": wrong})["model"])
        if answer["ok"] or "node-4/customers-mirror" not in (answer.get("dryRun") or ""):
            raise RuntimeError(f"node-4 should refuse: {answer}")
        return "node-4 refuses revenue rows for customers_mirror"

    check("a source brick by a hair", source_by_a_hair)
    check("a sink brick by a hair", sink_by_a_hair)
    check("relays", relays)
    check("a designer's check", designer_check)
    check("a sink that refuses", sink_refuses)
    return failures[0]


def sandbox_pass():
    """On sandbox nodes any reader or writer is run, but a data node still only reads and writes: compute
    goes to the runner, and the node refuses it."""
    failures = [0]
    check = checker("sandbox", failures)
    _, narrower, _ = mirror_jobs()

    def loose():
        # A runner runs any fragment in either mode: only data nodes announce one.
        strict_nodes = [n["name"] for n in call("/api/nodes") if not n["sandbox"] and n["role"] != "Runner"]
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

    check("sandbox nodes", loose)
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
