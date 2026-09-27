#!/usr/bin/env python3
"""End-to-end check of the lab: every pipeline, distributed, against its monolithic witness.

Two passes. The Lab view's: each pipelines/*.yaml with its catalog layout, deployed and run one
after the other. The library's: each library pipeline distributed automatically and its plan
saved, then all of them deployed at once and their runs queued together, as the page does.
For every pipeline:
  1. the witness: the unsplit job run by dtpipe alone, writing a separate warehouse file;
  2. the lab: the distributed run, through the coordinator;
  3. the verdict: outcome Succeeded, every edge's two row counts equal, and the job's
     `# lab-check:` query returning the same row on both warehouses.

Usage: smoke.py <lab-url> <dtpipe> <state-dir> [lab|library] [pipeline-id-substring ...]
A leading `lab` or `library` runs that pass alone.
Exits 0 when every selected pipeline passes, 1 otherwise. Standard library only.
"""
import csv, io, json, os, subprocess, sys, time, urllib.error, urllib.request

URL, DTPIPE, STATE = sys.argv[1].rstrip("/"), sys.argv[2], sys.argv[3]
PASSES = {"lab", "library"}
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


def main():
    ctx = Context()
    failures = (catalog_pass(ctx) if ONLY in (None, "lab") else 0) + (library_pass(ctx) if ONLY in (None, "library") else 0)
    return 1 if failures else 0


def _finished(run_id):
    """A run is over once the journal holds it: the lab records a run when it ends."""
    return next((r for r in call("/api/runs") if r["runId"] == run_id), None)


if __name__ == "__main__":
    sys.exit(main())
