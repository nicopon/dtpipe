#!/usr/bin/env python3
"""End-to-end check of the lab: every catalog pipeline, distributed, against its monolithic witness.

For each pipelines/*.yaml:
  1. the witness: the unsplit job run by dtpipe alone, writing a separate warehouse file;
  2. the lab: the catalog layout deployed through the coordinator's API, then run;
  3. the verdict: outcome Succeeded, every edge's two row counts equal, and the job's
     `# lab-check:` query returning the same row on both warehouses.

Usage: smoke.py <lab-url> <dtpipe> <state-dir> [pipeline-id-substring ...]
Exits 0 when every selected pipeline passes, 1 otherwise. Standard library only.
"""
import csv, io, json, os, subprocess, sys, time, urllib.error, urllib.request

URL, DTPIPE, STATE = sys.argv[1].rstrip("/"), sys.argv[2], sys.argv[3]
SELECTED = sys.argv[4:]
RUN_TIMEOUT = 180


def call(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(URL + path, data, {"Content-Type": "application/json"}, method="POST" if data is not None else "GET")
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


def main():
    nodes = call("/api/nodes")
    variables = {d["variable"]: d for n in nodes for d in n["datasets"]}
    warehouse = next(d for v, d in variables.items() if "WAREHOUSE" in v)
    witness_db = os.path.join(STATE, "witness", "warehouse.duckdb")
    os.makedirs(os.path.dirname(witness_db), exist_ok=True)

    failures = 0
    for entry in call("/api/pipelines"):
        if SELECTED and not any(s in entry["id"] for s in SELECTED):
            continue
        started = time.time()
        try:
            check = check_of(entry["yaml"])
            if not check:
                raise RuntimeError("no `# lab-check:` query in the job header")

            # 1. Witness: the same job, unsplit, on this machine; only the warehouse differs.
            env = {**os.environ, **{v: d["path"] for v, d in variables.items()}}
            env[warehouse["variable"]] = witness_db
            job = os.path.join(STATE, "witness", entry["id"] + ".yaml")
            with open(job, "w") as f:
                f.write(entry["yaml"])
            out = subprocess.run([DTPIPE, "--job", job, "--no-stats"], capture_output=True, text=True, env=env)
            if out.returncode != 0:
                raise RuntimeError(f"witness failed: {out.stderr.strip().splitlines()[-3:]}")

            # 2. The distributed run, through the coordinator.
            body = {"pipelineId": entry["id"], "yaml": entry["yaml"], "cuts": entry["cuts"], "placement": entry["placement"]}
            until(lambda: (call("/api/state").get("run") or {}).get("state") != "running", RUN_TIMEOUT, "an earlier run")
            plan = call("/api/deploy", body)
            run = until(lambda: _start_run(), 60, "the lab to accept a run")
            final = until(lambda: _finished(run["runId"]), RUN_TIMEOUT, f"{run['runId']} to finish")

            # 3. Verdict.
            if final["state"] != "succeeded":
                raise RuntimeError(f"{final['runId']} {final['state']}: {final.get('refusal') or final.get('description')}")
            bad = [e for e in final["result"]["edgeCounts"] if e["sent"] is None or e["sent"] != e["received"]]
            if bad:
                raise RuntimeError(f"edge counts disagree: {bad}")
            expected = query(f"duck:{witness_db}", check)
            actual = query(f"{warehouse['engine']}:{warehouse['path']}", check)
            if expected != actual:
                raise RuntimeError(f"check differs\n    witness: {expected}\n    lab:     {actual}")

            rows = sum(e["received"] for e in final["result"]["edgeCounts"])
            print(f"PASS  {entry['id']:28} {len(plan['fragments'])} fragments, {len(plan['edges'])} edges, "
                  f"{rows:,} rows crossed  {actual[1]}  ({time.time() - started:.1f}s)")
        except Exception as e:
            failures += 1
            print(f"FAIL  {entry['id']:28} {e}")
    return 1 if failures else 0


def _start_run():
    try:
        return call("/api/runs", {})
    except RuntimeError as e:
        if "409" in str(e):
            return None
        raise


def _finished(run_id):
    state = call("/api/state")
    run = state.get("run") or {}
    if run.get("runId") == run_id and run.get("state") != "running":
        # Wait for the re-arm too, so the next pipeline deploys onto settled nodes.
        return run
    return None


if __name__ == "__main__":
    sys.exit(main())
