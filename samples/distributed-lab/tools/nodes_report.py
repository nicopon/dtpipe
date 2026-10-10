#!/usr/bin/env python3
"""Whether every node host is online and announces the given mode.

Usage: nodes_report.py <lab-url> <strict|sandbox> <timeout-seconds> <node-count>
Exits 0 when they all do, 1 when they do not within the timeout (0 checks once). The hosts'
own announcements are the only truth about their mode. Standard library only.
"""
import json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lab_session import Session  # noqa: E402

url, mode, timeout, count = sys.argv[1].rstrip("/"), sys.argv[2], float(sys.argv[3]), int(sys.argv[4])
deadline = time.time() + timeout
session = Session(url)
while True:
    try:
        nodes = session.call("/api/nodes")
        # A runner runs any fragment in either mode: only data nodes announce one. Every connected replica of a node must
        # announce the mode, not only the one the node shows.
        def in_mode(n):
            return n["role"] == "Runner" or all(("raw-fragments" in r["capabilities"]) == (mode == "sandbox") for r in n["replicas"] if r["connected"])
        if len(nodes) >= count and all(n["online"] and in_mode(n) for n in nodes):
            sys.exit(0)
    except Exception:
        pass
    if time.time() >= deadline:
        sys.exit(1)
    time.sleep(0.5)
