#!/usr/bin/env python3
"""Whether every node host is online and announces the given mode.

Usage: nodes_report.py <lab-url> <strict|sandbox> <timeout-seconds> <node-count>
Exits 0 when they all do, 1 when they do not within the timeout (0 checks once). The hosts'
own announcements are the only truth about their mode. Standard library only.
"""
import json, sys, time, urllib.request

url, mode, timeout, count = sys.argv[1].rstrip("/"), sys.argv[2], float(sys.argv[3]), int(sys.argv[4])
deadline = time.time() + timeout
while True:
    try:
        nodes = json.load(urllib.request.urlopen(url + "/api/nodes", timeout=20))
        if len(nodes) >= count and all(n["online"] and n["sandbox"] == (mode == "sandbox") for n in nodes):
            sys.exit(0)
    except Exception:
        pass
    if time.time() >= deadline:
        sys.exit(1)
    time.sleep(0.5)
