"""Which processes belong to the bench. A campaign judges what the lab did, so it starts only when every lab
process is one its pid files name: a host or a toxiproxy left over from an earlier session reconnects through the
proxies the campaign creates and doubles a node, which no verdict shows."""
import os
import re
import subprocess


def _ps():
    out = subprocess.run(["ps", "-axo", "pid=,ppid=,comm="], capture_output=True, text=True).stdout
    rows = []
    for line in out.splitlines():
        m = re.match(r"\s*(\d+)\s+(\d+)\s+(.*)$", line)
        if m:
            rows.append((int(m.group(1)), int(m.group(2)), m.group(3).strip()))
    return rows


def _args(pid):
    return subprocess.run(["ps", "-o", "args=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()


def lab_processes():
    """(pid, ppid, kind, args): node hosts, coordinators, toxiproxy servers and fragment children."""
    rows = []
    for pid, ppid, comm in _ps():
        base = os.path.basename(comm)
        if base == "dotnet":
            args = _args(pid)
            if "Lab.NodeHost.dll" in args:
                rows.append((pid, ppid, "host", args))
            elif "Lab.Coordinator.dll" in args:
                rows.append((pid, ppid, "coordinator", args))
        elif base == "toxiproxy-server":
            rows.append((pid, ppid, "toxiproxy", ""))
        elif base == "dtpipe":
            args = _args(pid)
            if "--job" in args:
                rows.append((pid, ppid, "fragment child", args))
    return rows


def known_pids(state_dir, names):
    known = set()
    for name in names:
        try:
            with open(os.path.join(state_dir, "run", name + ".pid")) as f:
                known.add(int(f.read().strip()))
        except (OSError, ValueError):
            pass
    return known


def foreign(state_dir, node_names):
    """What runs that the lab's pid files do not name, and every toxiproxy and fragment child: between campaigns
    there are none."""
    known = known_pids(state_dir, list(node_names) + ["coordinator"])
    return [row for row in lab_processes()
            if row[2] in ("toxiproxy", "fragment child") or row[0] not in known]


def exactly_the_hosts(state_dir, node_names):
    """None when the node hosts running are exactly the ones the pid files name, else what is wrong."""
    hosts = [row for row in lab_processes() if row[2] == "host"]
    known = known_pids(state_dir, node_names)
    running = {row[0] for row in hosts}
    if len(hosts) != len(node_names) or running != known:
        return f"{len(hosts)} node host(s) running {sorted(running)}, the pid files name {sorted(known)}"
    return None
