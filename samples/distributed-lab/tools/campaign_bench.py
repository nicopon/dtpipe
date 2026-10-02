"""Which processes belong to the bench. A campaign judges what the lab did, so it starts only when every lab
process is one its pid files name: a host or a toxiproxy left over from an earlier session reconnects through the
proxies the campaign creates and doubles a node, which no verdict shows."""
import os
import re
import subprocess

# What a campaign must not share the machine with. An explicit dotnet verb is work in progress; a resident build
# server or an MSBuild node is only work when it uses the processor.
BUILD_VERBS = {"build", "test", "publish", "restore", "msbuild", "vstest", "pack", "clean"}
RESIDENT = ("MSBuild.dll", "VBCSCompiler", "testhost")
BUSY_CPU = 5.0


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


def _ps_full():
    """(pid, cpu %, executable, args) for every process; the executable is the first word of the arguments, since
    `comm` can hold spaces."""
    out = subprocess.run(["ps", "-axo", "pid=,pcpu=,args="], capture_output=True, text=True).stdout
    rows = []
    for line in out.splitlines():
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        try:
            rows.append((int(parts[0]), float(parts[1]), parts[2].split()[0], parts[2]))
        except ValueError:
            continue
    return rows


def _cwd(pid):
    out = subprocess.run(["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"], capture_output=True, text=True).stdout
    return next((l[1:] for l in out.splitlines() if l.startswith("n")), "")


def busy_machine(state_dir, node_names, transportr_dir=None):
    """What else works on this machine: a compilation, a test run, or a TransportR process (found by its working
    directory as well as its command line, since `dotnet test` run inside the TransportR checkout names neither). The
    lab's own processes are not counted. Each entry is (pid, why, args); none means the machine is at rest."""
    parents = {pid: ppid for pid, ppid, _ in _ps()}
    mine = set()  # this process and its ancestors: the shell that launched the campaign names whatever it was given
    pid = os.getpid()
    while pid and pid not in mine:
        mine.add(pid)
        pid = parents.get(pid, 0)
    lab = {row[0] for row in lab_processes()} | known_pids(state_dir, list(node_names) + ["coordinator"])
    found = []
    root = os.path.realpath(transportr_dir) + os.sep if transportr_dir else None
    for pid, cpu, comm, args in _ps_full():
        if pid in lab or pid in mine:
            continue
        base = os.path.basename(comm)
        words = args.split()
        if base == "dotnet" or "MSBuild" in base or "VBCSCompiler" in base or base.startswith("testhost"):
            verb = next((w for w in words[1:] if not w.startswith("-") and not w.endswith(".dll") and "/" not in w), "")
            if verb in BUILD_VERBS:
                found.append((pid, f"dotnet {verb}", args))
            elif any(r in args or r in base for r in RESIDENT) and cpu >= BUSY_CPU:
                found.append((pid, f"build server at {cpu:.0f}% cpu", args))
            elif root and (_cwd(pid) + os.sep).startswith(root):
                found.append((pid, "dotnet process inside the TransportR checkout", args))
        if "TransportR" in args and pid not in {f[0] for f in found}:
            found.append((pid, "TransportR on its command line", args))
    return found


def launched_binaries(state_dir, node_names):
    """The `--dtpipe` values the coordinator and the hosts were started with: the binary every fragment child runs."""
    values = set()
    for _, _, kind, args in lab_processes():
        if kind in ("host", "coordinator"):
            m = re.search(r"--dtpipe\s+(\S+)", args)
            if m:
                values.add(m.group(1))
    return values


def _git(repo, *args):
    return subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True).stdout.strip()


def frozen_problems(repo, dtpipe, state_dir, node_names, expected=None):
    """Why the version under test is not the frozen one, as a list of sentences (empty when it is).

    The frozen version is a commit (`expected`, else HEAD). The checks are neutral about where the SDK came from:
    the binary must start and report that commit's full hash, the binary the hosts were launched with must be the
    one given, the tracked tree the lab is rebuilt from (`lab.sh up` builds it) must be clean, and the lab's own
    assemblies must carry the frozen commit's hash (a binary built from a dirty tree carries it too: the clean-tree
    check is the other half)."""
    problems = []
    want = expected or _git(repo, "rev-parse", "HEAD")
    if not want:
        return ["no frozen commit: HEAD of the repository cannot be read"]
    binaries = {os.path.realpath(dtpipe)} | {os.path.realpath(b) for b in launched_binaries(state_dir, node_names)}
    for binary in sorted(binaries):
        try:
            out = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=30)
        except (OSError, subprocess.TimeoutExpired) as e:
            problems.append(f"{binary} does not start: {e}")
            continue
        if out.returncode != 0:
            problems.append(f"{binary} --version exits {out.returncode}: {(out.stderr or out.stdout).strip()[-200:]}")
            continue
        m = re.search(r"\+([0-9a-f]{7,40})\b", out.stdout)
        got = m.group(1) if m else ""
        if not got or not (want.startswith(got) or got.startswith(want)):
            problems.append(f"{binary} reports {out.stdout.strip()!r}, the frozen commit is {want}")
    if len(binaries) > 1:
        problems.append(f"the lab was launched with another dtpipe than the one given: {sorted(binaries)}")
    dirty = _git(repo, "status", "--porcelain", "--untracked-files=no", "--", "src", "samples/distributed-lab",
                 "Directory.Build.props").splitlines()
    if dirty:
        problems.append(f"{len(dirty)} tracked file(s) changed since the frozen commit, e.g. {dirty[0].strip()}")
    for dll in (os.path.join("node", "Lab.NodeHost.dll"), os.path.join("coordinator", "Lab.Coordinator.dll")):
        path = os.path.join(state_dir, "bin", dll)
        try:
            with open(path, "rb") as f:
                hashes = set(m.decode() for m in re.findall(rb"\d+\.\d+\.\d+\+([0-9a-f]{40})", f.read()))
        except OSError:
            problems.append(f"the lab's assembly is not built: {path}")
            continue
        if want not in hashes:
            problems.append(f"{os.path.basename(path)} was built at {sorted(hashes) or 'no commit'}, the frozen commit is {want}: "
                            "./lab.sh up rebuilds it")
    return problems
