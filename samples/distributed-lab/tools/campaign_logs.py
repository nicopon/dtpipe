"""The lab logs: the tail of each, taken before a recovery restarts the lab (which truncates them), and a follower that
keeps every line, stamped, for the whole campaign."""
import os
import re
import threading
import time

# The lines a lab process logs when the machine's ephemeral ports run out. Linux says "Cannot assign", macOS "Can't
# assign": the match is on the part both share.
PORT_EXHAUSTION = re.compile(r"assign requested address|address already in use", re.IGNORECASE)
_port_lines = {"n": 0}


def capture(state_dir, lines=120):
    """{log name: its last `lines` lines}. Nothing here may fail a trial: a missing log is skipped."""
    out = {}
    logs = os.path.join(state_dir, "logs")
    try:
        names = sorted(os.listdir(logs))
    except OSError:
        return out
    for name in names:
        if not name.endswith(".log") or name.startswith("build"):
            continue
        try:
            with open(os.path.join(logs, name), errors="replace") as f:
                out[name] = [line.rstrip()[:300] for line in f.readlines()[-lines:]]
        except OSError:
            continue
    return out


def port_exhaustion_lines():
    """How many port-exhaustion lines the follower has read so far, across every log it follows."""
    return _port_lines["n"]


NAMES = ("coordinator", "node-1", "node-2", "node-3", "node-4", "runner-1")


def follow(state_dir, out_dir, names=NAMES):
    """Appends every line the lab logs gain to <out_dir>/<name>.all, each stamped with the time it was read, whatever
    happens to the logs (a restart truncates them). Returns the event that stops it. Never fails a trial."""
    os.makedirs(out_dir, exist_ok=True)
    stop = threading.Event()
    pos = {}
    for n in names:  # from where each log is now: what it holds already belongs to an earlier run
        try:
            pos[n] = os.path.getsize(os.path.join(state_dir, "logs", n + ".log"))
        except OSError:
            pos[n] = 0

    def run():
        while not stop.is_set():
            for n in names:
                path = os.path.join(state_dir, "logs", n + ".log")
                try:
                    size = os.path.getsize(path)
                    if size < pos[n]:
                        pos[n] = 0
                    if size == pos[n]:
                        continue
                    with open(path, "rb") as f:
                        f.seek(pos[n])
                        data = f.read(size - pos[n])
                    pos[n] = size
                    stamp = time.strftime("%H:%M:%S")
                    with open(os.path.join(out_dir, n + ".all"), "a") as out:
                        for line in data.decode("utf-8", "replace").splitlines():
                            out.write(f"{stamp} {line[:400]}\n")
                            if PORT_EXHAUSTION.search(line):
                                _port_lines["n"] += 1
                except OSError:
                    continue
            stop.wait(0.5)

    threading.Thread(target=run, daemon=True).start()
    return stop
