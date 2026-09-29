"""The tail of every lab log, taken before a recovery restarts the lab (which truncates them)."""
import os


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
