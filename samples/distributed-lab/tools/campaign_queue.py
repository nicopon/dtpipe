"""Whether the lab's run queue is stuck: a run still queued or running some seconds after its trial ended.

The campaign harness restarts the whole lab in that case (recorded), since no run will ever leave the
queue by itself and every later trial would fail on a 409 instead of measuring anything.
"""
import time


def queue_is_stuck(call, settle_seconds=5, looks=2):
    """`call` is the harness's HTTP helper. True when the queue is still not empty after `looks` looks."""
    for _ in range(looks):
        try:
            if not call("/api/runs/queue"):
                return False
        except Exception:
            return True
        time.sleep(settle_seconds)
    return True
