"""The branch each brick of the lab declares, and a job with its brick references replaced by those branches.

The coordinator composes a job by reference and the engine never reads `brick:`, so what runs a job whole, as the
witness does, needs the text the node holds. It is read from `nodes/*.json`, the owners' own declarations, never from
the coordinator. A branch is written as one line of flow mapping: JSON is YAML, and dtpipe reads it as it reads a block.
"""
import json, os, re

LAB = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SOURCE = re.compile(r"^(?P<alias>[A-Za-z][\w-]*):\n  input: brick:(?P<key>[\w-]+/[\w-]+)\n", re.M)
SINK = re.compile(r"^(?P<alias>[A-Za-z][\w-]*):\n  from: (?P<feed>[\w-]+)\n  output: brick:(?P<key>[\w-]+/[\w-]+)\n", re.M)


def declared():
    """`node/brick` -> the branch its node declares."""
    bricks = {}
    nodes = os.path.join(LAB, "nodes")
    for name in sorted(os.listdir(nodes)):
        with open(os.path.join(nodes, name)) as f:
            node = json.load(f)
        for brick in node.get("bricks", []):
            bricks[f"{node['name']}/{brick['id']}"] = brick["branch"]
    return bricks


def expand(yaml):
    """`yaml` with every branch that references a brick written out as the branch its node declares."""
    bricks = declared()

    def branch(key):
        if key not in bricks:
            raise RuntimeError(f"no node file declares brick {key}")
        return bricks[key]

    def source(m):
        return f"{m['alias']}: {json.dumps(branch(m['key']))}\n"

    def sink(m):
        return f"{m['alias']}: {json.dumps({'from': m['feed'], **branch(m['key'])})}\n"

    return SOURCE.sub(source, SINK.sub(sink, yaml))


def references(yaml):
    """alias -> `node/brick`, for every branch of `yaml` that references a brick."""
    return {m["alias"]: m["key"] for m in (*SOURCE.finditer(yaml), *SINK.finditer(yaml))}
