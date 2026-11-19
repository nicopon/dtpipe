"""A host the rights do not know, as the lab's checks launch it.

Its token is valid (the IdP issues it) and the rights name no identity for it, so it waits for an
administrator: it proposes a name and a description, which authorise nothing. smoke.py's enrollment
pass and ui_check.py's "Waiting for approval" card both need one, on the local lab (a process of the
node host) or on the compose stack (a throwaway container of the stack's node image, on its
network). The stack is a target of the checks, never a second set of them. Standard library only.

    from enroll_host import UnknownHost
    with UnknownHost(state, issuer, url, dtpipe, stack=False) as host:
        ...

`state` is the lab's state directory (.state, or .stack for the stack); the host keeps its own under
`state/enroll`. `url` is the coordinator the process reaches; the container reaches it by its name.
"""
import json
import os
import subprocess
import time

CLIENT = "node-enroll"
SECRET = "node-enroll-lab-secret"
CONTAINER = "lab-enroll-host"
NETWORK = "dtpipe-lab-stack_default"
IMAGE = "dtpipe-lab-stack/node"


class UnknownHost:
    def __init__(self, state, issuer, url, dtpipe, stack=False, name="node-2", description="proposes to be node-2"):
        self.state, self.issuer, self.url, self.dtpipe, self.stack = state, issuer, url, dtpipe, stack
        self.name, self.description = name, description
        self._process = None

    def __enter__(self):
        root = os.path.join(self.state, "enroll")
        os.makedirs(root, exist_ok=True)
        config = os.path.join(root, "node.json")
        with open(config, "w") as f:
            json.dump({"name": self.name, "description": self.description, "clientId": CLIENT, "secret": SECRET}, f)
        quiet = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
        if self.stack:
            # A host the stack did not declare, reaching the coordinator directly and the IdP by the one name
            # browsers and containers share.
            subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
            self._process = subprocess.Popen(
                ["docker", "run", "--rm", "--init", "--name", CONTAINER, "--network", NETWORK,
                 "--user", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp", "-v", f"{root}:/enroll",
                 "--entrypoint", "dotnet", IMAGE, "/app/Lab.NodeHost.dll",
                 "--config", "/enroll/node.json", "--state", "/enroll", "--coordinator", "http://coordinator:8080",
                 "--idp", self.issuer, "--dtpipe", "/usr/local/bin/dtpipe"], **quiet)
        else:
            self._process = subprocess.Popen(
                ["dotnet", os.path.join(self.state, "bin", "node", "Lab.NodeHost.dll"), "--config", config,
                 "--state", root, "--coordinator", self.url, "--idp", self.issuer, "--dtpipe", self.dtpipe], **quiet)
        return self

    def __exit__(self, *exc):
        if self.stack:
            subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
        else:
            self._process.terminate()
        self._process.wait(10)


def forget(call):
    """Takes the host out of the coordinator: a host waiting stays in the queue for a day after it is gone,
    and one set aside is not shown again. Accepted under a throwaway name, its identity is removed at once.
    `call` is the check's way to reach /api."""
    if not any(p["subject"] == CLIENT for p in call("/api/rights")["pending"]):
        return
    call(f"/api/rights/pending/{CLIENT}/accept", {"name": "forgotten-host", "groups": ["crm"]})
    call(f"/api/rights/identities/{CLIENT}", None, "DELETE")
    deadline = time.time() + 30
    while time.time() < deadline and any(n["name"] == "forgotten-host" for n in call("/api/nodes")):
        time.sleep(0.3)
