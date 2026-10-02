"""The campaign's security cells: what a client that is not the coordinator can and cannot do.

Standard library only. A minimal SignalR client over the long-polling transport (JSON protocol) lets a
script call the hub the way a compromised peer would; nothing here needs a WebSocket library.
"""
import base64
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

RECORD_SEPARATOR = "\x1e"
COMMAND_HUB = "/hub/command"
LAB_HUB = "/lab"


def http(url, method="GET", headers=None, body=None, timeout=20):
    """(status, text) whatever the status is; never raises for an HTTP error."""
    request = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")
    except urllib.error.URLError as e:
        return 0, str(e.reason)


def token_for(base, client_id, secret):
    """(status, access token or None) from the coordinator's identity provider."""
    form = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": client_id, "client_secret": secret}).encode()
    status, text = http(base + "/connect/token", "POST", {"Content-Type": "application/x-www-form-urlencoded"}, form)
    try:
        return status, json.loads(text).get("access_token")
    except ValueError:
        return status, None


def bogus_jwt():
    """A well-formed token nobody signed: three segments, a random signature."""
    def part(data):
        return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()
    return f"{part({'alg': 'RS256', 'typ': 'JWT'})}.{part({'sub': 'node-1', 'scope': 'transportr:group:runner', 'iss': 'x'})}.c2ln"


def negotiate_status(base, hub_path, token):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return http(f"{base}{hub_path}/negotiate?negotiateVersion=1", "POST", headers)[0]


def stream_status(base, token):
    """The data plane's HTTP endpoint, called with no transfer behind it."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return http(f"{base}/api/stream/00000000-0000-0000-0000-000000000000/send", "POST", headers, b"")[0]


class LongPollingHub:
    """One connection to a SignalR hub: handshake, invoke, close."""

    def __init__(self, base, hub_path, token):
        self.base, self.hub_path, self.token = base, hub_path, token
        self.headers = {"Authorization": f"Bearer {token}"}
        self.messages = []
        self.stopped = False
        self._lock = threading.Lock()

    def open(self):
        status, text = http(f"{self.base}{self.hub_path}/negotiate?negotiateVersion=1", "POST", self.headers)
        if status != 200:
            raise RuntimeError(f"negotiate refused: HTTP {status}")
        answer = json.loads(text)
        self.id = answer.get("connectionToken") or answer["connectionId"]
        if not any(t["transport"] == "LongPolling" for t in answer.get("availableTransports", [])):
            raise RuntimeError("the hub does not offer long polling")
        self.url = f"{self.base}{self.hub_path}?id={urllib.parse.quote(self.id)}"
        self._poller = threading.Thread(target=self._poll, daemon=True)
        self._poller.start()
        self._post(json.dumps({"protocol": "json", "version": 1}) + RECORD_SEPARATOR)
        self._wait(lambda m: m == {} or m.get("type") is None and "error" not in m, 10)
        return self

    def _post(self, text):
        status, body = http(self.url, "POST", {**self.headers, "Content-Type": "text/plain;charset=UTF-8"}, text.encode())
        if status not in (200, 202):
            raise RuntimeError(f"POST refused: HTTP {status} {body[:100]}")

    def _poll(self):
        while not self.stopped:
            status, text = http(self.url, "GET", self.headers, timeout=30)
            if status == 204 or status == 0 and self.stopped:
                return
            if status != 200:
                with self._lock:
                    self.messages.append({"http": status})
                return
            for chunk in text.split(RECORD_SEPARATOR):
                if chunk.strip():
                    with self._lock:
                        self.messages.append(json.loads(chunk))

    def _wait(self, predicate, seconds):
        deadline = time.time() + seconds
        while time.time() < deadline:
            with self._lock:
                for message in self.messages:
                    if predicate(message):
                        return message
            time.sleep(0.05)
        raise RuntimeError("no answer from the hub in time")

    def invoke(self, target, *arguments):
        """The completion of the call: {"result": ...} or {"error": "..."}."""
        self._post(json.dumps({"type": 1, "invocationId": "1", "target": target, "arguments": list(arguments)}) + RECORD_SEPARATOR)
        return self._wait(lambda m: m.get("type") == 3 and m.get("invocationId") == "1", 15)

    def close(self):
        self.stopped = True
        http(self.url, "DELETE", self.headers, timeout=5)
