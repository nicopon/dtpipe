"""Minimal Chrome DevTools Protocol driver, standard library only: headless Chrome, JS, screenshots.

Chrome's own --screenshot mode stops the page at load, before the lab's fetches finish, and its
--virtual-time-budget never ends while the event stream is open; driving the page over the DevTools
protocol has neither problem. Set CHROME to the browser binary if it is not at the macOS default.
"""
import base64, json, os, socket, struct, subprocess, tempfile, time, urllib.request, shutil

CHROME = os.environ.get("CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")

class WS:
    def __init__(self, url):
        assert url.startswith("ws://")
        hostport, path = url[5:].split("/", 1)
        host, port = hostport.split(":")
        self.s = socket.create_connection((host, int(port)))
        key = base64.b64encode(os.urandom(16)).decode()
        self.s.sendall((f"GET /{path} HTTP/1.1\r\nHost: {hostport}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                        f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf: buf += self.s.recv(4096)
        self.rest = buf.split(b"\r\n\r\n", 1)[1]
    def _recv(self, n):
        while len(self.rest) < n:
            chunk = self.s.recv(1 << 20)
            if not chunk: raise EOFError
            self.rest += chunk
        out, self.rest = self.rest[:n], self.rest[n:]
        return out
    def send(self, text):
        data = text.encode(); mask = os.urandom(4)
        header = bytes([0x81])
        n = len(data)
        if n < 126: header += bytes([0x80 | n])
        elif n < 65536: header += bytes([0x80 | 126]) + struct.pack(">H", n)
        else: header += bytes([0x80 | 127]) + struct.pack(">Q", n)
        self.s.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))
    def recv(self):
        msg = b""
        while True:
            b1, b2 = self._recv(2)
            n = b2 & 0x7F
            if n == 126: n = struct.unpack(">H", self._recv(2))[0]
            elif n == 127: n = struct.unpack(">Q", self._recv(8))[0]
            payload = self._recv(n)
            msg += payload
            if b1 & 0x80: return msg.decode()

class Browser:
    def __init__(self, width=1500, height=1400, port=9333):
        self.profile = tempfile.mkdtemp(prefix="cdp-")
        self.proc = subprocess.Popen([CHROME, "--headless=new", "--disable-gpu", "--no-first-run", f"--user-data-dir={self.profile}",
                                      f"--remote-debugging-port={port}", f"--window-size={width},{height}", "about:blank"],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            try:
                pages = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/json"))
                page = next(p for p in pages if p["type"] == "page"); break
            except Exception: time.sleep(0.2)
        self.ws = WS(page["webSocketDebuggerUrl"]); self.id = 0; self.console = []
        self.cmd("Runtime.enable"); self.cmd("Page.enable")
        self.cmd("Emulation.setDeviceMetricsOverride", width=width, height=height, deviceScaleFactor=1, mobile=False)
    def cmd(self, method, **params):
        self.id += 1; my = self.id
        self.ws.send(json.dumps({"id": my, "method": method, "params": params}))
        while True:
            m = json.loads(self.ws.recv())
            if m.get("method") == "Runtime.consoleAPICalled":
                self.console.append(" ".join(str(a.get("value", a.get("description", ""))) for a in m["params"]["args"]))
            if m.get("method") == "Runtime.exceptionThrown":
                self.console.append("EXCEPTION " + json.dumps(m["params"]["exceptionDetails"])[:400])
            if m.get("id") == my:
                if "error" in m: raise RuntimeError(m["error"])
                return m.get("result", {})
    def goto(self, url, wait=2.0):
        self.cmd("Page.navigate", url=url); time.sleep(wait)
    def js(self, expr):
        r = self.cmd("Runtime.evaluate", expression=expr, awaitPromise=True, returnByValue=True)
        if "exceptionDetails" in r: raise RuntimeError(r["exceptionDetails"])
        return r.get("result", {}).get("value")
    def wait_for(self, expr, timeout=30):
        t = time.time()
        while time.time() - t < timeout:
            if self.js(expr): return True
            time.sleep(0.25)
        return False
    def mouse(self, kind, x, y):
        self.cmd("Input.dispatchMouseEvent", type=kind, x=x, y=y, button="left",
                 buttons=0 if kind == "mouseReleased" else 1, clickCount=1, pointerType="mouse")
    def drag(self, a, b, steps=10):
        """Presses at a, moves to b in steps, releases: what a user's drag sends to pointer handlers."""
        self.mouse("mouseMoved", *a); self.mouse("mousePressed", *a)
        for i in range(1, steps + 1):
            self.mouse("mouseMoved", a[0] + (b[0] - a[0]) * i / steps, a[1] + (b[1] - a[1]) * i / steps)
        self.mouse("mouseReleased", *b)
    def center(self, selector):
        """Viewport centre of the first element matching selector, scrolled into view first."""
        r = self.js(f"(() => {{ const e = document.querySelector({json.dumps(selector)}); if (!e) return null;"
                    " e.scrollIntoView({block: 'nearest', inline: 'nearest'}); const b = e.getBoundingClientRect();"
                    " return [b.left + b.width / 2, b.top + b.height / 2]; })()")
        if r is None: raise RuntimeError(f"no element matches {selector}")
        return r
    def shot(self, path, full=True):
        params = {"format": "png"}
        if full:
            h = self.js("document.documentElement.scrollHeight")
            params["clip"] = {"x": 0, "y": 0, "width": 1500, "height": h, "scale": 1}
            params["captureBeyondViewport"] = True
        data = self.cmd("Page.captureScreenshot", **params)["data"]
        open(path, "wb").write(base64.b64decode(data)); return path
    def close(self):
        self.proc.terminate()
        try: self.proc.wait(5)
        except Exception: self.proc.kill()
        shutil.rmtree(self.profile, ignore_errors=True)
