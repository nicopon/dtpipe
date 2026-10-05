"""A person's session on the lab's coordinator, signed in through the IdP as a browser would.

The coordinator's /api serves people only, by a session cookie, and a request that changes something
carries the CSRF header. Every lab tool reaches /api through `Session`: it follows the real flow (the
page sends it to the IdP, the IdP's login form, the code back to /signin-oidc) as the lab's admin,
whom `lab.sh` lists in Coordinator:Roles:Admin. Standard library only.

    from lab_session import Session
    session = Session("http://127.0.0.1:5180")
    nodes = session.call("/api/nodes")
"""
import html.parser
import http.cookiejar
import json
import os
import urllib.error
import urllib.parse
import urllib.request

ADMIN = os.environ.get("LAB_ADMIN", "labadmin")
CSRF_HEADER = "X-Dtpipe-Csrf"


class _Hidden(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.fields = {}

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "input" and a.get("type") == "hidden":
            self.fields[a["name"]] = a.get("value", "")


class ApiError(RuntimeError):
    def __init__(self, path, status, message):
        super().__init__(f"{path}: HTTP {status}: {message}")
        self.status = status


class Session:
    def __init__(self, base, user=ADMIN, timeout=180):
        self.base = base.rstrip("/")
        self.user = user
        self.timeout = timeout
        self.jar = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.jar))
        self._signed_in = False

    def sign_in(self):
        """Opens the coordinator's page, signs in on the IdP's form, and follows the code back."""
        # Back on /api/me, which any person signed in may read, whatever their roles.
        with self._opener.open(self.base + "/account/login?returnUrl=/api/me", timeout=30) as page:
            login_url, text = page.geturl(), page.read().decode()
        if login_url.startswith(self.base):
            self._signed_in = True  # the IdP still had a session for this jar
            return self
        parser = _Hidden()
        parser.feed(text)
        form = urllib.parse.urlencode({**parser.fields, "user": self.user, "password": self.user + "pwd"}).encode()
        action = urllib.parse.urljoin(login_url, "/account/login")
        with self._opener.open(urllib.request.Request(action, form), timeout=30) as back:
            if not back.geturl().startswith(self.base):
                raise RuntimeError(f"signing {self.user} in did not lead back to the coordinator: {back.geturl()}")
        self._signed_in = True
        return self

    def cookie(self, name="dtpipe-coordinator"):
        """The session cookie, for a browser driven by a tool (tools/ui_check.py)."""
        if not self._signed_in:
            self.sign_in()
        return next(c for c in self.jar if c.name == name)

    def call(self, path, body=None, method=None):
        """JSON in, JSON out; raises ApiError on an HTTP error. Signs in on first use and once again
        when the session has ended."""
        if not self._signed_in:
            self.sign_in()
        for attempt in (1, 2):
            data = json.dumps(body).encode() if body is not None else None
            request = urllib.request.Request(self.base + path, data, {"Content-Type": "application/json", CSRF_HEADER: "1"},
                                             method=method or ("POST" if data is not None else "GET"))
            try:
                raw = self._opener.open(request, timeout=self.timeout).read()
                return json.loads(raw) if raw else {}
            except urllib.error.HTTPError as e:
                text = e.read().decode(errors="replace")
                if e.code == 401 and attempt == 1:
                    self.sign_in()
                    continue
                try:
                    payload = json.loads(text)
                    message = payload.get("error") or payload.get("detail") or payload.get("title") or text
                except ValueError:
                    message = text
                raise ApiError(path, e.code, message) from None


if __name__ == "__main__":
    # lab.sh's shell helpers: `lab_session.py <url> <path> [METHOD]` prints the JSON answer.
    import sys
    method = sys.argv[3] if len(sys.argv) > 3 else None
    print(json.dumps(Session(sys.argv[1]).call(sys.argv[2], {} if method == "POST" else None, method)))
