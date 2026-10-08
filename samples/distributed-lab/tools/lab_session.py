"""A person's session on the lab's coordinator, signed in through the IdP as a browser would.

The coordinator's /api serves people only, by a session cookie, and a request that changes something
carries the CSRF header. Every lab tool reaches /api through `Session`: it follows the real flow (the
page sends it to the IdP, the IdP's login form, the code back to /signin-oidc) as the lab's admin,
whom `lab.sh` lists in Coordinator:Roles:Admin. Standard library only.

    from lab_session import Session
    session = Session("http://127.0.0.1:5180")
    nodes = session.call("/api/nodes")
"""
import base64
import hashlib
import html.parser
import http.cookiejar
import json
import os
import secrets
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


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


STUDIO_CLIENT = "dtpipe-studio"


def studio_tokens(base, user, port=53190):
    """A person's tokens through dtpipe studio's client, as the studio obtains them: the coordinator's
    metadata names the IdP, the IdP's login form signs `user` in, and the code is read off the
    redirect to the loopback (nothing needs to listen there), then exchanged with its PKCE verifier."""
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar), _NoRedirect)

    def get(url, form=None):
        data = urllib.parse.urlencode(form).encode() if form is not None else None
        try:
            with opener.open(urllib.request.Request(url, data), timeout=30) as r:
                return r.status, r.headers, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.headers, e.read().decode()

    _, _, body = get(base.rstrip("/") + "/.well-known/oauth-protected-resource")
    issuer = json.loads(body)["authorization_servers"][0].rstrip("/")
    _, _, body = get(issuer + "/.well-known/openid-configuration")
    meta = json.loads(body)
    verifier = secrets.token_urlsafe(48)
    redirect = f"http://127.0.0.1:{port}/callback"
    authorize = meta["authorization_endpoint"] + "?" + urllib.parse.urlencode({
        "client_id": STUDIO_CLIENT, "response_type": "code", "redirect_uri": redirect,
        "scope": "openid email profile offline_access", "state": "lab",
        "code_challenge": base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode(),
        "code_challenge_method": "S256"})
    _, headers, _ = get(authorize)
    login = urllib.parse.urljoin(issuer, headers.get("Location", ""))
    _, _, page = get(login)
    parser = _Hidden()
    parser.feed(page)
    get(urllib.parse.urljoin(login, "/account/login"), {**parser.fields, "user": user, "password": user + "pwd"})
    _, headers, _ = get(authorize)
    landed = headers.get("Location", "")
    if not landed.startswith(redirect + "?"):
        raise RuntimeError(f"signing {user} in for the studio did not reach the loopback: {landed}")
    code = urllib.parse.parse_qs(urllib.parse.urlparse(landed).query)["code"][0]
    status, _, body = get(meta["token_endpoint"], {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect,
                                                    "client_id": STUDIO_CLIENT, "code_verifier": verifier})
    if status != 200:
        raise RuntimeError(f"the studio's code was not exchanged: {status} {body}")
    return json.loads(body)


if __name__ == "__main__":
    # lab.sh's shell helpers: `lab_session.py <url> <path> [METHOD]` prints the JSON answer.
    import sys
    method = sys.argv[3] if len(sys.argv) > 3 else None
    print(json.dumps(Session(sys.argv[1]).call(sys.argv[2], {} if method == "POST" else None, method)))
