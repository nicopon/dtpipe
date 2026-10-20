#!/usr/bin/env python3
"""Checks the lab's test IdP (DtPipe.Coordinator.TrialIdp) end to end, as its clients use it.

    python3 tools/idp_check.py [http://127.0.0.1:5190]

Client credentials (a node), authorization code with PKCE (the coordinator's front end, through
the login form; dtpipe studio, a native client on the loopback, with refresh tokens), and the
refusals that matter: a wrong secret, a wrong password, a redirect URI the client did not register. Standard library only; prints one line per check and exits non-zero on the
first failure.
"""
import base64
import hashlib
import html.parser
import http.cookiejar
import json
import secrets
import sys
import urllib.error
import urllib.parse
import urllib.request

IDP = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5190").rstrip("/")
UI = {"id": "coordinator-ui", "secret": "coordinator-ui-lab-secret",
      "redirect": "http://127.0.0.1:5180/signin-oidc", "logout": "http://127.0.0.1:5180/signout-callback-oidc"}
NODE = {"id": "node-1", "secret": "node-1-lab-secret"}
STUDIO = {"id": "dtpipe-studio", "audience": "dtpipe-coordinator"}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar), NoRedirect)


def call(url, form=None):
    """(status, headers, body) of one request; redirects are returned, never followed."""
    data = urllib.parse.urlencode(form).encode() if form is not None else None
    try:
        with opener.open(urllib.request.Request(url, data=data), timeout=15) as r:
            return r.status, r.headers, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read().decode()


def claims(jwt):
    payload = jwt.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


def check(label, condition, detail=""):
    print(("ok   " if condition else "FAIL ") + label + ("" if condition else f": {detail}"))
    if not condition:
        sys.exit(1)


class Hidden(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.fields = {}

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "input" and a.get("type") == "hidden":
            self.fields[a["name"]] = a.get("value", "")


def hidden_fields(page):
    parser = Hidden()
    parser.feed(page)
    return parser.fields


# Discovery
status, _, body = call(IDP + "/.well-known/openid-configuration")
check("discovery answers", status == 200, status)
meta = json.loads(body)
check("issuer is the configured one", meta["issuer"].rstrip("/") == IDP, meta["issuer"])
for grant in ("authorization_code", "client_credentials", "refresh_token"):
    check(f"grant {grant} advertised", grant in meta["grant_types_supported"], meta["grant_types_supported"])

# Client credentials: a node
status, _, body = call(meta["token_endpoint"], {"grant_type": "client_credentials", "client_id": NODE["id"], "client_secret": NODE["secret"]})
check("node-1 obtains a token", status == 200, body)
token = claims(json.loads(body)["access_token"])
check("its subject is its client id", token.get("sub") == NODE["id"], token)
check("its audience is the coordinator", token.get("aud") in ("dtpipe-coordinator", ["dtpipe-coordinator"]), token)
check("it carries no group", not any(str(s).startswith("transportr:group:") for s in str(token.get("scope", "")).split()), token)

status, _, body = call(meta["token_endpoint"], {"grant_type": "client_credentials", "client_id": NODE["id"], "client_secret": "wrong"})
check("a wrong secret is refused", status in (400, 401) and "invalid_client" in body, (status, body))

status, _, body = call(meta["token_endpoint"], {"grant_type": "client_credentials", "client_id": UI["id"], "client_secret": UI["secret"]})
check("the front end may not use client credentials", status in (400, 401, 403), (status, body))

# Authorization code with PKCE: the coordinator's front end signs toto in
verifier = secrets.token_urlsafe(48)
challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
state = secrets.token_urlsafe(12)
authorize = meta["authorization_endpoint"] + "?" + urllib.parse.urlencode({
    "client_id": UI["id"], "response_type": "code", "redirect_uri": UI["redirect"], "scope": "openid email profile",
    "state": state, "code_challenge": challenge, "code_challenge_method": "S256"})

status, headers, _ = call(authorize)
login_url = urllib.parse.urljoin(IDP, headers.get("Location", ""))
check("an anonymous user is sent to the login form", status == 302 and "/account/login" in login_url, (status, login_url))

status, _, page = call(login_url)
fields = hidden_fields(page)
check("the login form carries an antiforgery token", status == 200 and len(fields) >= 2, fields)

status, _, page = call(IDP + "/account/login", {**fields, "user": "toto", "password": "toto"})
check("a wrong password is refused", status == 401 and "wrong password" in page, status)

status, headers, _ = call(IDP + "/account/login", {**hidden_fields(page), "user": "toto", "password": "totopwd"})
back = urllib.parse.urljoin(IDP, headers.get("Location", ""))
check("toto / totopwd signs in and goes back to the request", status == 302 and back.startswith(meta["authorization_endpoint"]), (status, back))

status, headers, _ = call(back)
redirect = urllib.parse.urlparse(headers.get("Location", ""))
query = urllib.parse.parse_qs(redirect.query)
check("the code reaches the registered redirect URI",
      status == 302 and f"{redirect.scheme}://{redirect.netloc}{redirect.path}" == UI["redirect"] and query.get("state") == [state],
      (status, headers.get("Location")))

exchange = {"grant_type": "authorization_code", "code": query["code"][0], "redirect_uri": UI["redirect"],
            "client_id": UI["id"], "client_secret": UI["secret"], "code_verifier": verifier}
status, _, body = call(meta["token_endpoint"], {**exchange, "code_verifier": secrets.token_urlsafe(48)})
check("a code with the wrong PKCE verifier is refused", status == 400 and "invalid_grant" in body, (status, body))

status, headers, _ = call(back)
code = urllib.parse.parse_qs(urllib.parse.urlparse(headers.get("Location", "")).query)["code"][0]
status, _, body = call(meta["token_endpoint"], {**exchange, "code": code})
check("the code is exchanged for tokens", status == 200, body)
tokens = json.loads(body)
identity = claims(tokens["id_token"])
check("the identity token names toto", identity.get("sub") == "toto" and identity.get("name") == "toto", identity)
check("and gives the email toto@dtpipe.test", identity.get("email") == "toto@dtpipe.test", identity)
check("and no role or group", not any(k in identity for k in ("role", "roles", "groups")), identity)
check("the front end's access token names no audience", "aud" not in claims(tokens["access_token"]), claims(tokens["access_token"]))


# dtpipe studio: a native public client on the loopback, any port, with refresh tokens
def studio_authorize(redirect, verifier):
    return meta["authorization_endpoint"] + "?" + urllib.parse.urlencode({
        "client_id": STUDIO["id"], "response_type": "code", "redirect_uri": redirect,
        "scope": "openid email profile offline_access", "state": "s",
        "code_challenge": base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode(),
        "code_challenge_method": "S256"})


studio_verifier = secrets.token_urlsafe(48)
studio_redirect = "http://127.0.0.1:53123/callback"
status, headers, _ = call(studio_authorize(studio_redirect, studio_verifier))
landed = headers.get("Location", "")
check("the studio's code reaches the loopback at a port of its choosing",
      status == 302 and landed.startswith(studio_redirect + "?"), (status, landed))
for other in ("http://localhost:53123/callback", "http://127.0.0.1:53123/other"):
    status, headers, _ = call(studio_authorize(other, studio_verifier))
    check(f"another host or path is refused: {other}", status == 400 and not headers.get("Location", "").startswith(other), (status, headers.get("Location")))

studio_code = urllib.parse.parse_qs(urllib.parse.urlparse(landed).query)["code"][0]
status, _, body = call(meta["token_endpoint"], {"grant_type": "authorization_code", "code": studio_code, "redirect_uri": studio_redirect,
                                                "client_id": STUDIO["id"], "code_verifier": studio_verifier})
check("the studio exchanges its code without a secret", status == 200, body)
studio = json.loads(body)
access = claims(studio["access_token"])
check("its access token names the coordinator, the studio and toto's email",
      access.get("aud") in (STUDIO["audience"], [STUDIO["audience"]]) and access.get("client_id") == STUDIO["id"]
      and access.get("sub") == "toto" and access.get("email") == "toto@dtpipe.test", access)
check("and carries no role or group", not any(k in access for k in ("role", "roles", "groups")), access)
check("a refresh token comes with it", bool(studio.get("refresh_token")), studio.keys())

refresh = studio["refresh_token"]
for turn in (1, 2):
    status, _, body = call(meta["token_endpoint"], {"grant_type": "refresh_token", "refresh_token": refresh, "client_id": STUDIO["id"]})
    check(f"refresh {turn} gives a new access token", status == 200 and "access_token" in body, (status, body))
    renewed = json.loads(body)
    again = claims(renewed["access_token"])
    check(f"refresh {turn} keeps the audience and the client", again.get("aud") == access.get("aud") and again.get("client_id") == STUDIO["id"], again)
    # A rolling refresh token replaces the one spent: the next refresh uses the new one.
    refresh = renewed.get("refresh_token") or refresh

wrong = authorize.replace(urllib.parse.quote(UI["redirect"], safe=""), urllib.parse.quote("http://evil.test/cb", safe=""))
status, headers, _ = call(wrong)
check("an unregistered redirect URI is refused", status == 400 and "evil.test" not in headers.get("Location", ""), (status, headers.get("Location")))

# Logout
status, headers, _ = call(meta["end_session_endpoint"] + "?" + urllib.parse.urlencode(
    {"id_token_hint": tokens["id_token"], "post_logout_redirect_uri": UI["logout"]}))
check("logout returns to the front end", status == 302 and headers.get("Location", "").startswith(UI["logout"]), (status, headers.get("Location")))
status, headers, _ = call(authorize)
check("and the session is gone", status == 302 and "/account/login" in headers.get("Location", ""), (status, headers.get("Location")))

print("IdP checks passed.")
