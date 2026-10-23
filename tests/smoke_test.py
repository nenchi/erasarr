#!/usr/bin/env python3
"""End-to-end check against a running Erasarr (stdlib only), with fake Jellyfin and Radarr servers.
CI runs it against the built image (docker run --network host); locally against `python app/app.py`.

  python3 tests/smoke_test.py [http://127.0.0.1:5000]

Covers: first-user registration, login/logout, every page renders, config import, a dry run (changes
nothing) and a live run (the watched movie gets unmonitored and its file deleted in Radarr), and that
Jellyfin is only called with the Authorization header (v1.1.2 fix: Jellyfin 12 rejects legacy auth).
"""
import http.cookiejar
import json
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000").rstrip("/")
VERSION = re.search(r'APP_VERSION = "([^"]+)"', open("app/app.py").read())[1]
API_KEY = "smoke-key"
IMDB = "tt0133093"

seen = {"jellyfin": [], "radarr": []}  # (method, path, headers) per request
radarr_movie = {"id": 1, "title": "The Matrix", "imdbId": IMDB, "monitored": True, "tags": [],
                "movieFile": {"id": 11, "size": 4_000_000_000}}


class Fake(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, code, data):
        body = json.dumps(data).encode() if not isinstance(data, bytes) else data
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n)) if n else None


class FakeJellyfin(Fake):
    def do_GET(self):
        url = urllib.parse.urlsplit(self.path)
        seen["jellyfin"].append(("GET", self.path, dict(self.headers)))
        if url.path == "/System/Ping":  # unauthenticated on real Jellyfin too
            return self.reply(200, b'"Jellyfin Server"')
        if self.headers.get("Authorization") != f'MediaBrowser Token="{API_KEY}"':
            return self.reply(401, {"error": "Jellyfin 12: legacy auth disabled"})
        if url.path == "/System/Info":
            return self.reply(200, {"ServerName": "fake-jellyfin", "ProductName": "Jellyfin Server"})
        if url.path == "/Users":
            return self.reply(200, [{"Id": "u1", "Name": "alice", "Policy": {"IsDisabled": False}}])
        if url.path == "/Users/u1/Items":
            items = [{"Id": "m1", "Type": "Movie", "Name": "The Matrix", "ProviderIds": {"Imdb": IMDB},
                      "UserData": {"Played": True, "LastPlayedDate": "2026-01-01T20:00:00Z"}}]
            return self.reply(200, {"Items": items, "TotalRecordCount": len(items)})
        self.reply(404, {"error": url.path})


class FakeRadarr(Fake):
    def do_GET(self):
        path = urllib.parse.urlsplit(self.path).path
        seen["radarr"].append(("GET", path, {}))
        if path == "/api/v3/system/status":
            return self.reply(200, {"appName": "Radarr", "version": "5.0.0"})
        if path == "/api/v3/movie":
            return self.reply(200, [radarr_movie])
        if path == "/api/v3/tag":
            return self.reply(200, [])
        self.reply(404, {"error": path})

    def do_PUT(self):
        path = urllib.parse.urlsplit(self.path).path
        body = self.body()
        seen["radarr"].append(("PUT", path, body))
        if path == "/api/v3/movie/1":
            radarr_movie.update(body)
            return self.reply(202, radarr_movie)
        self.reply(404, {"error": path})

    def do_DELETE(self):
        path = urllib.parse.urlsplit(self.path).path
        seen["radarr"].append(("DELETE", path, None))
        self.reply(200 if path == "/api/v3/moviefile/11" else 404, {})


def serve(handler):
    srv = ThreadingHTTPServer(("0.0.0.0", 0), handler)  # 0.0.0.0: reachable from the container (host network)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{srv.server_address[1]}"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


browser = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()), NoRedirect)


def call(path, form=None, files=None):
    """GET, or POST a form / multipart upload. Returns (status, body text, Location)."""
    data, headers = None, {}
    if files:
        boundary = uuid.uuid4().hex
        parts = [f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"; filename="{fname}"\r\n'
                 f'Content-Type: application/json\r\n\r\n{content}\r\n' for name, (fname, content) in files.items()]
        data = ("".join(parts) + f"--{boundary}--\r\n").encode()
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    elif form is not None:
        data = urllib.parse.urlencode(form).encode()
    req = urllib.request.Request(BASE + path, data=data, headers=headers)
    try:
        with browser.open(req, timeout=60) as r:
            return r.status, r.read().decode(), r.headers.get("Location", "")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(), e.headers.get("Location", "")


def ok(cond, what):
    if not cond:
        sys.exit(f"FAIL: {what}")
    print(f"ok   {what}")


def main():
    jellyfin, radarr = serve(FakeJellyfin), serve(FakeRadarr)
    for _ in range(60):  # container / gunicorn start
        try:
            if call("/health")[0] == 200:
                break
        except OSError:
            pass
        time.sleep(1)
    code, body, _ = call("/health")
    ok(code == 200 and json.loads(body)["status"] == "ok", "health")

    # first start: everything leads to registration
    code, _, loc = call("/")
    ok(code == 302 and loc.endswith("/register"), "no user yet → register")
    ok(call("/register", {"username": "admin", "password": "short", "confirm_password": "short"})[0] == 200,
       "short password refused (form shown again)")
    code, _, loc = call("/register", {"username": "admin", "password": "smoke-pass-123", "confirm_password": "smoke-pass-123"})
    ok(code == 302 and loc.endswith("/login"), "register first user")
    ok(call("/register")[2].endswith("/login"), "registration closed after the first user")

    code, _, loc = call("/")
    ok(code == 302 and "/login" in loc, "logged out → login")
    code, body, _ = call("/login", {"username": "admin", "password": "wrong"})
    ok(code == 200 and "Invalid credentials" in body, "wrong password refused")
    code, _, loc = call("/login", {"username": "admin", "password": "smoke-pass-123"})
    ok(code == 302, "login")

    for page in ("/", "/servers", "/arr", "/settings", "/change-password"):
        code, body, _ = call(page)
        ok(code == 200 and "<html" in body.lower(), f"page {page}")
    ok(VERSION in call("/")[1], f"version {VERSION} shown")

    cfg = {
        "media_servers": [{"id": "s1", "name": "fake-jellyfin", "type": "jellyfin", "enabled": True,
                           "address": jellyfin, "api_key": API_KEY}],
        "radarr_instances": [{"id": "r1", "name": "fake-radarr", "enabled": True, "address": radarr, "api_key": API_KEY}],
        "sonarr_instances": [],
        "action_rules": [{"id": "rule1", "name": "Movies watched", "enabled": True, "applies_to_users": [],
                          "applies_to_servers": [], "content_type": "movies", "precondition_tag": "",
                          "actions": {"unmonitor": True, "delete_file": True, "add_tag": "", "delay_days": 0}}],
        "schedule": {"mode": "manual"},
    }
    code, body, _ = call("/api/import-config", files={"config_file": ("erasarr-config.json", json.dumps(cfg))})
    ok(code == 200 and json.loads(body)["ok"], "import config")
    exported = json.loads(call("/api/export-config")[1])
    ok(exported["media_servers"][0]["address"] == jellyfin and exported["action_rules"][0]["id"] == "rule1", "export config")

    # dry run: reads everything, changes nothing
    ok(call("/run", {"dry_run": "1"})[0] == 302, "dry run")
    ok(not [r for r in seen["radarr"] if r[0] != "GET"] and radarr_movie["monitored"], "dry run changed nothing in Radarr")
    ok(any(p.startswith("/Users/u1/Items") for _, p, _ in seen["jellyfin"]), "dry run read Jellyfin watch history")

    # live run: the watched movie is unmonitored and its file deleted
    ok(call("/run", {"dry_run": "0"})[0] == 302, "live run")
    ok(("DELETE", "/api/v3/moviefile/11", None) in seen["radarr"], "movie file deleted in Radarr")
    ok(radarr_movie["monitored"] is False, "movie unmonitored in Radarr")
    state = json.loads(call("/api/state")[1])
    ok(f"rule1:movie:{IMDB}" in state.get("processed", {}), "movie marked as processed")
    n = len(seen["radarr"])
    call("/run", {"dry_run": "0"})
    ok(not [r for r in seen["radarr"][n:] if r[0] != "GET"], "second run doesn't process it again")

    # Jellyfin 12: never the legacy api_key query or X-Emby-Token
    authed = [(p, h) for m, p, h in seen["jellyfin"] if not p.startswith("/System/Ping")]
    ok(authed and all("api_key" not in p and "X-Emby-Token" not in h for p, h in authed),
       "Jellyfin called with Authorization header only")

    code, _, loc = call("/logout")
    ok(code == 302 and call("/")[0] == 302 and call("/api/state")[0] == 302, "logout")
    print("smoke test passed")


if __name__ == "__main__":
    main()
