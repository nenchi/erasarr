#!/usr/bin/env python3
"""End-to-end check against a running Erasarr (stdlib only), with fake Jellyfin, Emby, Radarr and Sonarr servers.
CI runs it against the built image (docker run --network host); locally against `python app/app.py`.

  CLI="docker exec erasarr python cli.py --data /data" python3 tests/smoke_test.py http://127.0.0.1:5000
  CLI="python app/cli.py --data /tmp/erasarr-test"     python3 tests/smoke_test.py   # local

Web: first-user registration, login/logout, every page, config import/export.
Monitor, one dry run (changes nothing) then a live run:
  - movies (Jellyfin + Emby users): unmonitor, tag, delete file; a movie watched just now waits for the 7-day delay
  - episodes (Jellyfin only): keep the last 2 watched, so only older files are deleted; unwatched ones untouched
  - episodes (Emby only, precondition tag "cleanup"): only the tagged show is unmonitored
  - a second run changes nothing; with Sonarr's tag API down the precondition rule acts on nothing (not everything)
  - Jellyfin only gets the Authorization header, Emby only X-Emby-Token
CLI: cli.py --dry-run and a live run from the same database change nothing more.
"""
import http.cookiejar
import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000").rstrip("/")
CLI = shlex.split(os.environ.get("CLI", ""))
VERSION = re.search(r'APP_VERSION = "([^"]+)"', open("app/app.py").read())[1]
KEY = "smoke-key"
OLD = "2026-01-01T20:00:00Z"                                            # watched long ago
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")         # watched just now (inside the delay)

writes = []  # (app, method, path) of every change the fakes received
fail_sonarr_tags = [False]  # Sonarr's tag API down (precondition must then select nothing)
calls = {"jellyfin": [], "emby": []}  # (path, headers) of every media server request


def movie(mid, title, imdb):
    return {"id": mid, "title": title, "imdbId": imdb, "monitored": True, "tags": [], "movieFile": {"id": mid * 10, "size": 4_000_000_000}}


def episode(eid, series_id, s, e):
    return {"id": eid, "seriesId": series_id, "seasonNumber": s, "episodeNumber": e, "monitored": True,
            "hasFile": True, "episodeFileId": eid * 10}


radarr_movies = {1: movie(1, "The Matrix", "tt0133093"), 2: movie(2, "Inception", "tt1375666"), 3: movie(3, "Dune", "tt1160419")}
radarr_tags = []
sonarr_tags = [{"id": 7, "label": "cleanup"}]
sonarr_series = {1: {"id": 1, "title": "Show A", "tvdbId": 100, "tags": []},
                 2: {"id": 2, "title": "Show B", "tvdbId": 200, "tags": []},
                 3: {"id": 3, "title": "Show C", "tvdbId": 300, "tags": [7]}}
sonarr_episodes = {e["id"]: e for e in [episode(100 + n, 1, 1, n) for n in range(1, 6)] + [episode(201, 2, 1, 1), episode(301, 3, 1, 1)]}


def watched_movie(iid, title, imdb, when):
    return {"Id": iid, "Type": "Movie", "Name": title, "ProviderIds": {"Imdb": imdb}, "UserData": {"Played": True, "LastPlayedDate": when}}


def watched_episode(iid, show, tvdb, s, e):
    return {"Id": iid, "Type": "Episode", "Name": f"Episode {e}", "SeriesName": show, "ParentIndexNumber": s, "IndexNumber": e,
            "SeriesProviderIds": {"Tvdb": str(tvdb)}, "ProviderIds": {}, "UserData": {"Played": True, "LastPlayedDate": OLD}}


alice_items = [watched_movie("m1", "The Matrix", "tt0133093", OLD), watched_movie("m3", "Dune", "tt1160419", NOW)] + \
              [watched_episode(f"a{n}", "Show A", 100, 1, n) for n in range(1, 5)]
bob_items = [watched_movie("m2", "Inception", "tt1375666", OLD),
             watched_episode("b1", "Show B", 200, 1, 1), watched_episode("c1", "Show C", 300, 1, 1)]


class Fake(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, code, data):
        body = data if isinstance(data, bytes) else json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n)) if n else None

    @property
    def route(self):
        return urllib.parse.urlsplit(self.path).path


class FakeJellyfin(Fake):
    def do_GET(self):
        calls["jellyfin"].append((self.path, dict(self.headers)))
        if self.route == "/System/Ping":
            return self.reply(200, b'"Jellyfin Server"')
        if self.headers.get("Authorization") != f'MediaBrowser Token="{KEY}"':  # Jellyfin 12: legacy auth off
            return self.reply(401, {"error": "unauthorized"})
        if self.route == "/System/Info":
            return self.reply(200, {"ServerName": "fake-jellyfin"})
        if self.route == "/Users":
            return self.reply(200, [{"Id": "alice", "Name": "alice", "Policy": {"IsDisabled": False}}])
        if self.route == "/Users/alice/Items":
            return self.reply(200, {"Items": alice_items, "TotalRecordCount": len(alice_items)})
        self.reply(404, {"error": self.route})


class FakeEmby(Fake):
    def do_GET(self):
        calls["emby"].append((self.path, dict(self.headers)))
        if self.route == "/System/Ping":
            return self.reply(200, b'"Emby Server"')
        if self.headers.get("X-Emby-Token") != KEY:
            return self.reply(401, {"error": "unauthorized"})
        if self.route == "/System/Info":
            return self.reply(200, {"ServerName": "fake-emby"})
        if self.route == "/Users":
            return self.reply(200, [{"Id": "bob", "Name": "bob", "Policy": {"IsDisabled": False}}])
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        if self.route == "/Items" and q.get("userId") == ["bob"]:
            return self.reply(200, {"Items": bob_items, "TotalRecordCount": len(bob_items)})
        self.reply(404, {"error": self.route})


class FakeRadarr(Fake):
    def do_GET(self):
        if self.route == "/api/v3/system/status":
            return self.reply(200, {"appName": "Radarr", "version": "5.0.0"})
        if self.route == "/api/v3/movie":
            return self.reply(200, list(radarr_movies.values()))
        if self.route == "/api/v3/tag":
            return self.reply(200, radarr_tags)
        self.reply(404, {"error": self.route})

    def do_POST(self):
        body = self.body()
        writes.append(("radarr", "POST", self.route))
        if self.route == "/api/v3/tag":
            tag = {"id": len(radarr_tags) + 5, "label": body["label"]}
            radarr_tags.append(tag)
            return self.reply(201, tag)
        self.reply(404, {"error": self.route})

    def do_PUT(self):
        body = self.body()
        writes.append(("radarr", "PUT", self.route))
        m = re.fullmatch(r"/api/v3/movie/(\d+)", self.route)
        if m and int(m[1]) in radarr_movies:
            radarr_movies[int(m[1])].update(body)
            return self.reply(202, radarr_movies[int(m[1])])
        self.reply(404, {"error": self.route})

    def do_DELETE(self):
        writes.append(("radarr", "DELETE", self.route))
        self.reply(200, {})


class FakeSonarr(Fake):
    def do_GET(self):
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        if self.route == "/api/v3/system/status":
            return self.reply(200, {"appName": "Sonarr", "version": "4.0.0"})
        if self.route == "/api/v3/series":
            return self.reply(200, list(sonarr_series.values()))
        if self.route == "/api/v3/tag":
            return self.reply(500, {"error": "down"}) if fail_sonarr_tags[0] else self.reply(200, sonarr_tags)
        if self.route == "/api/v3/episode" and "seriesId" in q:
            return self.reply(200, [e for e in sonarr_episodes.values() if e["seriesId"] == int(q["seriesId"][0])])
        m = re.fullmatch(r"/api/v3/(series|episode|episodefile)/(\d+)", self.route)
        if m and m[1] == "series" and int(m[2]) in sonarr_series:
            return self.reply(200, sonarr_series[int(m[2])])
        if m and m[1] == "episode" and int(m[2]) in sonarr_episodes:
            return self.reply(200, sonarr_episodes[int(m[2])])
        if m and m[1] == "episodefile":
            return self.reply(200, {"id": int(m[2]), "size": 1_000_000_000})
        self.reply(404, {"error": self.route})

    def do_POST(self):
        body = self.body()
        writes.append(("sonarr", "POST", self.route))
        if self.route == "/api/v3/tag":
            tag = {"id": len(sonarr_tags) + 7, "label": body["label"]}
            sonarr_tags.append(tag)
            return self.reply(201, tag)
        self.reply(404, {"error": self.route})

    def do_PUT(self):
        body = self.body()
        writes.append(("sonarr", "PUT", self.route))
        m = re.fullmatch(r"/api/v3/(series|episode)/(\d+)", self.route)
        store = sonarr_series if m and m[1] == "series" else sonarr_episodes
        if m and int(m[2]) in store:
            store[int(m[2])].update(body)
            return self.reply(202, store[int(m[2])])
        self.reply(404, {"error": self.route})

    def do_DELETE(self):
        writes.append(("sonarr", "DELETE", self.route))
        self.reply(200, {})


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
        print("writes so far:", *writes, sep="\n  ")
        sys.exit(f"FAIL: {what}")
    print(f"ok   {what}")


def changes(app=None):
    return [w for w in writes if app in (None, w[0])]


def wait_for_app():
    for _ in range(60):  # container / gunicorn start
        try:
            if call("/health")[0] == 200:
                return
        except OSError:
            pass
        time.sleep(1)
    sys.exit("FAIL: app not reachable on " + BASE)


def web_basics():
    code, body, _ = call("/health")
    ok(code == 200 and json.loads(body)["status"] == "ok", "health (before any user exists)")
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
    ok(call("/login", {"username": "admin", "password": "smoke-pass-123"})[0] == 302, "login")
    for page in ("/", "/servers", "/arr", "/settings", "/change-password"):
        code, body, _ = call(page)
        ok(code == 200 and "<html" in body.lower(), f"page {page}")
    ok(VERSION in call("/")[1], f"version {VERSION} shown")


def config(jellyfin, emby, radarr, sonarr):
    rule = lambda rid, name, **kw: {"id": rid, "name": name, "enabled": True, "applies_to_users": [], "precondition_tag": "",
                                    "applies_to_servers": [], "content_type": "all", "schedule": {"use_global": True}, **kw}
    return {
        "media_servers": [
            {"id": "s1", "name": "fake-jellyfin", "type": "jellyfin", "enabled": True, "address": jellyfin, "api_key": KEY},
            {"id": "s2", "name": "fake-emby", "type": "emby", "enabled": True, "address": emby, "api_key": KEY}],
        "radarr_instances": [{"id": "r1", "name": "fake-radarr", "enabled": True, "address": radarr, "api_key": KEY}],
        "sonarr_instances": [{"id": "n1", "name": "fake-sonarr", "enabled": True, "address": sonarr, "api_key": KEY}],
        "action_rules": [
            rule("movies", "Movies, 7 day delay", content_type="movies",
                 actions={"unmonitor": True, "delete_file": True, "add_tag": "erasarr-done", "delay_days": 7}),
            rule("keep2", "Episodes keep last 2", content_type="episodes", applies_to_servers=["s1"],
                 actions={"unmonitor": True, "delete_file": True, "keep_last_episodes": 2, "delay_days": 0}),
            rule("tagged", "Tagged shows only", content_type="episodes", applies_to_servers=["s2"], precondition_tag="cleanup",
                 actions={"unmonitor": True, "delete_file": False, "delay_days": 0}),
        ],
        "schedule": {"mode": "manual"},
    }


def monitor(cfg):
    code, body, _ = call("/api/import-config", files={"config_file": ("erasarr-config.json", json.dumps(cfg))})
    ok(code == 200 and json.loads(body)["ok"], "import config (Jellyfin, Emby, Radarr, Sonarr, 3 rules)")
    exported = json.loads(call("/api/export-config")[1])
    ok([r["id"] for r in exported["action_rules"]] == ["movies", "keep2", "tagged"], "export config")

    ok(call("/run", {"dry_run": "1"})[0] == 302, "dry run")
    ok(not changes(), "dry run changed nothing in Radarr or Sonarr")
    ok(any(p.startswith("/Users/alice/Items") for p, _ in calls["jellyfin"]) and any(p.startswith("/Items?") for p, _ in calls["emby"]),
       "dry run read Jellyfin and Emby watch history")

    ok(call("/run", {"dry_run": "0"})[0] == 302, "live run")
    deleted = {p for app, m, p in writes if m == "DELETE"}
    # movies: Matrix (Jellyfin) + Inception (Emby) processed, Dune waits for the delay
    for mid, title in ((1, "The Matrix"), (2, "Inception")):
        m = radarr_movies[mid]
        ok(not m["monitored"] and f"/api/v3/moviefile/{mid * 10}" in deleted and m["tags"] == [radarr_tags[0]["id"]],
           f"movie {title}: unmonitored, tagged erasarr-done, file deleted")
    ok(radarr_movies[3]["monitored"] and "/api/v3/moviefile/30" not in deleted, "movie Dune watched just now: waits for the 7 day delay")
    ok(("radarr", "POST", "/api/v3/tag") in writes and radarr_tags[0]["label"] == "erasarr-done", "Radarr tag created")
    # episodes, keep last 2: E01-E04 watched → all unmonitored, only E01 + E02 files deleted, E05 untouched
    ok(all(not sonarr_episodes[101 + i]["monitored"] for i in range(4)), "Show A E01-E04 unmonitored")
    ok({p for p in deleted if "episodefile" in p} >= {"/api/v3/episodefile/1010", "/api/v3/episodefile/1020"}, "Show A E01 + E02 files deleted")
    ok(not deleted & {"/api/v3/episodefile/1030", "/api/v3/episodefile/1040"}, "Show A E03 + E04 files kept (keep last 2)")
    ok(sonarr_episodes[105]["monitored"] and "/api/v3/episodefile/1050" not in deleted, "Show A E05 (not watched) untouched")
    # precondition tag: Show C (tagged cleanup) unmonitored, Show B (no tag) untouched; this rule deletes nothing
    ok(not sonarr_episodes[301]["monitored"] and "/api/v3/episodefile/3010" not in deleted, "Show C (tagged): unmonitored, file kept")
    ok(sonarr_episodes[201]["monitored"] and not any("/201" in p or "2010" in p for _, _, p in writes), "Show B (not tagged): untouched")

    processed = json.loads(call("/api/state")[1]).get("processed", {})
    ok({"movies:movie:tt0133093", "movies:movie:tt1375666"} <= set(processed) and "movies:movie:tt1160419" not in processed,
       "processed: the two movies, not Dune")
    n = len(writes)
    call("/run", {"dry_run": "0"})
    ok(not [w for w in writes[n:] if w[1] == "DELETE"] and radarr_movies[3]["monitored"], "second run: nothing deleted again, Dune still waits")

    fail_sonarr_tags[0] = True
    call("/run", {"dry_run": "0"})
    fail_sonarr_tags[0] = False
    ok(sonarr_episodes[201]["monitored"], "Sonarr tag API down: precondition rule acts on nothing (Show B still untouched)")

    authed = [(p, h) for p, h in calls["jellyfin"] if not p.startswith("/System/Ping")]
    ok(authed and all("api_key" not in p and "X-Emby-Token" not in h for p, h in authed), "Jellyfin: Authorization header only")
    authed = [(p, h) for p, h in calls["emby"] if not p.startswith("/System/Ping")]
    ok(authed and all("api_key" not in p and h.get("X-Emby-Token") == KEY for p, h in authed), "Emby: X-Emby-Token header only")


def cli():
    if not CLI:
        return print("skip CLI (set CLI=... to test cli.py)")
    n = len(writes)
    r = subprocess.run(CLI + ["--dry-run"], capture_output=True, text=True, timeout=120)
    ok(r.returncode == 0 and "[DRY RUN]" in r.stdout and "Done." in r.stdout, "cli.py --dry-run")
    ok("Rule: Episodes keep last 2" in r.stdout and len(writes) == n, "cli.py --dry-run: reads the web UI's config, changes nothing")
    r = subprocess.run(CLI, capture_output=True, text=True, timeout=120)
    ok(r.returncode == 0 and "Done." in r.stdout and not [w for w in writes[n:] if w[1] == "DELETE"],
       "cli.py live run: nothing new to delete (state shared with the web UI)")
    r = subprocess.run(CLI[:-1] + ["/nonexistent"] if CLI[-2:-1] == ["--data"] else CLI + ["--data", "/nonexistent"],
                       capture_output=True, text=True, timeout=60)
    ok(r.returncode == 1 and "Database not found" in r.stdout, "cli.py without a database: clear error")


def main():
    servers = serve(FakeJellyfin), serve(FakeEmby), serve(FakeRadarr), serve(FakeSonarr)
    wait_for_app()
    web_basics()
    monitor(config(*servers))
    cli()
    code, _, _ = call("/logout")
    ok(code == 302 and call("/")[0] == 302 and call("/api/state")[0] == 302, "logout")
    print("smoke test passed")


if __name__ == "__main__":
    main()
