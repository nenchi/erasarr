#!/usr/bin/env python3
"""
Erasarr - Flask Web Application
"""

import os
import json
import logging
import secrets
import sqlite3
from datetime import datetime, timedelta
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, jsonify
)
from werkzeug.security import generate_password_hash, check_password_hash
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from monitor import ErasarrMonitor, EmbyClient, JellyfinClient, RadarrClient, SonarrClient

# ─────────────────────────────────────────────
#  App Setup
# ─────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("erasarr.web")

DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_FILE = os.path.join(DATA_DIR, "erasarr.db")
STATE_FILE = os.path.join(DATA_DIR, "state.json")

os.makedirs(DATA_DIR, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

@app.template_filter("compact")
def compact_number(n):
    """Format large numbers as 1.2K, 3.4M etc."""
    n = int(n)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M".rstrip("0").rstrip(".")
    if n >= 1_000:
        return f"{n/1_000:.1f}K".rstrip("0").rstrip(".")
    return str(n)

@app.template_filter("filesize")
def filesize_filter(n):
    """Format bytes as human-readable size: 1.2 GB, 3.4 TB, 1.1 PB, etc."""
    n = int(n or 0)
    units = [
        ("EB", 1_152_921_504_606_846_976),
        ("PB", 1_125_899_906_842_624),
        ("TB", 1_099_511_627_776),
        ("GB", 1_073_741_824),
        ("MB", 1_048_576),
        ("KB", 1_024),
    ]
    for unit, divisor in units:
        if n >= divisor:
            val = n / divisor
            return f"{val:.1f} {unit}".replace(".0 ", " ")
    return f"{n} B"


# ─────────────────────────────────────────────
#  Database
# ─────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                must_change INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                data TEXT NOT NULL DEFAULT '{}'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS run_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT NOT NULL,
                entries TEXT NOT NULL
            )
        """)
        conn.commit()
    _migrate_from_json()

def _migrate_from_json():
    """One-time migration from old JSON files to SQLite on first run."""
    config_json = os.path.join(DATA_DIR, "config.json")
    auth_json = os.path.join(DATA_DIR, "auth.json")

    if os.path.exists(config_json):
        with get_db() as conn:
            row = conn.execute("SELECT id FROM config WHERE id = 1").fetchone()
            if not row:
                with open(config_json) as f:
                    data = json.load(f)
                conn.execute("INSERT INTO config (id, data) VALUES (1, ?)", (json.dumps(data),))
                conn.commit()
                logger.info("Migrated config.json → SQLite")

    if os.path.exists(auth_json):
        with get_db() as conn:
            if conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
                with open(auth_json) as f:
                    auth = json.load(f)
                username = auth.get("username", "admin")
                must_change = int(auth.get("must_change", True))
                conn.execute(
                    "INSERT INTO users (username, password_hash, must_change) VALUES (?, ?, ?)",
                    (username, generate_password_hash("watchmon"), must_change)
                )
                conn.commit()
                logger.info("Migrated auth.json → SQLite (password remains 'erasarr', please change)")

init_db()

# ─────────────────────────────────────────────
#  Auth Helpers
# ─────────────────────────────────────────────

def user_count() -> int:
    with get_db() as conn:
        return conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]

def load_user(username: str):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        return dict(row) if row else None

def create_user(username: str, password: str, must_change: bool = False):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO users (username, password_hash, must_change) VALUES (?, ?, ?)",
            (username, generate_password_hash(password), 1 if must_change else 0)
        )
        conn.commit()

def update_user_password(username: str, new_password: str, must_change: bool = False):
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET password_hash = ?, must_change = ? WHERE username = ?",
            (generate_password_hash(new_password), 1 if must_change else 0, username)
        )
        conn.commit()

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# ─────────────────────────────────────────────
#  Config Helpers
# ─────────────────────────────────────────────

def _default_config() -> dict:
    return {
        "media_servers": [],
        "radarr_instances": [],
        "sonarr_instances": [],
        "action_rules": [],
        "schedule": {
            "mode": "manual",
            "interval_hours": 24,
            "cron": "0 3 * * *",
            "day_of_month": 1,
        }
    }

def _migrate_config(cfg: dict) -> dict:
    """Migrate old single-action config to action_rules format."""
    if "action_rules" not in cfg:
        old_actions = cfg.pop("actions", {})
        old_pretag = cfg.pop("precondition_tag", "")
        old_users = cfg.pop("selected_users", [])
        cfg["action_rules"] = [{
            "id": secrets.token_hex(4),
            "name": "Default Rule",
            "enabled": True,
            "applies_to_users": [f"{u['server_id']}:{u['user_id']}" for u in old_users],
            "applies_to_servers": [],
            "content_type": "all",
            "precondition_tag": old_pretag,
            "actions": {
                "unmonitor": old_actions.get("unmonitor", True),
                "delete_file": old_actions.get("delete_file", False),
                "add_tag": old_actions.get("add_tag", ""),
                "delay_days": old_actions.get("delay_days", 0),
                "keep_last_episodes": 0,
            },
        }]
        save_config(cfg)
    # Ensure migration defaults exist on all rules
    for rule in cfg.get("action_rules", []):
        rule.setdefault("applies_to_servers", [])
        rule.setdefault("schedule", {"use_global": True})
    # Migrate old single radarr config to radarr_instances list
    if "radarr" in cfg and "radarr_instances" not in cfg:
        old_r = cfg.pop("radarr")
        if old_r.get("address"):
            cfg["radarr_instances"] = [{
                "id": secrets.token_hex(4),
                "name": "Radarr",
                "enabled": old_r.get("enabled", False),
                "address": old_r.get("address", ""),
                "api_key": old_r.get("api_key", ""),
            }]
        else:
            cfg["radarr_instances"] = []
        save_config(cfg)
    return cfg

def load_config() -> dict:
    with get_db() as conn:
        row = conn.execute("SELECT data FROM config WHERE id = 1").fetchone()
        if row:
            return _migrate_config(json.loads(row["data"]))
    return _default_config()

def save_config(cfg: dict):
    data = json.dumps(cfg)
    with get_db() as conn:
        conn.execute(
            "INSERT INTO config (id, data) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET data = excluded.data",
            (data,)
        )
        conn.commit()

def load_run_log() -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT time, entries FROM run_log ORDER BY id DESC LIMIT 20"
        ).fetchall()
        return [{"time": r["time"], "entries": json.loads(r["entries"])} for r in rows]

def save_run_log(log_entries: list):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO run_log (time, entries) VALUES (?, ?)",
            (datetime.now().isoformat(), json.dumps(log_entries, default=str))
        )
        conn.execute(
            "DELETE FROM run_log WHERE id NOT IN (SELECT id FROM run_log ORDER BY id DESC LIMIT 20)"
        )
        conn.commit()


# ─────────────────────────────────────────────
#  Scheduler
# ─────────────────────────────────────────────

scheduler = BackgroundScheduler()
scheduler.start()

def _make_trigger(schedule: dict):
    """Return (trigger, description) or (None, 'manual') for a schedule dict."""
    mode = schedule.get("mode", "manual")
    if mode == "interval":
        hours = int(schedule.get("interval_hours") or 24)
        return IntervalTrigger(hours=hours), f"every {hours}h"
    elif mode == "daily":
        cron = schedule.get("cron", "0 3 * * *")
        parts = cron.split()
        if len(parts) == 5:
            return CronTrigger(
                minute=parts[0], hour=parts[1],
                day=parts[2], month=parts[3], day_of_week=parts[4]
            ), f"cron {cron}"
    elif mode == "monthly":
        dom = int(schedule.get("day_of_month") or 1)
        return CronTrigger(day=dom, hour=3), f"day {dom} monthly"
    return None, "manual"

def run_monitor_job(only_rule_ids=None, skip_custom_scheduled=False, dry_run=False):
    logger.info("Scheduled monitor run starting...")
    cfg = load_config()
    monitor = ErasarrMonitor(cfg, STATE_FILE)
    log = monitor.run(dry_run=dry_run, only_rule_ids=only_rule_ids, skip_custom_scheduled=skip_custom_scheduled)
    save_run_log(log)
    logger.info("Scheduled monitor run complete.")

def reschedule(cfg: dict):
    for job in list(scheduler.get_jobs()):
        if job.id.startswith("monitor"):
            scheduler.remove_job(job.id)

    # Global job — runs all actions that use the default schedule
    global_sched = cfg.get("schedule", {})
    trigger, desc = _make_trigger(global_sched)
    if trigger:
        scheduler.add_job(run_monitor_job, trigger, id="monitor_global",
            kwargs={"skip_custom_scheduled": True}, replace_existing=True)
        logger.info(f"Global schedule: {desc}")

    # Per-action custom schedule jobs
    for rule in cfg.get("action_rules", []):
        rule_sched = rule.get("schedule", {})
        if rule_sched.get("use_global", True):
            continue
        if not rule.get("enabled", True):
            continue
        trigger, desc = _make_trigger(rule_sched)
        if trigger:
            job_id = f"monitor_rule_{rule['id']}"
            scheduler.add_job(run_monitor_job, trigger, id=job_id,
                kwargs={"only_rule_ids": [rule["id"]]}, replace_existing=True)
            logger.info(f"Rule '{rule.get('name')}' custom schedule: {desc}")

# Load schedule on startup
try:
    reschedule(load_config())
except Exception as e:
    logger.warning(f"Could not load schedule on startup: {e}")


# ─────────────────────────────────────────────
#  Setup Check
# ─────────────────────────────────────────────

@app.before_request
def check_setup():
    """Redirect to registration if no users exist yet."""
    if request.endpoint in ("register", "static") or not request.endpoint:
        return
    if user_count() == 0:
        return redirect(url_for("register"))


# ─────────────────────────────────────────────
#  Routes - Auth
# ─────────────────────────────────────────────

@app.route("/register", methods=["GET", "POST"])
def register():
    if user_count() > 0:
        return redirect(url_for("login"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm_password", "")
        if not username:
            flash("Username is required", "error")
        elif len(password) < 8:
            flash("Password must be at least 8 characters", "error")
        elif password != confirm:
            flash("Passwords do not match", "error")
        else:
            create_user(username, password)
            flash("Account created! Please sign in.", "success")
            return redirect(url_for("login"))
    return render_template("register.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        user = load_user(username)
        if user and check_password_hash(user["password_hash"], password):
            session["logged_in"] = True
            session["username"] = username
            if user["must_change"]:
                return redirect(url_for("change_password"))
            return redirect(url_for("dashboard"))
        flash("Invalid credentials", "error")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/change-password", methods=["GET", "POST"])
@login_required
def change_password():
    if request.method == "POST":
        new_pass = request.form.get("new_password", "")
        confirm = request.form.get("confirm_password", "")
        if len(new_pass) < 8:
            flash("Password must be at least 8 characters", "error")
        elif new_pass != confirm:
            flash("Passwords do not match", "error")
        else:
            update_user_password(session["username"], new_pass)
            flash("Password changed successfully", "success")
            return redirect(url_for("dashboard"))
    return render_template("change_password.html")


# ─────────────────────────────────────────────
#  Routes - Dashboard
# ─────────────────────────────────────────────

@app.route("/")
@login_required
def dashboard():
    import re as _re
    cfg = load_config()
    run_log = load_run_log()
    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)

    def _build_pending_groups(source_dict):
        by_show = {}
        movies = []
        for _key, item in source_dict.items():
            itype = item.get("type", "")
            if itype in ("episode", "emby_episode", "server_episode"):
                title = item.get("title", "")
                m = _re.match(r"^(.*) S(\d+)E(\d+)$", title)
                series_name = m.group(1) if m else title
                if series_name not in by_show:
                    by_show[series_name] = {"rule": item.get("rule", ""), "seasons": {}, "type": itype}
                s, e = item.get("season") or 0, item.get("episode") or 0
                by_show[series_name]["seasons"].setdefault(s, []).append(e)
            else:
                movies.append({"title": item.get("title", ""), "watched_at": item.get("watched_at", ""), "rule": item.get("rule", "")})
        for show in by_show.values():
            for season in show["seasons"]:
                show["seasons"][season] = sorted(show["seasons"][season])
        return by_show, movies

    # Build pending_by_show: group episode/emby_episode pending items by series name
    pending_by_show, pending_movies = _build_pending_groups(state.get("pending", {}))
    preview_by_show, preview_movies = _build_pending_groups(state.get("dry_run_preview", {}))

    # Processed counts by time window
    now = datetime.now()
    cutoff_7d  = now - timedelta(days=7)
    cutoff_30d = now - timedelta(days=30)
    processed_7d = processed_30d = 0
    freed_bytes_movies = freed_bytes_episodes = freed_bytes_total = 0
    freed_movies_count = freed_episodes_count = 0
    for item in state.get("processed", {}).values():
        ts = item.get("processed_at", "")
        if not ts:
            continue
        try:
            item_dt = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            continue
        if item_dt >= cutoff_7d:
            processed_7d += 1
        if item_dt >= cutoff_30d:
            processed_30d += 1
        size = item.get("size_bytes", 0) or 0
        if size > 0:
            freed_bytes_total += size
            if item.get("type") == "movie":
                freed_bytes_movies += size
                freed_movies_count += 1
            elif item.get("type") == "episode":
                freed_bytes_episodes += size
                freed_episodes_count += 1
    scheduled_jobs = []
    for job in scheduler.get_jobs():
        if job.id.startswith("monitor") and job.next_run_time:
            if job.id == "monitor_global":
                label = "All actions (default schedule)"
            else:
                rule_id = job.id[len("monitor_rule_"):]
                label = next((r["name"] for r in cfg.get("action_rules", []) if r["id"] == rule_id), rule_id)
            scheduled_jobs.append({"label": label, "next_run": job.next_run_time})
    scheduled_jobs.sort(key=lambda x: x["next_run"])

    return render_template("dashboard.html",
        cfg=cfg,
        run_log=run_log,
        state=state,
        pending_by_show=pending_by_show,
        pending_movies=pending_movies,
        preview_by_show=preview_by_show,
        preview_movies=preview_movies,
        scheduled_jobs=scheduled_jobs,
        processed_7d=processed_7d,
        processed_30d=processed_30d,
        freed_bytes_total=freed_bytes_total,
        freed_bytes_movies=freed_bytes_movies,
        freed_bytes_episodes=freed_bytes_episodes,
        freed_movies_count=freed_movies_count,
        freed_episodes_count=freed_episodes_count,
    )


# ─────────────────────────────────────────────
#  Routes - Media Servers
# ─────────────────────────────────────────────

@app.route("/servers", methods=["GET", "POST"])
@login_required
def servers():
    cfg = load_config()
    if request.method == "POST":
        action = request.form.get("action")

        if action == "add_server":
            cfg["media_servers"].append({
                "id": secrets.token_hex(4),
                "name": request.form.get("name", ""),
                "type": request.form.get("type", "jellyfin"),
                "address": request.form.get("address", "").rstrip("/"),
                "api_key": request.form.get("api_key", ""),
                "enabled": True,
            })
            save_config(cfg)
            flash("Media server added", "success")

        elif action == "remove_server":
            sid = request.form.get("server_id")
            cfg["media_servers"] = [s for s in cfg["media_servers"] if s.get("id") != sid]
            save_config(cfg)
            flash("Media server removed", "success")

        elif action == "toggle_server":
            sid = request.form.get("server_id")
            for s in cfg["media_servers"]:
                if s.get("id") == sid:
                    s["enabled"] = not s.get("enabled", True)
            save_config(cfg)

        return redirect(url_for("servers"))
    return render_template("servers.html", cfg=cfg)

@app.route("/api/test-server", methods=["POST"])
@login_required
def api_test_server():
    data = request.json
    stype = data.get("type", "jellyfin")
    address = data.get("address", "").rstrip("/")
    api_key = data.get("api_key", "")
    if stype == "emby":
        client = EmbyClient(address, api_key)
    else:
        client = JellyfinClient(address, api_key)
    ok, msg = client.test_connection()
    return jsonify({"ok": ok, "msg": msg})

@app.route("/api/server-users", methods=["POST"])
@login_required
def api_server_users():
    data = request.json
    stype = data.get("type", "jellyfin")
    address = data.get("address", "").rstrip("/")
    api_key = data.get("api_key", "")
    if stype == "emby":
        client = EmbyClient(address, api_key)
    else:
        client = JellyfinClient(address, api_key)
    try:
        users = client.get_users()
        return jsonify({"ok": True, "users": users})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})


# ─────────────────────────────────────────────
#  Routes - Arr Apps
# ─────────────────────────────────────────────

@app.route("/arr", methods=["GET", "POST"])
@login_required
def arr():
    cfg = load_config()
    if request.method == "POST":
        action = request.form.get("action")

        if action == "add_radarr":
            cfg.setdefault("radarr_instances", []).append({
                "id": secrets.token_hex(4),
                "name": request.form.get("radarr_name", "Radarr"),
                "address": request.form.get("radarr_address", "").rstrip("/"),
                "api_key": request.form.get("radarr_api_key", ""),
                "enabled": True,
            })
            save_config(cfg)
            flash("Radarr instance added", "success")

        elif action == "remove_radarr":
            rid = request.form.get("radarr_id")
            cfg["radarr_instances"] = [r for r in cfg.get("radarr_instances", []) if r.get("id") != rid]
            save_config(cfg)
            flash("Radarr instance removed", "success")

        elif action == "toggle_radarr":
            rid = request.form.get("radarr_id")
            for r in cfg.get("radarr_instances", []):
                if r.get("id") == rid:
                    r["enabled"] = not r.get("enabled", True)
            save_config(cfg)

        elif action == "add_sonarr":
            cfg.setdefault("sonarr_instances", []).append({
                "id": secrets.token_hex(4),
                "name": request.form.get("sonarr_name", "Sonarr"),
                "address": request.form.get("sonarr_address", "").rstrip("/"),
                "api_key": request.form.get("sonarr_api_key", ""),
                "enabled": True,
            })
            save_config(cfg)
            flash("Sonarr instance added", "success")

        elif action == "remove_sonarr":
            sid = request.form.get("sonarr_id")
            cfg["sonarr_instances"] = [s for s in cfg.get("sonarr_instances", []) if s.get("id") != sid]
            save_config(cfg)
            flash("Sonarr instance removed", "success")

        elif action == "toggle_sonarr":
            sid = request.form.get("sonarr_id")
            for s in cfg.get("sonarr_instances", []):
                if s.get("id") == sid:
                    s["enabled"] = not s.get("enabled", True)
            save_config(cfg)

        return redirect(url_for("arr"))
    return render_template("arr.html", cfg=cfg)

@app.route("/api/test-radarr", methods=["POST"])
@login_required
def api_test_radarr():
    data = request.json
    client = RadarrClient(data.get("address", ""), data.get("api_key", ""))
    ok, msg = client.test_connection()
    return jsonify({"ok": ok, "msg": msg})

@app.route("/api/test-sonarr", methods=["POST"])
@login_required
def api_test_sonarr():
    data = request.json
    client = SonarrClient(data.get("address", ""), data.get("api_key", ""))
    ok, msg = client.test_connection()
    return jsonify({"ok": ok, "msg": msg})

@app.route("/api/radarr-tags", methods=["POST"])
@login_required
def api_radarr_tags():
    data = request.json
    client = RadarrClient(data.get("address", ""), data.get("api_key", ""))
    try:
        tags = client.get_tags()
        return jsonify({"ok": True, "tags": [t["label"] for t in tags]})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})

@app.route("/api/sonarr-tags", methods=["POST"])
@login_required
def api_sonarr_tags():
    data = request.json
    client = SonarrClient(data.get("address", ""), data.get("api_key", ""))
    try:
        tags = client.get_tags()
        return jsonify({"ok": True, "tags": [t["label"] for t in tags]})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})

@app.route("/api/update-server", methods=["POST"])
@login_required
def api_update_server():
    data = request.json
    sid = data.get("id", "")
    cfg = load_config()
    for server in cfg.get("media_servers", []):
        if server["id"] == sid:
            server["name"] = data.get("name", server["name"])
            server["type"] = data.get("type", server["type"])
            server["address"] = data.get("address", server["address"]).rstrip("/")
            server["api_key"] = data.get("api_key", server["api_key"])
            break
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/update-sonarr", methods=["POST"])
@login_required
def api_update_sonarr():
    data = request.json
    sid = data.get("id", "")
    cfg = load_config()
    for sonarr in cfg.get("sonarr_instances", []):
        if sonarr["id"] == sid:
            sonarr["name"] = data.get("name", sonarr["name"])
            sonarr["address"] = data.get("address", sonarr["address"]).rstrip("/")
            sonarr["api_key"] = data.get("api_key", sonarr["api_key"])
            break
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/toggle-sonarr", methods=["POST"])
@login_required
def api_toggle_sonarr():
    data = request.json
    sid = data.get("id", "")
    cfg = load_config()
    for sonarr in cfg.get("sonarr_instances", []):
        if sonarr["id"] == sid:
            sonarr["enabled"] = not sonarr.get("enabled", True)
            break
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/toggle-radarr", methods=["POST"])
@login_required
def api_toggle_radarr():
    data = request.json or {}
    rid = data.get("id", "")
    cfg = load_config()
    for r in cfg.get("radarr_instances", []):
        if r.get("id") == rid:
            r["enabled"] = not r.get("enabled", True)
            break
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/update-radarr", methods=["POST"])
@login_required
def api_update_radarr():
    data = request.json
    rid = data.get("id", "")
    cfg = load_config()
    for r in cfg.get("radarr_instances", []):
        if r["id"] == rid:
            r["name"] = data.get("name", r["name"])
            r["address"] = data.get("address", r["address"]).rstrip("/")
            r["api_key"] = data.get("api_key", r["api_key"])
            break
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/add-server", methods=["POST"])
@login_required
def api_add_server():
    data = request.json or {}
    cfg = load_config()
    cfg["media_servers"].append({
        "id": secrets.token_hex(4),
        "name": data.get("name", "").strip(),
        "type": data.get("type", "jellyfin"),
        "address": data.get("address", "").rstrip("/"),
        "api_key": data.get("api_key", ""),
        "enabled": True,
    })
    save_config(cfg)
    return jsonify({"ok": True})

@app.route("/api/add-arr", methods=["POST"])
@login_required
def api_add_arr():
    data = request.json or {}
    arr_type = data.get("arr_type", "radarr")
    cfg = load_config()
    entry = {
        "id": secrets.token_hex(4),
        "name": data.get("name", "").strip() or arr_type.capitalize(),
        "address": data.get("address", "").rstrip("/"),
        "api_key": data.get("api_key", ""),
        "enabled": True,
    }
    if arr_type == "sonarr":
        cfg.setdefault("sonarr_instances", []).append(entry)
    else:
        cfg.setdefault("radarr_instances", []).append(entry)
    save_config(cfg)
    return jsonify({"ok": True})

# ─────────────────────────────────────────────
#  Routes - Action Rules
# ─────────────────────────────────────────────

def _rule_from_request(data: dict, existing: dict = None) -> dict:
    base = existing or {}
    if data.get("all_users", True if not data.get("applies_to_users") else False):
        applies_to_users = []
    else:
        applies_to_users = data.get("applies_to_users", base.get("applies_to_users", []))
    if data.get("all_servers", True if not data.get("applies_to_servers") else False):
        applies_to_servers = []
    else:
        applies_to_servers = data.get("applies_to_servers", base.get("applies_to_servers", []))
    sched_in = data.get("schedule", {})
    if sched_in.get("use_global", True):
        rule_schedule = {"use_global": True}
    else:
        rule_schedule = {
            "use_global": False,
            "mode": sched_in.get("mode", "manual"),
            "interval_hours": int(sched_in.get("interval_hours") or 24),
            "cron": sched_in.get("cron", "0 3 * * *"),
            "day_of_month": int(sched_in.get("day_of_month") or 1),
        }
    return {
        "id": base.get("id", secrets.token_hex(4)),
        "name": data.get("name") or base.get("name", "Unnamed Rule"),
        "enabled": base.get("enabled", True),
        "applies_to_users": applies_to_users,
        "applies_to_servers": applies_to_servers,
        "content_type": data.get("content_type", base.get("content_type", "all")),
        "precondition_tag": data.get("precondition_tag", base.get("precondition_tag", "")).strip(),
        "schedule": rule_schedule,
        "actions": {
            "unmonitor": bool(data.get("unmonitor", False)),
            "delete_file": bool(data.get("delete_file", False)),
            "delete_from_emby": bool(data.get("delete_from_emby", False)),
            "require_all_users_watched": bool(data.get("require_all_users_watched", False)),
            "keep_last_by_watch_time": bool(data.get("keep_last_by_watch_time", False)),
            "add_tag": (data.get("add_tag") or "").strip(),
            "delay_days": int(data.get("delay_days") or 0),
            "keep_last_episodes": int(data.get("keep_last_episodes") or 0),
        },
    }

@app.route("/api/rules", methods=["POST"])
@login_required
def api_add_rule():
    rule = _rule_from_request(request.json or {})
    cfg = load_config()
    cfg.setdefault("action_rules", []).append(rule)
    save_config(cfg)
    reschedule(cfg)
    return jsonify({"ok": True, "rule": rule})

@app.route("/api/rules/<rule_id>", methods=["POST"])
@login_required
def api_update_rule(rule_id):
    cfg = load_config()
    for i, rule in enumerate(cfg.get("action_rules", [])):
        if rule["id"] == rule_id:
            cfg["action_rules"][i] = _rule_from_request(request.json or {}, existing=rule)
            break
    save_config(cfg)
    reschedule(cfg)
    return jsonify({"ok": True})

@app.route("/api/rules/<rule_id>/delete", methods=["POST"])
@login_required
def api_delete_rule(rule_id):
    cfg = load_config()
    cfg["action_rules"] = [r for r in cfg.get("action_rules", []) if r["id"] != rule_id]
    save_config(cfg)
    reschedule(cfg)
    return jsonify({"ok": True})

@app.route("/api/rules/<rule_id>/toggle", methods=["POST"])
@login_required
def api_toggle_rule(rule_id):
    cfg = load_config()
    for rule in cfg.get("action_rules", []):
        if rule["id"] == rule_id:
            rule["enabled"] = not rule.get("enabled", True)
            break
    save_config(cfg)
    reschedule(cfg)
    return jsonify({"ok": True})


# ─────────────────────────────────────────────
#  Routes - Rules & Schedule
# ─────────────────────────────────────────────

@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    cfg = load_config()

    # Build full user list from all configured servers
    all_users = []
    for server in cfg.get("media_servers", []):
        if not server.get("enabled"):
            continue
        try:
            stype = server.get("type", "jellyfin")
            if stype == "emby":
                client = EmbyClient(server["address"], server["api_key"], server.get("name"))
            else:
                client = JellyfinClient(server["address"], server["api_key"], server.get("name"))
            users = client.get_users()
            for u in users:
                all_users.append({
                    "server_id": server["id"],
                    "server_name": server.get("name", stype),
                    "user_id": u["id"],
                    "user_name": u["name"],
                    "key": f"{server['id']}:{u['id']}"
                })
        except Exception as e:
            logger.warning(f"Could not fetch users from {server.get('name')}: {e}")

    if request.method == "POST":
        action = request.form.get("action")

        if action == "save_schedule":
            cfg["schedule"] = {
                "mode": request.form.get("schedule_mode", "manual"),
                "interval_hours": int(request.form.get("interval_hours", 24) or 24),
                "cron": request.form.get("cron_expr", "0 3 * * *"),
                "day_of_month": int(request.form.get("day_of_month", 1) or 1),
            }
            save_config(cfg)
            reschedule(cfg)
            flash("Schedule saved", "success")

        return redirect(url_for("settings"))

    return render_template("settings.html", cfg=cfg, all_users=all_users)


# ─────────────────────────────────────────────
#  Routes - Run
# ─────────────────────────────────────────────

@app.route("/run", methods=["POST"])
@login_required
def run_now():
    dry_run = request.form.get("dry_run") == "1"
    cfg = load_config()
    monitor = ErasarrMonitor(cfg, STATE_FILE)
    log = monitor.run(dry_run=dry_run)
    save_run_log(log)
    flash(f"Run complete ({'dry run' if dry_run else 'live'}). Check the log below.", "success")
    return redirect(url_for("dashboard"))

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/api/state")
@login_required
def api_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return jsonify(json.load(f))
    return jsonify({})

@app.route("/api/clear-state", methods=["POST"])
@login_required
def api_clear_state():
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
    flash("State cleared", "success")
    return redirect(url_for("dashboard"))


# ─────────────────────────────────────────────
#  Routes - Config Backup / Restore
# ─────────────────────────────────────────────

@app.route("/api/export-config")
@login_required
def export_config():
    import io
    cfg = load_config()
    buf = io.BytesIO(json.dumps(cfg, indent=2).encode("utf-8"))
    buf.seek(0)
    from flask import send_file
    return send_file(buf, mimetype="application/json", as_attachment=True,
                     download_name="erasarr-config.json")

@app.route("/api/import-config", methods=["POST"])
@login_required
def import_config():
    f = request.files.get("config_file")
    if not f:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400
    try:
        cfg = json.loads(f.read().decode("utf-8"))
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Invalid JSON: {exc}"}), 400
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "Config must be a JSON object"}), 400
    cfg = _migrate_config(cfg)
    save_config(cfg)
    reschedule(cfg)
    return jsonify({"ok": True})


# ─────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
