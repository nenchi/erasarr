#!/usr/bin/env python3
"""
Erasarr - Core monitoring logic
Connects Emby/Jellyfin watch history to Sonarr/Radarr actions
"""

import os
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("erasarr")


def _safe_int(v) -> Optional[int]:
    """Convert a value to int, returning None if not possible."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────
#  Media Server Clients
# ─────────────────────────────────────────────

class EmbyClient:
    def __init__(self, address: str, api_key: str, name: str = "Emby"):
        self.address = address.rstrip("/")
        self.api_key = api_key
        self.name = name

    def _get(self, path: str, params: dict = None):
        params = params or {}
        params["api_key"] = self.api_key
        r = requests.get(f"{self.address}{path}", params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    def _detect_app_type(self) -> str:
        """Return 'jellyfin', 'emby', or '' (unknown).
        /System/Ping is unauthenticated and returns the app name as a plain
        string on all modern Jellyfin and Emby builds. Falls back to
        ProductName from the public info endpoint if ping doesn't help."""
        try:
            r = requests.get(f"{self.address}/System/Ping", timeout=10)
            if r.ok:
                text = r.text.strip().strip('"').lower()
                if "jellyfin" in text:
                    return "jellyfin"
                if "emby" in text:
                    return "emby"
        except Exception:
            pass
        try:
            r = requests.get(f"{self.address}/System/Info/Public", timeout=10)
            if r.ok:
                product = r.json().get("ProductName", "").lower()
                if "jellyfin" in product:
                    return "jellyfin"
                if "emby" in product:
                    return "emby"
        except Exception:
            pass
        return ""

    def test_connection(self):
        try:
            data = self._get("/System/Info")
            detected = self._detect_app_type()
            if detected and detected != "emby":
                return False, f"Wrong app — connected to {detected.capitalize()}, expected Emby"
            return True, data.get("ServerName", self.name)
        except Exception as e:
            return False, str(e)

    def get_users(self):
        data = self._get("/Users")
        return [
            {"id": u["Id"], "name": u["Name"]}
            for u in data
            if not u.get("Policy", {}).get("IsDisabled", False)
        ]

    def get_watched_items(self, user_id: str, since_days: int = None, since_date: datetime = None):
        """Return list of watched items for a user."""
        base_params = {
            "userId": user_id,
            "IsPlayed": "true",
            "IncludeItemTypes": "Movie,Episode",
            "Recursive": "true",
            "Fields": "ProviderIds,SeriesInfo,SeriesProviderIds,DateLastSaved",
            "Limit": 1000,
        }
        if since_date:
            base_params["MinDateLastSavedForUser"] = since_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        raw_items = []
        start = 0
        while True:
            params = {**base_params, "StartIndex": start}
            data = self._get("/Items", params)
            page = data.get("Items", [])
            raw_items.extend(page)
            total = data.get("TotalRecordCount", 0)
            start += len(page)
            if start >= total or not page:
                break

        items = []
        for item in raw_items:
            watched_at = item.get("UserData", {}).get("LastPlayedDate") or item.get("DateLastSaved")
            item_type = item.get("Type")
            # SeriesProviderIds is the series-level TVDB ID (when Emby exposes it);
            # fall back to ProviderIds.Tvdb which is also usually the series ID for episodes.
            if item_type == "Episode":
                series_tvdb = (
                    item.get("SeriesProviderIds", {}).get("Tvdb")
                    or item.get("ProviderIds", {}).get("Tvdb")
                )
            else:
                series_tvdb = item.get("ProviderIds", {}).get("Tvdb")
            items.append({
                "type": item_type,
                "title": item.get("Name"),
                "series_name": item.get("SeriesName"),
                "season": item.get("ParentIndexNumber"),
                "episode": item.get("IndexNumber"),
                "imdb_id": item.get("ProviderIds", {}).get("Imdb"),
                "tvdb_id": item.get("ProviderIds", {}).get("Tvdb"),
                "series_tvdb_id": series_tvdb,
                "watched_at": watched_at,
                "user_id": user_id,
                "item_id": item.get("Id"),
            })
        return items

    def delete_item(self, item_id: str):
        """Delete an item and its media file directly from the server."""
        r = requests.delete(
            f"{self.address}/Items/{item_id}",
            params={"api_key": self.api_key, "deleteMedia": "true"},
            timeout=15,
        )
        return r


class JellyfinClient(EmbyClient):
    """Jellyfin uses the same API as Emby with minor differences."""

    def test_connection(self):
        try:
            data = self._get("/System/Info")
            detected = self._detect_app_type()
            if detected and detected != "jellyfin":
                return False, f"Wrong app — connected to {detected.capitalize()}, expected Jellyfin"
            return True, data.get("ServerName", "Jellyfin")
        except Exception as e:
            return False, str(e)

    def get_watched_items(self, user_id: str, since_days: int = None, since_date: datetime = None):
        base_params = {
            "userId": user_id,
            "IsPlayed": "true",
            "IncludeItemTypes": "Movie,Episode",
            "Recursive": "true",
            "Fields": "ProviderIds,SeriesProviderIds,DateLastSaved",
            "Limit": 1000,
        }
        if since_date:
            base_params["MinDateLastSavedForUser"] = since_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        raw_items = []
        start = 0
        while True:
            params = {**base_params, "StartIndex": start}
            data = self._get(f"/Users/{user_id}/Items", params)
            page = data.get("Items", [])
            raw_items.extend(page)
            total = data.get("TotalRecordCount", 0)
            start += len(page)
            if start >= total or not page:
                break

        items = []
        for item in raw_items:
            user_data = item.get("UserData", {})
            watched_at = user_data.get("LastPlayedDate") or item.get("DateLastSaved")
            item_type = item.get("Type")
            if item_type == "Episode":
                series_tvdb = (
                    item.get("SeriesProviderIds", {}).get("Tvdb")
                    or item.get("ProviderIds", {}).get("Tvdb")
                )
            else:
                series_tvdb = item.get("ProviderIds", {}).get("Tvdb")
            items.append({
                "type": item_type,
                "title": item.get("Name"),
                "series_name": item.get("SeriesName"),
                "season": item.get("ParentIndexNumber"),
                "episode": item.get("IndexNumber"),
                "imdb_id": item.get("ProviderIds", {}).get("Imdb"),
                "tvdb_id": item.get("ProviderIds", {}).get("Tvdb"),
                "series_tvdb_id": series_tvdb,
                "watched_at": watched_at,
                "user_id": user_id,
                "item_id": item.get("Id"),
            })
        return items


# ─────────────────────────────────────────────
#  Arr Clients
# ─────────────────────────────────────────────

class RadarrClient:
    def __init__(self, address: str, api_key: str):
        self.address = address.rstrip("/")
        self.api_key = api_key

    def _get(self, path):
        r = requests.get(f"{self.address}/api/v3{path}?apikey={self.api_key}", timeout=15)
        r.raise_for_status()
        return r.json()

    def _put(self, path, data):
        r = requests.put(f"{self.address}/api/v3{path}?apikey={self.api_key}", json=data, timeout=15)
        r.raise_for_status()
        return r

    def _delete(self, path):
        r = requests.delete(f"{self.address}/api/v3{path}?apikey={self.api_key}", timeout=15)
        r.raise_for_status()
        return r

    def test_connection(self):
        try:
            data = self._get("/system/status")
            app_name = data.get("appName", "")
            if app_name and app_name.lower() != "radarr":
                return False, f"Wrong app — connected to {app_name}, expected Radarr"
            return True, data.get("version", "OK")
        except Exception as e:
            return False, str(e)

    def get_movies(self):
        return self._get("/movie")

    def get_tags(self):
        return self._get("/tag")

    def ensure_tag(self, label: str) -> int:
        for tag in self.get_tags():
            if tag["label"].lower() == label.lower():
                return tag["id"]
        r = requests.post(
            f"{self.address}/api/v3/tag?apikey={self.api_key}",
            json={"id": 0, "label": label}, timeout=15
        )
        return r.json()["id"]

    def find_movie_by_imdb(self, imdb_id: str, movies=None):
        movies = movies or self.get_movies()
        for m in movies:
            if m.get("imdbId") == imdb_id:
                return m
        return None

    def unmonitor_movie(self, movie: dict):
        movie["monitored"] = False
        return self._put(f"/movie/{movie['id']}", movie)

    def add_tag_to_movie(self, movie: dict, tag_id: int):
        if tag_id not in movie.get("tags", []):
            movie["tags"].append(tag_id)
        return self._put(f"/movie/{movie['id']}", movie)

    def movie_has_tag(self, movie: dict, tag_id: int) -> bool:
        return tag_id in movie.get("tags", [])

    def delete_movie_file(self, movie: dict):
        if "movieFile" in movie:
            file_id = movie["movieFile"]["id"]
            return self._delete(f"/moviefile/{file_id}")
        return None


class SonarrClient:
    def __init__(self, address: str, api_key: str):
        self.address = address.rstrip("/")
        self.api_key = api_key

    def _get(self, path, params=""):
        url = f"{self.address}/api/v3{path}?apikey={self.api_key}"
        if params:
            url += f"&{params}"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()

    def _put(self, path, data):
        r = requests.put(f"{self.address}/api/v3{path}?apikey={self.api_key}", json=data, timeout=15)
        r.raise_for_status()
        return r

    def _delete(self, path):
        r = requests.delete(f"{self.address}/api/v3{path}?apikey={self.api_key}", timeout=15)
        r.raise_for_status()
        return r

    def test_connection(self):
        try:
            data = self._get("/system/status")
            app_name = data.get("appName", "")
            if app_name and app_name.lower() != "sonarr":
                return False, f"Wrong app — connected to {app_name}, expected Sonarr"
            return True, data.get("version", "OK")
        except Exception as e:
            return False, str(e)

    def get_series(self):
        return self._get("/series")

    def get_tags(self):
        return self._get("/tag")

    def ensure_tag(self, label: str) -> int:
        for tag in self.get_tags():
            if tag["label"].lower() == label.lower():
                return tag["id"]
        r = requests.post(
            f"{self.address}/api/v3/tag?apikey={self.api_key}",
            json={"id": 0, "label": label}, timeout=15
        )
        return r.json()["id"]

    def series_has_tag(self, series: dict, tag_id: int) -> bool:
        return tag_id in series.get("tags", [])

    def add_tag_to_series(self, series: dict, tag_id: int):
        series_data = self._get(f"/series/{series['id']}")
        if tag_id not in series_data.get("tags", []):
            series_data["tags"].append(tag_id)
            self._put(f"/series/{series['id']}", series_data)

    def get_episodes(self, series_id: int):
        return self._get("/episode", f"seriesId={series_id}")

    def get_episode(self, episode_id: int):
        return self._get(f"/episode/{episode_id}")

    def unmonitor_episode(self, episode: dict):
        episode["monitored"] = False
        return self._put(f"/episode/{episode['id']}", episode)

    def get_episode_file_size(self, episode: dict) -> int:
        """Return file size in bytes for an episode, or 0 if unavailable."""
        file_id = episode.get("episodeFileId", 0)
        if file_id and file_id > 0:
            try:
                data = self._get(f"/episodefile/{file_id}")
                return data.get("size", 0)
            except Exception:
                pass
        return 0

    def delete_episode_file(self, episode: dict):
        if "episodeFileId" in episode and episode["episodeFileId"] > 0:
            return self._delete(f"/episodefile/{episode['episodeFileId']}")
        return None

    def find_series_by_tvdb(self, tvdb_id: int, series=None):
        series = series or self.get_series()
        for s in series:
            if s.get("tvdbId") == tvdb_id:
                return s
        return None

    def find_series_by_title(self, title: str, series=None):
        if not title:
            return None
        series = series or self.get_series()
        title_lower = title.strip().lower()
        # Exact match first
        for s in series:
            if s.get("title", "").strip().lower() == title_lower:
                return s
        # Substring match fallback
        for s in series:
            s_title = s.get("title", "").strip().lower()
            if title_lower in s_title or s_title in title_lower:
                return s
        return None

    def find_episode(self, series_id: int, season: int, episode_num: int, episodes=None):
        episodes = episodes or self.get_episodes(series_id)
        for ep in episodes:
            if ep["seasonNumber"] == season and ep["episodeNumber"] == episode_num:
                return ep
        return None


# ─────────────────────────────────────────────
#  State / History Tracker
# ─────────────────────────────────────────────

class StateTracker:
    def __init__(self, state_file: str):
        self.state_file = state_file
        self.state = self._load()

    def _load(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file) as f:
                    return json.load(f)
            except Exception:
                pass
        return {"processed": {}, "pending": {}, "dry_run_preview": {}, "last_run": None}

    def save(self):
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, indent=2, default=str)

    def is_processed(self, key: str) -> bool:
        return key in self.state["processed"]

    def mark_processed(self, key: str, info: dict):
        self.state["processed"][key] = {**info, "processed_at": datetime.now().isoformat()}
        # Remove from pending if it was there
        self.state["pending"].pop(key, None)
        self.save()

    def unmark_processed(self, key: str):
        self.state["processed"].pop(key, None)
        self.save()

    def add_pending(self, key: str, info: dict):
        if key not in self.state["processed"] and key not in self.state["pending"]:
            self.state["pending"][key] = {**info, "added_at": datetime.now().isoformat()}
            self.save()

    def add_dry_run_preview(self, key: str, info: dict):
        self.state.setdefault("dry_run_preview", {})[key] = {**info, "added_at": datetime.now().isoformat()}

    def get_pending(self):
        return self.state["pending"]

    def update_last_run(self):
        self.state["last_run"] = datetime.now().isoformat()
        self.save()

    def get_last_run(self) -> Optional[datetime]:
        if self.state.get("last_run"):
            try:
                return datetime.fromisoformat(self.state["last_run"])
            except Exception:
                pass
        return None


# ─────────────────────────────────────────────
#  Main Monitor
# ─────────────────────────────────────────────

class ErasarrMonitor:
    def __init__(self, config: dict, state_file: str = "/data/state.json"):
        self.config = config
        self.state = StateTracker(state_file)
        self.log = []

    def _log(self, msg: str, level: str = "info"):
        entry = {"time": datetime.now().isoformat(), "level": level, "msg": msg}
        self.log.append(entry)
        getattr(logger, level)(msg)

    def _build_media_clients(self):
        clients = []
        for server in self.config.get("media_servers", []):
            if not server.get("enabled"):
                continue
            stype = server.get("type", "jellyfin").lower()
            if stype == "emby":
                c = EmbyClient(server["address"], server["api_key"], server.get("name", "Emby"))
            else:
                c = JellyfinClient(server["address"], server["api_key"], server.get("name", "Jellyfin"))
            c.server_id = server.get("id", "")
            clients.append(c)
        return clients

    def _build_radarr_instances(self):
        instances = []
        for cfg in self.config.get("radarr_instances", []):
            if cfg.get("enabled"):
                instances.append((cfg, RadarrClient(cfg["address"], cfg["api_key"])))
        return instances

    def _build_sonarr_instances(self):
        instances = []
        for cfg in self.config.get("sonarr_instances", []):
            if cfg.get("enabled"):
                instances.append((cfg, SonarrClient(cfg["address"], cfg["api_key"])))
        return instances

    def _delay_elapsed(self, watched_at_str: str, delay_days: int = 0) -> bool:
        if not delay_days:
            return True
        try:
            # Normalise to a naive UTC datetime for comparison with datetime.now()
            normalised = watched_at_str.strip()
            if normalised.endswith("Z"):
                normalised = normalised[:-1]  # strip Z → naive UTC
            else:
                # Strip any +HH:MM or -HH:MM offset so fromisoformat gives a naive datetime
                import re as _re_tz
                normalised = _re_tz.sub(r'[+-]\d{2}:\d{2}$', '', normalised)
            watched_at = datetime.fromisoformat(normalised)
            return datetime.now() >= watched_at + timedelta(days=delay_days)
        except Exception:
            return True

    def _format_seasons(self, eps: set) -> list:
        """Convert a set of (season, episode) tuples into compact indented season lines."""
        by_season: dict = {}
        for s, e in eps:
            by_season.setdefault(s, []).append(e)
        return [
            f"      Season {s:02d}: {', '.join(f'{e:02d}' for e in sorted(ep_list))}"
            for s, ep_list in sorted(by_season.items())
        ]

    def run(self, dry_run: bool = False, only_rule_ids=None, skip_custom_scheduled=False):
        self._log("=" * 50)
        self._log(f"Erasarr run started {'[DRY RUN]' if dry_run else ''}")

        # Reset preview bucket: dry-run fills dry_run_preview; real run clears it
        self.state.state["dry_run_preview"] = {}
        if not dry_run:
            self.state.save()

        # Support legacy single-action config for CLI backwards compat
        action_rules = self.config.get("action_rules")
        if not action_rules and "actions" in self.config:
            old = self.config.get("actions", {})
            old_users = self.config.get("selected_users", [])
            action_rules = [{
                "id": "legacy",
                "name": "Default Rule",
                "enabled": True,
                "applies_to_users": [f"{u['server_id']}:{u['user_id']}" for u in old_users],
                "content_type": "all",
                "precondition_tag": self.config.get("precondition_tag", ""),
                "actions": old,
            }]

        if not action_rules:
            self._log("No action rules configured. Nothing to do.", "warning")
            if not dry_run:
                self.state.update_last_run()
            return self.log

        # Build clients
        media_clients = self._build_media_clients()
        clients_by_server_id = {c.server_id: c for c in media_clients}
        radarr_instances_list = self._build_radarr_instances()
        sonarr_instances = self._build_sonarr_instances()

        # Cache arr data upfront
        radarr_movies_cache = {}
        for cfg_r, radarr_client in radarr_instances_list:
            try:
                radarr_movies_cache[cfg_r["id"]] = radarr_client.get_movies()
            except Exception as _e:
                self._log(f"Error fetching movies from Radarr '{cfg_r.get('name', '')}': {_e}", "error")
                radarr_movies_cache[cfg_r["id"]] = []
        sonarr_series_caches = {}
        for cfg_s, sonarr in sonarr_instances:
            sonarr_series_caches[cfg_s["id"]] = sonarr.get_series()

        # Collect ALL watched items from every user on every server
        all_watched = []
        all_user_keys: set = set()  # every user key we fetched (for require_all_users logic)
        # Always fetch full watch history — no date filter.
        # The is_processed() check makes re-fetching old items harmless, and a date-
        # capped window would break keep-last protection for episodes watched long ago.
        for client in media_clients:
            try:
                users = client.get_users()
                for user in users:
                    self._log(f"Fetching watch history for {user['name']} on {client.name}")
                    items = client.get_watched_items(user["id"])
                    self._log(f"  Found {len(items)} watched items")
                    user_key = f"{client.server_id}:{user['id']}"
                    all_user_keys.add(user_key)
                    for item in items:
                        item["_server_id"] = client.server_id
                        item["_user_key"] = user_key
                    all_watched.extend(items)
            except Exception as e:
                self._log(f"Error fetching from {client.name}: {e}", "error")

        # Build a lookup: (series_name_lower, season, episode) -> set of user_keys that watched it
        episode_watched_by: dict = {}
        for item in all_watched:
            if item["type"] != "Episode":
                continue
            name_key = (item.get("series_name") or "").strip().lower()
            s, e = item.get("season"), item.get("episode")
            if not name_key or s is None or e is None:
                continue
            ekey = (name_key, s, e)
            episode_watched_by.setdefault(ekey, set()).add(item["_user_key"])

        # Build a lookup: imdb_id -> set of user_keys that watched the movie
        movie_watched_by: dict = {}
        for item in all_watched:
            if item["type"] != "Movie":
                continue
            imdb = item.get("imdb_id")
            if imdb:
                movie_watched_by.setdefault(imdb, set()).add(item["_user_key"])

        # Process each rule
        for rule in action_rules:
            if not rule.get("enabled", True):
                continue
            if only_rule_ids is not None and rule["id"] not in only_rule_ids:
                continue
            if skip_custom_scheduled and not rule.get("schedule", {}).get("use_global", True):
                continue

            rule_id = rule["id"]
            rule_name = rule.get("name", rule_id)
            self._log(f"\n── Rule: {rule_name} ──")

            applies_to_users = set(rule.get("applies_to_users") or [])
            applies_to_servers = set(rule.get("applies_to_servers") or [])
            content_type = rule.get("content_type", "all")
            precondition_tag = rule.get("precondition_tag", "")
            rule_actions = rule.get("actions") or {}
            delay_days = int(rule_actions.get("delay_days") or 0)
            keep_last = int(rule_actions.get("keep_last_episodes") or 0)
            keep_last_by_time = bool(rule_actions.get("keep_last_by_watch_time", False))
            delete_from_emby = bool(rule_actions.get("delete_from_emby", False))
            require_all_watched = bool(rule_actions.get("require_all_users_watched", False))
            # force_delete_older works well alongside require_all_watched:
            # explicit episodes are gated by all-users-watched; implicit older episodes
            # are gated by the per-season required_user_keys check.
            force_delete_older = bool(rule_actions.get("force_delete_older_if_newer_watched", False))
            # Users that must have watched an episode before it can be processed.
            # If the rule applies to specific users, use those; otherwise use everyone.
            required_user_keys = applies_to_users if applies_to_users else all_user_keys

            # Filter items for this rule
            rule_watched = []
            for item in all_watched:
                if applies_to_servers and item.get("_server_id") not in applies_to_servers:
                    continue
                if applies_to_users and item.get("_user_key") not in applies_to_users:
                    continue
                if content_type == "movies" and item["type"] != "Movie":
                    continue
                if content_type == "episodes" and item["type"] != "Episode":
                    continue
                rule_watched.append(item)

            # Tag IDs for each Radarr instance
            radarr_tag_ids = {}  # instance_id -> (precond_tag_id, action_tag_id)
            for cfg_r, radarr_client in radarr_instances_list:
                rid = cfg_r["id"]
                try:
                    precond_id_r = radarr_client.ensure_tag(precondition_tag) if precondition_tag else None
                except Exception:
                    precond_id_r = None
                try:
                    action_tag_id_r = radarr_client.ensure_tag(rule_actions["add_tag"]) if rule_actions.get("add_tag") else None
                except Exception:
                    action_tag_id_r = None
                radarr_tag_ids[rid] = (precond_id_r, action_tag_id_r)

            # Tag IDs for each Sonarr instance
            sonarr_tag_ids = {}
            for cfg_s, sonarr in sonarr_instances:
                sid = cfg_s["id"]
                try:
                    precond_id = sonarr.ensure_tag(precondition_tag) if precondition_tag else None
                except Exception:
                    precond_id = None
                try:
                    action_tag_id = sonarr.ensure_tag(rule_actions["add_tag"]) if rule_actions.get("add_tag") else None
                except Exception:
                    action_tag_id = None
                sonarr_tag_ids[sid] = (precond_id, action_tag_id)

            # keep-last protection:
            #  - By default (keep_last_by_time=False): uses Sonarr's full episode list sorted by
            #    (season, episode) number — always keeps the N highest-numbered episodes.
            #  - When keep_last_by_time=True: uses rule_watched sorted by watched_at timestamp —
            #    keeps the N most recently watched episodes by the rule's users.
            # Cache: series_id -> set of (season, episode) to protect (episode-number mode)
            sonarr_ep_protections: dict = {}
            # Cache: series_id -> full set of (season, episode) known to Sonarr
            # Used in dry-run to skip episodes Sonarr doesn't manage (find_episode returns None)
            sonarr_ep_sets: dict = {}
            # Cache: series_id -> full list of episode dicts (for implicit older-episode pass)
            sonarr_ep_full_cache: dict = {}
            # For watch-time mode: name_key -> set of (season, episode) to protect
            series_protected_by_time: dict = {}
            if keep_last > 0 and keep_last_by_time:
                series_time_eps: dict = {}  # name_key -> list of (watched_at_str, s, e)
                for item in rule_watched:
                    if item["type"] != "Episode":
                        continue
                    name_key = (item.get("series_name") or "").strip().lower()
                    s, e = item.get("season"), item.get("episode")
                    if not name_key or s is None or e is None:
                        continue
                    series_time_eps.setdefault(name_key, []).append(
                        (item.get("watched_at") or "", s, e)
                    )
                for name_key, eps in series_time_eps.items():
                    # Sort by (watched_at, season, episode) descending so that:
                    # 1. Most recently watched timestamp comes first.
                    # 2. When timestamps are identical/missing, higher-numbered episodes
                    #    come first — matching the intuitive "last episodes in the show".
                    eps.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
                    seen: set = set()
                    protected: set = set()
                    for _, s, e in eps:
                        if (s, e) not in seen:
                            seen.add((s, e))
                            protected.add((s, e))
                            if len(protected) >= keep_last:
                                break
                    series_protected_by_time[name_key] = protected

            # ── Movies ──
            if radarr_instances_list and content_type in ("all", "movies"):
                self._log("  [Movies]")

                # Dry-run: show a summary per movie before acting
                if dry_run:
                    movie_items = [i for i in rule_watched if i["type"] == "Movie" and i.get("imdb_id")]
                    if not movie_items:
                        self._log("    No movies in watch history match this rule's user/content filter.")
                    else:
                        self._log(f"    Dry-run preview — {len(movie_items)} watched movies checked:")
                        for item in movie_items:
                            imdb = item["imdb_id"]
                            title = item.get("title") or imdb
                            found_movie_dry = None
                            found_cfg_r_dry = None
                            found_client_dry = None
                            for cfg_r, radarr_client in radarr_instances_list:
                                m = radarr_client.find_movie_by_imdb(imdb, radarr_movies_cache.get(cfg_r["id"], []))
                                if m:
                                    found_movie_dry = m
                                    found_cfg_r_dry = cfg_r
                                    found_client_dry = radarr_client
                                    break
                            if not found_movie_dry:
                                self._log(f"    ✗ {title} — not found in any Radarr → SKIP")
                                continue
                            precond_id_r_dry, _ = radarr_tag_ids.get(found_cfg_r_dry["id"], (None, None))
                            if precond_id_r_dry is not None and not found_client_dry.movie_has_tag(found_movie_dry, precond_id_r_dry):
                                self._log(f"    ✗ {title} — missing tag '{precondition_tag}' in Radarr → SKIP")
                                continue
                            key = f"{rule_id}:movie:{imdb}"
                            if self.state.is_processed(key):
                                self._log(f"    ✓ {title} — already processed → SKIP")
                                continue
                            if require_all_watched:
                                watchers = movie_watched_by.get(imdb, set())
                                if not required_user_keys.issubset(watchers):
                                    missing = len(required_user_keys) - len(required_user_keys & watchers)
                                    self._log(f"    ⏳ {title} — {missing} user(s) haven't watched yet → WAITING")
                                    continue
                            if not self._delay_elapsed(item.get("watched_at", ""), delay_days):
                                self._log(f"    ⏳ {title} — waiting {delay_days}d delay → PENDING")
                                continue
                            acts = []
                            if rule_actions.get("unmonitor"): acts.append("unmonitor")
                            if rule_actions.get("delete_file"): acts.append("delete file")
                            if rule_actions.get("add_tag"): acts.append(f"tag:{rule_actions['add_tag']}")
                            instance_label = f"[{found_cfg_r_dry.get('name','Radarr')}] " if len(radarr_instances_list) > 1 else ""
                            self._log(f"    → WOULD: {title} — {instance_label}{' + '.join(acts) or 'no actions'}")
                        self._log("")

                for item in rule_watched:
                    if item["type"] != "Movie":
                        continue
                    imdb = item.get("imdb_id")
                    if not imdb:
                        continue
                    # Find movie across all Radarr instances
                    found_movie = None
                    found_cfg_r = None
                    found_radarr_client = None
                    for cfg_r, radarr_client in radarr_instances_list:
                        m = radarr_client.find_movie_by_imdb(imdb, radarr_movies_cache.get(cfg_r["id"], []))
                        if m:
                            found_movie = m
                            found_cfg_r = cfg_r
                            found_radarr_client = radarr_client
                            break
                    if not found_movie:
                        continue
                    radarr_precond_tag_id_r, radarr_action_tag_id_r = radarr_tag_ids.get(found_cfg_r["id"], (None, None))
                    if radarr_precond_tag_id_r is not None and not found_radarr_client.movie_has_tag(found_movie, radarr_precond_tag_id_r):
                        continue

                    key = f"{rule_id}:movie:{imdb}"
                    if self.state.is_processed(key):
                        continue

                    if require_all_watched:
                        watchers = movie_watched_by.get(imdb, set())
                        if not required_user_keys.issubset(watchers):
                            if not dry_run:
                                missing = len(required_user_keys) - len(required_user_keys & watchers)
                                self._log(f"    ⏳ Waiting ({missing} user(s) haven't watched): {found_movie['title']}")
                            continue

                    if dry_run:
                        self.state.add_dry_run_preview(key, {
                            "title": found_movie["title"], "type": "movie",
                            "watched_at": item.get("watched_at", ""),
                            "imdb_id": imdb, "rule": rule_name,
                        })
                    else:
                        self.state.add_pending(key, {
                            "title": found_movie["title"], "type": "movie",
                            "watched_at": item.get("watched_at", ""),
                            "imdb_id": imdb, "rule": rule_name,
                        })

                    if not self._delay_elapsed(item.get("watched_at", ""), delay_days):
                        if not dry_run:
                            self._log(f"    ⏳ Pending (delay): {found_movie['title']}")
                        continue

                    if not dry_run:
                        try:
                            self._log(f"    🎬 Processing: {found_movie['title']}")
                            if rule_actions.get("unmonitor"):
                                found_radarr_client.unmonitor_movie(found_movie)
                                self._log("      ✓ Unmonitored")
                            if radarr_action_tag_id_r:
                                found_radarr_client.add_tag_to_movie(found_movie, radarr_action_tag_id_r)
                                self._log(f"      ✓ Tagged: {rule_actions['add_tag']}")
                            if rule_actions.get("delete_file"):
                                _movie_size = found_movie.get("movieFile", {}).get("size", 0)
                                found_radarr_client.delete_movie_file(found_movie)
                                self._log("      ✓ File deleted")
                            else:
                                _movie_size = 0
                            self.state.mark_processed(key, {"title": found_movie["title"], "type": "movie", "rule": rule_name, "size_bytes": _movie_size})
                        except Exception as _err:
                            self._log(f"      ✗ Failed: {_err}", "error")

            # ── Episodes ──
            if sonarr_instances and content_type in ("all", "episodes"):
                self._log("  [Episodes]")

                # Dry-run: grouped per-show summary with compact season/episode breakdown
                if dry_run:
                    # Group by series name (lowercase) — reliable across all Emby/Jellyfin versions.
                    # Also collect every TVDB ID seen per group for Sonarr lookup.
                    series_groups: dict = {}  # name_key -> {tvdb_ids, name, items}
                    for item in rule_watched:
                        if item["type"] != "Episode":
                            continue
                        label = (item.get("series_name") or "").strip() or "Unknown"
                        name_key = label.lower()
                        tvdb_str = item.get("series_tvdb_id") or item.get("tvdb_id")
                        try:
                            tvdb_val = int(tvdb_str) if tvdb_str else None
                        except (ValueError, TypeError):
                            tvdb_val = None
                        if name_key not in series_groups:
                            series_groups[name_key] = {"tvdb_ids": set(), "name": label, "items": []}
                        if tvdb_val:
                            series_groups[name_key]["tvdb_ids"].add(tvdb_val)
                        series_groups[name_key]["items"].append(item)

                    if not series_groups:
                        self._log("    No episodes in watch history match this rule's user/content filter.")
                    else:
                        not_in_sonarr_names: list = []
                        show_output: list = []

                        for name_key, group in sorted(series_groups.items()):
                            series_name = group["name"]
                            tvdb_ids_g = group["tvdb_ids"]
                            # Deduplicate episodes across users
                            unique_eps: dict = {}  # (s, e) -> item
                            for itm in group["items"]:
                                s, e = itm.get("season"), itm.get("episode")
                                if s is not None and e is not None and (s, e) not in unique_eps:
                                    unique_eps[(s, e)] = itm

                            found_in_sonarr = False
                            for cfg_s, sonarr in sonarr_instances:
                                sid = cfg_s["id"]
                                series_list = sonarr_series_caches.get(sid, [])
                                # Try every collected TVDB ID, then fall back to title match
                                series = None
                                for tid in tvdb_ids_g:
                                    series = sonarr.find_series_by_tvdb(tid, series_list)
                                    if series:
                                        break
                                if not series:
                                    series = sonarr.find_series_by_title(series_name, series_list)
                                if not series:
                                    continue
                                found_in_sonarr = True
                                sonarr_tvdb_id = series.get("tvdbId")
                                precond_id, _ = sonarr_tag_ids.get(sid, (None, None))

                                if precond_id is not None and not sonarr.series_has_tag(series, precond_id):
                                    show_output.append(f"  ✗ {series_name}  [missing tag '{precondition_tag}']:")
                                    show_output += self._format_seasons(set(unique_eps.keys()))
                                    break

                                # Tag OK — classify each unique episode
                                # Compute protection set for this series
                                if keep_last > 0:
                                    if keep_last_by_time:
                                        this_protected = series_protected_by_time.get(series_name.lower(), set())
                                    else:
                                        s_id = series["id"]
                                        if s_id not in sonarr_ep_protections:
                                            # Protect the last N *watched* episodes only,
                                            # not the last N episodes in Sonarr's full list.
                                            ep_nums = sorted(unique_eps.keys())
                                            sonarr_ep_protections[s_id] = set(ep_nums[-keep_last:])
                                            # Still fetch all Sonarr episodes for sonarr_ep_sets
                                            # (used to skip episodes Sonarr doesn't manage).
                                            all_ser_eps = sonarr.get_episodes(s_id)
                                            sonarr_ep_sets[s_id] = {
                                                (ep["seasonNumber"], ep["episodeNumber"])
                                                for ep in all_ser_eps if ep["seasonNumber"] > 0
                                            }
                                            sonarr_ep_full_cache[s_id] = all_ser_eps
                                        this_protected = sonarr_ep_protections[s_id]
                                else:
                                    this_protected = set()

                                # Build full episode set for accurate dry-run filtering.
                                # In real runs, find_episode() returns None for episodes Sonarr
                                # doesn't manage, so they're silently skipped. Mirror that here.
                                dry_s_id = series["id"]
                                if dry_s_id not in sonarr_ep_sets:
                                    _all_eps = sonarr.get_episodes(dry_s_id)
                                    sonarr_ep_sets[dry_s_id] = {
                                        (ep["seasonNumber"], ep["episodeNumber"])
                                        for ep in _all_eps if ep["seasonNumber"] > 0
                                    }
                                    sonarr_ep_full_cache[dry_s_id] = _all_eps
                                sonarr_ep_set_dry = sonarr_ep_sets[dry_s_id]

                                # Implicit older episodes: on disk in Sonarr but not in watch history
                                would_act_implicit: set = set()
                                if force_delete_older and unique_eps:
                                    # Per-season, per-user max episode.
                                    # user_season_maxes_dry: {season -> {user_key -> max_ep}}
                                    _user_season_maxes_dry: dict = {}
                                    for _itm_dry in group["items"]:
                                        _uk_d = _itm_dry.get("_user_key", "")
                                        _sd = _itm_dry.get("season")
                                        _ed = _itm_dry.get("episode")
                                        if _sd is None or _ed is None:
                                            continue
                                        if _sd not in _user_season_maxes_dry:
                                            _user_season_maxes_dry[_sd] = {}
                                        if _ed > _user_season_maxes_dry[_sd].get(_uk_d, 0):
                                            _user_season_maxes_dry[_sd][_uk_d] = _ed
                                    # All users who have any watch in this series
                                    _all_series_users_dry: set = set()
                                    for _su in _user_season_maxes_dry.values():
                                        _all_series_users_dry.update(_su.keys())
                                    _watched_se_g = set(unique_eps.keys())
                                    if dry_s_id not in sonarr_ep_full_cache:
                                        sonarr_ep_full_cache[dry_s_id] = sonarr.get_episodes(dry_s_id)
                                    for _ep_impl in sonarr_ep_full_cache[dry_s_id]:
                                        _s_i = _ep_impl["seasonNumber"]
                                        _e_i = _ep_impl["episodeNumber"]
                                        if _s_i == 0:
                                            continue
                                        # Skip seasons where any RULE-SCOPED user has no recorded watch.
                                        # required_user_keys covers all users the rule targets — if any
                                        # of them has zero watches in this season, skip the season.
                                        _s_users_dry = _user_season_maxes_dry.get(_s_i, {})
                                        if not required_user_keys.issubset(set(_s_users_dry.keys())):
                                            continue
                                        _season_wm_dry = min(_s_users_dry.values())
                                        if _e_i >= _season_wm_dry:
                                            continue
                                        if (_s_i, _e_i) in _watched_se_g:
                                            continue
                                        if _ep_impl.get("episodeFileId", 0) == 0:
                                            continue
                                        _ep_key_i = f"{rule_id}:ep:{sonarr_tvdb_id}:S{_s_i:02d}E{_e_i:02d}"
                                        if self.state.is_processed(_ep_key_i):
                                            continue
                                        if keep_last > 0 and (_s_i, _e_i) in this_protected:
                                            continue
                                        would_act_implicit.add((_s_i, _e_i))

                                would_act: set = set()
                                would_keep: set = set()
                                would_pending: set = set()
                                would_waiting_users: set = set()
                                already_done = 0
                                for (s, e), ep_item in unique_eps.items():
                                    ep_key = f"{rule_id}:ep:{sonarr_tvdb_id}:S{s:02d}E{e:02d}"
                                    if self.state.is_processed(ep_key):
                                        already_done += 1
                                        continue
                                    if require_all_watched:
                                        watchers = episode_watched_by.get((series_name.lower(), s, e), set())
                                        if not required_user_keys.issubset(watchers):
                                            would_waiting_users.add((s, e))
                                            continue
                                    if not self._delay_elapsed(ep_item.get("watched_at", ""), delay_days):
                                        would_pending.add((s, e))
                                        continue
                                    if (s, e) in this_protected:
                                        would_keep.add((s, e))
                                    elif (s, e) in sonarr_ep_set_dry:
                                        would_act.add((s, e))
                                    # else: not in Sonarr's DB — find_episode() would return None
                                    # in a real run, so silently omit from the preview too

                                acts = []
                                if rule_actions.get("unmonitor"): acts.append("unmonitor")
                                if rule_actions.get("delete_file"): acts.append("delete file")
                                if rule_actions.get("add_tag"): acts.append(f"tag:{rule_actions['add_tag']}")
                                acts_str = " + ".join(acts) or "no actions"

                                if already_done and not would_act and not would_act_implicit and not this_protected and not would_pending and not would_waiting_users:
                                    show_output.append(f"  ✓ {series_name}:  [all {already_done} eps already processed]")
                                    break

                                show_output.append(f"  ✓ {series_name}:")
                                if would_act:
                                    show_output.append(f"    Process ({acts_str}):")
                                    show_output += self._format_seasons(would_act)
                                if would_act_implicit:
                                    show_output.append(f"    Process ({acts_str}) [older — not in watch history]:")
                                    show_output += self._format_seasons(would_act_implicit)
                                if this_protected:
                                    # Show every episode in the protection window, including those
                                    # not yet watched (they won't be in would_keep but are still safe).
                                    show_output.append(f"    Keep on disk (keep-last-{keep_last}):")
                                    show_output += self._format_seasons(this_protected)
                                if would_pending:
                                    show_output.append(f"    Pending ({delay_days}d delay):")
                                    show_output += self._format_seasons(would_pending)
                                if would_waiting_users:
                                    show_output.append(f"    Waiting (not all users watched yet):")
                                    show_output += self._format_seasons(would_waiting_users)
                                if already_done:
                                    show_output.append(f"    ({already_done} of {len(unique_eps)} eps already processed)")
                                # Populate dry_run_preview for dashboard (honours keep-last)
                                for (s, e) in would_act | would_pending | would_act_implicit:
                                    ep_item = unique_eps.get((s, e), {})
                                    self.state.add_dry_run_preview(
                                        f"{rule_id}:ep:{sonarr_tvdb_id}:S{s:02d}E{e:02d}",
                                        {"title": f"{series_name} S{s:02d}E{e:02d}",
                                         "type": "episode",
                                         "watched_at": ep_item.get("watched_at", ""),
                                         "tvdb_id": sonarr_tvdb_id, "season": s,
                                         "episode": e, "rule": rule_name})
                                break

                            if not found_in_sonarr:
                                if delete_from_emby and not precondition_tag:
                                    show_output.append(f"  ✓ {series_name}  [direct delete from media server]:")
                                    all_se = set(unique_eps.keys())
                                    if keep_last > 0:
                                        if keep_last_by_time:
                                            protected_se = series_protected_by_time.get(series_name.lower(), set())
                                        else:
                                            sorted_se = sorted(all_se, key=lambda x: (x[0], x[1]), reverse=True)
                                            protected_se = set(sorted_se[:keep_last])
                                        would_delete = all_se - protected_se
                                        kept_in_watched = protected_se & all_se
                                        if would_delete:
                                            show_output.append(f"    Process (delete from media server):")
                                            show_output += self._format_seasons(would_delete)
                                        if kept_in_watched:
                                            show_output.append(f"    Keep on disk (keep-last-{keep_last}):")
                                            show_output += self._format_seasons(kept_in_watched)
                                        if not would_delete and not kept_in_watched:
                                            show_output.append(f"    (nothing to delete)")
                                    else:
                                        show_output.append(f"    Process (delete from media server):")
                                        show_output += self._format_seasons(all_se)
                                    # Populate dry_run_preview for dashboard
                                    _direct_delete = would_delete if keep_last > 0 else all_se
                                    for (s, e) in _direct_delete:
                                        ep_item = unique_eps.get((s, e), {})
                                        self.state.add_dry_run_preview(
                                            f"{rule_id}:ep:direct:{series_name}:S{s:02d}E{e:02d}",
                                            {"title": f"{series_name} S{s:02d}E{e:02d}",
                                             "type": "server_episode",
                                             "watched_at": ep_item.get("watched_at", ""),
                                             "season": s, "episode": e, "rule": rule_name})
                                else:
                                    not_in_sonarr_names.append(series_name)

                        skip_note = f", {len(not_in_sonarr_names)} not in Sonarr — skipped" if not_in_sonarr_names else ""
                        self._log(f"  Dry-run preview — {len(series_groups)} unique shows{skip_note}:")
                        for line in show_output:
                            self._log(line)
                        if not_in_sonarr_names:
                            self._log(f"  [{len(not_in_sonarr_names)} shows not in Sonarr — SKIP]")
                        self._log("")

                # Per-series high-watermark tracking for force_delete_older feature
                series_watermarks_map: dict = {}
                # Track which (sonarr_instance_id, series_id) combos have been tagged
                # this run — tag is show-level, no need to re-apply for every episode
                tagged_series_this_run: set = set()

                for item in rule_watched:
                    if item["type"] != "Episode":
                        continue
                    season = item.get("season")
                    episode_num = item.get("episode")
                    if season is None or episode_num is None:
                        continue
                    tvdb_str = item.get("series_tvdb_id") or item.get("tvdb_id")
                    try:
                        tvdb_id_item = int(tvdb_str) if tvdb_str else None
                    except (ValueError, TypeError):
                        tvdb_id_item = None

                    for cfg_s, sonarr in sonarr_instances:
                        sid = cfg_s["id"]
                        series_list = sonarr_series_caches.get(sid, [])
                        # Try TVDB ID first, then fall back to title match
                        series = None
                        if tvdb_id_item:
                            series = sonarr.find_series_by_tvdb(tvdb_id_item, series_list)
                        if not series:
                            series = sonarr.find_series_by_title(item.get("series_name", ""), series_list)
                        if not series:
                            continue

                        tvdb_id = series.get("tvdbId") or tvdb_id_item or 0
                        precond_id, action_tag_id = sonarr_tag_ids.get(sid, (None, None))
                        if precond_id is not None and not sonarr.series_has_tag(series, precond_id):
                            continue

                        show_title = item.get("series_name") or series["title"]
                        ep_label = f"{show_title} S{season:02d}E{episode_num:02d}"
                        # Track per-series watermark for force_delete_older feature.
                        # user_maxes tracks each user's personal maximum episode so we can
                        # take the min-of-maxes as the safe watermark for multi-user rules.
                        if force_delete_older:
                            _inst_key = (sid, series["id"])
                            _user_key_item = item.get("_user_key", "")
                            _wm = series_watermarks_map.get(_inst_key)
                            if _wm is None:
                                series_watermarks_map[_inst_key] = {
                                    "sonarr": sonarr, "series": series,
                                    "tvdb_id": tvdb_id, "show_title": show_title,
                                    "user_maxes": {_user_key_item: (season, episode_num)},
                                    "watched_se": {(season, episode_num)},
                                    "sid": sid,
                                }
                            else:
                                _wm["watched_se"].add((season, episode_num))
                                _cur_max = _wm["user_maxes"].get(_user_key_item, (0, 0))
                                if (season, episode_num) > _cur_max:
                                    _wm["user_maxes"][_user_key_item] = (season, episode_num)
                        key = f"{rule_id}:ep:{tvdb_id}:S{season:02d}E{episode_num:02d}"
                        if self.state.is_processed(key):
                            continue

                        if not dry_run:
                            self.state.add_pending(key, {
                                "title": ep_label, "type": "episode",
                                "watched_at": item.get("watched_at", ""),
                                "tvdb_id": tvdb_id, "season": season,
                                "episode": episode_num, "rule": rule_name,
                            })

                        if not self._delay_elapsed(item.get("watched_at", ""), delay_days):
                            if not dry_run:
                                self._log(f"    ⏳ Pending (delay): {ep_label}")
                            continue

                        if require_all_watched:
                            name_key = (item.get("series_name") or series.get("title", "")).strip().lower()
                            watchers = episode_watched_by.get((name_key, season, episode_num), set())
                            if not required_user_keys.issubset(watchers):
                                missing = len(required_user_keys) - len(required_user_keys & watchers)
                                if not dry_run:
                                    self._log(f"    ⏳ Waiting ({missing} user(s) haven't watched): {ep_label}")
                                continue

                        if keep_last > 0:
                            if keep_last_by_time:
                                name_key = (item.get("series_name") or series.get("title", "")).strip().lower()
                                is_protected = (season, episode_num) in series_protected_by_time.get(name_key, set())
                            else:
                                s_id = series["id"]
                                if s_id not in sonarr_ep_protections:
                                    # Protect the last N *watched* episodes for this series only.
                                    # Normalise to int: Emby/Jellyfin return TVDB IDs as strings,
                                    # Sonarr returns them as integers.
                                    series_tvdb = series.get("tvdbId")
                                    try:
                                        series_tvdb_int = int(series_tvdb)
                                    except (TypeError, ValueError):
                                        series_tvdb_int = None
                                    show_name_lower = series["title"].strip().lower()
                                    watched_se = sorted(set(
                                        (w["season"], w["episode"])
                                        for w in rule_watched
                                        if w["type"] == "Episode"
                                        and w.get("season") is not None
                                        and w.get("episode") is not None
                                        and (
                                            # Primary: match by series TVDB ID
                                            (series_tvdb_int is not None and (
                                                _safe_int(w.get("series_tvdb_id")) == series_tvdb_int
                                                or _safe_int(w.get("tvdb_id")) == series_tvdb_int
                                            ))
                                            # Fallback: match by series name when TVDB IDs are
                                            # missing or episode-level (Emby doesn't always
                                            # populate SeriesProviderIds.Tvdb)
                                            or (w.get("series_name") or "").strip().lower() == show_name_lower
                                        )
                                    ))
                                    sonarr_ep_protections[s_id] = set(watched_se[-keep_last:])
                                is_protected = (season, episode_num) in sonarr_ep_protections[s_id]
                        else:
                            is_protected = False

                        ep_data = sonarr.find_episode(series["id"], season, episode_num)
                        if not ep_data:
                            continue

                        if not dry_run:
                            try:
                                suffix = " [keep-last protected]" if is_protected else ""
                                self._log(f"    📺 Processing: {ep_label}{suffix}")
                                if rule_actions.get("unmonitor"):
                                    sonarr.unmonitor_episode(ep_data)
                                    self._log("      ✓ Unmonitored")
                                if action_tag_id:
                                    _tag_key = (sid, series["id"])
                                    if _tag_key not in tagged_series_this_run:
                                        sonarr.add_tag_to_series(series, action_tag_id)
                                        tagged_series_this_run.add(_tag_key)
                                        self._log(f"      ✓ Show tagged: {rule_actions['add_tag']}")
                                if rule_actions.get("delete_file") and not is_protected:
                                    _ep_size = sonarr.get_episode_file_size(ep_data)
                                    sonarr.delete_episode_file(ep_data)
                                    self._log("      ✓ File deleted")
                                elif is_protected and rule_actions.get("delete_file"):
                                    _ep_size = 0
                                    self._log(f"      ⏸ File kept (keep last {keep_last})")
                                else:
                                    _ep_size = 0
                                # Only mark processed if the file was actually deleted or
                                # no delete action is configured. Protected episodes must
                                # stay unprocessed so they get re-evaluated each run —
                                # once a newer episode is watched, protection shifts.
                                if not (is_protected and rule_actions.get("delete_file")):
                                    self.state.mark_processed(key, {
                                        "title": ep_label, "type": "episode", "rule": rule_name,
                                        "size_bytes": _ep_size,
                                    })
                            except Exception as _err:
                                self._log(f"      ✗ Failed to process {ep_label}: {_err}", "error")

                # ── Implicit older episode deletion (force_delete_older_if_newer_watched) ──
                if force_delete_older and not dry_run and series_watermarks_map:
                    self._log("  [Episodes — implicit older, not in watch history]")
                    for (_inst_sid, _ser_id), _sdata in series_watermarks_map.items():
                        _sonarr = _sdata["sonarr"]
                        _series = _sdata["series"]
                        _tvdb_id = _sdata["tvdb_id"]
                        _show_title = _sdata["show_title"]
                        _user_season_maxes = _sdata.get("user_season_maxes", {})
                        # Use required_user_keys (all users the rule targets) — not just those
                        # who watched this series — so any in-scope user with zero season watches
                        # blocks implicit deletion for that season.
                        _watched_se = _sdata["watched_se"]
                        _, _act_tag_id = sonarr_tag_ids.get(_inst_sid, (None, None))
                        if _ser_id not in sonarr_ep_full_cache:
                            sonarr_ep_full_cache[_ser_id] = _sonarr.get_episodes(_ser_id)
                        for _ep_data in sonarr_ep_full_cache[_ser_id]:
                            _s = _ep_data["seasonNumber"]
                            _e = _ep_data["episodeNumber"]
                            if _s == 0:
                                continue
                            # Only process seasons where ALL rule-scoped users have a recorded watch
                            _season_users = _user_season_maxes.get(_s, {})
                            if not required_user_keys.issubset(set(_season_users.keys())):
                                continue
                            # Per-season watermark = min of each user's max episode in this season
                            _season_wm = min(_season_users.values())
                            if _e >= _season_wm:
                                continue
                            if (_s, _e) in _watched_se:
                                continue
                            if _ep_data.get("episodeFileId", 0) == 0:
                                continue
                            _ep_label = f"{_show_title} S{_s:02d}E{_e:02d}"
                            _key = f"{rule_id}:ep:{_tvdb_id}:S{_s:02d}E{_e:02d}"
                            if self.state.is_processed(_key):
                                continue
                            if keep_last > 0:
                                if keep_last_by_time:
                                    _is_protected = (_s, _e) in series_protected_by_time.get(
                                        _show_title.strip().lower(), set())
                                else:
                                    _is_protected = (_s, _e) in sonarr_ep_protections.get(_ser_id, set())
                            else:
                                _is_protected = False
                            try:
                                _sfx = " [keep-last]" if _is_protected else " [older — not in library]"
                                self._log(f"    📺 Processing: {_ep_label}{_sfx}")
                                if rule_actions.get("unmonitor"):
                                    _sonarr.unmonitor_episode(_ep_data)
                                    self._log("      ✓ Unmonitored")
                                if _act_tag_id:
                                    _tk = (_inst_sid, _ser_id)
                                    if _tk not in tagged_series_this_run:
                                        _sonarr.add_tag_to_series(_series, _act_tag_id)
                                        tagged_series_this_run.add(_tk)
                                        self._log(f"      ✓ Show tagged: {rule_actions['add_tag']}")
                                if rule_actions.get("delete_file") and not _is_protected:
                                    _ep_size = _sonarr.get_episode_file_size(_ep_data)
                                    _sonarr.delete_episode_file(_ep_data)
                                    self._log("      ✓ File deleted")
                                elif _is_protected and rule_actions.get("delete_file"):
                                    _ep_size = 0
                                    self._log(f"      ⏸ File kept (keep last {keep_last})")
                                else:
                                    _ep_size = 0
                                if not (_is_protected and rule_actions.get("delete_file")):
                                    self.state.mark_processed(_key, {
                                        "title": _ep_label, "type": "episode",
                                        "rule": rule_name, "size_bytes": _ep_size,
                                    })
                            except Exception as _err2:
                                self._log(f"      ✗ Failed: {_ep_label}: {_err2}", "error")

            # ── Media Server Direct Delete (episodes not managed by Sonarr) ──
            # Applies to both Emby and Jellyfin servers.
            # Skip if precondition_tag is set — we can't verify the tag for non-Sonarr shows.
            if delete_from_emby and not dry_run and not precondition_tag and content_type in ("all", "episodes"):
                sonarr_tvdb_ids: set = set()
                for sid, series_list in sonarr_series_caches.items():
                    for s in series_list:
                        tv = s.get("tvdbId")
                        if tv:
                            sonarr_tvdb_ids.add(tv)

                # Build keep-last protection sets for non-Sonarr shows using watch history
                non_sonarr_protected: dict = {}  # series_name.lower() -> set of (season, ep)
                if keep_last > 0:
                    non_sonarr_eps: dict = {}  # name_key -> list of (watched_at, season, ep)
                    for item in rule_watched:
                        if item["type"] != "Episode":
                            continue
                        tvdb_str = item.get("series_tvdb_id") or item.get("tvdb_id")
                        try:
                            tvdb_id_ns = int(tvdb_str) if tvdb_str else None
                        except (ValueError, TypeError):
                            tvdb_id_ns = None
                        if tvdb_id_ns and tvdb_id_ns in sonarr_tvdb_ids:
                            continue
                        nk = (item.get("series_name") or "Unknown").strip().lower()
                        s_n = item.get("season") or 0
                        e_n = item.get("episode") or 0
                        non_sonarr_eps.setdefault(nk, []).append(
                            (item.get("watched_at") or "", s_n, e_n)
                        )
                    for nk, eps in non_sonarr_eps.items():
                        if keep_last_by_time:
                            eps.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
                        else:
                            eps.sort(key=lambda x: (x[1], x[2]), reverse=True)
                        seen_ns: set = set()
                        protected_ns: set = set()
                        for tup in eps:
                            s_n, e_n = tup[1], tup[2]
                            if (s_n, e_n) not in seen_ns:
                                seen_ns.add((s_n, e_n))
                                protected_ns.add((s_n, e_n))
                                if len(protected_ns) >= keep_last:
                                    break
                        non_sonarr_protected[nk] = protected_ns

                emby_count = 0
                kept_count = 0
                for item in rule_watched:
                    if item["type"] != "Episode":
                        continue
                    tvdb_str = item.get("series_tvdb_id") or item.get("tvdb_id")
                    try:
                        tvdb_id = int(tvdb_str) if tvdb_str else None
                    except (ValueError, TypeError):
                        tvdb_id = None
                    if tvdb_id and tvdb_id in sonarr_tvdb_ids:
                        continue  # already handled by the Sonarr loop
                    item_id = item.get("item_id")
                    server_id = item.get("_server_id")
                    if not item_id or not server_id:
                        continue
                    season = item.get("season") or 0
                    ep_num = item.get("episode") or 0
                    ep_label = f"{item.get('series_name', 'Unknown')} S{season:02d}E{ep_num:02d}"
                    key = f"{rule_id}:emby-ep:{item_id}"
                    if self.state.is_processed(key):
                        continue
                    # Track in pending so dashboard shows delayed direct-delete items
                    self.state.add_pending(key, {
                        "title": ep_label, "type": "server_episode",
                        "watched_at": item.get("watched_at", ""),
                        "season": season, "episode": ep_num, "rule": rule_name,
                    })
                    if not self._delay_elapsed(item.get("watched_at", ""), delay_days):
                        self._log(f"    ⏳ Pending (delay): {ep_label}")
                        continue
                    # Apply keep-last protection
                    nk = (item.get("series_name") or "Unknown").strip().lower()
                    if keep_last > 0 and (season, ep_num) in non_sonarr_protected.get(nk, set()):
                        self._log(f"    ⏸ Kept (keep-last-{keep_last}): {ep_label}")
                        kept_count += 1
                        continue
                    client = clients_by_server_id.get(server_id)
                    if not client:
                        continue
                    self._log(f"    🗑 Deleting from {client.name}: {ep_label}")
                    try:
                        client.delete_item(item_id)
                        self._log("      ✓ Deleted")
                    except Exception as exc:
                        self._log(f"      ✗ Delete failed: {exc}", "error")
                    self.state.mark_processed(key, {"title": ep_label, "type": "server_episode", "rule": rule_name})
                    emby_count += 1
                if emby_count:
                    self._log(f"  [{emby_count} episodes deleted directly from media server]")
                if kept_count:
                    self._log(f"  [{kept_count} episodes kept (keep-last-{keep_last})]")

        if dry_run:
            self.state.save()  # persist dry_run_preview so dashboard can read it
            # Do NOT update last_run on dry-runs — history window must stay intact
            # so subsequent dry-runs and real runs see the full history
        else:
            self.state.update_last_run()
        self._log("\nRun complete.")
        return self.log
