# Erasarr

**Automatic media cleanup for Emby/Jellyfin → Sonarr/Radarr**

No Trakt.tv required. Reads watch history directly from your media server.

---

## What it does

1. Connects to your **Emby** or **Jellyfin** server(s)
2. Checks which movies/episodes specific users have watched
3. Finds those items in **Radarr** (movies) and/or **Sonarr** (TV shows) — multiple instances of each supported
4. Optionally applies a **precondition tag** — only process items you've tagged
5. Takes configurable **actions** per rule: unmonitor, delete file, add tag
6. Respects a **delay** — wait N days after watching before acting
7. Supports **Require All Users Watched** — waits until every selected user has watched before acting
8. Runs on a **schedule** you define, or manually via the web UI

---

## Quick Start (Docker — recommended)

```bash
git clone https://github.com/nenchi/erasarr.git
cd erasarr
docker compose up -d
```

Open **http://localhost:5000** in your browser.

On first boot you'll be prompted to **create an account** — choose any username and password you like.

---

## Quick Start (Python / manual)

```bash
pip install -r requirements.txt
DATA_DIR=./data python app.py
```

Or CLI-only (no web UI):

```bash
python cli.py              # normal run
python cli.py --dry-run    # preview, no changes made
python cli.py --data /custom/path   # custom data directory
```

---

## Configuration (via Web UI)

1. **Media Servers** → Add your Jellyfin/Emby address + API key
2. **Arr Apps** → Add Radarr and/or as many Sonarr instances as you need
3. **Actions** → Create actions that define what happens when media is watched
4. **Schedule** → Choose manual, interval, cron, or monthly

### Actions

Each action is an independent policy that can target:

- **Specific media servers** — All servers or a custom selection
- **Specific users** — All users or individual user accounts
- **Content type** — All, Movies only, or Episodes only

Actions are processed in order. You can create multiple actions for different scenarios (e.g. one for movies, another for TV with a longer delay).

### Precondition Tag (important!)

If you set a **Precondition Tag** on a rule (e.g. `erasarr`), only items that have that tag in Radarr/Sonarr will be processed. This is the safest way to run — you explicitly opt in each title.

**Workflow:**
1. In Radarr: Settings → Tags → create tag `erasarr`
2. Apply the tag to any movie you want auto-managed after watching
3. Set `erasarr` as the precondition tag in the rule
4. Same for Sonarr series

Leave the precondition tag **empty** to process all watched items.

---

## Actions Explained

| Action | What happens |
|--------|-------------|
| **Unmonitor** | Tells Radarr/Sonarr to stop looking for upgrades |
| **Delete File** | Deletes the media file from disk (⚠ irreversible!) |
| **Delete from Emby/Jellyfin** | Removes **episodes not in Sonarr** via the media server API. Only active when Precondition Tag is blank. Does not apply to movies. |
| **Add Tag** | Adds a tag to the movie/show (e.g. `processed`) |
| **Delay** | Waits N days after watched before doing anything |
| **Keep Last N Episodes** | Protects the N most recent episodes per show from deletion (episodes only, per series) |
| **Require All Users Watched** | Only acts when every user covered by the rule has watched the item (movies + episodes) |

You can combine any of these. A common setup:
- Unmonitor ✓, Delete File ✓, Delay = 7 days
- This means: wait a week, then unmonitor + delete

---

## Multiple Sonarr / Radarr Instances

You can add multiple Sonarr **and** Radarr instances (e.g. SD + 4K libraries).
Erasarr will check all instances of each type for every watched item — the first instance that contains the item wins.

---

## Backup & Restore

You can export and import your entire configuration (servers, Arr apps, rules, schedule) from the sidebar in the web UI:

- **⬇ Export config** — downloads `erasarr-config.json`
- **⬆ Import config** — uploads a previously exported JSON and applies it immediately

This is useful for migrating between servers or keeping a configuration backup.

---

## Dark / Light Mode

The UI supports both dark (default) and light themes. Toggle with the ☀️/🌙 button in the top-right corner of every page. Your preference is saved in the browser.

---

## State & History

Erasarr keeps a state file (`data/state.json`) to remember what it has already
processed so it doesn't act on the same item twice.

- **Pending** items = watched but delay hasn't elapsed yet
- **Processed** items = action already taken, won't be touched again

You can clear the state from the Dashboard if needed.

---

## Storage

All configuration and credentials are stored in a **SQLite database** (`data/erasarr.db`).
Passwords are hashed with PBKDF2 (via werkzeug) — never stored in plain text.

If you have an existing `config.json` or `auth.json` from an earlier version, Erasarr will automatically migrate them into the database on first run.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `/data` | Where the database and state file are stored |
| `PORT` | `5000` | Web UI port |
| `SECRET_KEY` | random | Flask session key (set a fixed value to keep sessions across restarts) |

---

## File Structure

```
erasarr/
├── app.py              # Flask web application
├── monitor.py          # Core monitoring logic
├── cli.py              # Command-line interface
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── templates/
│   ├── base.html
│   ├── register.html
│   ├── login.html
│   ├── dashboard.html
│   ├── servers.html
│   ├── arr.html
│   ├── settings.html       # Actions & schedule
│   └── change_password.html
└── data/               # Created automatically (mount as volume)
    ├── erasarr.db      # SQLite database (config + auth + run log)
    └── state.json      # Processed item tracking
```

---

## Reverse Proxy (Nginx example)

```nginx
server {
    listen 80;
    server_name erasarr.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

Add SSL via Let's Encrypt / Certbot.

---

## Troubleshooting

**Can't connect to Jellyfin/Emby?**
- Check the address includes `http://` and the correct port
- Make sure the API key has read access
- Test connection in the web UI before saving

**Items not being processed?**
- Check the precondition tag — is it applied to the media in Radarr/Sonarr?
- Check the delay — items may be in "Pending" status
- Try a Dry Run first to see what would happen

**Want to reprocess something?**
- Clear state from the Dashboard (resets all tracking)
- Or manually edit `data/state.json`
