# Erasarr

**Automatic media cleanup for Emby/Jellyfin → Sonarr/Radarr**

No Trakt.tv required. Reads watch history directly from your media server.

---

## What it does

1. Connects to your **Emby** or **Jellyfin** server(s)
2. Checks which movies/episodes specific users have watched
3. Finds those items in **Radarr** (movies) and/or **Sonarr** (TV shows)
4. Optionally applies a **precondition tag** — only process items you've tagged
5. Takes configurable **actions** per rule: unmonitor, delete file, add tag
6. Respects a **delay** — wait N days after watching before acting
7. Runs on a **schedule** you define, or manually via the web UI

---

## Quick Start (prebuilt image — no build step)

Skip cloning the repo and building locally — pull the published image straight from GHCR:

```bash
docker run -d \
  --name erasarr \
  -p 5000:5000 \
  -v ./data:/data \
  ghcr.io/nenchi/erasarr:latest
```

Or in your own `docker-compose.yml`, use `image:` instead of `build: .`:

```yaml
services:
  erasarr:
    image: ghcr.io/nenchi/erasarr:latest
    ports:
      - "5000:5000"
    volumes:
      - ./data:/data
    restart: unless-stopped
```

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
| **Delete from Emby/Jellyfin** | Removes items not in Sonarr/Radarr via the media server API |
| **Add Tag** | Adds a tag to the movie/show (e.g. `processed`) |
| **Delay** | Waits N days after watched before doing anything |
| **Keep Last N Episodes** | Protects the N most recent episodes per show from deletion |
| **Require All Users Watched** | Only acts when every user covered by the rule has watched the item |

You can combine any of these. A common setup:
- Unmonitor ✓, Delete File ✓, Delay = 7 days
- This means: wait a week, then unmonitor + delete

---

## Multiple Sonarr Instances

You can add multiple Sonarr instances (e.g. Sonarr SD + Sonarr 4K).
Erasarr will check all of them for each watched episode.

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


---

## Disclaimer

This tool is provided **as-is**, for personal use only, as an automation utility.

- The author(s) take **no responsibility** for any data loss, deleted files, or unintended consequences resulting from the use of this software.
- **You are solely responsible** for how you configure and run this tool on your own systems.
- This project does **not** download, distribute, or interact with copyrighted content in any way — it only manages metadata and local files you already own and store on your own infrastructure.
- Use this software **at your own risk**. Always test with a **Dry Run** before enabling live deletions.

---

## License

MIT — free to use, modify, fork, and distribute. See [LICENSE](LICENSE) for the full text.
