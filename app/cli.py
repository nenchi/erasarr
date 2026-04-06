#!/usr/bin/env python3
"""
Erasarr CLI - Run from command line or cron
Usage:
  python cli.py                  # Normal run
  python cli.py --dry-run        # Preview only, no changes
  python cli.py --data /path     # Custom data directory
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime

from monitor import ErasarrMonitor

DATA_DIR = os.environ.get("DATA_DIR", "./data")
DB_FILE = os.path.join(DATA_DIR, "erasarr.db")
STATE_FILE = os.path.join(DATA_DIR, "state.json")

def load_config_from_db(db_file: str) -> dict:
    if not os.path.exists(db_file):
        return None
    conn = sqlite3.connect(db_file)
    row = conn.execute("SELECT data FROM config WHERE id = 1").fetchone()
    conn.close()
    return json.loads(row[0]) if row else None

def main():
    parser = argparse.ArgumentParser(description="Erasarr CLI")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without making changes")
    parser.add_argument("--data", default=DATA_DIR, help="Path to data directory")
    parser.add_argument("--state", default=STATE_FILE, help="Path to state.json")
    args = parser.parse_args()

    db_file = os.path.join(args.data, "erasarr.db")
    cfg = load_config_from_db(db_file)
    if cfg is None:
        print(f"ERROR: Database not found at {db_file}")
        print("Start the web UI first to configure Erasarr.")
        sys.exit(1)

    os.makedirs(args.data, exist_ok=True)

    print(f"Erasarr CLI {'[DRY RUN] ' if args.dry_run else ''}— {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    monitor = ErasarrMonitor(cfg, args.state)
    log_entries = monitor.run(dry_run=args.dry_run)

    print("\n── Log ──")
    for entry in log_entries:
        level_icon = {"info": "  ", "warning": "⚠ ", "error": "✗ "}.get(entry["level"], "  ")
        print(f"{entry['time'][11:19]}  {level_icon}{entry['msg']}")

    print("\nDone.")

if __name__ == "__main__":
    main()
