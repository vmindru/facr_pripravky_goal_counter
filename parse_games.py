#!/usr/bin/python  
"""
Save Czech FA match pages to games_database.db

Usage:
    python save_matches.py --games-url URL1 URL2 ...

A new row is written to `games` for every page and the goal
table is refreshed for that game_id (safe to re-run).

PREREQ: pip install requests beautifulsoup4 lxml
"""

import argparse, re, sqlite3, unicodedata
from datetime import datetime

import requests
from bs4 import BeautifulSoup


###############################################################################
# util helpers
###############################################################################
def strip_accents(txt: str) -> str:
    txt = unicodedata.normalize("NFKD", txt)
    return "".join(c for c in txt if not unicodedata.combining(c))


def make_id(txt: str) -> str:
    txt = strip_accents(txt).lower()
    txt = re.sub(r"\s+", "_", txt.strip())
    return re.sub(r"[^\w]", "", txt)          # keep a-z0-9_


###############################################################################
# HTML → data
###############################################################################
def parse_match(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    # ── meta ────────────────────────────────────────────────────────────────
    meta_parts = soup.select_one("h1.Match-meta").get_text(",").split(",")
    dt_raw, round_part = meta_parts[0].strip(), (meta_parts[1].strip() if len(meta_parts) > 1 else "")
    date_iso = datetime.strptime(dt_raw, "%d. %m. %Y %H:%M").strftime("%Y-%m-%d")

    teams_el = soup.select(".Match-teams .Match-team a")
    home_name, guest_name = (t.get_text(strip=True) for t in teams_el[:2])
    home_id, guest_id = make_id(home_name), make_id(guest_name)

    details = soup.select_one(".Match-detailsContainer").get_text(" ", strip=True)
    facr_game_id = re.search(r"Číslo utkání:\s*([0-9A-Z.]+)", details).group(1)
    venue = (re.search(r"Hřiště:\s*([^.]+)", details) or ["", ""]).group(1).strip()
    spectators = (re.search(r"Diváků:\s*(\d+)", details))
    spectators = int(spectators.group(1) if spectators else 0 )

    halftime_score = soup.select_one(".Match-result p").get_text(strip=True).strip("()")
    final_score = soup.select_one(".Match-result strong").get_text(strip=True)

    game_id = f"{home_id}_{guest_id}_{date_iso}_{round_part}"

    # ── 1. all squad players ───────────────────────────────────────────────
    squad = {}  # { (player_id, team_id): (player_name, team_name) }
    for section in soup.select(".Match-statsGrid section"):
        team_name = section.select_one("h2").get_text(strip=True)
        team_id = make_id(team_name)
        for row in section.select("tbody tr"):
            cols = row.find_all("td")
            if len(cols) >= 3:
                name_cell = cols[2].get_text(strip=True)
                name = re.sub(r"\s*\[.*?\]", "", name_cell)  # drop [K] etc.
                pid = make_id(name)
                squad[(pid, team_id)] = (name, team_name)

    # ── 2. goals from timeline ─────────────────────────────────────────────
    goal_counts = {k: 0 for k in squad}  # start every listed player with 0
    for li in soup.select(".MatchTimeline-item"):
        name = li.select_one("p").get_text(strip=True)
        team_name = home_name if "MatchTimeline-item--home" in li["class"] else guest_name
        team_id = home_id if team_name == home_name else guest_id
        pid = make_id(name)
        key = (pid, team_id)
        # Player might not be in squad list (rare) – add on the fly
        if key not in squad:
            squad[key] = (name, team_name)
            goal_counts[key] = 0
        goal_counts[key] += 1

    return {
        "game":

###############################################################################
# DB schema & helpers
###############################################################################
DDL = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS teams (
    team_id   TEXT PRIMARY KEY,
    team_name TEXT
);
CREATE TABLE IF NOT EXISTS players (
    player_id   TEXT PRIMARY KEY,
    player_name TEXT,
    team_id     TEXT,
    team_name   TEXT,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);
CREATE TABLE IF NOT EXISTS games (
    game_id        TEXT PRIMARY KEY,
    facr_game_id   TEXT,
    date           TEXT,
    round          TEXT,
    home_team_id      TEXT,
    guest_team_id     TEXT,
    venue          TEXT,
    spectators     INTEGER,
    halftime_score TEXT,
    final_score    TEXT,
    home_team_goals INTEGER,
    guest_team_goals INTEGER,
    FOREIGN KEY(home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY(guest_team_id) REFERENCES teams(team_id)

);
CREATE TABLE IF NOT EXISTS goals (
    goal_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id      TEXT,
    facr_game_id TEXT,
    player_id    TEXT,
    team_id      TEXT,
    goals_scored INTEGER,
    FOREIGN KEY (game_id)   REFERENCES games(game_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id)   REFERENCES teams(team_id)
);
"""

def ensure_schema(conn: sqlite3.Connection):
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()


###############################################################################
# Main
###############################################################################
def main():
    ap = argparse.ArgumentParser(description="Store FAČR matches, including non-scoring players.")
    ap.add_argument("--games-url", required=True, help="Text file with match URLs, one per line")
    args = ap.parse_args()

    with open(args.games_url, encoding="utf-8") as fp:
        urls = [u.strip() for u in fp if u.strip()]

    conn = sqlite3.connect("games_database.db")
    ensure_schema(conn)
    cur = conn.cursor()

    for url in urls:
        try:
            print(f"Fetching {url}")
            html = requests.get(url, timeout=20).text
            data = parse_match(html)

            # TEAMS
            for tid, tname in data["teams"].items():
                cur.execute("INSERT OR IGNORE INTO teams (team_id, team_name) VALUES (?,?)",
                            (tid, tname))

            # GAME
            g = data["game"]
            cur.execute("""
                INSERT OR REPLACE INTO games
                  (game_id, facr_game_id, date, round, home_team_id, guest_team_id,
                   venue, spectators, halftime_score, final_score, home_team_goals, guest_team_goals)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (g["game_id"], g["facr_game_id"], g["date"], g["round"],
                  g["home_team_id"], g["guest_team_id"], g["venue"], g["spectators"],
                  g["halftime_score"], g["final_score"], g["home_team_goals"],g["guest_team_goals"]))

            # PLAYERS
            for (pid, tid), (pname, tname) in data["players"].items():
                cur.execute("""
                    INSERT OR IGNORE INTO players
                      (player_id, player_name, team_id, team_name)
                    VALUES (?,?,?,?)
                """, (pid, pname, tid, tname))

            # GOALS
            cur.execute("DELETE FROM goals WHERE game_id = ?", (g["game_id"],))
            for (pid, tid), goals in data["goals"].items():
                cur.execute("""
                    INSERT INTO goals
                      (game_id, facr_game_id, player_id, team_id, goals_scored)
                    VALUES (?,?,?,?,?)
                """, (g["game_id"], g["facr_game_id"], pid, tid, goals))

            conn.commit()
            print(f"Stored {g['game_id']} (incl. {len(data['players'])} players)")

        except Exception as exc:
            conn.rollback()
            print(f"Skipped {url}: {exc}")

    conn.close()


if __name__ == "__main__":
    main()
