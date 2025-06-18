#!/usr/bin/python
import sqlite3
from pathlib import Path
from typing import List, Dict
import argparse


def calculate_points(db_path: str | Path, league_prefix: str) -> List[Dict]:
    """
    Calculate points per team for a given league_id prefix.
    """
    sql = """
    SELECT
        t.team_id,
        t.team_name,
        SUM(r.points) AS points
    FROM (
            -- home side
            SELECT
                home_team_id AS team_id,
                CASE
                    WHEN home_team_goals > guest_team_goals THEN 3
                    WHEN home_team_goals = guest_team_goals THEN 1
                    ELSE 0
                END AS points
            FROM games
            WHERE home_team_goals IS NOT NULL
              AND facr_game_id LIKE ?

          UNION ALL

            -- away side
            SELECT
                guest_team_id AS team_id,
                CASE
                    WHEN guest_team_goals > home_team_goals THEN 3
                    WHEN guest_team_goals = home_team_goals THEN 1
                    ELSE 0
                END AS points
            FROM games
            WHERE guest_team_goals IS NOT NULL
              AND facr_game_id LIKE ?
    ) AS r
    JOIN teams t ON t.team_id = r.team_id
    GROUP BY t.team_id, t.team_name
    ORDER BY points DESC, t.team_name;
    """

    like_pattern = f"{league_prefix}%"

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, (like_pattern, like_pattern)).fetchall()
        return [dict(r) for r in rows]


def main():
    parser = argparse.ArgumentParser(description="Calculate football points per team for a given league ID.")
    parser.add_argument("--db_file", default="games_database.db", help="Path to the SQLite database (default: games_database.db)")
    parser.add_argument("--league_id", required=True, help="League ID prefix to filter facr_game_id (e.g., 2024623H1)")

    args = parser.parse_args()

    results = calculate_points(args.db_file, args.league_id)

    print(f"Standings for league_id LIKE '{args.league_id}%'")
    for row in results:
        print(f"{row['team_name']:<25} {row['points']:>3} pts")


if __name__ == "__main__":
    main()
