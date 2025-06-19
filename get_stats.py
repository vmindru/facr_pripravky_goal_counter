#!/usr/bin/python
import sqlite3
import argparse
from pathlib import Path
from typing import List, Dict


def get_top_scorers(db_path: str | Path, league_prefix: str, team_id, limit) -> List[Dict]:
    """
    Returns top scorers for a specific team in a given league,
    with player names instead of player IDs.
    """
    sql = """
    SELECT
        p.player_name,
        t.team_name,
        g.team_id,
        SUM(g.goals_scored) AS total_goals,
        COUNT(DISTINCT g.game_id) AS total_games
    FROM goals g
    INNER JOIN players p ON g.player_id = p.player_id
    INNER JOIN teams t on g.team_id = t.team_id
    WHERE g.facr_game_id LIKE ?
    """
    like_pattern = f"{league_prefix}%"
    if team_id:
        sql += """AND g.team_id = ?"""
        params = [like_pattern, team_id]
    else:
        params = [like_pattern]
    sql += """
    GROUP BY g.player_id
    ORDER BY total_goals DESC
    """

    if limit:
        sql += """
        LIMIT ?;"""
        params.append(limit)

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def main():
    parser = argparse.ArgumentParser(description="Get top scorers of a team in a given league (with player names).")
    parser.add_argument("--db_file", default="games_database.db", help="Path to the SQLite database (default: games_database.db)")
    parser.add_argument("--league_id", required=True, help="League ID prefix for facr_game_id (e.g., 2024622G1B)")
    parser.add_argument("--team_id", required=False, help="Team ID (e.g., bohunice_a)", default=False)
    parser.add_argument("--limit_result", required=False, help="Limit number of printed results", default=False)

    args = parser.parse_args()
    scorers = get_top_scorers(args.db_file, args.league_id, args.team_id, args.limit_result)

    print(f"\nTop scorers for team '{args.team_id}' in league '{args.league_id}%':\n")
    print(f"{'Player Name':<30} {'Team Name':<40} {'Goals':>5} {'Games':>6}")
    print("-" * 85)
    for row in scorers:
        print(f"{row['player_name']:<30} {row['team_name']:<36}  {row['total_goals']:>5} {row['total_games']:>6}")


if __name__ == "__main__":
    main()
