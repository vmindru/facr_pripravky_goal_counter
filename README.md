# Go find the competition from of your club from fotbal.cz for category U11,U9 

https://www.fotbal.cz/souteze/turnaje/hlavni/89d2518d-10c2-42b9-88a7-38ca90b2024c 

grab the page source, grep for all `zapasy/zapas` and save to file

# run the crawler

```
./parse_games.py --games-url /tmp/BOHUN_ZAP 
```

## Select goals  replace '%G1B%' by competition ID from fotbal.cz

```
sqlite3  games_database.db
select player_id,team_id,sum(goals_scored) as total_goals, count(game_id) as total_games from goals  WHERE facr_game_id like '%G1B%' GROUP by player_id  ORDER  by total_goals DESC;
```

