import os
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from datetime import datetime

def fetch_games():
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2024-25', league_id_nullable='00')
    games_df = gamefinder.get_data_frames()[0]

    # Regroupe par date
    games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'])

    # Parcourt les dates uniques et sauvegarde par date
    for game_date in games_df['GAME_DATE'].dt.date.unique():
        date_folder = game_date.strftime('%Y%m%d')  # Format YYYYMMDD
        day_games_df = games_df[games_df['GAME_DATE'].dt.date == game_date]

        # Structure du datalake
        layer = "raw"
        group = "nba_api"
        table_name = "games"
        raw_path = f"./data_lake/{layer}/{group}/{table_name}/{date_folder}/"
        os.makedirs(raw_path, exist_ok=True)

        # Sauvegarde le fichier
        output_file = os.path.join(raw_path, "games.json")
        day_games_df.to_json(output_file, orient='records', lines=True)

        print(f"✅ Saved games for {game_date} at {output_file}")