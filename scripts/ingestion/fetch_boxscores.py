import os
import time
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from datetime import datetime

def fetch_boxscores(season="2024-25"):
    print(f"Fetching boxscores for season {season}...")

    # Récupère tous les matchs de la saison
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season, league_id_nullable='00')
    games_df = gamefinder.get_data_frames()[0]
    games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'])

    # Paramètres pour le retry
    max_retries = 5
    retry_delay = 3  # en secondes

    # Pour chaque date de match, regroupe les boxscores
    for game_date in games_df['GAME_DATE'].dt.date.unique():
        date_folder = game_date.strftime('%Y%m%d')
        games_on_date = games_df[games_df['GAME_DATE'].dt.date == game_date]

        all_boxscores = []

        for _, game in games_on_date.iterrows():
            game_id = game["GAME_ID"]

            attempt = 0
            success = False
            while attempt < max_retries and not success:
                try:
                    boxscore_df = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id).player_stats.get_data_frame()
                    all_boxscores.append(boxscore_df)
                    print(f"✅ Fetched boxscore for {game_id}")
                    success = True  # Sortie de la boucle de retry
                except Exception as e:
                    attempt += 1
                    print(f"❌ Error fetching boxscore for {game_id} [Attempt {attempt}/{max_retries}]: {e}")
                    if attempt < max_retries:
                        print(f"🔄 Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        print(f"🚫 Failed to fetch boxscore for {game_id} after {max_retries} attempts.")

            time.sleep(0.35)  # Pause légère après chaque requête

        if all_boxscores:
            # Concatène et supprime les doublons
            final_df = pd.concat(all_boxscores, ignore_index=True).drop_duplicates()

            # Structure datalake
            layer = "raw"
            group = "nba_api"
            table_name = "boxscores"
            raw_path = f"./data_lake/{layer}/{group}/{table_name}/{date_folder}/"
            os.makedirs(raw_path, exist_ok=True)

            output_file = os.path.join(raw_path, "boxscores.json")
            final_df.to_json(output_file, orient='records', lines=True)

            print(f"✅ Saved boxscores for {game_date} at {output_file}")

    print("✅ All boxscores fetched and saved!")
