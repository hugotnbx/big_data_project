import os
import json
import time
from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo

def fetch_players():
    layer = "raw"
    group = "nba_api"
    table_name = "players"
    raw_path = f"./data_lake/{layer}/{group}/{table_name}/"
    os.makedirs(raw_path, exist_ok=True)

    active_players = players.get_active_players()
    print(f"🔎 Found {len(active_players)} active players.")

    all_players_info = []

    max_retries = 5
    retry_delay = 3 

    for idx, player in enumerate(active_players):
        player_id = player['id']
        player_name = player['full_name']

        attempt = 0
        success = False
        while attempt < max_retries and not success:
            try:
                info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
                player_info_df = info.common_player_info.get_data_frame()

                player_data = player_info_df.iloc[0].to_dict()
                all_players_info.append(player_data)
                print(f"✅ Retrieved info for {player_name} ({idx+1}/{len(active_players)})")
                success = True  
            except Exception as e:
                attempt += 1
                print(f"❌ Error for {player_name} ({player_id}) [Attempt {attempt}/{max_retries}]: {e}")
                if attempt < max_retries:
                    print(f"🔄 Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print(f"🚫 Failed to retrieve info for {player_name} after {max_retries} attempts.")

        time.sleep(0.5)

    output_file = os.path.join(raw_path, "players.json")
    with open(output_file, "w") as f:
        for player_data in all_players_info:
            f.write(json.dumps(player_data) + "\n")

    print(f"✅ All players' info saved in {output_file}")

if __name__ == "__main__":
    fetch_players()