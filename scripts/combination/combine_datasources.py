from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, from_unixtime, unix_timestamp
import os

def combine_datasources():
    spark = SparkSession.builder.appName("NBA Data Combination").getOrCreate()

    layer = "formatted"
    group = "nba_api"

    players_path = f"/opt/spark/data_lake/{layer}/{group}/players/players.parquet"
    players_df = spark.read.parquet(players_path)

    games_base_path = f"/opt/spark/data_lake/{layer}/{group}/games/"
    dates = [d for d in os.listdir(games_base_path) if os.path.isdir(os.path.join(games_base_path, d))]
    dates.sort()

    for date in dates:
        print(f"\n🔵 Processing date: {date}")

        games_path = f"/opt/spark/data_lake/{layer}/{group}/games/{date}/games.parquet"
        boxscores_path = f"/opt/spark/data_lake/{layer}/{group}/boxscores/{date}/boxscores.parquet"

        if not os.path.exists(games_path) or not os.path.exists(boxscores_path):
            print(f"⚠️ Fichier manquant pour la date {date}: date suivante")
            continue

        games_df = spark.read.parquet(games_path)
        boxscores_df = spark.read.parquet(boxscores_path)

        games_df.select("game_id")

        combined_df = boxscores_df.alias("b").join(
            players_df.alias("p"), col("b.player_id") == col("p.player_id"), how="left"
        ).join(
            games_df.alias("g"),
            (col("b.game_id") == col("g.game_id")) & (col("b.team_id") == col("g.team_id")),
            how="left"
        )

        cleaned_df = combined_df.filter(combined_df.player_name.isNotNull())

        # Colonnes finales
        final_df = cleaned_df.select(
            col("b.game_id"),
            col("g.game_date"),
            col("g.matchup"),
            col("g.team_id"),
            col("g.team_abbreviation"),
            col("g.team_name"),
            col("g.team_pts"),
            col("g.win"),
            col("b.player_id"),
            col("p.player_name"),
            col("p.position"),
            col("b.pts"),
            col("b.ast"),
            col("b.reb"),
            col("b.stl"),
            col("b.blk"),
            col("b.min"),
            col("p.birthdate"),
            col("p.height"),
            col("p.weight"),
            col("p.season_exp"),
            col("p.country"),
        )

        # Sauvegarde
        output_path = f"/opt/spark/data_lake/usage/nba_data/season_24_25/{date}/"
        final_df.write.mode("overwrite").parquet(output_path)
        print(f"✅ Saved combined data for {date} at {output_path}")

    print("\n🎉 All dates processed and saved.")
    spark.stop()

if __name__ == "__main__":
    combine_datasources()
