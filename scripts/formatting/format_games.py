from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, when
import os

def format_games():
    spark = SparkSession.builder.appName("NBA Games Formatting").getOrCreate()

    input_path = "/opt/spark/data_lake/raw/nba_api/games/*/*.json"
    print(f"🔎 Lecture des fichiers JSON depuis: {input_path}")
    df = spark.read.json(input_path)

    selected_cols = [
        col("SEASON_ID").alias("season_id"),
        col("TEAM_ID").alias("team_id"),
        col("TEAM_NAME").alias("team_name"),
        col("TEAM_ABBREVIATION").alias("team_abbreviation"),
        col("GAME_ID").alias("game_id"),
        col("GAME_DATE").alias("game_date"),
        col("MATCHUP").alias("matchup"),
        col("WL").alias("win_raw"),
        col("PTS").alias("pts")
    ]
    df = df.select(*selected_cols)

    df = df.withColumn("season_id", col("season_id").cast("int")) \
           .withColumn("team_id", col("team_id").cast("int")) \
           .withColumn("game_id", col("game_id").cast("string")) \
           .withColumn("pts", col("pts").cast("int")) \
           .withColumn("game_date", from_unixtime(col("game_date") / 1000).cast("timestamp"))

    df = df.withColumn("win", when(col("win_raw") == "W", True).otherwise(False)) \
           .drop("win_raw")

    dates = df.select("game_date").distinct().collect()
    print(f"📅 Dates trouvées: {[row['game_date'] for row in dates]}")

    for row in dates:
        date_str = row["game_date"].strftime("%Y%m%d")
        print(f"➡️ Traitement de la date: {date_str}")

        df_date = df.filter(col("game_date").cast("date") == row["game_date"].date())
        print(f"🔢 Nombre de lignes pour {date_str}: {df_date.count()}")

        tmp_output_dir = f"/opt/spark/data_lake/formatted/nba_api/games/{date_str}_tmp"
        final_output_dir = f"/opt/spark/data_lake/formatted/nba_api/games/{date_str}/"
        os.makedirs(final_output_dir, exist_ok=True)

        print(f"💾 Écriture dans un seul fichier parquet temporaire…")
        df_date.coalesce(1).write.mode("overwrite").parquet(tmp_output_dir)

        # Trouver le part-*.parquet et le renommer en games.parquet
        parquet_file = None
        for file in os.listdir(tmp_output_dir):
            print(f"🔍 Fichier trouvé: {file}")
            if file.endswith(".parquet"):
                parquet_file = file
                break

        if parquet_file:
            os.rename(
                os.path.join(tmp_output_dir, parquet_file),
                os.path.join(final_output_dir, "games.parquet")
            )
            print(f"✅ Fichier unique games.parquet sauvegardé pour {date_str}")
        else:
            print("⚠️ Aucun fichier Parquet trouvé après écriture !")

        # Nettoyage des fichiers temporaires
        for file in os.listdir(tmp_output_dir):
            os.remove(os.path.join(tmp_output_dir, file))
        os.rmdir(tmp_output_dir)
        print(f"🧹 Dossier temporaire {tmp_output_dir} supprimé.")

    print("🎉 Formatage terminé pour toutes les dates !")
    spark.stop()

if __name__ == "__main__":
    format_games()
