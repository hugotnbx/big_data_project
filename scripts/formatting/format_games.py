import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, when, date_format

def format_games():
    spark = SparkSession.builder.appName("NBA Games Formatting").getOrCreate()

    input_base = "/opt/spark/data_lake/raw/nba_api/games/"
    output_base = "/opt/spark/data_lake/formatted/nba_api/games/"

    date_folders = [d for d in os.listdir(input_base) if os.path.isdir(os.path.join(input_base, d))]
    print(f"📅 Dossiers date trouvés: {date_folders}")

    for date_str in date_folders:
        input_path = os.path.join(input_base, date_str, "games.json")

        print(f"➡️ Traitement de la date: {date_str}")

        df = spark.read.json(input_path)

        selected_cols = [
            col("TEAM_ID").alias("team_id"),
            col("TEAM_NAME").alias("team_name"),
            col("TEAM_ABBREVIATION").alias("team_abbreviation"),
            col("GAME_ID").alias("game_id"),
            col("GAME_DATE").alias("game_date"),
            col("MATCHUP").alias("matchup"),
            col("WL").alias("win_raw"),
            col("PTS").alias("team_pts")
        ]

        df = df.select(*selected_cols)

        df = df.withColumn("team_id", col("team_id").cast("int")) \
               .withColumn("game_id", col("game_id").cast("int")) \
               .withColumn("team_pts", col("team_pts").cast("int")) \
               .withColumn("game_date_ts", from_unixtime(col("game_date") / 1000).cast("timestamp")) \
               .withColumn("game_date", date_format(col("game_date_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
               .drop("game_date_ts")

        df = df.withColumn("win", when(col("win_raw") == "W", True).otherwise(False)) \
               .drop("win_raw")

        output_dir = os.path.join(output_base, date_str)
        os.makedirs(output_dir, exist_ok=True)
        df.write.mode("overwrite").parquet(output_dir)

        print(f"✅ Fichiers écrits pour {date_str}")

    spark.stop()

if __name__ == "__main__":
    format_games()
