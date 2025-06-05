from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def format_boxscores():
    spark = SparkSession.builder.appName("NBA Boxscores Formatting").getOrCreate()

    input_path = "/opt/spark/data_lake/raw/nba_api/boxscores/"
    output_path = "/opt/spark/data_lake/formatted/nba_api/boxscores/"

    date_folders = [d for d in os.listdir(input_path) if os.path.isdir(os.path.join(input_path, d))]
    print(f"📅 Dossiers date trouvés: {date_folders}")

    for date_str in date_folders:
        input_dir = os.path.join(input_path, date_str, "boxscores.json")

        with open(input_dir, 'r') as f:
            content = f.read().strip()
            if not content:
                print(f"⚠️ Fichier vide ou sans contenu utile pour la date {date_str}, passage à la suivante.")
                continue

        print(f"➡️ Traitement de la date: {date_str}")

        df = spark.read.json(input_dir)

        selected_cols = [
            col("GAME_ID").alias("game_id"),
            col("TEAM_ID").alias("team_id"),
            col("PLAYER_ID").alias("player_id"),
            col("MIN").alias("min"),
            col("REB").alias("reb"),
            col("AST").alias("ast"),
            col("STL").alias("stl"),
            col("BLK").alias("blk"),
            col("PTS").alias("pts")
        ]
        df = df.select(*selected_cols)

        df = df.withColumn("team_id", col("team_id").cast("int")) \
               .withColumn("player_id", col("player_id").cast("int")) \
               .withColumn("game_id", col("game_id").cast("int")) \
               .withColumn("reb", col("reb").cast("int")) \
               .withColumn("ast", col("ast").cast("int")) \
               .withColumn("stl", col("stl").cast("int")) \
               .withColumn("blk", col("blk").cast("int")) \
               .withColumn("pts", col("pts").cast("int"))

        output_dir = os.path.join(output_path, date_str)
        os.makedirs(output_dir, exist_ok=True)
        df.write.mode("overwrite").parquet(output_dir)
        print(f"✅ Fichiers écrits pour {date_str}")

    spark.stop()

if __name__ == "__main__":
    format_boxscores()
