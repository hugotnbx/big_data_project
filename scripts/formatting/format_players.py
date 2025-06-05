from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp, to_date, concat_ws

def format_players():
    spark = SparkSession.builder.appName("NBA Players Formatting").getOrCreate()

    input_path = "/opt/spark/data_lake/raw/nba_api/players/players.json"
    output_path = "/opt/spark/data_lake/formatted/nba_api/players/"

    df = spark.read.json(input_path)

    df_clean = df.withColumn("birthdate_clean", regexp_replace(col("birthdate"), "T", " "))
    df_clean = df_clean.withColumn("birthdate_date", to_date(to_timestamp(col("birthdate_clean"), "yyyy-MM-dd HH:mm:ss")))

    df_with_player_name = df_clean.withColumn("player_name", concat_ws(" ", col("first_name"), col("last_name")))
    df_with_team_name = df_with_player_name.withColumn("team_name", concat_ws(" ", col("team_city"), col("team_name")))

    df_selected = df_with_team_name.select(
        col("person_id").cast("int").alias("player_id"),
        col("player_name"),
        col("birthdate_date").alias("birthdate"),
        col("country"),
        col("height"),
        col("weight").cast("int"),
        col("season_exp").cast("int"),
        col("position"),
        col("team_id").cast("int"),
        col("team_name"),
    )

    df_selected.write.mode("overwrite").parquet(output_path)

    print(f"✅ Fichier players.parquet écrit dans {output_path}")

    spark.stop()

if __name__ == "__main__":
    format_players()
