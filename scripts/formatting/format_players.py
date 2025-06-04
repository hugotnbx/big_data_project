from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp, to_date

def format_players():
    spark = SparkSession.builder.appName("NBA Players Formatting").getOrCreate()

    input_path = "/opt/spark/data_lake/raw/nba_api/players/players.json"
    output_path = "/opt/spark/data_lake/formatted/nba_api/players/"

    df = spark.read.json(input_path)

    df_clean = df.withColumn("birthdate_clean", regexp_replace(col("birthdate"), "T", " "))
    df_clean = df_clean.withColumn("birthdate_date", to_date(to_timestamp(col("birthdate_clean"), "yyyy-MM-dd HH:mm:ss")))

    df_selected = df_clean.select(
        col("person_id").cast("int").alias("player_id"),
        col("first_name"),
        col("last_name"),
        col("birthdate_date").alias("birthdate"),
        col("school"),
        col("country"),
        col("height"),
        col("weight").cast("int"),
        col("season_exp").cast("int"),
        col("jersey").cast("int"),
        col("position"),
        col("team_id").cast("int"),
        col("team_name"),
        col("team_city"),
    )

    df_selected.write.mode("overwrite").parquet(output_path)

    print(f"✅ Fichier players.parquet écrit dans {output_path}")

    spark.stop()

if __name__ == "__main__":
    format_players()
