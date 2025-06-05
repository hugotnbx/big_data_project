from pyspark.sql import SparkSession
import os

def index_data_to_elastic():
    spark = SparkSession.builder \
        .appName("Index NBA Usage Data to Elasticsearch") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .config("spark.es.nodes.wan.only", "true") \
        .getOrCreate()

    base_path = "/opt/spark/data_lake/usage/nba_data/season_24_25"
    dates = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]

    for date in dates:
        parquet_path = f"{base_path}/{date}/nba_data.parquet"
        print(f"➡️ Indexing data for date: {date}")

        # ➡️ Lecture directe sans transformation JSON
        df_to_index = spark.read.parquet(parquet_path)

        # ➡️ Écriture dans Elasticsearch (chaque colonne sera un champ séparé)
        df_to_index.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "nba_usage") \
            .mode("append") \
            .save()

        print(f"✅ Indexed data for {date}")

    spark.stop()

if __name__ == "__main__":
    index_data_to_elastic()
