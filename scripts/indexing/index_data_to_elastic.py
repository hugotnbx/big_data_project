from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import concat_ws

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

        df_to_index = spark.read.parquet(parquet_path)

        df_to_index = df_to_index.withColumn("doc_id", concat_ws("_", "game_id", "player_id"))

        df_to_index.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "nba_usage") \
            .option("es.mapping.id", "doc_id") \
            .mode("append") \
            .save()

        print(f"✅ Indexed data for {date}")

    spark.stop()

if __name__ == "__main__":
    index_data_to_elastic()
