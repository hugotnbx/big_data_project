import os
import pyarrow.parquet as pq

def clean_combine_files():
    base_dir="/opt/spark/data_lake/usage/nba_data/season_24_25/"
    dates = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]

    for date in dates:
        folder = os.path.join(base_dir, date)
        parquet_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".parquet")]

        if not parquet_files:
            print(f"⚠️ Aucun fichier Parquet trouvé pour {date}")
            continue

        print(f"📦 Fusion des fichiers part-* pour {date}")
        dataset = pq.ParquetDataset(parquet_files)
        table = dataset.read()
        output_file = os.path.join(folder, "nba_data.parquet")
        pq.write_table(table, output_file)
        print(f"✅ Fichier unique sauvegardé: {output_file}")

        for f in os.listdir(folder):
            if f != "nba_data.parquet":
                file_path = os.path.join(folder, f)
                try:
                    os.remove(file_path)
                    print(f"🗑️ Supprimé: {file_path}")
                except Exception as e:
                    print(f"⚠️ Erreur en supprimant {file_path}: {e}")


if __name__ == "__main__":
    clean_combine_files()