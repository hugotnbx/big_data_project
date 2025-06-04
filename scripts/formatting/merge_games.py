import os
import pyarrow.parquet as pq

def merge_and_clean_parquets(base_dir="/opt/spark/data_lake/formatted/nba_api/games/"):
    print(f"🔎 Dossier à traiter: {base_dir}")
    dates = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    
    for date in dates:
        folder = os.path.join(base_dir, date)
        parquet_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".parquet")]

        if not parquet_files:
            print(f"⚠️ Aucun fichier Parquet trouvé pour {date}")
            continue

        print(f"📦 Fusion des fichiers Parquet pour {date}")
        dataset = pq.ParquetDataset(parquet_files)
        table = dataset.read()
        output_file = os.path.join(folder, "games.parquet")
        pq.write_table(table, output_file)

        print(f"✅ Fichier unique sauvegardé: {output_file}")

        # Supprimer les anciens part-*.parquet
        for f in parquet_files:
            if os.path.exists(f):
                os.remove(f)
        print(f"🧹 Anciens part-* supprimés pour {date}")

    print("🎉 Worker 2 terminé !")
