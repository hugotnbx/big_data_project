import os
import pyarrow.parquet as pq

def merge_players_files():
    base_dir = "/opt/spark/data_lake/formatted/nba_api/players/"
    
    parquet_files = [os.path.join(base_dir, f) for f in os.listdir(base_dir) if f.endswith(".parquet")]

    if not parquet_files:
        print(f"⚠️ Aucun fichier Parquet trouvé dans {base_dir}")
        return

    print(f"📦 Fusion des fichiers Parquet dans {base_dir}")
    dataset = pq.ParquetDataset(parquet_files)
    table = dataset.read()
    output_file = os.path.join(base_dir, "players.parquet")
    pq.write_table(table, output_file)
    print(f"✅ Fichier unique sauvegardé: {output_file}")

    for f in os.listdir(base_dir):
        if f != "players.parquet":
            file_path = os.path.join(base_dir, f)
            try:
                os.remove(file_path)
                print(f"🗑️ Supprimé: {file_path}")
            except Exception as e:
                print(f"⚠️ Erreur en supprimant {file_path}: {e}")

if __name__ == "__main__":
    merge_players_files()
