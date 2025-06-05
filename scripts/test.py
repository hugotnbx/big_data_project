import pandas as pd

file_path = r"c:\Users\Hugo Troonbeeckx\OneDrive - EPHEC asbl\Documents\BAC3\Q2 (Erasmus ISEP)\Data Bases & Big Data\Projet\big_data_project\data_lake\formatted\nba_api\games\20241225\games.parquet"
df = pd.read_parquet(file_path, engine='pyarrow')
print(df)