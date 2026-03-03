import pandas as pd
from pathlib import Path

# folder containing your CSV files
folder_path = Path("C:/Users/rdb104/Documents/repos/legi_scrape-master/ohio/ohiocsvs")

# get all csv files
csv_files = list(folder_path.glob("*.csv"))

# read and combine
df_list = [pd.read_csv(file) for file in csv_files]
combined_df = pd.concat(df_list, ignore_index=True)

# optional: remove exact duplicate rows
combined_df = combined_df.drop_duplicates()

# write to new file
combined_df.to_csv("combined_ohio.csv", index=False)

