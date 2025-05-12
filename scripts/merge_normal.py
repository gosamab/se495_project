import os
import pandas as pd
from glob import glob

INPUT_DIR = "binetflow_files"
OUTPUT_FILE = "merged_binetflow.csv"

def read_binetflow_file(file_path):
    try:
        with open(file_path, 'r') as f:
            first_line = f.readline()
            delimiter = ',' if ',' in first_line else '\t'
        
        df = pd.read_csv(file_path, delimiter=delimiter, skiprows=0, engine='python')
        
        if df.shape[1] <= 1:
            df = pd.read_csv(file_path, delimiter=delimiter, skiprows=1, engine='python')
        
        df['source_file'] = os.path.basename(file_path)
        return df
    except Exception as e:
        print(f"[ERROR] Failed to read {file_path}: {e}")
        return pd.DataFrame()

def merge_all_files(input_dir):
    files = glob(os.path.join(input_dir, "*.binetflow")) + glob(os.path.join(input_dir, "*.binetflow.labeled"))
    all_dfs = []

    for file in files:
        print(f"[INFO] Reading: {file}")
        df = read_binetflow_file(file)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        print("[WARN] No valid files found.")
        return
    
    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_df.to_csv(OUTPUT_FILE, index=False)
    print(f"[DONE] Merged {len(all_dfs)} files into {OUTPUT_FILE}")

if __name__ == "__main__":
    merge_all_files(INPUT_DIR)
