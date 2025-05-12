import os
import pandas as pd

DATA_DIR = "binetflow_files"
TARGET_EXTS = [".binetflow", ".labeled"]

def inspect_file(filepath):
    print(f"\n--- Inspecting: {os.path.basename(filepath)} ---")
    try:
        df = pd.read_csv(
            filepath,
            comment='#',
            sep=filepath.endswith(".labeled") and "\t" or ",",
            skipinitialspace=True,
            nrows=5
        )
        print("Columns:", list(df.columns))
        print(df.head())
    except Exception as e:
        print(f"[ERROR] Could not read {filepath}: {e}")

def main():
    for fname in os.listdir(DATA_DIR):
        if any(fname.endswith(ext) for ext in TARGET_EXTS):
            inspect_file(os.path.join(DATA_DIR, fname))

if __name__ == "__main__":
    main()
