import pandas as pd
import os
from collections import Counter

df = pd.read_csv("datasets/all_files.csv")

df["Extension"] = df["FileName"].apply(lambda x: os.path.splitext(x)[1].lower())

ext_counts = df["Extension"].value_counts()

print("\nFile Extension Counts:")
print(ext_counts.to_string())
