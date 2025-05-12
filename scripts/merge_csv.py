import pandas as pd

binetflow_file = "data/merged_binetflow.csv"
normal_file = "data/merged_normal.csv"
output_file = "data/final_merged_dataset.csv"

df_binetflow = pd.read_csv(binetflow_file)
df_normal = pd.read_csv(normal_file)

df_binetflow["dataset"] = "merged_binetflow"
df_normal["dataset"] = "merged_normal"

target_columns = list(df_binetflow.columns)
df_normal_aligned = df_normal.copy()

df_normal_aligned = df_normal_aligned[[col for col in target_columns if col in df_normal_aligned.columns]]

for col in target_columns:
    if col not in df_normal_aligned.columns:
        df_normal_aligned[col] = pd.NA

df_normal_aligned = df_normal_aligned[target_columns]

final_df = pd.concat([df_binetflow, df_normal_aligned], ignore_index=True)

final_df.to_csv(output_file, index=False)
print(f"[DONE] Final merged dataset saved as {output_file}")
