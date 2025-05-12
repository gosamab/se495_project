import pandas as pd
import numpy as np
import os
import re
from sklearn.feature_selection import VarianceThreshold
from sklearn.model_selection import train_test_split

INPUT_FILE = "data/final_merged_dataset.csv"
OUTPUT_FILE = "data/final_cleaned_dataset.csv"

def map_label(label):
    label = label.lower()

    if "from-botnet" in label:
        return "Botnet"
    elif "from-normal" in label or label.startswith("normal"):
        return "Normal"
    elif label.startswith("background") or "to-background" in label or "from-background" in label:
        return "Background"
    elif "dns" in label or "cvut-dns-server" in label:
        return "DNS"
    elif "ntp" in label:
        return "NTP"
    elif "spam" in label:
        return "Spam"
    elif any(keyword in label for keyword in ["http", "web", "windowsupdate", "microsoft", "analytics"]):
        return "Web"
    elif "irc" in label:
        return "IRC"
    elif "binary-download" in label or "encrypted-data" in label or "download" in label:
        return "Binary"
    else:
        return "Other"

def clean_combined_dataset():
    print(f"[INFO] Loading {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"[INFO] Initial shape: {df.shape}")

    # === Basic Cleanup ===
    df['StartTime'] = pd.to_datetime(df['StartTime'], errors='coerce')
    df = df.dropna(subset=['StartTime'])

    df.rename(columns={
        "Dur": "Duration",
        "Proto": "Protocol",
        "SrcAddr": "SrcIP",
        "DstAddr": "DstIP",
        "Sport": "SrcPort",
        "Dport": "DstPort"
    }, inplace=True)

    df["SrcPort"] = pd.to_numeric(df["SrcPort"], errors="coerce").fillna(-1).astype(int)
    df["DstPort"] = pd.to_numeric(df["DstPort"], errors="coerce").fillna(-1).astype(int)

    df["State"] = df.get("State", pd.Series(["UNKNOWN"] * len(df))).fillna("UNKNOWN")

    df["sTos"] = pd.to_numeric(df.get("sTos", 0), errors="coerce").fillna(0).astype(int)
    df["dTos"] = pd.to_numeric(df.get("dTos", 0), errors="coerce").fillna(0).astype(int)

    # Fill missing labels with "Normal"
    df["Label"] = df["Label"].fillna("Normal")

    # === Label Engineering ===
    df["label_binary"] = df["Label"].str.contains("From-Botnet", case=False, na=False).astype(int)
    df["label_multi"] = df["Label"].str.replace("flow=", "", regex=False).str.lower()

    # === Feature Engineering ===
    df['PktByteRatio'] = df['TotPkts'] / df['TotBytes'].replace(0, np.nan)
    df['BytePerPkt'] = df['TotBytes'] / df['TotPkts'].replace(0, np.nan)
    df['SrcByteRatio'] = df['SrcBytes'] / df['TotBytes'].replace(0, np.nan)

    # === Feature Selection ===
    numeric_df = df.select_dtypes(include=[np.number])
    selector = VarianceThreshold(threshold=0.0)
    selector.fit(numeric_df)
    retained_columns = numeric_df.columns[selector.get_support()]
    df_low_variance = numeric_df[retained_columns]
    print("Retained after low variance filter:", list(retained_columns))

    def drop_highly_correlated_features(df, threshold=0.95):
        corr_matrix = df.corr().abs()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        to_drop = [column for column in upper.columns if any(upper[column] > threshold)]
        return df.drop(columns=to_drop), to_drop

    df_final, dropped_corr_features = drop_highly_correlated_features(df_low_variance)
    print("Dropped due to high correlation:", dropped_corr_features)
    print("Final selected features:", df_final.columns.tolist())

    # === Select Final Columns ===
    base_cols = [
        'Duration', 'SrcPort', 'DstPort', 'sTos', 'dTos', 'TotPkts', 'TotBytes',
        'SrcBytes', "Label", "label_binary", "label_multi", "source_file", "dataset"
    ]
    engineered_cols = ["PktByteRatio", "BytePerPkt", "SrcByteRatio"]
    selected_columns = base_cols + engineered_cols

    df_selected = df[selected_columns].copy()
    
    df_selected["label_multi"] = df_selected["label_multi"].apply(map_label)
    
    label_counts = df_selected.groupby(["label_multi"]).size().reset_index(name='count')
    print(label_counts)
    
    label_counts = df_selected.groupby(["label_binary"]).size().reset_index(name='count')
    print(label_counts)

    # print(f"[INFO] Saving cleaned data to {OUTPUT_FILE}")
    # df_selected.to_csv(OUTPUT_FILE, index=False)

    # # === Split Dataset ===
    # print("[INFO] Splitting into train/val/test sets (80/10/10)")
    # train_df, temp_df = train_test_split(df_selected, test_size=0.2, random_state=42)
    # val_df, test_df = train_test_split(temp_df, test_size=0.5, random_state=42)

    # print(f"Train: {train_df.shape}, Val: {val_df.shape}, Test: {test_df.shape}")

    # train_df.to_csv("data/train.csv", index=False)
    # val_df.to_csv("data/val.csv", index=False)
    # test_df.to_csv("data/test.csv", index=False)

    # print("[DONE] Preprocessing complete.")
    
    # from datasets import Dataset, DatasetDict

    # train_ds = Dataset.from_pandas(train_df)
    # val_ds = Dataset.from_pandas(val_df)
    # test_ds = Dataset.from_pandas(test_df)

    # dataset_dict = DatasetDict({
    #     "train": train_ds,
    #     "validation": val_ds,
    #     "test": test_ds
    # })

    # repo_name = "gosamab/binetflow-dataset"
    # dataset_dict.push_to_hub(repo_name)

if __name__ == "__main__":
    clean_combined_dataset()
