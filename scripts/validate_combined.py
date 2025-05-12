import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

FILE = "data/final_merged_dataset.csv"
df = pd.read_csv(FILE)

print("=== Basic Info ===")
print(df.shape)
print(df.columns.tolist())
print()

print("=== Null Value Count ===")
print(df.isnull().sum())
print()
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
print("=== Unique Values in 'Label' ===")
print(df["Label"].dropna().unique())
for label in df["Label"].dropna().unique():
    print(f"Label: {label}")
print()

print("\n=== Port Columns: Types ===")
print(df[["Sport", "Dport"]].dtypes)

print("\n=== Protocols ===")
print(df["Proto"].value_counts())

print("\n=== Correlation Matrix ===")
corr = df.select_dtypes(include='number').corr()
sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Correlation Heatmap")
plt.savefig("correlation_heatmap.png")
plt.show()
