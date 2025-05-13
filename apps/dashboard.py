import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

st.set_page_config(layout="wide", page_title="Malware Detection Dashboard", page_icon="🔐")

# Load data
@st.cache_data
def load_dataset():
    df = pd.read_csv("data/test.csv")
    df["is_malicious"] = df["label_binary"].apply(lambda x: "Malicious" if x == 1 else "Legitimate")
    df["malware_type"] = df["label_multi"].fillna("Unknown")
    return df

@st.cache_data
def load_results():
    binary = pd.DataFrame({
        "Model": ["Logistic Regression", "XGBoost", "LSTM", "Linear SVM"],
        "Accuracy": [0.9780, 0.9806, 0.9907, 0.9780],
        "Weighted F1": [0.9672, 0.9807, 0.9903, 0.9672],
        "Macro F1": [0.4945, 0.7766, 0.8824, 0.4945]
    })
    multi = pd.DataFrame({
        "Model": ["Logistic Regression", "XGBoost", "LSTM", "Linear SVM"],
        "Accuracy": [0.9780, 0.9970, 0.9901, 0.9780],
        "Weighted F1": [0.9672, 0.9970, 0.9904, 0.9672],
        "Macro F1": [0.4945, 0.9648, 0.8923, 0.4945]
    })
    return binary, multi

binary_results, multiclass_results = load_results()
df = load_dataset()

# Sidebar
st.sidebar.title("📍 Navigation")
page = st.sidebar.radio("Go to", ["Overview", "Model Performance", "Data Explorer", "Flow Samples", "Clustering & Stats"])

# Pages
if page == "Overview":
    st.title("📊 Dataset Overview")

    col1, col2 = st.columns(2)
    with col1:
        st.metric("📦 Total Flows", len(df))
        st.metric("🦠 Malicious", df['label_binary'].sum())
    with col2:
        st.metric("📁 Unique Malware Types", df['malware_type'].nunique())

    st.subheader("🧷 First Look at Data")
    st.dataframe(df.head(10), use_container_width=True)

    st.subheader("📌 Multi-Class Label Distribution")
    label_counts = df['malware_type'].value_counts()
    fig, ax = plt.subplots(figsize=(12, 4))
    sns.barplot(x=label_counts.index, y=label_counts.values, ax=ax)
    ax.set_ylabel("Count")
    ax.set_xlabel("Malware Type")
    plt.xticks(rotation=45)
    st.pyplot(fig)

    st.subheader("📈 Feature Correlation Heatmap")
    numeric = df.select_dtypes(include=['int64', 'float64'])
    corr = numeric.corr()
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(corr, cmap='coolwarm', annot=False, ax=ax)
    st.pyplot(fig)

elif page == "Model Performance":
    st.title("🧠 Model Performance")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Binary Classification")
        st.dataframe(binary_results.style.format(precision=4).highlight_max(color='lightgreen', axis=0), use_container_width=True)
    with col2:
        st.subheader("Multi-Class Classification")
        st.dataframe(multiclass_results.style.format(precision=4).highlight_max(color='lightgreen', axis=0), use_container_width=True)

elif page == "Data Explorer":
    st.title("🔍 Interactive Data Explorer")

    malware_options = df['malware_type'].unique()
    selected_class = st.selectbox("Select malware type", malware_options)

    filtered = df[df['malware_type'] == selected_class]
    st.write(f"✅ Found {len(filtered)} records for type: **{selected_class}**")

    st.dataframe(filtered.head(100), use_container_width=True)

    st.subheader("🎚️ Filter by Source Port")
    port_range = st.slider("Source Port Range", min_value=0, max_value=65535, value=(0, 65535))
    port_filtered = filtered[(filtered['SrcPort'] >= port_range[0]) & (filtered['SrcPort'] <= port_range[1])]

    st.write(f"🎯 After port filter: {len(port_filtered)} records")
    st.dataframe(port_filtered.head(100), use_container_width=True)

elif page == "Flow Samples":
    st.title("🧾 Random Flow Samples")

    sample_size = st.slider("Select sample size", 10, 200, 25)
    sample_df = df.sample(sample_size)

    st.dataframe(sample_df[["SrcPort", "DstPort", "Duration", "is_malicious", "malware_type"]], use_container_width=True)

elif page == "Clustering & Stats":
    st.title("🧠 Flow Clustering & PCA Projection")

    st.write("This section applies unsupervised learning (K-Means) and dimensionality reduction (PCA) for exploratory visualization.")

    features = df.select_dtypes(include=['float64', 'int64']).drop(columns=['label_binary'], errors='ignore')
    features = features.dropna()
    features_scaled = StandardScaler().fit_transform(features)

    k = st.slider("🔢 Choose number of clusters (K)", 2, 10, 3)
    kmeans = KMeans(n_clusters=k, random_state=42)
    cluster_labels = kmeans.fit_predict(features_scaled)

    pca = PCA(n_components=2)
    pca_result = pca.fit_transform(features_scaled)
    pca_df = pd.DataFrame(pca_result, columns=['PC1', 'PC2'])
    pca_df['Cluster'] = cluster_labels

    st.subheader("📉 PCA Clustering Visualization")
    fig, ax = plt.subplots()
    sns.scatterplot(data=pca_df, x='PC1', y='PC2', hue='Cluster', palette='tab10', ax=ax, alpha=0.7)
    plt.title("K-Means Clustering (PCA Projection)")
    st.pyplot(fig)

    st.subheader("📏 Cluster Sizes")
    cluster_counts = pd.Series(cluster_labels).value_counts().sort_index()
    st.bar_chart(cluster_counts)
