# --- FULL SETUP INSTRUCTIONS FOR WINDOWS USERS ---
# 1. Open PowerShell as Administrator and allow script execution:
#    Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
#    (Type 'Y' and press Enter if prompted)
#
# 2. Create a virtual environment (in your project folder):
#    python -m venv venv
#
# 3. Activate the virtual environment:
#    .\venv\Scripts\Activate
#
# 4. Upgrade pip and install required Python packages:
#    pip install --upgrade pip setuptools wheel
#    pip install pandas
#    pip install telethon openpyxl pyarrow fastparquet scikit-learn
#    pip install hdbscan
#    pip install bertopic
#
# 5. If you get an error about building hdbscan:
#    - Download and install Microsoft C++ Build Tools from:
#      https://visualstudio.microsoft.com/visual-cpp-build-tools/
#    - During installation, select "Desktop development with C++" workload (default options are fine)
#    - Restart your computer after installation
#    - Then try installing hdbscan and bertopic again
# -----------------------------------------------


import time
print("Starting script...")
import pandas as pd
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer

# Load your parquet file
print("Reading parquet file...")
start = time.time()
df = pd.read_parquet("korpus.parquet")
print(f"File loaded in {time.time() - start:.2f} seconds")


# --- REMOVE URLS FROM TEXTS ---
import re
def remove_urls(text):
    return re.sub(r'http[s]?://\S+|www\.\S+', '', text)

print("Columns:", df.columns)
documents = df['Content'].astype(str).apply(remove_urls).tolist()
# --- END REMOVE URLS ---

# For testing: only use a sample of the data (e.g. first 1000 messages)
SAMPLE_SIZE = 10000  # Change this number as needed
if len(documents) > SAMPLE_SIZE:
    print(f"Using only the first {SAMPLE_SIZE} documents for testing.")
    documents = documents[:SAMPLE_SIZE]
print(f"Loaded documents: {len(documents)}")



# Optionally, customize the vectorizer
print("Initializing vectorizer...")
vectorizer_model = CountVectorizer(stop_words="english")

# --- REDUCE NUMBER OF TOPICS: Use HDBSCAN with higher min_cluster_size ---
# By increasing min_cluster_size, you force BERTopic to create fewer, larger topics.
# Try 30, 50, or higher. The higher the value, the fewer topics (but each topic is broader).
from hdbscan import HDBSCAN
cluster_model = HDBSCAN(min_cluster_size=50)  # Change this value as needed
# --- END REDUCE NUMBER OF TOPICS ---

# Create and fit BERTopic model
print("Starting BERTopic modeling...")
start = time.time()
topic_model = BERTopic(vectorizer_model=vectorizer_model, hdbscan_model=cluster_model)
topics, probs = topic_model.fit_transform(documents)
print(f"BERTopic finished after {time.time() - start:.2f} seconds")



# View topics (prints a summary table)
print("Topic overview:")
print(topic_model.get_topic_info())

# Save topics to a CSV file for later analysis
# This file contains the topic number, frequency, and top words for each topic
topic_model.get_topic_info().to_csv("topics.csv", index=False)

# Save topic assignment for each document (so you know which message got which topic)
df_sample = df.iloc[:len(documents)].copy()
df_sample['topic'] = topics
df_sample.to_csv("documents_with_topics.csv", index=False)

# Show keywords for each topic (prints the top words for each topic)
print("\nTop keywords per topic:")
for topic_num in topic_model.get_topic_freq().Topic:
    print(f"Topic {topic_num}: {topic_model.get_topic(topic_num)}")

# Find the most common topic
from collections import Counter
most_common_topic, count = Counter(topics).most_common(1)[0]
print(f"\nMost common topic: {most_common_topic} with {count} documents")

# Show average topic probability (confidence)
import numpy as np
avg_prob = np.nanmean([p.max() if p is not None else np.nan for p in probs])
print(f"\nAverage topic assignment confidence: {avg_prob:.3f}")

# List all topics sorted by frequency
topic_freq = topic_model.get_topic_freq()
print("\nTopics sorted by frequency:")
print(topic_freq.sort_values('Count', ascending=False))

# Show the top 5 topics and their top 5 keywords
print("\nTop 5 topics and their top 5 keywords:")
for topic_num in topic_freq.sort_values('Count', ascending=False).head(5)['Topic']:
    words = [w for w, _ in topic_model.get_topic(topic_num)[:5]]
    print(f"Topic {topic_num}: {', '.join(words)}")

# Visualizations (open in browser or notebook)
# These will open interactive plots in your browser
try:
    print("\nOpening topic visualizations...")
    topic_model.visualize_topics().show()
    topic_model.visualize_barchart().show()
    topic_model.visualize_heatmap().show()
except Exception as e:
    print(f"Visualization error: {e}")

# More BERTopic visualizations (these require plotly)
try:
    print("\nOpening additional topic visualizations...")
    # Visualize topic similarity as a hierarchical dendrogram
    topic_model.visualize_hierarchy().show()
    # Visualize the distribution of topics over documents
    topic_model.visualize_distribution(probs[0]).show()  # For the first document
    # Visualize term score decline for a topic (e.g., topic 0)
    topic_model.visualize_term_rank(topic=0).show()
except Exception as e:
    print(f"Additional visualization error: {e}")