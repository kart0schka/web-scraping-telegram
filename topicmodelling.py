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


"""
IMPROVED PREPROCESSING & LEMMATIZATION
--------------------------------------
Dieser Abschnitt bereinigt die Texte, entfernt URLs, Emojis, Satzzeichen und Stopwords (inklusive einer eigenen Liste typischer Füllwörter).
Zusätzlich werden alle Wörter mit dem WordNetLemmatizer auf ihre Grundform gebracht (Lemmatization),
damit z.B. "thing" und "things" als ein Wort behandelt werden. Das sorgt für konsistentere und sinnvollere Topics.

Du kannst die custom_stopwords-Liste beliebig erweitern, um weitere unerwünschte Wörter zu entfernen.
"""

# --- REMOVE URLS FROM TEXTS ---

# --- IMPROVED PREPROCESSING ---
import re
import string
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import nltk


# --- CUSTOM STOPWORDS & LEMMATIZER ---
try:
    stop_words = set(stopwords.words('english'))
except LookupError:
    nltk.download('stopwords')
    stop_words = set(stopwords.words('english'))
try:
    lemmatizer = WordNetLemmatizer()
    _ = lemmatizer.lemmatize('things')
except LookupError:
    nltk.download('wordnet')
    lemmatizer = WordNetLemmatizer()

# Add your own stopwords here (extend as needed)
custom_stopwords = {
    'make', 'makes', 'get', 'got', 'thing', 'stuff', 'use', 'used', 'using', 'give', 'given', 'take', 'taken', 'video', 'link', 'day',
    'put', 'see', 'seen', 'go', 'goes', 'went', 'say', 'says', 'said', 'know', 'known', 'want', 'wanted', 'need', 'share', 'join', 
    'really', 'just', 'like', 'one', 'two', 'three', 'also', 'still', 'even', 'much', 'many', 'lot', 'lots', 'every','bqqqqqqqm', 'news',
    'something', 'anything', 'everything', 'nothing','makes', 'everyone', 'anyone', 'someone', 'thing', 'things', 'way', 'ways', 'air',
    'today', 'tomorrow', 'yesterday', 'now', 'then', 'here', 'there', 'read','gets','where', 'when', 'how', 'why', 'can', 'could', 'would', 'should',
    'will', 'may', 'might', 'must', 'shall', 'let', 'etc', 'etc.', 'amp', 'im', 'dont', 'didnt', 'doesnt', 'cant', 'wont', 'isnt', 'arent', 'wasnt', 'werent', 'havent', 'hasnt', 'hadnt', 'youre', 'theyre', 'weve', 'ive', 'youve', 'theyll', 'ill', 'hes', 'shes', 'its', 'whats', 'thats', 'theres', 'heres', 'whos', 'whom', 'whose', 'about', 'above', 'below', 'between', 'among', 'upon', 'without', 'within', 'across', 'toward', 'towards', 'against', 'around', 'through', 'during', 'before', 'after', 'again', 'further', 'once', 'always', 'never', 'sometimes', 'often', 'usually', 'rarely', 'maybe', 'perhaps', 'almost', 'already', 'yet', 'soon', 'early', 'late', 'new', 'old', 'good', 'bad', 'better', 'best', 'worst', 'big', 'small', 'great', 'little', 'long', 'short', 'high', 'low', 'right', 'left', 'far', 'near', 'close', 'open', 'closed', 'full', 'empty', 'same', 'different', 'other', 'another', 'next', 'last', 'first', 'second', 'third', 'more', 'less', 'most', 'least', 'such', 'quite', 'rather', 'very', 'too', 'enough', 'just', 'almost', 'about', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now'
}
stop_words = stop_words.union(custom_stopwords)

def clean_text(text):
    # Remove URLs
    text = re.sub(r'http[s]?://\S+|www\.\S+', '', str(text))
    # Remove emojis
    text = re.sub(r'[\U00010000-\U0010FFFF]', '', text)
    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))
    # Lowercase
    text = text.lower()
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Tokenize, remove stopwords, lemmatize
    words = [lemmatizer.lemmatize(w) for w in text.split() if w not in stop_words and len(w) > 2]
    return ' '.join(words)

print("Columns:", df.columns)
documents = df['Content'].astype(str).apply(clean_text)
documents = [doc for doc in documents if len(doc.split()) > 3]  # Remove very short messages
# --- END IMPROVED PREPROCESSING ---

# For testing: only use a sample of the data (e.g. first 1000 messages)
# SAMPLE_SIZE = 20000  # Change this number as needed
# if len(documents) > SAMPLE_SIZE:
    # print(f"Using only the first {SAMPLE_SIZE} documents for testing.")
    # documents = documents[:SAMPLE_SIZE]
# print(f"Loaded documents: {len(documents)}")



# Optionally, customize the vectorizer
print("Initializing vectorizer...")
vectorizer_model = CountVectorizer(stop_words="english", ngram_range=(1,2))

# --- REDUCE NUMBER OF TOPICS: Use HDBSCAN with higher min_cluster_size ---
# By increasing min_cluster_size, you force BERTopic to create fewer, larger topics.
# Try 30, 50, or higher. The higher the value, the fewer topics (but each topic is broader).
from hdbscan import HDBSCAN
# Try different min_cluster_size and min_samples for better topic separation
cluster_model = HDBSCAN(min_cluster_size=900, min_samples=60)
# --- END REDUCE NUMBER OF TOPICS ---



# Create and fit BERTopic model
print("Starting BERTopic modeling...")
start = time.time()
topic_model = BERTopic(vectorizer_model=vectorizer_model, hdbscan_model=cluster_model)
topics, probs = topic_model.fit_transform(documents)
print(f"BERTopic finished after {time.time() - start:.2f} seconds")

# Optional: Reduce number of topics for interpretability
# (Uncomment and set nr_topics as needed)
# topic_model = topic_model.reduce_topics(documents, nr_topics=25)

# Optionally, filter out generic topics (topic -1 is usually 'outliers/noise')
# topic_info = topic_model.get_topic_info()
# print("\nFiltered topic overview (excluding outliers):")
# print(topic_info[topic_info.Topic != -1])



# View topics 
print("Topic overview:")
print(topic_model.get_topic_info())

# Save topics to a CSV file for later analysis
# This file contains the topic number, frequency, and top words for each topic
topic_model.get_topic_info().to_csv("topics.csv", index=False)

# Save topic assignment for each document
df_sample = df.iloc[:len(documents)].copy()
df_sample['topic'] = topics
df_sample.to_csv("documents_with_topics.csv", index=False)

# Show keywords for each topic 
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

# Speichere Topic-Frequenz als Tabelle (PNG)
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table

sorted_freq = topic_freq.sort_values('Count', ascending=False)
fig, ax = plt.subplots(figsize=(8, min(0.5 + 0.4*len(sorted_freq), 15)))
ax.axis('off')
tbl = table(ax, sorted_freq, loc='center', colWidths=[0.2]*len(sorted_freq.columns))
tbl.auto_set_font_size(False)
tbl.set_fontsize(10)
tbl.scale(1, 1.5)
plt.tight_layout()
plt.savefig('topic_overview_table.png')
print('Saved: topic_overview_table.png')



# --- TOPIC FREQUENCY BARCHART (mehr Keywords) ---
try:
    print("\nOpening Topic Frequency Barchart (top 10 topics)...")
    fig = topic_model.visualize_barchart(top_n_topics=10)
    fig.show()
    fig.write_image("topic_barchart_top10.png")
    print("Saved: topic_barchart_top10.png")
except Exception as e:
    print(f"Barchart error: {e}")



# --- INTERAKTIVE VISUALISIERUNGEN ALS BILDER SPEICHERN ---
try:
    print("\nOpening interactive 2D-Map of all topics...")
    fig = topic_model.visualize_topics()
    fig.show()
    fig.write_image("topic_2dmap.png")
    print("Saved: topic_2dmap.png")
except Exception as e:
    print(f"2D-Map error: {e}")

try:
    print("\nOpening Topic Frequency Barchart...")
    fig = topic_model.visualize_barchart(top_n_topics=12)
    fig.show()
    fig.write_image("topic_barchart.png")
    print("Saved: topic_barchart.png")
except Exception as e:
    print(f"Barchart error: {e}")



# --- HIERARCHICAL DENDROGRAM ---
try:
    print("\nOpening Hierarchical Dendrogram...")
    fig = topic_model.visualize_hierarchy()
    fig.show()
    fig.write_image("topic_dendrogram.png")
    print("Saved: topic_dendrogram.png")
except Exception as e:
    print(f"Dendrogram error: {e}")

# --- TOP KEYWORDS PER TOPIC (Tabellarisch + Plot) ---
print("\nTop keywords per topic (tabular):")
top_n = 10
for topic_num in topic_freq.sort_values('Count', ascending=False).head(top_n)['Topic']:
    words = [w for w, _ in topic_model.get_topic(topic_num)[:10]]
    print(f"Topic {topic_num}: {', '.join(words)}")

# Speichere Top-Keywords pro Topic als PNG-Tabelle
import pandas as pd
import matplotlib.pyplot as plt
from pandas.plotting import table

keywords_table = []
for topic_num in topic_freq.sort_values('Count', ascending=False)['Topic']:
    words = [w for w, _ in topic_model.get_topic(topic_num)[:10]]
    keywords_table.append({'Topic': topic_num, 'Top Keywords': ', '.join(words)})
df_keywords = pd.DataFrame(keywords_table)
fig, ax = plt.subplots(figsize=(10, min(0.5 + 0.4*len(df_keywords), 15)))
ax.axis('off')
tbl = table(ax, df_keywords, loc='center', colWidths=[0.1, 0.7])
tbl.auto_set_font_size(False)
tbl.set_fontsize(10)
tbl.scale(1, 1.5)
plt.tight_layout()
plt.savefig('top_keywords_per_topic_table.png')
print('Saved: top_keywords_per_topic_table.png')



# Optional: Plot Top Keywords for Top Topics and save as PNG
import matplotlib.pyplot as plt
for topic_num in topic_freq.sort_values('Count', ascending=False).head(5)['Topic']:
    keywords = topic_model.get_topic(topic_num)
    if keywords:
        words, scores = zip(*keywords[:10])
        plt.figure(figsize=(8,3))
        plt.bar(words, scores)
        plt.title(f'Topic {topic_num}: Top Keywords')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()

# --- MANUELLE TOPIC LABELS (Beispiel) ---
# custom_labels = {
#     0: "Storm is Coming",
#     1: "Trust the Plan",
#     2: "Great Awakening",
#     # ...
# }
# topic_model.set_topic_labels(custom_labels)

# --- REPRESENTATIVE DOCUMENTS ---
print("\nRepresentative documents for top topics:")
for topic_num in topic_freq.sort_values('Count', ascending=False).head(5)['Topic']:
    docs = df_sample[df_sample['topic'] == topic_num]['Content'].head(3).tolist()
    print(f"\nTopic {topic_num} examples:")
    for doc in docs:
        print(f"- {doc[:200]}{'...' if len(doc)>200 else ''}")


# --- BARCHARTS DER HÄUFIGSTEN 7 WÖRTER DER HÄUFIGSTEN 12 TOPICS IN EINER DATEI ---
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

with PdfPages('top_topics_word_barcharts.pdf') as pdf:
    top_topics = topic_freq.sort_values('Count', ascending=False).head(12)['Topic']
    for topic_num in top_topics:
        keywords = topic_model.get_topic(topic_num)
        if keywords:
            words, scores = zip(*keywords[:7])
            plt.figure(figsize=(8,3))
            plt.bar(words, scores)
            plt.title(f'Topic {topic_num}: Top 7 Words')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            pdf.savefig()
            plt.close()
print('Saved: top_topics_word_barcharts.pdf')