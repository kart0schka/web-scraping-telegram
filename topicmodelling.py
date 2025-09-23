# --- SETUP INSTRUCTIONS ---
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
# 5. If there is an error about building hdbscan:
#    - Download and install Microsoft C++ Build Tools from:
#      https://visualstudio.microsoft.com/visual-cpp-build-tools/
#    - During installation, select "Desktop development with C++" workload (default options are fine)
#    - Restart computer after installation
#    - Then try installing hdbscan and bertopic again
# -----------------------------------------------


import time
print("Starting script...")
import pandas as pd
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer

# Load parquet file
print("Reading parquet file...")
start = time.time()
df = pd.read_parquet("korpus.parquet")
print(f"File loaded in {time.time() - start:.2f} seconds")


# PREPROCESSING
import re
import string
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import nltk

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

custom_stopwords = {
    'make', 'makes', 'get', 'got', 'thing', 'stuff', 'use', 'used', 'using', 'give', 'given', 'take', 'taken', 'video', 'link', 'day', 'xpost', 'life', 'posting', 'world', 'click', 'podcast','show',
    'put', 'see', 'seen', 'go', 'goes', 'went', 'say', 'says', 'said', 'know', 'known', 'want', 'wanted', 'need', 'share', 'join', 'telegram', 'time', 'tonight', 'live', 'year', 'coming', 'chat',
    'really', 'just', 'like', 'one', 'two', 'three', 'also', 'still', 'even', 'much', 'many', 'lot', 'lots', 'every','bqqqqqqqm', 'news', 'rumble', 'subscribe', 'whats', '2025', 'frens', 'happy', 'drop',
    'something', 'anything', 'everything', 'nothing','makes', 'everyone', 'anyone', 'someone', 'thing', 'things', 'way', 'ways', 'scroll', 'people', 'yes', 'guess', 'thank', 'bot', 'morning','april',
    'today', 'tomorrow', 'yesterday', 'now', 'then', 'here', 'there', 'read','gets','where', 'when', 'how', 'why', 'can', 'could', 'would', 'should', 'channel', 'live', 'truth', 'confirm', 'yall', 'come',
    'will', 'may', 'might', 'must', 'shall', 'let', 'etc', 'etc.', 'amp', 'im', 'dont', 'didnt', 'doesnt', 'cant', 'wont', 'isnt', 'arent', 'wasnt', 'werent', 'havent', 'hasnt', 'hadnt', 'youre', 'theyre', 'weve', 'ive', 'youve', 'theyll', 'ill', 'hes', 'shes', 'its', 'whats', 'thats', 'theres', 'heres', 'whos', 'whom', 'whose', 'about', 'above', 'below', 'between', 'among', 'upon', 'without', 'within', 'across', 'toward', 'towards', 'against', 'around', 'through', 'during', 'before', 'after', 'again', 'further', 'once', 'always', 'never', 'sometimes', 'often', 'usually', 'rarely', 'maybe', 'perhaps', 'almost', 'already', 'yet', 'soon', 'early', 'late', 'new', 'old', 'good', 'bad', 'better', 'best', 'worst', 'big', 'small', 'great', 'little', 'long', 'short', 'high', 'low', 'right', 'left', 'far', 'near', 'close', 'open', 'closed', 'full', 'empty', 'same', 'different', 'other', 'another', 'next', 'last', 'first', 'second', 'third', 'more', 'less', 'most', 'least', 'such', 'quite', 'rather', 'very', 'too', 'enough', 'just', 'almost', 'about', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now'
}
stop_words = set(stopwords.words('english')).union(custom_stopwords)

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


# Vectorizer
print("Initializing vectorizer...")
vectorizer_model = CountVectorizer(stop_words=list(stop_words))


# Reduce number of topics
from hdbscan import HDBSCAN
cluster_model = HDBSCAN(min_cluster_size=900, min_samples=60)


# BERTopic model
print("Starting BERTopic modeling...")
start = time.time()
topic_model = BERTopic(
    vectorizer_model=vectorizer_model,
    hdbscan_model=cluster_model
)
topics, probs = topic_model.fit_transform(documents)
print(f"BERTopic finished after {time.time() - start:.2f} seconds")


# Topic overview 
print("Topic overview:")
print(topic_model.get_topic_info())

# Save topics with keywords
topic_table = []
for topic_num in topic_model.get_topic_freq().Topic:
    words = [w for w, _ in topic_model.get_topic(topic_num)[:10]] 
    topic_table.append({'Topic': topic_num, 'Count': topic_model.get_topic_freq().set_index('Topic').loc[topic_num, 'Count'], 'Name': '_'.join(words)})
pd.DataFrame(topic_table).to_csv("topics.csv", index=False)

# Save topic assignment
df_sample = df.iloc[:len(documents)].copy()
df_sample['topic'] = topics
df_sample.to_csv("documents_with_topics.csv", index=False)

# Show keywords 
print("\nTop keywords per topic:")
for topic_num in topic_model.get_topic_freq().Topic:
    print(f"Topic {topic_num}: {topic_model.get_topic(topic_num)}")

# Find most common topic
from collections import Counter
most_common_topic, count = Counter(topics).most_common(1)[0]
print(f"\nMost common topic: {most_common_topic} with {count} documents")

# Show average topic probability (confidence)
import numpy as np
avg_prob = np.nanmean([p.max() if p is not None else np.nan for p in probs])
print(f"\nAverage topic assignment confidence: {avg_prob:.3f}")

# List all topics
topic_freq = topic_model.get_topic_freq()
print("\nTopics sorted by frequency:")
print(topic_freq.sort_values('Count', ascending=False))

# Export topic overview for labeling
topic_overview = []
for topic_num in topic_model.get_topic_freq().Topic:
    keywords = ', '.join([word for word, _ in topic_model.get_topic(topic_num)[:10]])
    topic_overview.append({'Topic': topic_num, 'Top Keywords': keywords})

df_topic_overview = pd.DataFrame(topic_overview)
df_topic_overview.to_csv('topic_number_keywords_for_labeling.csv', index=False)
print('Saved: topic_number_keywords_for_labeling.csv (Topic Nummer + Keywords für Label-Zuordnung)')

topic_model.save("bertopic_model") 
print("Saved BERTopic model as bertopic_model") 

print("\nAnalysis complete.")

