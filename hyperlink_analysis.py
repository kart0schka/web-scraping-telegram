"""
Hyperlink Analysis Script for Telegram Messages
------------------------------------------------
This script extracts, counts, and summarizes hyperlinks (URLs) from the 'Content' column of your Telegram message dataset.

- Loads the 'korpus.parquet' file.
- Extracts all URLs from each message.
- Counts most common domains and full URLs.
- Saves results to CSV files for further analysis.
- Extensively commented for clarity and extension.

Best practice: Run this script separately from topic modeling to keep analyses modular.
"""

import pandas as pd
import re
from urllib.parse import urlparse
from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.io as pio
import networkx as nx

# Load the dataset
print("Reading parquet file...")
df = pd.read_parquet("korpus.parquet")
print(f"Loaded {len(df)} messages.")

# Function to extract all URLs from a text string
def extract_urls(text):
    # Robust regex: matches http(s), www, t.me, naked domains
    url_pattern = r"https?://\S+|www\.\S+|t\.me/\S+|\b\w+\.\w{2,}\b"
    return re.findall(url_pattern, str(text))

def normalize_domain(domain):
    # Remove www., m., etc. from start
    domain = re.sub(r'^(www\.|m\.|mobile\.|amp\.)', '', domain)
    # Only keep main domain (youtube.com from subdomain.youtube.com)
    parts = domain.split('.')
    if len(parts) > 2:
        domain = '.'.join(parts[-2:])
    return domain

def get_domain(url):
    try:
        parsed = urlparse(url if url.startswith('http') else 'http://' + url)
        domain = parsed.netloc.lower()
        return normalize_domain(domain)
    except Exception:
        return None

# Extract URLs from each message
print("Extracting URLs from messages...")
df['urls'] = df['Content'].apply(extract_urls)

# Effizientere Zählung mit pandas
all_urls = df['urls'].explode().dropna()
print(f"Found {len(all_urls)} total URLs.")
url_counts = all_urls.value_counts()
print("Top 10 most common URLs:")
for url, count in url_counts.head(10).items():
    print(f"{url}: {count}")

df['domains'] = df['urls'].apply(lambda urls: [get_domain(url) for url in urls])
all_domains = pd.Series([domain for domains in df['domains'] for domain in domains if domain])
domain_counts = all_domains.value_counts()
print("\nTop 10 most common domains:")
for domain, count in domain_counts.head(10).items():
    print(f"{domain}: {count}")

# Save results to CSV for further analysis
pd.DataFrame({'url': url_counts.index, 'count': url_counts.values}).to_csv("url_counts.csv", index=False)
pd.DataFrame({'domain': domain_counts.index, 'count': domain_counts.values}).to_csv("domain_counts.csv", index=False)

# Optionally, save messages with at least one URL for qualitative review
df_with_urls = df[df['urls'].apply(len) > 0]
df_with_urls.to_csv("messages_with_urls.csv", index=False)

# --- PAGE RANK NETZWERKANALYSE ---
# Baue einen einfachen Domain-Graphen: Jede Nachricht mit mehreren Domains erzeugt Kanten zwischen diesen Domains
print("\nBuilding domain graph for PageRank analysis...")
G = nx.Graph()
for domains in df['domains']:
    # Nur echte Domains, keine None
    domains = [d for d in domains if d]
    # Füge Kanten für alle Paare in einer Nachricht hinzu
    for i in range(len(domains)):
        for j in range(i+1, len(domains)):
            G.add_edge(domains[i], domains[j])

# Berechne PageRank
pagerank = nx.pagerank(G)
pagerank_sorted = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)
print("\nTop 10 domains by PageRank:")
for domain, score in pagerank_sorted[:10]:
    print(f"{domain}: {score:.5f}")

# Speichere PageRank als CSV
pd.DataFrame(pagerank_sorted, columns=['domain', 'pagerank']).to_csv('domain_pagerank.csv', index=False)
print("Saved: domain_pagerank.csv (PageRank scores for domains)")

# --- TIME TRENDS AND VISUALIZATIONS ---

# --- Time trend: URLs per day (with HTML export) ---
if 'Date' in df.columns:
    print("\nAnalyzing URL frequency over time...")
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['has_url'] = df['urls'].apply(lambda x: len(x) > 0)
    url_trend = df.groupby(df['Date'].dt.date)['has_url'].sum()
    fig = px.line(url_trend, title='Number of Messages with URLs per Day', labels={'value': 'Messages with URLs', 'index': 'Date'})
    fig.write_html('url_trend_per_day.html')
    print("Saved: url_trend_per_day.html (interactive)")
    try:
        fig.write_image('url_trend_per_day.png')
    except Exception as e:
        print(f"Image export error (url_trend_per_day.png): {e}\nInstall 'kaleido' for image export.")
    print("Saved: url_trend_per_day.png (static)")
else:
    print("No 'Date' column found for time trend analysis.")

# --- Visualization: Top 10 domains (with HTML export) ---
print("\nVisualizing top 10 domains...")
top_domains = domain_counts.head(10)
domains, counts = top_domains.index, top_domains.values
fig = px.bar(x=list(domains), y=list(counts), labels={'x': 'Domain', 'y': 'Count'}, title='Top 10 Most Common Domains')
fig.update_layout(xaxis_tickangle=-45)
fig.write_html('top_10_domains.html')
print("Saved: top_10_domains.html (interactive)")
try:
    fig.write_image('top_10_domains.png')
except Exception as e:
    print(f"Image export error (top_10_domains.png): {e}\nInstall 'kaleido' for image export.")
print("Saved: top_10_domains.png (static)")

# --- Visualization: Top 10 URLs (with HTML export) ---
print("\nVisualizing top 10 URLs...")
top_urls = url_counts.head(10)
urls, url_counts_ = top_urls.index, top_urls.values
fig = px.bar(x=list(urls), y=list(url_counts_), labels={'x': 'URL', 'y': 'Count'}, title='Top 10 Most Common URLs')
fig.update_layout(xaxis_tickangle=-45)
fig.write_html('top_10_urls.html')
print("Saved: top_10_urls.html (interactive)")
try:
    fig.write_image('top_10_urls.png')
except Exception as e:
    print(f"Image export error (top_10_urls.png): {e}\nInstall 'kaleido' for image export.")
print("Saved: top_10_urls.png (static)")

print("\nAnalysis complete. Results saved:")
print("- url_counts.csv: Frequency of each unique URL")
print("- domain_counts.csv: Frequency of each domain")
print("- messages_with_urls.csv: All messages containing at least one URL")
print("- url_trend_per_day.html: Time trend of messages with URLs (interactive)")
print("- url_trend_per_day.png: Time trend of messages with URLs (static)")
print("- top_10_domains.html: Bar chart of top 10 domains (interactive)")
print("- top_10_domains.png: Bar chart of top 10 domains (static)")
print("- top_10_urls.html: Bar chart of top 10 URLs (interactive)")
print("- top_10_urls.png: Bar chart of top 10 URLs (static)")
