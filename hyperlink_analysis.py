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

# Load the dataset
print("Reading parquet file...")
df = pd.read_parquet("korpus.parquet")
print(f"Loaded {len(df)} messages.")

# Function to extract all URLs from a text string
def extract_urls(text):
    # Regex matches http(s) and www URLs
    url_pattern = r"http[s]?://\S+|www\.\S+"
    return re.findall(url_pattern, str(text))

# Extract URLs from each message
print("Extracting URLs from messages...")
df['urls'] = df['Content'].apply(extract_urls)

# Flatten all URLs into a single list
all_urls = [url for urls in df['urls'] for url in urls]
print(f"Found {len(all_urls)} total URLs.")

# Count most common full URLs
url_counts = Counter(all_urls)
print("Top 10 most common URLs:")
for url, count in url_counts.most_common(10):
    print(f"{url}: {count}")

# Extract domains from URLs
def get_domain(url):
    try:
        parsed = urlparse(url if url.startswith('http') else 'http://' + url)
        return parsed.netloc.lower()
    except Exception:
        return None

df['domains'] = df['urls'].apply(lambda urls: [get_domain(url) for url in urls])
all_domains = [domain for domains in df['domains'] for domain in domains if domain]
domain_counts = Counter(all_domains)

print("\nTop 10 most common domains:")
for domain, count in domain_counts.most_common(10):
    print(f"{domain}: {count}")

# Save results to CSV for further analysis
print("\nSaving URL and domain counts to CSV files...")
pd.DataFrame(url_counts.most_common(), columns=['url', 'count']).to_csv("url_counts.csv", index=False)
pd.DataFrame(domain_counts.most_common(), columns=['domain', 'count']).to_csv("domain_counts.csv", index=False)

# Optionally, save messages with at least one URL for qualitative review
df_with_urls = df[df['urls'].apply(len) > 0]
df_with_urls.to_csv("messages_with_urls.csv", index=False)


# --- TIME TRENDS AND VISUALIZATIONS ---
import matplotlib.pyplot as plt
import seaborn as sns


# --- Time trend: URLs per day (with HTML export) ---
import plotly.express as px
import plotly.io as pio

if 'Date' in df.columns:
    print("\nAnalyzing URL frequency over time...")
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['has_url'] = df['urls'].apply(lambda x: len(x) > 0)
    url_trend = df.groupby(df['Date'].dt.date)['has_url'].sum()
    # Plotly interactive line plot
    fig = px.line(url_trend, title='Number of Messages with URLs per Day', labels={'value': 'Messages with URLs', 'index': 'Date'})
    fig.write_html('url_trend_per_day.html')
    print("Saved: url_trend_per_day.html (interactive)")
    # Also save static PNG
    fig.write_image('url_trend_per_day.png')
    print("Saved: url_trend_per_day.png (static)")
else:
    print("No 'Date' column found for time trend analysis.")


# --- Visualization: Top 10 domains (with HTML export) ---
print("\nVisualizing top 10 domains...")
top_domains = domain_counts.most_common(10)
domains, counts = zip(*top_domains)
fig = px.bar(x=domains, y=counts, labels={'x': 'Domain', 'y': 'Count'}, title='Top 10 Most Common Domains')
fig.update_layout(xaxis_tickangle=-45)
fig.write_html('top_10_domains.html')
print("Saved: top_10_domains.html (interactive)")
fig.write_image('top_10_domains.png')
print("Saved: top_10_domains.png (static)")


# --- Visualization: Top 10 URLs (with HTML export) ---
print("\nVisualizing top 10 URLs...")
top_urls = url_counts.most_common(10)
urls, url_counts_ = zip(*top_urls)
fig = px.bar(x=urls, y=url_counts_, labels={'x': 'URL', 'y': 'Count'}, title='Top 10 Most Common URLs')
fig.update_layout(xaxis_tickangle=-45)
fig.write_html('top_10_urls.html')
print("Saved: top_10_urls.html (interactive)")
fig.write_image('top_10_urls.png')
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
