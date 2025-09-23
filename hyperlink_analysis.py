import pandas as pd
import re
from urllib.parse import urlparse
from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.io as pio
import networkx as nx
import os
import plotly.graph_objects as go
from pandas.plotting import table

# Load the dataset
print("Reading Parquet file...")
df = pd.read_parquet("korpus.parquet")
print(f"Loaded {len(df)} messages.")
print('Spaltennamen:', df.columns)

# Extract all URLs 
def extract_urls(text):
    url_pattern = r"https?://\S+|www\.\S+|t\.me/\S+|\b\w+\.\w{2,}\b"
    return re.findall(url_pattern, str(text))

def normalize_domain(domain):
    domain = re.sub(r'^(www\.|m\.|mobile\.|amp\.)', '', domain)
    public_suffixes = ['co.uk', 'ac.uk', 'gov.uk', 'org.uk', 'sch.uk', 'net.uk', 'ltd.uk', 'plc.uk', 'me.uk']
    for suffix in public_suffixes:
        if domain.endswith('.' + suffix):
            parts = domain.split('.')
            if len(parts) > 2:
                domain = '.'.join(parts[-3:])
            else:
                domain = '.'.join(parts[-2:])
            return domain
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

# Extract domains from URLs
df['domains'] = df['urls'].apply(lambda urls: [get_domain(url) for url in urls])
all_domains = pd.Series([domain for domains in df['domains'] for domain in domains if domain])

# URL and domain counting
all_urls = pd.Series([url for urls in df['urls'] for url in urls if url])
url_counts = all_urls.value_counts()

all_domains = pd.Series([domain for domains in df['domains'] for domain in domains if domain])
domain_counts = all_domains.value_counts()

# Filter domains
ignore_domains = ['bit.ly', 'cutt.ly']
filtered_domains = all_domains[~all_domains.isin(ignore_domains)]
filtered_domain_counts = filtered_domains.value_counts()

print("\nTop 10 most common filtered domains:")
for domain, count in filtered_domain_counts.head(10).items():
    print(f"{domain}: {count}")

# Calculate most common filtered domains
filtered_domain_counts = filtered_domains.value_counts()
print("\nTop 10 most common filtered domains:")
for domain, count in filtered_domain_counts.head(10).items():
    print(f"{domain}: {count}")


# Save results for further analysis
pd.DataFrame({'url': url_counts.index, 'count': url_counts.values}).to_csv("url_counts.csv", index=False)
pd.DataFrame({'domain': domain_counts.index, 'count': domain_counts.values}).to_csv("domain_counts.csv", index=False)

# Page Rank Network Analysis
print("\nBuilding domain graph for PageRank analysis...")
G = nx.Graph()
for domains in df['domains']:
    domains = [d for d in domains if d]
    for i in range(len(domains)):
        for j in range(i+1, len(domains)):
            G.add_edge(domains[i], domains[j])

# Calculate PageRank
pagerank = nx.pagerank(G)
pagerank_sorted = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)
print("\nTop 10 domains by PageRank:")
for domain, score in pagerank_sorted[:10]:
    print(f"{domain}: {score:.5f}")

# Save PageRank
pd.DataFrame(pagerank_sorted, columns=['domain', 'pagerank']).to_csv('domain_pagerank.csv', index=False)
print("Saved: domain_pagerank.csv (PageRank scores for domains)")

# Page Rank Network Analysis for Groups
print("\nBuilding group graph for influencer (PageRank) analysis...")
G_group = nx.Graph()
url_to_groups = {}
for idx, row in df.iterrows():
    group = row['Group']  
    for url in row['urls']:
        url_to_groups.setdefault(url, set()).add(group)
for groups in url_to_groups.values():
    groups = list(set(groups))
    for i in range(len(groups)):
        for j in range(i+1, len(groups)):
            G_group.add_edge(groups[i], groups[j])
pagerank_group = nx.pagerank(G_group)
pagerank_group_sorted = sorted(pagerank_group.items(), key=lambda x: x[1], reverse=True)
print("\nTop 10 groups by PageRank:")
for group, score in pagerank_group_sorted[:10]:
    print(f"{group}: {score:.5f}")
pd.DataFrame(pagerank_group_sorted, columns=['group', 'pagerank']).to_csv('group_pagerank.csv', index=False)
print("Saved: group_pagerank.csv (PageRank scores for groups)")

# Visualization
pagerank_df_group = pd.DataFrame(pagerank_group_sorted, columns=['group', 'pagerank'])
plt.figure(figsize=(10,6))
sns.barplot(x='pagerank', y='group', data=pagerank_df_group.head(20), palette='mako')
plt.title('Top 20 Groups (PageRank)')
plt.xlabel('PageRank Score')
plt.ylabel('Group')
plt.tight_layout()
plt.savefig('top_groups_pagerank.png')
print('Saved: top_groups_pagerank.png')
plt.figure(figsize=(12,12))
pos = nx.spring_layout(G_group, k=0.15)
nx.draw_networkx_nodes(G_group, pos, node_size=50, node_color='green', alpha=0.7)
nx.draw_networkx_edges(G_group, pos, alpha=0.3)
nx.draw_networkx_labels(G_group, pos, font_size=8)
plt.title('Group Network (shared URLs)')
plt.axis('off')
plt.tight_layout()
plt.savefig('group_network_graph.png')
print('Saved: group_network_graph.png')

# Page Rank Network Analysis for Authors
print("\nBuilding author graph for influencer (PageRank) analysis...")
G_author = nx.Graph()
url_to_authors = {}
for idx, row in df.iterrows():
    author = row.get('Author') or row.get('author')
    for url in row['urls']:
        url_to_authors.setdefault(url, set()).add(author)
for authors in url_to_authors.values():
    authors = list(set(authors))
    authors = [a for a in set(authors) if a] 
    for i in range(len(authors)):
        for j in range(i+1, len(authors)):
            G_author.add_edge(authors[i], authors[j])
pagerank_author = nx.pagerank(G_author)
pagerank_author_sorted = sorted(pagerank_author.items(), key=lambda x: x[1], reverse=True)
print("\nTop 10 authors by PageRank:")
for author, score in pagerank_author_sorted[:10]:
    print(f"{author}: {score:.5f}")
pd.DataFrame(pagerank_author_sorted, columns=['author', 'pagerank']).to_csv('author_pagerank.csv', index=False)
print("Saved: author_pagerank.csv (PageRank scores for authors)")

# Visualization
pagerank_df_author = pd.DataFrame(pagerank_author_sorted, columns=['author', 'pagerank'])
plt.figure(figsize=(10,6))
sns.barplot(x='pagerank', y='author', data=pagerank_df_author.head(20), palette='rocket')
plt.title('Top 20 Authors (PageRank)')
plt.xlabel('PageRank Score')
plt.ylabel('Author')
plt.tight_layout()
plt.savefig('top_authors_pagerank.png')
print('Saved: top_authors_pagerank.png')
plt.figure(figsize=(12,12))
pos = nx.spring_layout(G_author, k=0.15)
nx.draw_networkx_nodes(G_author, pos, node_size=50, node_color='red', alpha=0.7)
nx.draw_networkx_edges(G_author, pos, alpha=0.3)
nx.draw_networkx_labels(G_author, pos, font_size=8)
plt.title('Author Network (shared URLs)')
plt.axis('off')
plt.tight_layout()
plt.savefig('author_network_graph.png')
print('Saved: author_network_graph.png')

# Visualizations of Top Domains
print("\nVisualizing top 10 filtered domains (without shorteners)...")
ignore_domains = ['bit.ly', 'cutt.ly', 'linktr.ee']
filtered_domains = all_domains[~all_domains.isin(ignore_domains)]
filtered_domain_counts = filtered_domains.value_counts()
top_filtered_domains = filtered_domain_counts.head(10)
domains, counts = top_filtered_domains.index, top_filtered_domains.values
fig = px.bar(x=list(domains), y=list(counts), labels={'x': 'Domain', 'y': 'Count'}, title='Top 10 Most Common Domains')
fig.update_layout(xaxis_tickangle=-45)
try:
    fig.write_image('top_10_filtered_domains.png')
except Exception as e:
    print(f"Image export error (top_10_filtered_domains.png): {e}\nInstall 'kaleido' for image export.")
print("Saved: top_10_filtered_domains.png (static)")

# Network Statistics for the author network
if G_author.number_of_nodes() > 0:
    print("\nNetwork statistics for the author network:")

    density_author = nx.density(G_author)

    if nx.is_connected(G_author):
        diameter_author = nx.diameter(G_author)
        avg_path_length_author = nx.average_shortest_path_length(G_author)
    else:
        diameter_author = max(nx.diameter(G_author.subgraph(c)) for c in nx.connected_components(G_author))
        avg_path_length_author = None

    from networkx.algorithms.community import greedy_modularity_communities
    communities_author = list(greedy_modularity_communities(G_author))
    modularity_author = nx.algorithms.community.quality.modularity(G_author, communities_author)

    degrees_author = dict(G_author.degree())
    max_deg_author = max(degrees_author.values()) if degrees_author else 0
    if len(G_author.nodes()) > 2:
        centralization_author = sum(max_deg_author - deg for deg in degrees_author.values()) / ((len(G_author.nodes()) - 1) * (len(G_author.nodes()) - 2))
    else:
        centralization_author = 0

    network_stats_author = {
        'Density': round(density_author, 4),
        'Diameter': diameter_author,
        'Average Path Length': round(avg_path_length_author, 4) if avg_path_length_author else 'n/a',
        'Modularity': round(modularity_author, 4),
        'Degree Centralization': round(centralization_author, 4)
    }
    df_author_stats = pd.DataFrame({
        'Metric': list(network_stats_author.keys()),
        'Value': list(network_stats_author.values())
    }).reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(6, 3))
    ax.axis('off')
    tbl = table(ax, df_author_stats, loc='center', colWidths=[0.6, 0.3])  
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(14)
    tbl.scale(1.2, 2.0)
    plt.title("Author Network Statistics", fontsize=16)
    plt.tight_layout()
    plt.savefig('author_network_statistics_overview.png', dpi=400)
    plt.close(fig)
    print('Saved: author_network_statistics_overview.png')
else:
    print("Author network is empty, no statistics calculated.")

# Network Statistics for the group network
if G_group.number_of_nodes() > 0:
    print("\nNetwork statistics for the group network:")

    density_group = nx.density(G_group)

    if nx.is_connected(G_group):
        diameter_group = nx.diameter(G_group)
        avg_path_length_group = nx.average_shortest_path_length(G_group)
    else:
        diameter_group = max(nx.diameter(G_group.subgraph(c)) for c in nx.connected_components(G_group))
        avg_path_length_group = None

    communities_group = list(greedy_modularity_communities(G_group))
    modularity_group = nx.algorithms.community.quality.modularity(G_group, communities_group)

    degrees_group = dict(G_group.degree())
    max_deg_group = max(degrees_group.values()) if degrees_group else 0
    if len(G_group.nodes()) > 2:
        centralization_group = sum(max_deg_group - deg for deg in degrees_group.values()) / ((len(G_group.nodes()) - 1) * (len(G_group.nodes()) - 2))
    else:
        centralization_group = 0

    network_stats_group = {
        'Density': round(density_group, 4),
        'Diameter': diameter_group,
        'Average Path Length': round(avg_path_length_group, 4) if avg_path_length_group else 'n/a',
        'Modularity': round(modularity_group, 4),
        'Degree Centralization': round(centralization_group, 4)
    }
    df_group_stats = pd.DataFrame({
        'Metric': list(network_stats_group.keys()),
        'Value': list(network_stats_group.values())
    }).reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(6, 3))
    ax.axis('off')
    tbl = table(ax, df_group_stats, loc='center', colWidths=[0.6, 0.3])  
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(14)
    tbl.scale(1.2, 2.0)
    plt.title("Group Network Statistics", fontsize=16)
    plt.tight_layout()
    plt.savefig('group_network_statistics_overview.png', dpi=400)
    plt.close(fig)
    print('Saved: group_network_statistics_overview.png')
else:
    print("Group network is empty, no statistics calculated.")

print("\nAnalysis complete.")
