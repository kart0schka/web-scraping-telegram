import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table
from scipy.cluster.hierarchy import dendrogram, linkage
import numpy as np
from bertopic import BERTopic

# --- Load Data ---
df_sample = pd.read_csv('documents_with_topics.csv')
topic_info = pd.read_csv('topics.csv')
topic_freq = topic_info[['Topic', 'Count']].copy()

# --- Custom Labels ---
custom_labels = {
     -1: "Unassigned Documents",
     0: "Christianity",
     1: "MAGA",
     2: "Pharmaceuticals & vaccines",
     3: "Government efficiency",
     4: "Trade war",
     5: "Ukraine war",
     6: "Deep state & military",
     7: "Epstein files",
     8: "Justice system",
     9: "Immigration & border",
     10: "California wildfires",
     11: "Gender issues & education",
     12: "Finance",
     13: "JFK assassination",
     14: "Plane crashes",
     15: "Middle East conflict"
}
df_sample['Topic_Label'] = df_sample['topic'].map(custom_labels)
topic_freq['Topic_Label'] = topic_freq['Topic'].map(custom_labels)

# Saving topic overview
sorted_freq = topic_freq.sort_values('Count', ascending=False)
fig, ax = plt.subplots(figsize=(8, min(0.5 + 0.4*len(sorted_freq), 15)))
ax.axis('off')
tbl = table(ax, sorted_freq[['Topic', 'Topic_Label', 'Count']], loc='center', colWidths=[0.15, 0.5, 0.2])
tbl.auto_set_font_size(False)
tbl.set_fontsize(12)
tbl.scale(1, 1.5)
plt.tight_layout()
plt.savefig('topic_overview_table.png', dpi=300)
print('Saved: topic_overview_table.png')

# Top Keywords per Topic
keywords_table = []
for topic_num in sorted_freq['Topic']:
    top_words = topic_info.loc[topic_info['Topic'] == topic_num, 'Name'].values
    if len(top_words) > 0:
        kws = top_words[0]
        kws_list = kws.split('_')[1:] if '_' in kws else kws.split(',')
        top_words_str = ', '.join(kws_list[:10])
    else:
        top_words_str = ''
    keywords_table.append({'Topic': topic_num, 'Topic_Label': custom_labels.get(topic_num, str(topic_num)), 'Top Keywords': top_words_str})
df_keywords = pd.DataFrame(keywords_table)
fig, ax = plt.subplots(figsize=(14, min(0.5 + 0.4*len(df_keywords), 18)))
ax.axis('off')
tbl = table(
    ax,
    df_keywords[['Topic', 'Topic_Label', 'Top Keywords']],
    loc='center',
    colWidths=[0.08, 0.22, 0.7]
)
tbl.auto_set_font_size(False)
tbl.set_fontsize(14)
tbl.scale(1, 1.5)

for key in tbl.get_celld():
    cell = tbl.get_celld()[key]
    if key[1] == 1: 
        cell.get_text().set_ha('left')
    if key[1] == 2: 
        cell.get_text().set_ha('left')

plt.tight_layout()
plt.savefig('top_keywords_per_topic_table.png', dpi=300)
plt.close(fig)
print('Saved: top_keywords_per_topic_table.png')

# Dendrogram of Topics 
print("\nCreating topic dendrogram...")

topic_model = BERTopic.load("bertopic_model")
topic_vectors = topic_model.topic_embeddings_
topic_nums = [t for t in sorted_freq['Topic'].tolist() if t != -1]  # Entferne Topic -1
topic_vectors = np.array([topic_vectors[topic_num] for topic_num in topic_nums])
linked = linkage(topic_vectors, method='ward')
labels = [custom_labels.get(t, str(t)) for t in topic_nums]

fig, ax = plt.subplots(figsize=(14, 8))
dendrogram(linked, labels=labels, orientation='top', leaf_rotation=45, ax=ax)
plt.title("Topic Dendrogram")
plt.tight_layout()
plt.savefig("topic_dendrogram.png", dpi=300)
plt.close(fig)
print("Saved: topic_dendrogram.png")

# Topic distribution (stacked barplot)
if 'topic' in df_sample.columns and 'Group' in df_sample.columns:
    print("\nMapping topic distributions across top 10 channels...")
    topic_channel_table = pd.crosstab(df_sample['Group'], df_sample['topic'])
    topic_channel_table = topic_channel_table.sort_index(axis=1)

    topic_channel_table.columns = [custom_labels.get(t, str(t)) for t in topic_channel_table.columns]

    if -1 in topic_channel_table.columns:
        topic_minus1_label = custom_labels.get(-1, "-1")
        if topic_minus1_label in topic_channel_table.columns:
            topic_channel_table = topic_channel_table.drop(columns=[topic_minus1_label])

    top_channels = topic_channel_table.sum(axis=1).sort_values(ascending=False).head(10).index
    topic_channel_table = topic_channel_table.loc[top_channels]

    topic_channel_table.plot(kind='bar', stacked=True, figsize=(max(10, topic_channel_table.shape[0]*0.5), 8), colormap='tab20')
    plt.title('Stacked Topic Distribution Across Top 10 Channels')
    plt.xlabel('Channel')
    plt.ylabel('Number of Messages')
    plt.legend(title='Topic', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig('topic_distribution_stacked_bar_top10.png')
    print('Saved: topic_distribution_stacked_bar_top10.png')
else:
    print("Topic or Group column not found for topic-channel mapping.")

print("\nAnalysis complete.")
