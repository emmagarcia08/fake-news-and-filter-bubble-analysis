import numpy as np
import pandas as pd
import pickle

def compute_topic_similarity_cosine(doc_topic_matrix):
    n_topics = doc_topic_matrix.shape[1]
    similarity_matrix = np.zeros((n_topics, n_topics))

    for i in range(n_topics):
        col_i = doc_topic_matrix[:, i]
        norm_i = np.linalg.norm(col_i)
        for j in range(n_topics):
            col_j = doc_topic_matrix[:, j]
            norm_j = np.linalg.norm(col_j)
            if norm_i == 0 or norm_j == 0:
                similarity = 0.0
            else:
                similarity = np.dot(col_i, col_j) / (norm_i * norm_j)
            similarity_matrix[i, j] = similarity
    return similarity_matrix

def compute_rao_diversity(p, dissimilarity_matrix):
    return np.sum(p[:, None] * p[None, :] * dissimilarity_matrix)

# Load the CSV file containing the document-topic count matrix
df = pd.read_csv("tweet_topic_matrix_with_users.csv")

# Identify topic columns
topic_columns = [col for col in df.columns if col.startswith("topic_")]

# Calculate the similarity/dissimilarity matrix
global_topic_matrix = df[topic_columns].values
similarity_matrix = compute_topic_similarity_cosine(global_topic_matrix)
dissimilarity_matrix = 1 - similarity_matrix

# Compute Rao's diversity score for each user
rao_results = {}

for username, group in df.groupby("username"):
    user_topic_counts = group[topic_columns].sum().values
    total = user_topic_counts.sum()

    if total == 0:
        rao_score = 0.0
    else:
        p = user_topic_counts / total
        rao_score = compute_rao_diversity(p, dissimilarity_matrix)

    rao_results[username] = {"rao_score": rao_score}

with open("rao_diversity_scores.pkl", "wb") as f:
    pickle.dump(rao_results, f)
