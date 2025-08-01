import gzip
import numpy as np
import pandas as pd

# Step 1: retrieve usernames from corpus.txt
usernames = []
with open("corpus.txt", "r", encoding="utf-8") as f:
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) == 3:
            usernames.append(parts[0])
        else:
            raise ValueError(f"Badly formed line in corpus.txt: {line.strip()}")

# Step 2: read state.mallet.gz and parse word by word
with gzip.open("state.mallet.gz", "rt", encoding="utf-8") as f:
    lines = [line for line in f if not line.startswith("#")]

columns = ["doc", "source", "pos", "typeindex", "type", "topic"]
df = pd.DataFrame([line.strip().split() for line in lines], columns=columns)
df["doc"] = df["doc"].astype(int)
df["topic"] = df["topic"].astype(int)

# Step 3: build tweet × topic matrix
D = df["doc"].max() + 1
T = df["topic"].max() + 1
doc_topic_counts = np.zeros((D, T), dtype=int)

for row in df.itertuples():
    doc_topic_counts[row.doc, row.topic] += 1

# Step 4: associate usernames
if len(usernames) != D:
    raise ValueError(f"Mismatch : {len(usernames)} usernames vs {D} documents in state.mallet.gz")

df_topics = pd.DataFrame(doc_topic_counts, columns=[f"topic_{i}" for i in range(T)])
df_topics.insert(0, "username", usernames)

df_topics.to_csv("tweet_topic_matrix_with_users.csv", index=False)
