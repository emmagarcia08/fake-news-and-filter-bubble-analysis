from DB_connection import DB_connection
import nltk
from nltk.corpus import stopwords
import pickle
import re
import stanza
import torch

# Downloads
nltk.download('stopwords')
stanza.download('en')

# NLP setup
nlp_stanza = stanza.Pipeline('en', processors='tokenize,lemma', use_gpu=torch.cuda.is_available())
stop_words = set(stopwords.words('english'))

# Basic cleaning
def basic_clean(text):
    text = re.sub(r"\'s\b", "", text)
    text = ''.join(c if c.isalnum() or c.isspace() else ' ' for c in text)
    return text

# Preprocessing function
def preprocess_text(text):
    text = basic_clean(text)
    doc = nlp_stanza(text)
    tokens = [
        word.lemma.strip()
        for sent in doc.sentences
        for word in sent.words
        if word.lemma.strip()
           and word.lemma.strip() not in stop_words
           and not word.lemma.strip().isdigit()
           and len(word.lemma.strip()) > 1
    ]
    return " ".join(tokens)

# Load DB
db_connection = DB_connection("GOSSIPCOP")
df_db = db_connection.select(
    "SELECT username, text_translation, quoted_translation, tweet_type FROM filtered_user_timelines"
).fillna("")

usernames = df_db['username'].unique()

preprocessed_by_user = {}

# Process all users
for idx, username in enumerate(usernames, 1):
    user_df = df_db[df_db['username'] == username][['text_translation', 'quoted_translation', 'tweet_type']]

    user_processed = {}

    for tweet_type in user_df['tweet_type'].unique():
        subset = user_df[user_df['tweet_type'] == tweet_type]

        if tweet_type == "quoted_tweet":
            texts = [
                f"{row['text_translation']} {row['quoted_translation']}".strip()
                for _, row in subset.iterrows()
            ]
        else:
            texts = subset['text_translation'].tolist()

        preprocessed = [preprocess_text(t) for t in texts if t.strip()]
        preprocessed = [t for t in preprocessed if t.strip()]

        if preprocessed:
            user_processed[tweet_type] = preprocessed

    if user_processed:
        preprocessed_by_user[username] = user_processed

# Save results
with open("preprocessed_by_user.pkl", "wb") as f:
    pickle.dump(preprocessed_by_user, f)

db_connection.close()
