import pickle

# Load data
with open("preprocessed_by_user.pkl", "rb") as f:
    user_docs = pickle.load(f)

# Write the data to a file with this structure: username, tweet type, tweet content
with open("corpus.txt", "w", encoding="utf-8") as f:
    for username, tweet_types in user_docs.items():
        for tweet_type, tweets in tweet_types.items():
            for tweet in tweets:
                if isinstance(tweet, str) and tweet.strip():
                    clean_tweet = tweet.strip().lower()
                    f.write(f"{username}\t{tweet_type}\t{clean_tweet}\n")
