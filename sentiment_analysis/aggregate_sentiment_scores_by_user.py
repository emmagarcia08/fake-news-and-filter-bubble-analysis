import pickle

# Load sentiment scores per tweet
with open("sentiment_scores_by_user.pkl", "rb") as f:
    scores_by_user = pickle.load(f)

# Compute the overall sentiment score for each user
global_sentiment_scores = {}

for username, tweet_scores in scores_by_user.items():
    tweet_values = list(tweet_scores.values())
    if tweet_values:
        abs_scores = [abs(score) for score in tweet_values]
        global_score = round(sum(abs_scores) / len(abs_scores), 4)
    else:
        global_score = 0.0
    global_sentiment_scores[username] = global_score

output_filename = "global_sentiment_scores_by_user.pkl"
with open(output_filename, "wb") as f:
    pickle.dump(global_sentiment_scores, f)
