import json
from nltk.corpus import sentiwordnet as swn
import pickle
from tqdm import tqdm
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Load the VADER lexicon
vader_analyzer = SentimentIntensityAnalyzer()

# Load the SenticNet lexicon
with open("senticnet.json", "r", encoding="utf-8") as f:
    senticnet = json.load(f)

# Function that retrieves the sub-ngrams of smaller size
def get_sub_ngrams(ngram):
    tokens = ngram.split()
    sub_ngrams = set()
    for i in range(len(tokens)):
        sub_ngrams.add(tokens[i])
        if i < len(tokens) - 1:
            sub_ngrams.add(" ".join(tokens[i:i+2]))
        if i < len(tokens) - 2:
            sub_ngrams.add(" ".join(tokens[i:i+3]))
    return sub_ngrams

# Function that retrieves the polarity of a term
def get_polarity(term):
    # 1. SentiWordNet
    synsets = list(swn.senti_synsets(term))
    if synsets:
        syn = synsets[0]
        return (syn.pos_score(), syn.neg_score())

    # 2. SenticNet
    if term in senticnet:
        polarity = senticnet[term][7]
        return (polarity if polarity > 0 else 0, -polarity if polarity < 0 else 0)

    # 3. VADER
    if term in vader_analyzer.lexicon:
        score = vader_analyzer.lexicon[term] / 4.0  # Normalisation entre -1 et 1
        return (score if score > 0 else 0, -score if score < 0 else 0)

    return (0, 0)

# Main function that calculates a polarity score per tweet
def sent_score(fourgrams, trigrams, bigrams, unigrams):
    sent_score = 0.0
    term_count = 0
    used_ngrams = set()
    blocked_terms = set()

    for f in fourgrams:
        if f in senticnet and f not in used_ngrams:
            polarity = senticnet[f][7]
            sent_score += polarity
            term_count += 1
            used_ngrams.add(f)
            blocked_terms.update(get_sub_ngrams(f))

    for t in trigrams:
        if t in senticnet and t not in used_ngrams and t not in blocked_terms:
            polarity = senticnet[t][7]
            sent_score += polarity
            term_count += 1
            used_ngrams.add(t)
            blocked_terms.update(get_sub_ngrams(t))

    for b in bigrams:
        if b in senticnet and b not in used_ngrams and b not in blocked_terms:
            polarity = senticnet[b][7]
            sent_score += polarity
            term_count += 1
            used_ngrams.add(b)
            blocked_terms.update(get_sub_ngrams(b))

    for u in unigrams:
        if u not in used_ngrams and u not in blocked_terms:
            pos, neg = get_polarity(u)
            if pos != 0 or neg != 0:
                sent_score += pos - neg
                term_count += 1
                used_ngrams.add(u)

    return round(sent_score / term_count, 4) if term_count > 0 else 0.0

# Load pre-processed tweets
with open("preprocessed_TSA_ngrams_by_user.pkl", "rb") as f:
    preprocessed_data = pickle.load(f)

scores_by_user = {}

for username in tqdm(preprocessed_data):
    user_tweets = preprocessed_data[username]
    tweet_scores = {}

    for idx, tweet_ngrams in enumerate(user_tweets, 1):
        fourgrams = [t.replace(" ", "_") for t in tweet_ngrams if len(t.split()) == 4]
        trigrams = [t.replace(" ", "_") for t in tweet_ngrams if len(t.split()) == 3]
        bigrams = [t.replace(" ", "_") for t in tweet_ngrams if len(t.split()) == 2]
        unigrams = [t for t in tweet_ngrams if len(t.split()) == 1]

        score = sent_score(fourgrams, trigrams, bigrams, unigrams)
        tweet_scores[str(idx)] = score

    scores_by_user[username] = tweet_scores

with open("sentiment_scores_by_user.pkl", "wb") as f:
    pickle.dump(scores_by_user, f)
