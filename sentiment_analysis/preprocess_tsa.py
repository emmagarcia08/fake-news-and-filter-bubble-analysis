import contractions
from DB_connection import DB_connection
import emoji
import nltk
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer
import pickle
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Download NLTK resources
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('punkt')

# Initialise the lemmatizer and the VADER lexicon
lemmatizer = WordNetLemmatizer()
vader_analyzer = SentimentIntensityAnalyzer()
vader_lexicon = set(vader_analyzer.lexicon.keys())

# Function to remove repeats (elongations)
def reduce_elongation(word):
    if wordnet.synsets(word):
        return word

    match = re.findall(r'(.)\1{2,}', word)
    if not match:
        return word

    word_mod = word

    while True:
        if wordnet.synsets(word_mod):
            return word_mod
        new_word = re.sub(r'(.)\1{2,}', lambda m: m.group(0)[:-1], word_mod)
        if new_word == word_mod:
            return new_word
        word_mod = new_word

# Function for managing negations with antonyms
def replace_negations(tokens):
    i = 0
    new_tokens = []
    while i < len(tokens):
        if tokens[i] in ['not', 'never'] and i+1 < len(tokens):
            antonyms = []
            for syn in wordnet.synsets(tokens[i+1]):
                for lemma in syn.lemmas():
                    if lemma.antonyms():
                        antonyms.append(lemma.antonyms()[0].name())
            if antonyms:
                new_tokens.append(antonyms[0])
                i += 2
                continue
        new_tokens.append(tokens[i])
        i += 1
    return new_tokens

# "Intelligent" lemmatisation function
def smart_lemmatize(token):
    if token in vader_lexicon:
        return token
    elif emoji.is_emoji(token):
        return token
    else :
        return lemmatizer.lemmatize(token) if token.isalpha() else None

# Function for generating uni-grams, bi-grams, tri-grams and four-grams
def generate_ngrams(tokens, max_n=4):
    ngrams = []
    for n in range(1, max_n + 1):
        ngrams.extend([" ".join(tokens[i:i+n]) for i in range(len(tokens)-n+1)])
    return ngrams

# Main pre-treatment function
def preprocess_tweet(text):
    # 1. Delete URLs, hashtags, mentions
    text = re.sub(r"http\S+|www.\S+|@\w+|#\w+", "", text)

    # 2. Expansion of contractions
    text = contractions.fix(text)

    # 3. Twitter-aware tokenisation
    tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True, reduce_len=True)
    tokens = tokenizer.tokenize(text)

    # 4. Elongation reduction
    tokens = [reduce_elongation(t) for t in tokens]

    # 5. Lemmatisation
    tokens = [smart_lemmatize(t) for t in tokens]
    tokens = [t for t in tokens if t]

    # 6. Replacing negations
    tokens = replace_negations(tokens)

    return generate_ngrams(tokens)


# DB connection
db_connection = DB_connection("GOSSIPCOP")
df = db_connection.select("SELECT username, text_translation FROM filtered_user_timelines").fillna("")

# Pre-processing by user
usernames = df['username'].unique()
preprocessed_by_user = {}

for idx, username in enumerate(usernames, 1):
    user_df = df[df['username'] == username]
    processed_tweets = []

    for text in user_df['text_translation']:
        if text.strip():
            try:
                ngrams = preprocess_tweet(text)
                if ngrams:
                    processed_tweets.append(ngrams)
            except Exception as e:
                print(f"Error with tweet from {username} : {text[:30]}... → {e}")

    if processed_tweets:
        preprocessed_by_user[username] = processed_tweets

with open("preprocessed_TSA_ngrams_by_user.pkl", "wb") as f:
    pickle.dump(preprocessed_by_user, f)
