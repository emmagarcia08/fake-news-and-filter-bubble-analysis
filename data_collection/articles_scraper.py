import sys
import os
sys.path.insert(0, os.path.abspath('../..'))

from tqdm import tqdm
from newspaper import Article
from sentence_transformers import SentenceTransformer, util

from DB_connection import DB_connection

def format_url(url):
    if 'https://' in url or 'http://' in url:
        return url
    else:
        return 'https://' + url

def get_article_text(url):
    url = format_url(url)
    article = Article(url)
    article.download()
    article.parse()
    return article.text


def add_news_text_in_db(db_connection):
    news_df = db_connection.select('SELECT * FROM news')
    urls = list(news_df["news_url"])

    texts = []
    for url in tqdm(urls):
        try:
            text = get_article_text(url)
            texts.append(text)
        except:
            texts.append(None)
            continue
    news_df["news_text"] = texts
    db_connection.save_df(news_df, 'news')


def compute_similarity_score_between_article_text_and_title(db_connection):
    # Load the sentence transformer model
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Fetch data from the database
    news_df = db_connection.select('SELECT * FROM news')
    titles = list(news_df["title"])
    texts = list(news_df["news_text"])

    similarity_score_column = []

    for title, text in tqdm(zip(titles, texts)):
        if text is None or len(text) == 0 or len(title) == 0 or title is None:
            similarity_score = 0
        else:
            # Encode the title and text using SBERT embeddings
            title_embedding = model.encode(title, convert_to_tensor=True)
            text_embedding = model.encode(text, convert_to_tensor=True)

            # Calculate cosine similarity
            similarity_score = util.pytorch_cos_sim(title_embedding, text_embedding).item()
        similarity_score_column.append(similarity_score)
    news_df["title_and_text_similarity"] = similarity_score_column
    db_connection.save_df(news_df,"news")





