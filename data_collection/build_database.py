import data_collection.load_data_into_database
from DB_connection import DB_connection
from data_collection.articles_scraper import add_news_text_in_db
from data_collection.articles_scraper import compute_similarity_score_between_article_text_and_title


Load_data = True # Set to True if you want to load the data into the database

if Load_data:
    print("Loading data into the database")
    data_collection.load_data_into_database.load_table_into_db()
    print("Data loaded into the database")

    db_connection_gossipcop = DB_connection("GOSSIPCOP")

    print("Adding news text in the database")
    add_news_text_in_db(db_connection_gossipcop)
    print("News text added in the database")

    print("Computing similarity score between article text and title")
    compute_similarity_score_between_article_text_and_title(db_connection_gossipcop)
    print("Similarity score computed and saved in the database")
