from apify_client import ApifyClient
from dotenv import load_dotenv
import json
import os
import sqlite3
import time

# Divide tweets to be scraped into batches
def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def scrape_initial_tweets(label_type):
    """
    Scrapes initial tweets from a list of tweet ids stored in a database table using an Apify actor.
    The initial tweets collected are in turn saved in another database table.

    Args:
     -  label_type (str): Either "fake" or "real" (relating to the type of news you want to process),
        used to select source and destination tables.
    """

    if label_type not in ["fake", "real"]:
        print("label_type must be 'fake' or 'real'")
        return

    load_dotenv()
    APIFY_TOKEN = os.getenv("APIFY_TOKEN")  # Retrieve the API key to use the scraper
    client = ApifyClient(APIFY_TOKEN)

    db_path = "../data/GossipCop/database.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    destination_table = f"collected_tweets_{label_type}"
    source_table = f"selected_tweet_ids_{label_type}"

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {destination_table} (
        tweet_id TEXT PRIMARY KEY,
        username TEXT,
        text TEXT,
        full_data TEXT
    )
    """)

    # Retrieve tweet ids to scrape
    cursor.execute(f"SELECT tweet_id FROM {source_table}")
    rows = cursor.fetchall()
    tweet_ids = [row[0] for row in rows]

    for batch in chunked(tweet_ids, 1000):
        try:
            run_input = {"tweetIds": batch}
            run = client.actor("coder_luffy/free-tweet-scraper").call(run_input=run_input)
            items = client.dataset(run["defaultDatasetId"]).list_items().items

            if not items:
                print("No results for this batch")
                continue

            for tweet in items:
                tweet_id = tweet.get('tweet_id')

                if tweet.get("tombstone", False):
                    continue
                if tweet.get("error", False):
                    continue

                username = tweet['user']['screen_name']
                text = tweet['text'].replace("\u00A0", " ").strip()  # Clean tweet text
                full_data = json.dumps(tweet)

                cursor.execute(f"""
                INSERT OR IGNORE INTO {destination_table} (tweet_id, username, text, full_data)
                VALUES (?, ?, ?, ?)
                """, (tweet_id, username, text, full_data))

            start_push = time.time()
            conn.commit()
            end_push = time.time()
            print(f"[Push_time={end_push - start_push:.2f}s]")  # Indicate when a batch of tweets has been added to the database

        except Exception as e:
            print(f"Error for batch {batch}: {e}")

    conn.close()


# Use of the function
#scrape_initial_tweets("fake")
#scrape_initial_tweets("real")
