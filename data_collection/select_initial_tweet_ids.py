import sqlite3

def select_initial_tweet_ids(label_type):
    """
    Selects and stores unique tweet ids from the "news" table based on predefined conditions.
    The selected tweet ids are saved into a separate table for later use.

    Args:
     -  label_type (str): Either "fake" or "real" (relating to the type of news you want to process),
        used to filter rows and name the destination table.
    """

    if label_type not in ["fake", "real"]:
        print("label_type must be 'fake' or 'real'")
        return

    db_path = "../data/GossipCop/database.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Define the conditions to be met by tweet ids
    query = f"""
    SELECT tweet_ids 
    FROM news
    WHERE 
        COALESCE(news_url, '') != '' AND
        COALESCE(title, '') != '' AND
        COALESCE(tweet_ids, '') != '' AND
        COALESCE(label, '') != '' AND
        COALESCE(news_text, '') != '' AND
        COALESCE(title_and_text_similarity, '') != '' AND
        title_and_text_similarity >= 0.4 AND
        label = '{label_type}';
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    tweet_ids = set()
    for row in rows:
        tweet_ids.update(row[0].split("\t"))

    destination_table = f"selected_tweet_ids_{label_type}"
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {destination_table} (
        tweet_id TEXT PRIMARY KEY
    )
    """)

    for tweet_id in tweet_ids:
        try:
            cursor.execute(f"INSERT OR IGNORE INTO {destination_table} (tweet_id) VALUES (?)", (tweet_id,))
        except Exception as e:
            print(f"Error when inserting ID {tweet_id}: {e}")

    conn.commit()
    conn.close()

    print(f"Inserted {len(tweet_ids)} unique tweet IDs into the '{destination_table}' table.")


# Use of the function
#select_initial_tweet_ids("fake")
#select_initial_tweet_ids("real")
