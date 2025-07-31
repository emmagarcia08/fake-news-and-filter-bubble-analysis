from apify_client import ApifyClient
from DB_connection import DB_connection
from dotenv import load_dotenv
import json
import os
import time

start_full = time.time()

load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")  # Retrieve the API key to use the scraper
client = ApifyClient(APIFY_TOKEN)

db_connection = DB_connection("GOSSIPCOP")


def get_users_to_exclude():
    """
    Exclude users based on certain criteria:
    1. All users who have a professional category.
    2. All users whose X account no longer exists (which means that their "full_data" column is empty).
    3. Users who do not have a professional category and whose bio contains specific keywords.
    4. Among the remaining users, exclude those with less than 100 tweets.
    """

    to_exclude = set()

    # 1. Exclude all users with a professional category
    rows = db_connection.select("SELECT username FROM users WHERE professional_category IS NOT NULL")
    to_exclude.update(rows["username"].tolist())

    # 2. Exclude all users whose X account no longer exists
    rows = db_connection.select("SELECT username FROM users WHERE full_data IS NULL")
    to_exclude.update(rows["username"].tolist())

    # 3. Exclude users with no professional category AND matching bio_translation keywords
    keyword_query = """
        SELECT username 
        FROM users
        WHERE professional_category IS NULL
          AND (
            bio_translation LIKE '%news%' OR
            bio_translation LIKE '%media%' OR
            bio_translation LIKE '%info%' OR
            bio_translation LIKE '%entertain%' OR
            bio_translation LIKE '%station%' OR
            bio_translation LIKE '%radio%' OR
            bio_translation LIKE '%celebrity%' OR
            bio_translation LIKE '%celebrities%' OR
            bio_translation LIKE '%fan%'
          )
    """
    rows = db_connection.select(keyword_query)
    to_exclude.update(rows["username"].tolist())

    # 4. From remaining users, exclude those with less than 100 tweets
    remaining_query = """
        SELECT username, full_data 
        FROM users 
        WHERE full_data IS NOT NULL
    """
    rows = db_connection.select(remaining_query)
    for _, row in rows.iterrows():
        username = row["username"]
        if username in to_exclude:
            continue  # Skip already excluded users

        try:
            data = json.loads(row["full_data"])
            tweet_count = data.get("tweet_counts", {}).get("tweets", 0)
            if tweet_count < 100:
                to_exclude.add(username)
        except (json.JSONDecodeError, TypeError):
            continue  # Skip if JSON is malformed

    return to_exclude


def extract_text_and_type(tweet):
    """
    Returns:
        - user_text: the main text from the user (retweet, quoted or normal)
        - quoted_text: text from the quoted tweet if available, else None
        - tweet_type: one of 'tweet', 'retweet' or 'quoted_tweet', depending on the format of the tweet
    """

    def clean(txt):
        return txt.replace("\u00A0", " ").strip() if isinstance(txt, str) else ""  # Clean tweet text

    raw_text = clean(tweet.get("text", ""))
    quoted_text = None

    # 1. Retweet
    if raw_text.startswith("RT @") and "retweeted_tweet" in tweet:
        try:
            retweeted_text = clean(tweet["retweeted_tweet"].get("text"))
            return retweeted_text if retweeted_text else raw_text, None, "retweet"
        except Exception:
            return raw_text, None, "retweet"

    # 2. Quoted tweet
    if "quoted" in tweet and isinstance(tweet["quoted"], dict):
        try:
            quoted_text = clean(tweet["quoted"].get("text"))
            return raw_text, quoted_text if quoted_text else None, "quoted_tweet"
        except Exception:
            return raw_text, None, "quoted_tweet"

    # 3. Normal tweet
    return raw_text, None, "tweet"


def scrape_user_timelines():
    """
    Scrapes the latest tweets from user timelines using an Apify actor.
    The users to scrape are retrieved from the 'users' table, excluding those returned by get_users_to_exclude().
    The tweets are stored in a 'user_timelines' table with columns: username, label, tweet_id, text, quoted_text,
    tweet_type, full_data.
    """

    db_connection.execute(f"""
    CREATE TABLE IF NOT EXISTS user_timelines (
        username TEXT,
        label TEXT,
        tweet_id TEXT,
        text TEXT,
        quoted_text TEXT,
        tweet_type TEXT,
        full_data TEXT,
        PRIMARY KEY (username, tweet_id)
    )
    """)

    users_to_exclude = get_users_to_exclude()

    # Retrieve users to scrape
    rows = db_connection.select("SELECT username, label FROM users")
    fake_users = []
    real_users = []

    for _, row in rows.iterrows():
        username = row["username"]
        label = row["label"]
        if username in users_to_exclude:
            continue
        if label == "fake":
            fake_users.append((username, label))
        elif label == "real":
            real_users.append((username, label))

    # Alternate fake and real users for balanced scraping
    min_len = min(len(fake_users), len(real_users))
    interleaved_users = [pair for i in range(min_len) for pair in (fake_users[i], real_users[i])]
    interleaved_users += fake_users[min_len:] + real_users[min_len:]
    usernames = interleaved_users

    print(f"Number of users to scrape: {len(usernames)}")

    max_users = 1000
    for idx, (username, label) in enumerate(usernames):
        if idx >= max_users:
            print(f"Scraping stopped after reaching the limit of {max_users} users.")
            break

        try:
            run_input = {
                "username": username,
                "max_posts": 120
            }
            run = client.actor("danek/twitter-timeline-ppr").call(run_input=run_input)
            items = client.dataset(run["defaultDatasetId"]).list_items().items

            if not items:
                print(f"No tweet found for {username}")
                continue

            for tweet in items:
                tweet_id = tweet.get("tweet_id")
                user_text, quoted_text, tweet_type = extract_text_and_type(tweet)
                full_data = json.dumps(tweet)

                db_connection.execute(f"""
                INSERT OR IGNORE INTO user_timelines (username, label, tweet_id, text, quoted_text, tweet_type, full_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, params=(username, label, tweet_id, user_text, quoted_text, tweet_type, full_data), commit=False)

            db_connection.commit()

            print(f"{len(items)} tweets saved for {username}")

        except Exception as e:
            print(f"Error for user @{username}: {e}")
            continue


# Use of the function
scrape_user_timelines()

end_full = time.time()
print(f"[Total_time={end_full - start_full:.2f}s]")
