from apify_client import ApifyClient
from dotenv import load_dotenv
import json
import os
import sqlite3
import time

load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN") # Retrieve the API key to use the scraper
client = ApifyClient(APIFY_TOKEN)

db_path = "../data/GossipCop/database.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Retrieve usernames to scrape
cursor.execute("SELECT username FROM users")
rows = cursor.fetchall()
usernames = [row[0] for row in rows]

# Divide usernames to be scraped into batches
def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

for batch in chunked(usernames, 1000):
    try:

        run_input = {"user_names": batch}
        run = client.actor("kaitoeasyapi/premium-twitter-user-scraper-pay-per-result").call(run_input=run_input)
        items = client.dataset(run["defaultDatasetId"]).list_items().items

        if not items:
            print(f"No results for this batch")
            continue

        for user_data in items:

            if "core" not in user_data:
                print(f"User without 'core'")
                continue

            username = user_data['core']['screen_name']

            raw_bio = user_data.get('profile_bio', {}).get('description', '')
            bio = raw_bio.replace("\u00A0", " ").strip() or None

            category = None
            if 'professional' in user_data:
                categories = user_data['professional'].get('category', [])
                if isinstance(categories, list) and categories:
                    category = categories[0].get('name', '').strip()

            full_data = json.dumps(user_data)

            cursor.execute("""
            UPDATE users
            SET bio = ?, professional_category = ?, full_data = ?
            WHERE username = ?
            """, (bio, category, full_data, username))

        start_push = time.time()
        conn.commit()
        end_push = time.time()
        print(f"[Push={end_push - start_push:.2f}s]")  # Indicate when a batch of user data has been added to the database

    except Exception as e:
        print(f"Error for batch {batch} : {e}")

conn.close()
