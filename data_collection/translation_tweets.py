from deep_translator import GoogleTranslator
from DB_connection import DB_connection

# === CONFIGURATION ===
BATCH_SIZE = 100  # Number of tweets per commit

# === TRANSLATION FUNCTION ===
def preprocess_tweet(text):
    if not text or not text.strip():
        return None
    try:
        translated = GoogleTranslator(source='auto', target='en').translate(text)
        return translated
    except Exception as e:
        print(f"Translation failed for: {text}\nError: {e}")
        return None

# === MAIN FUNCTION ===
def translate_user_timelines(db_connection):
    columns_df = db_connection.select("PRAGMA table_info(user_timelines);")
    columns = columns_df["username"].tolist()

    if "text_translation" not in columns:
        db_connection.connection.execute("ALTER TABLE user_timelines ADD COLUMN text_translation TEXT;")

    if "quoted_translation" not in columns:
        db_connection.connection.execute("ALTER TABLE user_timelines ADD COLUMN quoted_translation TEXT;")

    db_connection.connection.commit()

    tweets_df = db_connection.select("SELECT tweet_id, text, quoted_text FROM user_timelines;")

    for i, row in tweets_df.iterrows():
        tweet_id = row["tweet_id"]
        text = row["text"]
        quoted = row["quoted_text"]

        translation_text = preprocess_tweet(text)
        translation_quoted = preprocess_tweet(quoted)

        db_connection.connection.execute("""
            UPDATE user_timelines
            SET text_translation = ?, quoted_translation = ?
            WHERE tweet_id = ?;
        """, (translation_text, translation_quoted, tweet_id))

        if (i + 1) % BATCH_SIZE == 0:
            db_connection.connection.commit()

    db_connection.connection.commit()

if __name__ == "__main__":
    db_connection = DB_connection("GOSSIPCOP")
    translate_user_timelines(db_connection)
    db_connection.close()
