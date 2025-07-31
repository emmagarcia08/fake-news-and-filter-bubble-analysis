from deep_translator import GoogleTranslator
from DB_connection import DB_connection

# === CONFIGURATION ===
BATCH_SIZE = 100  # Number of users per commit

# === TRANSLATION FUNCTION ===
def preprocess_bio(text):
    if not text:
        return None
    try:
        translated = GoogleTranslator(source='auto', target='en').translate(text)
        return translated
    except Exception as e:
        print(f"Translation failed for: {text}\nError: {e}")
        return None

# === MAIN FUNCTION ===
def translate_user_bios(db_connection):
    columns_df = db_connection.select("PRAGMA table_info(users);")
    columns = columns_df["username"].tolist()

    if "bio_translation" not in columns:
        db_connection.connection.execute("ALTER TABLE users ADD COLUMN bio_translation TEXT;")
        db_connection.connection.commit()

    users_df = db_connection.select("SELECT username, bio FROM users;")

    for i, row in users_df.iterrows():
        username = row["username"]
        bio = row["bio"]

        translation = preprocess_bio(bio)

        db_connection.connection.execute("""
            UPDATE users
            SET bio_translation = ?
            WHERE username = ?;
        """, (translation, username))

        if (i + 1) % BATCH_SIZE == 0:
            db_connection.connection.commit()

    db_connection.connection.commit()

if __name__ == "__main__":
    db_connection = DB_connection("GOSSIPCOP")
    translate_user_bios(db_connection)
    db_connection.close()
