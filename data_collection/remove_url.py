import re
from DB_connection import DB_connection

url_pattern = re.compile(r'https?://\S+')

def clean_text(value):
    if value is None:
        return None
    value = value.lower()
    cleaned = url_pattern.sub('', value).strip()
    return cleaned if cleaned else None

def clean_user_timelines(db_connection, batch_size=1000):
    offset = 0
    rows_processed = 0

    while True:
        query = f"""
            SELECT rowid, text_translation, quoted_translation
            FROM user_timelines
            LIMIT {batch_size} OFFSET {offset}
        """
        rows = db_connection.select(query)

        if rows.empty:
            break

        for index, row in rows.iterrows():
            rowid = row["rowid"]
            new_text = clean_text(row["text_translation"])
            new_quoted = clean_text(row["quoted_translation"])

            db_connection.connection.execute("""
                UPDATE user_timelines
                SET text_translation = ?, quoted_translation = ?
                WHERE rowid = ?
            """, (new_text, new_quoted, rowid))

        db_connection.connection.commit()
        rows_processed += len(rows)
        offset += batch_size
        print(f"Processed {rows_processed} rows...")

if __name__ == "__main__":
    db_connection = DB_connection("GOSSIPCOP")
    clean_user_timelines(db_connection)
    db_connection.close()
    print("Cleaning completed.")
