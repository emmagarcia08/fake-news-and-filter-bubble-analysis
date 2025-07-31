from DB_connection import DB_connection
import pandas as pd

# === FILTER FUNCTION ===
def create_filtered_user_timelines(db_connection):
    query = """
        SELECT * FROM user_timelines
        WHERE tweet_id IS NOT NULL
          AND text IS NOT NULL AND TRIM(text) != ''
          AND (
              (text_translation IS NOT NULL AND TRIM(text_translation) != '')
              OR
              (quoted_translation IS NOT NULL AND TRIM(quoted_translation) != '')
          )
          AND username IN (
              SELECT username
              FROM user_timelines
              WHERE tweet_id IS NOT NULL
                AND text IS NOT NULL AND TRIM(text) != ''
                AND (
                    (text_translation IS NOT NULL AND TRIM(text_translation) != '')
                    OR
                    (quoted_translation IS NOT NULL AND TRIM(quoted_translation) != '')
                )
              GROUP BY username
              HAVING COUNT(*) >= 100
          )
    """
    df = db_connection.select(query)

    fake_users = df[df['label'] == 'fake'].groupby('username')
    real_users = df[df['label'] == 'real'].groupby('username')

    result_rows = []
    fake_iter = iter(fake_users)
    real_iter = iter(real_users)

    while True:
        added = False
        if fake_iter is not None:
            try:
                _, fake_df = next(fake_iter)
                result_rows.append(fake_df)
                added = True
            except StopIteration:
                fake_iter = None

        if real_iter is not None:
            try:
                _, real_df = next(real_iter)
                result_rows.append(real_df)
                added = True
            except StopIteration:
                real_iter = None

        if not added:
            break

    final_df = pd.concat(result_rows, ignore_index=True)
    db_connection.save_df(final_df, "filtered_user_timelines")

if __name__ == "__main__":
    db_connection = DB_connection("GOSSIPCOP")
    create_filtered_user_timelines(db_connection)
    db_connection.close()
