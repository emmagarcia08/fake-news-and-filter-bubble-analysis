from DB_connection import DB_connection
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

def fetch_retweet_proportions(db_connection):
    query = """
    SELECT username, label, tweet_type
    FROM filtered_user_timelines
    """
    df = db_connection.select(query)

    def compute_proportions(group):
        total = len(group)
        prop_other = (group['tweet_type'].isin(['retweet', 'quoted_tweet'])).sum() / total
        return pd.Series({'prop_other': prop_other, 'label': group['label'].iloc[0]})

    proportions = df.groupby('username').apply(compute_proportions).reset_index()
    return proportions

def split_proportions_by_label(proportions):
    fake_scores = proportions[proportions['label'] == 'fake']['prop_other'].values
    real_scores = proportions[proportions['label'] == 'real']['prop_other'].values
    return fake_scores, real_scores

def plot_proportion_distributions(fake_scores, real_scores):
    plt.hist(fake_scores, bins=20, alpha=0.6, label='Fake', color='red')
    plt.hist(real_scores, bins=20, alpha=0.6, label='Real', color='green')
    plt.xlabel('Proportion of retweets/quoted tweets')
    plt.ylabel('Number of users')
    plt.title('Distribution of proportions of retweets/quoted tweets by label')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def print_proportion_statistics(fake_scores, real_scores):
    def describe(scores, label):
        print(f"\nGroup statistics '{label}':")
        print(f" - Number of users: {len(scores)}")
        print(f" - Mean: {np.mean(scores):.4f}")
        print(f" - Median: {np.median(scores):.4f}")
        print(f" - Standard deviation: {np.std(scores):.4f}")
        print(f" - Min: {np.min(scores):.4f}")
        print(f" - Max: {np.max(scores):.4f}")

    describe(fake_scores, 'fake')
    describe(real_scores, 'real')

def run_mann_whitney_test(fake_scores, real_scores):
    stat, p_value = mannwhitneyu(fake_scores, real_scores, alternative='greater')
    print("\nMann-Whitney U test:")
    print(f" - U statistics : {stat:.4f}")
    print(f" - p-value : {p_value:.4e}")

    if p_value < 0.05:
        print("There is a statistically significant difference between the two groups (p < 0.05).")
    else:
        print("No significant difference detected (p >= 0.05).")

def main():
    db_connection = DB_connection("GOSSIPCOP")
    proportions = fetch_retweet_proportions(db_connection)
    fake_scores, real_scores = split_proportions_by_label(proportions)
    print_proportion_statistics(fake_scores, real_scores)
    run_mann_whitney_test(fake_scores, real_scores)
    plot_proportion_distributions(fake_scores, real_scores)
    db_connection.close()

if __name__ == '__main__':
    main()
