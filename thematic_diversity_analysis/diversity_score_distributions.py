from DB_connection import DB_connection
import matplotlib.pyplot as plt
import numpy as np
import pickle
from scipy.stats import mannwhitneyu, shapiro, probplot

def load_rao_scores(pkl_path):
    with open(pkl_path, 'rb') as f:
        return pickle.load(f)

def fetch_user_labels(db_connection, usernames):
    placeholders = ','.join(f"'{u}'" for u in usernames)
    query = f"SELECT username, label FROM users WHERE username IN ({placeholders})"
    df_labels = db_connection.select(query)
    return dict(zip(df_labels['username'], df_labels['label']))

def split_scores_by_label(rao_scores, user_labels):
    fake_scores = []
    real_scores = []
    for username, score_info in rao_scores.items():
        label = user_labels.get(username)
        score = score_info.get('rao_score')
        if score is not None:
            if label == 'fake':
                fake_scores.append(score)
            elif label == 'real':
                real_scores.append(score)
    return fake_scores, real_scores

def plot_score_distributions(fake_scores, real_scores):
    plt.hist(fake_scores, bins=20, alpha=0.6, label='Fake', color='red')
    plt.hist(real_scores, bins=20, alpha=0.6, label='Real', color='green')
    plt.xlabel('Rao diversity score')
    plt.ylabel('Number of users')
    plt.title('Distribution of Rao diversity scores by label')
    plt.legend()
    plt.grid(True)
    plt.show()

def print_score_statistics(fake_scores, real_scores):
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

def plot_qq_plots(fake_scores, real_scores):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    probplot(fake_scores, dist="norm", plot=axes[0])
    axes[0].set_title("Q-Q plot - 'Fake'")
    axes[0].grid(True)
    axes[0].set_ylabel("Data quantiles")

    probplot(real_scores, dist="norm", plot=axes[1])
    axes[1].set_title("Q-Q plot - 'Real'")
    axes[1].grid(True)
    axes[1].set_ylabel("Data quantiles")

    plt.tight_layout()
    plt.show()

def test_normality(fake_scores, real_scores):
    print("\nShapiro-Wilk normality test:")
    stat_fake, p_fake = shapiro(fake_scores)
    stat_real, p_real = shapiro(real_scores)

    print(f" - 'Fake' group: W = {stat_fake:.4f}, p-value = {p_fake:.4e}")
    print(f" - 'Real' group: W = {stat_real:.4f}, p-value = {p_real:.4e}")

    normal_fake = p_fake > 0.05
    normal_real = p_real > 0.05

    if normal_fake and normal_real:
        print("Both groups follow a normal distribution (p > 0.05).")
    else:
        print("At least one group does not follow a normal distribution (p <= 0.05).")

    return normal_fake and normal_real

def run_mann_whitney_test(fake_scores, real_scores):
    stat, p_value = mannwhitneyu(fake_scores, real_scores, alternative='less')
    print("\nMann-Whitney U test:")
    print(f" - U statistics: {stat:.4f}")
    print(f" - p-value: {p_value:.4e}")

    if p_value < 0.05:
        print("There is a statistically significant difference between the two groups (p < 0.05).")
    else:
        print("No significant difference detected (p >= 0.05).")


def main(pkl_path='rao_diversity_scores.pkl'):
    db_connection = DB_connection("GOSSIPCOP")
    rao_scores = load_rao_scores(pkl_path)
    usernames = list(rao_scores.keys())
    user_labels = fetch_user_labels(db_connection, usernames)
    fake_scores, real_scores = split_scores_by_label(rao_scores, user_labels)
    print_score_statistics(fake_scores, real_scores)
    plot_score_distributions(fake_scores, real_scores)
    test_normality(fake_scores, real_scores)
    plot_qq_plots(fake_scores, real_scores)
    run_mann_whitney_test(fake_scores, real_scores)
    db_connection.close()

if __name__ == '__main__':
    main()
