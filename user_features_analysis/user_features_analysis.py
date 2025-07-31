from datetime import datetime
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import shapiro, probplot, ttest_ind, mannwhitneyu, skew
import seaborn as sns

from DB_connection import DB_connection

sns.set(style="whitegrid")

def get_valid_usernames(db_connection):
    query = "SELECT DISTINCT username FROM filtered_user_timelines"
    df = db_connection.select(query)
    return set(df["username"])

def load_user_data(db_connection, valid_usernames):
    query = "SELECT username, label, full_data FROM users"
    rows = db_connection.select(query).values.tolist()

    users_data = []
    skipped = 0

    for username, label, full_data in rows:
        if username not in valid_usernames:
            continue
        try:
            if full_data is None:
                raise ValueError("Missing full_data")
            data = json.loads(full_data)

            verified = data.get("verification", {}).get("is_blue_verified", False)
            created_at_str = data.get("core", {}).get("created_at", None)
            created_at = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y") if created_at_str else None
            days_since_registration = (datetime.now(datetime.utcnow().astimezone().tzinfo) - created_at).days if created_at else None

            status_count = data.get("tweet_counts", {}).get("tweets", None)
            favor_count = data.get("action_counts", {}).get("favorites_count", None)
            follower_count = data.get("relationship_counts", {}).get("followers", None)
            following_count = data.get("relationship_counts", {}).get("following", None)
            tff_ratio = (follower_count + 1) / (following_count + 1) if follower_count is not None and following_count is not None else None

            users_data.append({
                "username": username,
                "label": label,
                "verified": verified,
                "register_days_ago": days_since_registration,
                "status_count": status_count,
                "favor_count": favor_count,
                "follower_count": follower_count,
                "following_count": following_count,
                "tff_ratio": tff_ratio
            })

        except Exception as e:
            print(f"Skipping {username} due to error: {e}")
            skipped += 1

    print(f"Total processed users: {len(users_data)}")
    print(f"Total skipped users: {skipped}")
    return pd.DataFrame(users_data)

def run_tweet_related_analysis(db_connection):
    df_tweets = db_connection.select("SELECT username, label, full_data FROM filtered_user_timelines")

    tweeting_times = {}
    reply_counts = {}
    retweet_counts = {}

    for _, row in df_tweets.iterrows():
        username = row["username"]
        label = row["label"]
        try:
            tweet_data = json.loads(row["full_data"])
            created_at_str = tweet_data.get("created_at", None)
            reply = tweet_data.get("replies", None)
            retweet = tweet_data.get("retweets", None)

            if username not in tweeting_times:
                tweeting_times[username] = {"dates": [], "label": label}
                reply_counts[username] = []
                retweet_counts[username] = []

            if created_at_str:
                created_at = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
                tweeting_times[username]["dates"].append(created_at)

            if reply is not None:
                reply_counts[username].append(reply)
            if retweet is not None:
                retweet_counts[username].append(retweet)

        except Exception:
            continue

    tweet_metrics = []
    for username in tweeting_times:
        dates = tweeting_times[username]["dates"]
        label = tweeting_times[username]["label"]
        tweeting_range = (max(dates) - min(dates)).days if len(dates) >= 2 else None
        mean_replies = np.mean(reply_counts[username]) if reply_counts[username] else None
        mean_retweets = np.mean(retweet_counts[username]) if retweet_counts[username] else None

        tweet_metrics.append({
            "username": username,
            "label": label,
            "tweeting_range_days": tweeting_range,
            "mean_replies": mean_replies,
            "mean_retweets": mean_retweets
        })

    return pd.DataFrame(tweet_metrics)

def t_test_and_print(group1, group2, label):
    g1, g2 = group1.dropna(), group2.dropna()
    t_stat, p_val = ttest_ind(g1, g2, equal_var=False)
    print(f"\n{label}:")
    print(f"U(f) mean: {g1.mean():.2f}, std: {g1.std():.2f}, N={len(g1)}")
    print(f"U(r) mean: {g2.mean():.2f}, std: {g2.std():.2f}, N={len(g2)}")
    print(f"T-test: t = {t_stat:.3f}, p = {p_val:.4f}")
    print("\u2192 Significant difference according to the t-test" if p_val < 0.05 else "\u2192 No significant difference according to the t-test")
    return p_val

def mann_whitney_and_print(group1, group2, label):
    g1, g2 = group1.dropna(), group2.dropna()
    stat, p_val = mannwhitneyu(g1, g2, alternative='two-sided')
    print(f"\n{label} (Mann-Whitney U test):")
    print(f"U(f) median: {np.median(g1):.2f}, N={len(g1)}")
    print(f"U(r) median: {np.median(g2):.2f}, N={len(g2)}")
    print(f"U = {stat:.3f}, p = {p_val:.4e}")
    print("→ Significant difference according to the Mann-Whitney U test" if p_val < 0.05 else "→ No significant difference according to the Mann-Whitney U test")
    return p_val

def plot_qq_plots(fake_scores, real_scores, feature_name=""):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    probplot(fake_scores.dropna(), dist="norm", plot=axes[0])
    axes[0].set_title(f"Q-Q plot for 'Fake' of {feature_name}")
    axes[0].grid(True)

    probplot(real_scores.dropna(), dist="norm", plot=axes[1])
    axes[1].set_title(f"Q-Q plot for 'Real' of {feature_name}")
    axes[1].grid(True)

    plt.tight_layout()
    plt.show()

def test_normality(fake_scores, real_scores, feature_name=""):
    print(f"\nShapiro-Wilk normality test for {feature_name}:")
    stat_fake, p_fake = shapiro(fake_scores.dropna())
    stat_real, p_real = shapiro(real_scores.dropna())

    print(f" - 'Fake' group: W = {stat_fake:.4f}, p-value = {p_fake:.4e}")
    print(f" - 'Real' group: W = {stat_real:.4f}, p-value = {p_real:.4e}")

    normal_fake = p_fake > 0.05
    normal_real = p_real > 0.05

    if normal_fake and normal_real:
        print("→ Both groups appear normally distributed (p > 0.05).")
    else:
        print("→ At least one group does not appear normally distributed (p ≤ 0.05).")

    return normal_fake and normal_real

def plot_verified_by_label(df):
    # Step 1: Count users by (label, verified)
    counts = df.groupby(['label', 'verified']).size().unstack(fill_value=0)

    # Step 2: Normalize within each label group
    percentages = counts.div(counts.sum(axis=1), axis=0) * 100

    # Step 3: Reshape for plotting
    percentages = percentages.reset_index().melt(id_vars='label',
                                                 value_vars=[False, True],
                                                 var_name='verified',
                                                 value_name='percentage')

    # Step 4: Label formatting
    verification_map = {False: 'Unverified', True: 'Verified'}
    percentages['verified'] = percentages['verified'].map(verification_map)
    label_order = ['fake', 'real']
    percentages['label'] = pd.Categorical(percentages['label'], categories=label_order, ordered=True)

    # Step 5: Plot
    plt.figure(figsize=(6, 4))
    sns.barplot(data=percentages, x='label', y='percentage', hue='verified',
                palette={"Unverified": "#3b4cc0", "Verified": "#f5e663"})

    plt.title("Percentage of verified users by label")
    plt.xlabel("User label")
    plt.ylabel("Percentage of users")
    plt.legend(title="Verification")
    plt.tight_layout()
    plt.show()

def plot_verified_by_label_percentage(df):
    # Step 1: Count users by (label, verified)
    counts = df.groupby(['label', 'verified']).size().unstack(fill_value=0)

    # Step 2: Normalize within each label group
    percentages = counts.div(counts.sum(axis=1), axis=0) * 100

    # Step 3: Reshape for plotting
    percentages = percentages.reset_index().melt(id_vars='label',
                                                 value_vars=[False, True],
                                                 var_name='verified',
                                                 value_name='percentage')

    # Step 4: Label formatting
    verification_map = {False: 'Unverified', True: 'Verified'}
    percentages['verified'] = percentages['verified'].map(verification_map)
    label_order = ['fake', 'real']
    percentages['label'] = pd.Categorical(percentages['label'], categories=label_order, ordered=True)

    # Step 5: Plot
    plt.figure(figsize=(6, 4))
    sns.barplot(data=percentages, x='label', y='percentage', hue='verified',
                palette={"Unverified": "#3b4cc0", "Verified": "#f5e663"})

    plt.title("Percentage of verified users by label")
    plt.xlabel("User label")
    plt.ylabel("Percentage of users")
    plt.legend(title="Verification")
    plt.tight_layout()
    plt.show()

def plot_log_rank(data1, data2, label1, label2, title, xlabel, p_val):
    plt.figure(figsize=(6, 4))
    sorted_1 = np.sort(data1)
    sorted_2 = np.sort(data2)
    rank_1 = np.arange(1, len(sorted_1) + 1)[::-1]
    rank_2 = np.arange(1, len(sorted_2) + 1)[::-1]
    plt.plot(sorted_1, rank_1, label=label1)
    plt.plot(sorted_2, rank_2, label=label2)
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel(xlabel)
    plt.ylabel("Number of users ≥ x")
    plt.title(title)
    plt.legend()
    plt.text(0.05, 0.05, f"p = {p_val:.2e}", transform=plt.gca().transAxes,
             fontsize=10, bbox=dict(facecolor='white', edgecolor='purple'))
    plt.tight_layout()
    plt.show()

def boxplot_feature(df, feature, ylabel, title, p_val=None, log=False):
    plt.figure(figsize=(6, 4))
    sns.boxplot(x="label", y=feature, data=df, showfliers=False)
    if log:
        plt.yscale("log")
    plt.title(title)
    plt.xlabel("User type")
    plt.ylabel(ylabel)
    if p_val is not None:
        plt.text(0.05, 0.95, f"p = {p_val:.2e}", transform=plt.gca().transAxes,
                 fontsize=10, bbox=dict(facecolor='white', edgecolor='purple'))
    plt.tight_layout()
    plt.show()

def plot_tff_histogram(uf_tff, ur_tff):
    bins = [0, 0.5, 1, 1.5, 3, 5, 10, 20, 50, 100, 250, 500, 1000, 5000]
    plt.figure(figsize=(8, 4))
    plt.hist(uf_tff, bins=bins, alpha=0.7, label="Fake", color='blue', edgecolor='black')
    plt.hist(ur_tff, bins=bins, alpha=0.7, label="Real", color='orange', edgecolor='black')
    plt.xscale('log')
    plt.xlabel("TFF ratio")
    plt.ylabel("Number of users")
    plt.title("TFF ratio comparison")
    plt.legend()
    plt.text(0.05, 0.95,
             f"Mean TFF (Fake): {uf_tff.mean():.2f}\nMean TFF (Real): {ur_tff.mean():.2f}",
             transform=plt.gca().transAxes,
             fontsize=10, bbox=dict(facecolor='white', edgecolor='purple'))
    plt.tight_layout()
    plt.show()

def apply_stat_test_and_plot(group1, group2, label, feature_name, df=None, plot_type="box", log=False, title=None):
    # Q-Q Plots and normality test
    plot_qq_plots(group1, group2, feature_name)
    normal = test_normality(group1, group2, feature_name)

    # Test selection
    if normal:
        p_val = t_test_and_print(group1, group2, label)
    else:
        p_val = mann_whitney_and_print(group1, group2, label)

    # Plotting
    if plot_type == "box" and df is not None:
        plot_title = title if title else f"Box plots of {label}"
        boxplot_feature(df, feature_name, label, plot_title, p_val, log)
    elif plot_type == "log_rank":
        plot_title = title if title else f"Log-log cumulative rank curves of {label}"
        plot_log_rank(group1.dropna(), group2.dropna(), "Fake", "Real", plot_title, feature_name, p_val)

    return p_val

def should_use_log_scale(series, threshold=1.0, feature_name=""):
    clean_series = series.dropna()
    if (clean_series <= 0).any():
        print(f"[LOG SCALE] '{feature_name}' contient des valeurs ≤ 0 → log impossible.")
        return False

    s = skew(clean_series)
    use_log = abs(s) > threshold
    print(f"[LOG SCALE] Analyse de '{feature_name}' → skew = {s:.2f} → log_scale = {use_log}")
    return use_log


def analyze_user_data(df):
    uf = df[df["label"] == "fake"]
    ur = df[df["label"] == "real"]

    print("\nVerified user counts:")
    print(df.groupby(['label', 'verified']).size().unstack(fill_value=0))
    plot_verified_by_label_percentage(df)

    log_scale = should_use_log_scale(df["register_days_ago"], feature_name="register_days_ago")
    apply_stat_test_and_plot(
        uf["register_days_ago"], ur["register_days_ago"],
        "Registration Time", "register_days_ago", df, log=log_scale,
        title="Box plots of user registration time"
    )
    apply_stat_test_and_plot(
        uf["status_count"], ur["status_count"],
        "Number of Tweets", "status_count", df, plot_type="log_rank",
        title="Log-log cumulative rank curves of the number of tweets"
    )
    apply_stat_test_and_plot(
        uf["favor_count"], ur["favor_count"],
        "Number of Likes", "favor_count", df, plot_type="log_rank",
        title="Log-log cumulative rank curves of the number of tweets liked"
    )
    log_scale = should_use_log_scale(df["follower_count"], feature_name = "follower_count")
    apply_stat_test_and_plot(
        uf["follower_count"], ur["follower_count"],
        "Number of Followers", "follower_count", df, log=log_scale,
        title="Box plots number of followers"
    )
    log_scale = should_use_log_scale(df["following_count"], feature_name = "following_count")
    apply_stat_test_and_plot(
        uf["following_count"], ur["following_count"],
        "Number of Followings", "following_count", df, log=log_scale,
        title="Box plots number of followings"
    )

    print("\nTFF Ratio (Followers / Following):")
    print(f"U(f): {uf['tff_ratio'].mean():.2f} | U(r): {ur['tff_ratio'].mean():.2f}")
    plot_tff_histogram(uf["tff_ratio"].dropna(), ur["tff_ratio"].dropna())



def analyze_tweet_metrics(df):
    uf = df[df["label"] == "fake"]
    ur = df[df["label"] == "real"]

    log_scale = should_use_log_scale(df["tweeting_range_days"], feature_name = "tweeting_range_days")
    apply_stat_test_and_plot(
        uf["tweeting_range_days"], ur["tweeting_range_days"],
        "Tweeting Time Span", "tweeting_range_days",df, log=log_scale,
        title="Box plots of the tweeting time range"
    )
    apply_stat_test_and_plot(
        uf["tweeting_range_days"], ur["tweeting_range_days"],
        "Tweeting Time Span", "tweeting_range_days",df, plot_type="log_rank",
        title="Log-log cumulative rank curves of the tweeting time range"
    )
    log_scale = should_use_log_scale(df["mean_replies"], feature_name = "mean_replies")
    apply_stat_test_and_plot(
        uf["mean_replies"], ur["mean_replies"],
        "Replies per Tweet", "mean_replies",df, log=log_scale,
        title="Box plots of the number of replies"
    )
    apply_stat_test_and_plot(
        uf["mean_replies"], ur["mean_replies"],
        "Replies per Tweet", "mean_replies",df, plot_type="log_rank",
        title="Log-log cumulative rank curves of the number of replies"
    )
    log_scale = should_use_log_scale(df["mean_retweets"], feature_name = "mean_retweets")
    apply_stat_test_and_plot(
        uf["mean_retweets"], ur["mean_retweets"],
        "Retweets per Tweet", "mean_retweets",df, log=log_scale,
        title="Box plots of the number of retweets"
    )
    apply_stat_test_and_plot(
        uf["mean_retweets"], ur["mean_retweets"],
        "Retweets per Tweet", "mean_retweets",df, plot_type="log_rank",
        title="Log-log cumulative rank curves of the number of retweets"
    )


if __name__ == "__main__":
    db_connection = DB_connection("GOSSIPCOP")
    valid_usernames = get_valid_usernames(db_connection)
    df_users = load_user_data(db_connection, valid_usernames)
    df_tweet_metrics = run_tweet_related_analysis(db_connection)
    db_connection.close()

    analyze_user_data(df_users)
    analyze_tweet_metrics(df_tweet_metrics)
