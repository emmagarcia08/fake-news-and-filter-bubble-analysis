if __name__ == "__main__":
    from dotenv import load_dotenv
    from gensim.corpora import Dictionary
    from gensim.models.coherencemodel import CoherenceModel
    import matplotlib.pyplot as plt
    import os
    import subprocess
    from tqdm import tqdm

    # Load variables .env
    load_dotenv()
    MALLET_PATH = os.getenv("MALLET_PATH")

    # Parameters
    corpus_txt = "corpus.txt"
    output_dir = "mallet_runs"
    topic_range = list(range(10, 301, 10))

    # Step 1: load the pre-tokenized corpus
    texts = []
    with open(corpus_txt, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 3:
                continue
            tokens = parts[2].split()
            texts.append(tokens)

    # Step 2: prepare dictionary and corpus
    dictionary = Dictionary(texts)
    corpus_bow = [dictionary.doc2bow(text) for text in texts]

    # Step 3: loop over different numbers of topics
    coherence_scores = []
    os.makedirs(output_dir, exist_ok=True)

    for num_topics in tqdm(topic_range, desc="Calcul cohérence"):
        prefix = os.path.join(output_dir, f"k{num_topics}")
        mallet_file = "corpus.mallet"
        state_file = prefix + "_state.gz"
        keys_file = prefix + "_keys.txt"

        # LDA training
        subprocess.run([
            MALLET_PATH, "train-topics",
            "--input", mallet_file,
            "--num-topics", str(num_topics),
            "--output-state", state_file,
            "--output-topic-keys", keys_file,
            "--num-iterations", "5000",
            "--optimize-interval", "10",
            "--beta", "0.01"
        ], check=True)

        # Load topics from keys_file
        topics = []
        with open(keys_file, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 3:
                    topic_words = parts[2].split()
                    topic_words = [w for w in topic_words if w in dictionary.token2id]
                    topics.append(topic_words)

        # Coherence calculation
        cm = CoherenceModel(topics=topics, texts=texts, dictionary=dictionary, coherence='c_v')
        coherence = cm.get_coherence()
        coherence_scores.append(coherence)

    # Step 4: build the graph
    plt.figure(figsize=(10, 6))
    plt.plot(topic_range, coherence_scores, marker='o')
    plt.xlabel("Number of topics (K)")
    plt.ylabel("Coherence C_v")
    plt.title("C_v coherence scores vs numbers of topics")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("coherence_plot.png")
    plt.show()
