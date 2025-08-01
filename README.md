# Fake news and filter bubble analysis
This project aims to study the link between the sharing of fake news and the filter bubble phenomenon on Twitter/X through various analyses. 
These python scripts were written and used in the context of our master's thesis.

## Content of this repository

This repository contains five folders:
- data_collection: Scripts used to collect the Twitter/X data used to perform our analyses.
- thematic_diversity_analysis: Scripts used to compute thematic diversity scores and analyse their distribution.
- publication_behaviour_analysis: Script used to analyse user publication behaviour.
- sentiment_analysis: Scripts used to compute sentiment scores and analyse their distribution.
- user_features_analysis: Script used to analyse user explicit features.

It also contains the "DB_connection.py" script, which makes it easier to manage database-related commands in other scripts.

## Requirements
- Python 3.9 or higher
- Dependencies listed in the "requirements.txt" file.
- To run the data collection scripts, you need to create an Apify account (https://console.apify.com/sign-up).

## Installation

### Clone git repo
```bash
git clone https://github.com/emmagarcia08/fake-news-and-filter-bubble-analysis.git
```
Navigate to the fake-news-and-filter-bubble-analysis folder inside your terminal.
```bash
cd fake-news-and-filter-bubble-analysis
```
Install dependencies based on the “requirements.txt” file provided in this repository.

### Add necessary files/folders
Please add a data folder containing the GossipCop data on which our data collection is based. You can download the two datasets at the following link: https://github.com/KaiDMML/FakeNewsNet/tree/master/dataset
```bash
fake-news-and-filter-bubble-analysis
    |__data
        |___ GossipCop
                |___ gossipcop_fake.csv
                |___ gossipcop_real.csv

```
To perform the analysis of thematic diversity, you also need to set up the Java-based package MALLET. You can download the latest version of MALLET (2.0.8) via this link: https://mallet.cs.umass.edu/download.php

Please also add a .env file at the root of this repository containing : 
```bash
DATA_PATH = "YOUR PATH TO THE DATA FILE"
MALLET_PATH = "YOUR PATH TO THE MALLET FILE"
APIFY_TOKEN = YOUR APIFY TOKEN TO ACCESS THE APIFY API
``` 

## How to run these scripts?

### data_collection
To run some of the scripts for this phase of the project, you need access to the Apify scraping platform.

To replicate the database used for the various analyses, you can run the scripts in the following order:
- build_database.py
- select_initial_tweet_ids.py
- initial_tweets_scraper.py
- user_data_scraper.py
- translation_bio.py
- user_tweets_scraper.py
- translation_tweets.py
- remove_url.py
- filter_user_timelines.py

### thematic_diversity_analysis
To perform the analysis of thematic diversity scores, you need to carry out these various steps and run the scripts in the following order:
- preprocess.py
- corpus_txt_user.py
- Run the following command in your terminal to convert the "corpus.txt" file to the .mallet format:
```bash
mallet import-file --input corpus.txt --output corpus.mallet --keep-sequence TRUE --remove-stopwords FALSE --line-regex '^(\S+)\t(\w+)\t(.*)$' --label 2 --name 1 --data 3
```
- comparison_param_mallet.py
- Run the following command in your terminal to train the LDA model:
```bash
mallet train-topics \
  --input corpus.mallet \
  --num-topics 30 \
  --num-iterations 5000 \
  --optimize-interval 10 \
  --alpha 0.02 \
  --beta 0.01 \
  --output-state state.mallet.gz \
  --output-topic-keys topic_keys.txt \
  --output-doc-topics doc_topics.txt
```
- doc_topics_count_user.py
- compute_diversity_scores.py
- diversity_score_distributions.py

### publication_behaviour_analysis
To perform the analysis of user publication behaviour, you need to run the "tweet_types_analysis.py" script.

### sentiment_analysis
The sentiment analysis uses the SenticNet lexicon. This lexicon is contained in the "senticnet.py" file, which we downloaded from the following link: https://sentic.net/downloads/.
To use this lexicon in our code, it is necessary to run the "parse_senticnet.py" script to obtain the lexicon in .json format.

Then you need to run the other scripts in the following order to perform the analysis of sentiment scores:
- preprocess_tsa.py
- compute_sentiment_scores.py
- aggregate_sentiment_scores_by_user.py
- sentiment_score_distributions.py

### user_features_analysis
To perform the analysis of user explicit features, you need to run the "user_features_analysis.py" script.

## Additional information
if you would like more information on the data collection phase, the various analyses, the results obtained or the project as a whole, you can consult our master's thesis. It is available on this platform: https://thesis.dial.uclouvain.be/home

Authors: Emma Garcia and Margaux Labar
