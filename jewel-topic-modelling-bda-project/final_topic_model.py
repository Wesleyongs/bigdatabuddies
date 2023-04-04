from io import StringIO
import boto3
import collections
import json
import gensim
import pandas as pd
import time
from datetime import datetime, timedelta
from collections import defaultdict

NUM_TOPICS = 3
HTML_WORDS = ["html", "http", "https", "www", "gt", "amp", "com", "nbsp", "em", "en", "lt", "quot"]

AWS_ACCESS_KEY_ID='AKIARJIOGROB7MVFOYGX'
AWS_SECRET_ACCESS_KEY='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'
BUCKET_NAME = 'project-analytics-output'

s3 = boto3.resource('s3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name='us-east-1')

BUCKET = s3.Bucket(BUCKET_NAME)

today = datetime.today()
yesterday = today - timedelta(days=1)

yesterday_str = yesterday.strftime('%d').lstrip('0')
current_month = yesterday.strftime('%b')

input_folder = "preprocessed"
input_file_name = '{}_{}_preprocessed.csv'.format(yesterday_str, current_month)
output_folder = "topic"
output_file_name = '{}_{}_sentiment_topics.json'.format(yesterday_str, current_month)


def topic_model_analytics():
    start_time = time.time()
    df = read_from_s3()
    print("topic now")
    grouped_df = get_sentiment_df(df)
    sentiment_topic_terms = get_sentiment_topic_terms(grouped_df)
    print("topic end")

    store_sentiment_topic_terms(sentiment_topic_terms)
    # upload_to_s3()

    end_time = time.time()
    elapsed = end_time - start_time
    print(f'Execution time: {time.strftime("%Hh%Mm%Ss", time.gmtime(elapsed))}')
  
def get_sentiment_df(df):
    df.dropna(inplace=True)
    df["sentiment"] = df.apply(label_sentiment, axis=1)
    return df.groupby("sentiment")

def label_sentiment(row):
    if row["compound"] < 0:
        return "negative"
    if row["compound"] == 0:
        return "neutral"
    else:
        return "positive"

def get_sentiment_topic_terms(grouped_df):
    sentiment_topic_terms = {}

    for sentiment, sentiment_df in grouped_df:
        print(sentiment)
        topic_terms = generate_sentiment_topic_model(sentiment_df)
        sentiment_topic_terms[sentiment] = topic_terms 
        
    return sentiment_topic_terms
    
def generate_sentiment_topic_model(df):
    chatgptreddit_vecs, chatgptreddit_dict = create_lda_vecs(df)
    topic_terms_dict = get_topic_terms(chatgptreddit_vecs, chatgptreddit_dict)
    
    return topic_terms_dict

""" Functions for LDA topic modelling """
def docs2vecs(docs, dictionary):
    # docs is a list of documents returned by corpus2docs.
    # dictionary is a gensim.corpora.Dictionary object.
    vecs = [dictionary.doc2bow(doc) for doc in docs]
    return vecs

def create_lda_vecs(df):
    
    chatgptreddit_docs = []
    for doc in df["clean_text"]:
        tokens = doc.split()  # split the sentence into a list of tokens
        tokens = [token for token in tokens if token not in HTML_WORDS]
        chatgptreddit_docs.append(tokens)
    
    chatgptreddit_dict = gensim.corpora.Dictionary(chatgptreddit_docs)
    chatgptreddit_vecs = docs2vecs(chatgptreddit_docs, chatgptreddit_dict)
    return chatgptreddit_vecs, chatgptreddit_dict

def get_topic_terms(chatgptreddit_vecs, chatgptreddit_dict):
    chatgptreddit_lda = gensim.models.ldamodel.LdaModel(corpus=chatgptreddit_vecs, id2word=chatgptreddit_dict, num_topics=NUM_TOPICS)
    topic_terms_dict = defaultdict(list)
    for topic_id, terms_list in chatgptreddit_lda.show_topics(num_topics=NUM_TOPICS, num_words=10, formatted=False):
        terms = []
        for term in terms_list:
            # if no variations of the word are found
            if len(term[0]) > 1:
                terms.append(term[0].lower())
        topic_terms_dict[str(topic_id)] = terms
    topic_terms_dict = dict(topic_terms_dict)
    return topic_terms_dict

def store_sentiment_topic_terms(sentiment_topic_terms):
    with open(output_file_name, "w") as new_file:
        json.dump(sentiment_topic_terms, new_file, indent=4)

def read_from_s3():
    s3_object = s3.Object(BUCKET_NAME, '{}/{}'.format(input_folder, input_file_name))
    body = s3_object.get()['Body'].read().decode('utf-8')
    csv_df = pd.read_csv(StringIO(body))
    print("Complete - Read CSV from S3")
    return csv_df

def upload_to_s3():
    with open(output_file_name, 'rb') as file:
        s3_path = '{}/{}'.format(output_folder, output_file_name)
        BUCKET.put_object(Key=s3_path, Body=file)
        print(f'{output_file_name} has been uploaded to S3 as {s3_path}')
    print("Complete - Upload topics json to S3")


if __name__ == "__main__":
    topic_model_analytics()