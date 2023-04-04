#For Preprocessing
import re    # RegEx for removing non-letter characters
import sys
import pandas as pd
import boto3
import io
from io import StringIO
from datetime import datetime, timedelta

import nltk
from nltk.stem import WordNetLemmatizer
from nltk.stem import PorterStemmer
from nltk.corpus import wordnet
from nltk.corpus import stopwords
from nltk.parse.malt import MaltParser
from nltk.corpus import words
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')
nltk.download('stopwords')

import html
from unicodedata import normalize

AWS_ACCESS_KEY_ID='AKIARJIOGROB7MVFOYGX'
AWS_SECRET_ACCESS_KEY='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'
SRC_BUCKET_NAME = 'bigdatabuddies-glue-output'
DEST_BUCKET_NAME = 'project-analytics-output'

s3 = boto3.resource('s3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='us-east-1')

SRC_BUCKET = s3.Bucket(SRC_BUCKET_NAME)
DEST_BUCKET = s3.Bucket(DEST_BUCKET_NAME)

today = datetime.today()
yesterday = today - timedelta(days=1)

yesterday_str = yesterday.strftime('%d').lstrip('0')
current_month = yesterday.strftime('%b')

def preprocess_data():
    df = read_from_s3()
    df.dropna(inplace=True)
    
    print("Cleaning data now")
    df['clean_text'] = df['text'].apply(clean_original_text)
    df['clean_tokens'] = df['clean_text'].apply(nltk.word_tokenize)
    
    print("Completed cleaning")
    df.to_csv("{}_{}_preprocessed.csv".format(yesterday, current_month), index=False)
    upload_to_s3("{}_{}_preprocessed.csv".format(yesterday, current_month))
    print("- THE END -")

def decode_text(text):
    # remove non-ASCII characters in string
    decoded_text = text.encode('ascii', 'ignore').decode('utf-8')

    # decode HTML entities
    decoded_html = html.unescape(decoded_text)
    return ''.join([word for word in decoded_html if word.isprintable()])

def remove_mentions(text):
    return re.sub("@[A-Za-z0-9_]+","", text)

def remove_stopwords(words_list):
    stop_list = stopwords.words("english")
    stop_list.append("filler")
    return [word for word in words_list if word not in stop_list]

def pos_to_wordnet(nltk_tag):
    if nltk_tag.startswith('J'):
        return wordnet.ADJ
    elif nltk_tag.startswith('V'):
        return wordnet.VERB
    elif nltk_tag.startswith('N'):
        return wordnet.NOUN
    elif nltk_tag.startswith('R'):
        return wordnet.ADV
    else:
        return None

def lemmatize_words(word_list):
    lemmatizer = WordNetLemmatizer()
    # POS (part-of-speech) tagging
    # nltk_tagged -> a list of tuples (word, pos tag)
    nltk_tagged = nltk.pos_tag(word_list)

    # returns a list of tuples of words and their wordnet_tag (after conversion from NLTK tag)
    wordnet_tagged = list(map(lambda x: (x[0], pos_to_wordnet(x[1])), nltk_tagged))

    # lemmatizing
    lemmatized_words = []
    for word, tag in wordnet_tagged:
        if tag is not None:
            # need POS tag as 2nd argument as it helps lemmatize the words more accurately
            lemmatized_words.append(lemmatizer.lemmatize(word, tag))
        elif tag in [wordnet.NOUN]:
            lemmatized_words.append(lemmatizer.lemmatize(word))
    return lemmatized_words

def clean_original_text(text):
    text = str(text)
    text = text.lower()
    clean_list = []
    sentence_list = nltk.sent_tokenize(text)
    for sentence in sentence_list:
        decoded_sentence = decode_text(sentence)
        words_list = nltk.RegexpTokenizer(r'\w+').tokenize(decoded_sentence)
        lemmatized_words = lemmatize_words(words_list)
        useful_words = remove_stopwords(lemmatized_words)

        if len(useful_words) > 0:
            clean_list.extend(useful_words)
    clean_text = ' '.join(clean_list)

    return clean_text

def read_from_s3():
    df_list = []
    print("reading from S3")
    for obj in SRC_BUCKET.objects.all():
        file_key = obj.key
        s3_object = s3.Object(SRC_BUCKET_NAME, file_key)
        body = s3_object.get()['Body'].read().decode('utf-8')
        csv_df = pd.read_csv(StringIO(body))
        df_list.append(csv_df)
    concat_df = pd.concat(df_list, ignore_index=True)
    print("Complete reading from S3")
    return concat_df

def upload_to_s3(file_name):
    print("uploading to s3")
    with open(file_name, 'rb') as file:
        s3_path = '{}/{}'.format("preprocessed", file_name)
        DEST_BUCKET.put_object(Key=s3_path, Body=file)
        print(f'{file_name} has been uploaded to S3 as {s3_path}')

if __name__ == "__main__":
    preprocess_data()
    