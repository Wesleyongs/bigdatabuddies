import tweepy
import os
import pytz
import re
#For Preprocessing
import re    # RegEx for removing non-letter characters

import nltk
from nltk.corpus import wordnet
from nltk.corpus import stopwords

import os
from datetime import datetime
import pymongo
import json

from nltk.sentiment.vader import SentimentIntensityAnalyzer


# Download NLTK data
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')
nltk.download('stopwords')
nltk.download('omw-1.4')
nltk.download('vader_lexicon')

# Lexicon setup
# New words and values
new_words = {
    'easy-to-use': 10,
    'productive': 5,
    'slow': -5,
    'frustrating': -10,
    'glitchy': -100,
}

# Instantiate the sentiment intensity analyzer with the existing lexicon
vader = SentimentIntensityAnalyzer()
# Update the lexicon
vader.lexicon.update(new_words)

# Set timezone to GMT+8
tz = pytz.timezone('Asia/Manila')

import html

# Import environment variables from .env file
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Helper functions
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
    lemmatizer = nltk.stem.WordNetLemmatizer()
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


username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
cluster_name = os.getenv("CLUSTER_NAME")
database_name = os.getenv("DATABASE_NAME")

# Create a class that inherits from the tweepy.StreamListener
class MyStream(tweepy.StreamingClient):
    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected. Streaming tweets...")

    def on_tweet(self, tweet):
        # Get the data
        text = re.sub('[^a-zA-Z]', ' ', tweet.text)
        
        # Clean the text
        clean_str = clean_original_text(text)

        # Get sentiment
        score = vader.polarity_scores(clean_str)

        score = {key: str(value) for key, value in score.items()}


        compound_score = score['compound']

        print(compound_score)
        try:
            # Connect to MongoDB
            client = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@{cluster_name}.igmsvhv.mongodb.net/{database_name}?retryWrites=true&w=majority")
            
            # Get the database
            db = client[database_name]
            
            # Get the collection
            collection = db["real-time"]

            # Get current minute of the day
            current_minute = datetime.now(tz).strftime('%Y-%m-%d %H:%M')

            # Check if current day exists in database
            current_day = datetime.now(tz).strftime('%Y-%m-%d')
            day_doc = collection.find_one({'_id': current_day})

            if not day_doc:
                # Create empty document for the day
                day_doc = {'_id': current_day, 'sentiment_data': {}}
                collection.insert_one(day_doc)

            # Update sentiment for current minute of the day
            sentiment_data = day_doc['sentiment_data']
            # sentiment_data[current_minute] = compound_score

            if current_minute not in sentiment_data:
                sentiment_data[current_minute] = {'compound': float(compound_score), 'count': 1, 'total': float(compound_score)}
            else:
                sentiment_data[current_minute]['count'] += 1
                sentiment_data[current_minute]['total'] += float(compound_score)
                sentiment_data[current_minute]['compound'] = sentiment_data[current_minute]['total'] / sentiment_data[current_minute]['count']


            result = collection.update_one({'_id': current_day}, {'$set': {'sentiment_data': sentiment_data}})
            print(result)


            # Close the connection
            client.close()
        except Exception as e:
            print(f"Error: {e}")
        return


stream = MyStream(bearer_token=os.getenv('BEARER_TOKEN'))
stream.add_rules(tweepy.StreamRule("chatgpt")) #adding the rules
stream.filter(expansions="author_id", user_fields="username") #runs the stream