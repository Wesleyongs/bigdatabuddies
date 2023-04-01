
import os
from datetime import datetime
import pymongo
import json

from nltk.sentiment.vader import SentimentIntensityAnalyzer

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


username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
cluster_name = os.getenv("CLUSTER_NAME")
database_name = os.getenv("DATABASE_NAME")

def lambda_handler(event, context):

    # Get tweet text
    data = json.loads(event['body'])

    # Clean text
    clean_str = data['text']

    # Get sentiment
    score = vader.polarity_scores(clean_str)

    score = {key: str(value) for key, value in score.items()}


    compound_score = score['compound']
    try:
        print(username)
        print(password)
        print(cluster_name)
        print(database_name)
        # Connect to MongoDB
        client = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@{cluster_name}.igmsvhv.mongodb.net/{database_name}?retryWrites=true&w=majority")
        
        # Get the database
        db = client[database_name]
        
        # Get the collection
        collection = db["real-time"]

        # Get current minute of the day
        current_minute = datetime.now().strftime('%Y-%m-%d %H:%M')

        # Check if current day exists in database
        current_day = datetime.now().strftime('%Y-%m-%d')
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