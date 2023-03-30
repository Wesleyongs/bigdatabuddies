import tweepy
import os
import json
import re
import requests

# Import environment variables from .env file
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file


# Create a class that inherits from the tweepy.StreamListener
class MyStream(tweepy.StreamingClient):
    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected. Streaming tweets...")

    def on_tweet(self, tweet):
       
       # Get the data
       id = str(tweet.id)
       username = str(tweet.author_id)
       text = re.sub('[^a-zA-Z]', ' ', tweet.text)


       # Pre processed text
       pre_processed_tweet = {
           'id': id, 
           'text': text,
           'username': username,
       }

       # TODO: Call lambda function to process tweet
       headers = {
           'Content-Type': 'application/json'
       }
       payload = json.dumps(pre_processed_tweet)

       r = requests.post(url=os.getenv('LAMBDA_URL'), headers=headers, data=payload)
       print(r)
       return


stream = MyStream(bearer_token=os.getenv('BEARER_TOKEN'))
stream.add_rules(tweepy.StreamRule("chatgpt")) #adding the rules
stream.filter(expansions="author_id", user_fields="username") #runs the stream