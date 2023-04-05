import pytz
import snscrape.modules.twitter as sntwitter
import re
import datetime
import boto3
import io
import pandas as pd



# Folder path
folder_path = "read/"

# Creating list to append tweet data to
tweets_list = []

# search terms
search_terms = 'chatgpt'

# Count tracker
count = 0

# Set the time zone to UTC
timezone = pytz.timezone("UTC")

# Calculate the start and end times
now = datetime.datetime.now(timezone)
end_time = now.replace(microsecond=0, second=0, minute=0)
start_time = end_time - datetime.timedelta(days=1)

# Convert the times to ISO format
start_time_iso = start_time.isoformat()
end_time_iso = end_time.isoformat()

# Date
start_date = start_time_iso[:10]
end_date = end_time_iso[:10]

# Build the query
query = search_terms + " lang:en since:" + start_date + " until:" + end_date
# print(query)

for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
    # if count > 10:
    #     break
    #declare the attributes to be returned
    # count += 1
    # print("Tweets received:" + str(count))/
    tweets_list.append([tweet.date, tweet.id, tweet.content, tweet.user.username])

# Creating a dataframe from the tweets list above
tweets_df = pd.DataFrame(tweets_list, columns=['Datetime', 'Tweet Id', 'Text', 'Username'])

# Convert "Tweet Id" column to string format
tweets_df['Tweet Id'] = tweets_df['Tweet Id'].astype(str)

# Convert "Datetime" column to string format
tweets_df['Datetime'] = tweets_df['Datetime'].astype(str)

# Remove all non-letter characters using regex
tweets_df['Text'] = tweets_df['Text'].apply(lambda x: re.sub('[^a-zA-Z]', ' ', x))

tweets_df.to_csv(start_date + ".csv", encoding='utf-8', index=False)

# Convert dataframe to CSV string
csv_buffer = io.StringIO()
tweets_df.to_csv(csv_buffer, index=False)
csv_string = csv_buffer.getvalue().encode('utf-8')

# Set up S3 Client
s3 = boto3.client('s3')

# Bucket set up
bucket_name = "is459-twitter-preprocessed"

# Upload to S3
result = s3.put_object(Bucket=bucket_name, Key=folder_path + start_date + ".csv", Body=csv_string)
